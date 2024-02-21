// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// context wraps all fields for executing state diffs.
type context struct {
	prevRoot    common.Hash
	postRoot    common.Hash
	accounts    map[common.Hash][]byte
	storages    map[common.Hash]map[common.Hash][]byte
	accountTrie trie.Trie
	nodes       *trienode.MergedNodeSet
}

// apply traverses the provided state diffs, applying them in the associated
// post-state and return the produced trie changes.
func apply(prevRoot common.Hash, postRoot common.Hash, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, opener trie.Opener) (map[common.Hash]map[string]*trienode.Node, error) {
	tr, err := opener.Open(trie.StateTrieID(postRoot))
	if err != nil {
		return nil, err
	}
	ctx := &context{
		prevRoot:    prevRoot,
		postRoot:    postRoot,
		accounts:    accounts,
		storages:    storages,
		accountTrie: tr,
		nodes:       trienode.NewMergedNodeSet(),
	}
	for addrHash, account := range accounts {
		var err error
		if len(account) == 0 {
			err = deleteAccount(ctx, opener, addrHash)
		} else {
			err = updateAccount(ctx, opener, addrHash)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to revert state, err: %w", err)
		}
	}
	root, result, err := tr.Commit(false)
	if err != nil {
		return nil, err
	}
	if root != prevRoot {
		return nil, fmt.Errorf("failed to revert state, want %#x, got %#x", prevRoot, root)
	}
	if err := ctx.nodes.Merge(result); err != nil {
		return nil, err
	}
	return ctx.nodes.Flatten(), nil
}

// updateAccount the account was present in prev-state, and may or may not
// existent in post-state. Apply the reverse diff and verify if the storage
// root matches the one in prev-state account.
func updateAccount(ctx *context, opener trie.Opener, addrHash common.Hash) error {
	// The account was present in prev-state, decode it from the
	// 'slim-rlp' format bytes.
	prev, err := types.FullAccount(ctx.accounts[addrHash])
	if err != nil {
		return err
	}
	// The account may or may not existent in post-state, try to
	// load it and decode if it's found.
	blob, err := ctx.accountTrie.Get(addrHash.Bytes())
	if err != nil {
		return err
	}
	post := types.NewEmptyStateAccount()
	if len(blob) != 0 {
		if err := rlp.DecodeBytes(blob, &post); err != nil {
			return err
		}
	}
	// Apply all storage changes into the post-state storage trie.
	st, err := opener.Open(trie.StorageTrieID(ctx.postRoot, addrHash, post.Root))
	if err != nil {
		return err
	}
	for key, val := range ctx.storages[addrHash] {
		var err error
		if len(val) == 0 {
			err = st.Delete(key.Bytes())
		} else {
			err = st.Update(key.Bytes(), val)
		}
		if err != nil {
			return err
		}
	}
	root, result, err := st.Commit(false)
	if err != nil {
		return err
	}
	if root != prev.Root {
		return errors.New("failed to reset storage trie")
	}
	// The returned set can be nil if storage trie is not changed
	// at all.
	if result != nil {
		if err := ctx.nodes.Merge(result); err != nil {
			return err
		}
	}
	// Write the prev-state account into the main trie
	full, err := rlp.EncodeToBytes(prev)
	if err != nil {
		return err
	}
	return ctx.accountTrie.Update(addrHash.Bytes(), full)
}

// deleteAccount the account was not present in prev-state, and is expected
// to be existent in post-state. Apply the reverse diff and verify if the
// account and storage is wiped out correctly.
func deleteAccount(ctx *context, opener trie.Opener, addrHash common.Hash) error {
	// The account must be existent in post-state, load the account.
	blob, err := ctx.accountTrie.Get(addrHash.Bytes())
	if err != nil {
		return err
	}
	if len(blob) == 0 {
		return fmt.Errorf("account is non-existent %#x", addrHash)
	}
	var post types.StateAccount
	if err := rlp.DecodeBytes(blob, &post); err != nil {
		return err
	}
	tr, err := opener.Open(trie.StorageTrieID(ctx.postRoot, addrHash, post.Root))
	if err != nil {
		return err
	}
	for key, val := range ctx.storages[addrHash] {
		if len(val) != 0 {
			return errors.New("expect storage deletion")
		}
		if err := tr.Delete(key.Bytes()); err != nil {
			return err
		}
	}
	root, result, err := tr.Commit(false)
	if err != nil {
		return err
	}
	if root != types.EmptyRootHash {
		return errors.New("failed to clear storage trie")
	}
	// The returned set can be nil if storage trie is not changed
	// at all.
	if result != nil {
		if err := ctx.nodes.Merge(result); err != nil {
			return err
		}
	}
	// Delete the post-state account from the main trie.
	return ctx.accountTrie.Delete(addrHash.Bytes())
}
