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

package triestate

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// Trie is an Ethereum state trie, can be implemented by Ethereum Merkle Patricia
// tree or Verkle tree.
type Trie interface {
	// Get returns the value for key stored in the trie.
	Get(key []byte) ([]byte, error)

	// Update associates key with value in the trie.
	Update(key, value []byte) error

	// Delete removes any existing value for key from the trie.
	Delete(key []byte) error

	// Commit the trie and returns a set of dirty nodes generated along with
	// the new root hash.
	Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet)

	GetNode(path []byte) ([]byte, int, error)
}

// TrieLoader wraps functions to load tries.
type TrieLoader interface {
	// OpenTrie opens the main account trie.
	OpenTrie(root common.Hash) (Trie, error)

	// OpenStorageTrie opens the storage trie of an account.
	OpenStorageTrie(stateRoot common.Hash, addrHash, root common.Hash) (Trie, error)
}

func decodeNibbles(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+2 {
		bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
	}
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

func hexToCompact(hex []byte) []byte {
	terminator := byte(0)
	if hasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[0] = terminator << 5 // the flag byte
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // odd flag
		buf[0] |= hex[0] // first nibble is contained in the first byte
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

// Set represents a collection of mutated states during a state transition.
// The value refers to the original content of state before the transition
// is made. Nil means that the state was not present previously.
type Set struct {
	Accounts   map[common.Hash][]byte                 // Mutated account set, nil means the account was not present
	Storages   map[common.Hash]map[common.Hash][]byte // Mutated storage set, nil means the slot was not present
	Incomplete map[common.Hash]struct{}               // Indicator whether the storage is incomplete due to large deletion
	size       common.StorageSize                     // Approximate size of set
}

// New constructs the state set with provided data.
func New(accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, incomplete map[common.Hash]struct{}) *Set {
	return &Set{
		Accounts:   accounts,
		Storages:   storages,
		Incomplete: incomplete,
	}
}

// Size returns the approximate memory size occupied by the set.
func (s *Set) Size() common.StorageSize {
	if s.size != 0 {
		return s.size
	}
	for _, account := range s.Accounts {
		s.size += common.StorageSize(common.HashLength + len(account))
	}
	for _, slots := range s.Storages {
		for _, val := range slots {
			s.size += common.StorageSize(common.HashLength + len(val))
		}
		s.size += common.StorageSize(common.HashLength)
	}
	s.size += common.StorageSize(common.HashLength * len(s.Incomplete))
	return s.size
}

// context wraps all fields for executing state diffs.
type context struct {
	prevRoot    common.Hash
	postRoot    common.Hash
	accounts    map[common.Hash][]byte
	storages    map[common.Hash]map[common.Hash][]byte
	accountTrie Trie
	nodes       *trienode.MergedNodeSet
}

// Apply traverses the provided state diffs, apply them in the associated
// post-state and return the generated dirty trie nodes. The state can be
// loaded via the provided trie loader.
func Apply(prevRoot common.Hash, postRoot common.Hash, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, loader TrieLoader) (map[common.Hash]string, map[common.Hash]map[string]*trienode.Node, error) {
	tr, err := loader.OpenTrie(postRoot)
	if err != nil {
		return nil, nil, err
	}
	ctx := &context{
		prevRoot:    prevRoot,
		postRoot:    postRoot,
		accounts:    accounts,
		storages:    storages,
		accountTrie: tr,
		nodes:       trienode.NewMergedNodeSet(),
	}
	info := make(map[common.Hash]string)
	for addrHash, account := range accounts {
		var err error
		if len(account) == 0 {
			err = deleteAccount(ctx, loader, addrHash, info)
		} else {
			err = updateAccount(ctx, loader, addrHash, info)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to revert state, err: %w", err)
		}
	}
	root, result := tr.Commit(false)
	if root != prevRoot {
		return nil, nil, fmt.Errorf("failed to revert state, want %#x, got %#x", prevRoot, root)
	}
	if err := ctx.nodes.Merge(result); err != nil {
		return nil, nil, err
	}
	return info, ctx.nodes.Flatten(), nil
}

// updateAccount the account was present in prev-state, and may or may not
// existent in post-state. Apply the reverse diff and verify if the storage
// root matches the one in prev-state account.
func updateAccount(ctx *context, loader TrieLoader, addrHash common.Hash, infos map[common.Hash]string) error {
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
	st, err := loader.OpenStorageTrie(ctx.postRoot, addrHash, post.Root)
	if err != nil {
		return err
	}
	var info string
	info += fmt.Sprintf(">>>>>> Process storage %s\n", addrHash.Hex())
	for key, val := range ctx.storages[addrHash] {
		postSlot, e := st.Get(key.Bytes())
		if e != nil {
			return e
		}
		if bytes.Equal(postSlot, val) {
			return fmt.Errorf("same slot value detected, v: %v, k: %v", postSlot, key.Bytes())
		}
		var err error
		if len(val) == 0 {
			err = st.Delete(key.Bytes())
			info += fmt.Sprintf("[BEFORE-DEL] %x - %x\n", key.Bytes(), postSlot)
			info += fmt.Sprintf("[DEL] %x\n", key.Bytes())
		} else {
			err = st.Update(key.Bytes(), val)
			info += fmt.Sprintf("[BEFORE-Update] %x - %x\n", key.Bytes(), postSlot)
			info += fmt.Sprintf("[Update] %x - %x\n", key.Bytes(), val)
		}
		if err != nil {
			return err
		}
	}
	root, result := st.Commit(false)
	if root != prev.Root {
		return errors.New("failed to reset storage trie")
	}
	// The returned set can be nil if storage trie is not changed
	// at all.
	if result != nil {
		if err := ctx.nodes.Merge(result); err != nil {
			return err
		}
		st2, err := loader.OpenStorageTrie(ctx.postRoot, addrHash, post.Root)
		if err != nil {
			return err
		}
		for path, n := range result.Nodes {
			blob, _, err := st2.GetNode(hexToCompact([]byte(path)))
			if err != nil {
				info += fmt.Sprintf("Failed to get node %v, err %v, Prev-state %v\n", []byte(path), err, n.Blob)
			} else {
				equal := bytes.Equal(blob, n.Blob)
				info += fmt.Sprintf("Post-State node %v, equal %t, blob %v, Prev-state %v\n", []byte(path), equal, blob, n.Blob)
			}
		}
	}
	info += fmt.Sprintf("<<<<<<< Processed storage %s\n", addrHash.Hex())
	infos[addrHash] = info

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
func deleteAccount(ctx *context, loader TrieLoader, addrHash common.Hash, infos map[common.Hash]string) error {
	// The account must be existent in post-state, load the account.
	blob, err := ctx.accountTrie.Get(addrHash.Bytes())
	if err != nil {
		return err
	}
	if len(blob) == 0 {
		return fmt.Errorf("account is not existent %#x", addrHash)
	}
	var post types.StateAccount
	if err := rlp.DecodeBytes(blob, &post); err != nil {
		return err
	}
	st, err := loader.OpenStorageTrie(ctx.postRoot, addrHash, post.Root)
	if err != nil {
		return err
	}
	var info string
	info += fmt.Sprintf(">>>>>> Process storage %s\n", addrHash.Hex())
	for key, val := range ctx.storages[addrHash] {
		if len(val) != 0 {
			return errors.New("expect storage deletion")
		}
		postSlot, err := st.Get(key.Bytes())
		if err != nil {
			return err
		}
		if len(postSlot) == 0 {
			return errors.New("storage is missing")
		}
		info += fmt.Sprintf("[BEFORE-DEL] %x - %x\n", key.Bytes(), postSlot)
		info += fmt.Sprintf("[DEL] %x\n", key.Bytes())
		if err := st.Delete(key.Bytes()); err != nil {
			return err
		}
	}
	root, result := st.Commit(false)
	if root != types.EmptyRootHash {
		return errors.New("failed to clear storage trie")
	}

	// The returned set can be nil if storage trie is not changed
	// at all.
	if result != nil {
		if err := ctx.nodes.Merge(result); err != nil {
			return err
		}
		st2, err := loader.OpenStorageTrie(ctx.postRoot, addrHash, post.Root)
		if err != nil {
			return err
		}
		for path, n := range result.Nodes {
			blob, _, err := st2.GetNode(hexToCompact([]byte(path)))
			if err != nil {
				info += fmt.Sprintf("Failed to get node %v, err %v, Prev-state %v\n", path, err, n.Blob)
			} else {
				equal := bytes.Equal(blob, n.Blob)
				info += fmt.Sprintf("Post-State node %v, equal %t, blob %v, Prev-state %v\n", path, equal, blob, n.Blob)
			}
		}
	}
	info += fmt.Sprintf("<<<<<<< Processed storage %s\n", addrHash.Hex())
	infos[addrHash] = info

	// Delete the post-state account from the main trie.
	return ctx.accountTrie.Delete(addrHash.Bytes())
}
