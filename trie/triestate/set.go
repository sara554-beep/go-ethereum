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
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// Set represents a collection of all modified states during a state transition.
// The state value refers to the original content of the state before the state
// transition is made. If it's nil, it means that the state was not present
// previously.
type Set struct {
	Size     int                                    // Approximate memory size of set
	Parent   common.Hash                            // State root before the transition was made
	Root     common.Hash                            // State root after the transition is applied
	Accounts map[common.Hash][]byte                 // Mutated account set, nil means the account was not present
	Storages map[common.Hash]map[common.Hash][]byte // Mutated storage set, nil means the slot was not present
}

// New constructs the state set with provided data.
func New(parent common.Hash, root common.Hash, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) *Set {
	var size int
	for _, data := range accounts {
		size += common.HashLength + len(data)
	}
	for _, slots := range storages {
		for _, slot := range slots {
			size += common.HashLength + len(slot)
		}
		size += common.HashLength
	}
	size += 2 * common.HashLength
	return &Set{
		Size:     size,
		Parent:   parent,
		Root:     root,
		Accounts: accounts,
		Storages: storages,
	}
}

func (set *Set) deleteAccount(loader TrieLoader, tr Trie, nodes *trienode.MergedNodeSet, addrHash common.Hash) error {
	blob, err := tr.Get(addrHash.Bytes())
	if err != nil {
		return err
	}
	if len(blob) == 0 {
		return fmt.Errorf("account is not existent %x", addrHash)
	}
	var account types.StateAccount
	if err := rlp.DecodeBytes(blob, &account); err != nil {
		return err
	}
	st, err := loader.OpenStorageTrie(set.Root, addrHash, account.Root)
	if err != nil {
		return err
	}
	for key, val := range set.Storages[addrHash] {
		if len(val) != 0 {
			return errors.New("expect storage deletion")
		}
		if err := st.Delete(key.Bytes()); err != nil {
			return err
		}
	}
	root, result := st.Commit(false)
	if root != types.EmptyRootHash {
		return errors.New("failed to clear storage trie")
	}
	if err := nodes.Merge(result); err != nil {
		return err
	}
	return tr.Delete(addrHash.Bytes())
}

func (set *Set) updateAccount(loader TrieLoader, tr Trie, nodes *trienode.MergedNodeSet, addrHash common.Hash) error {
	blob, err := tr.Get(addrHash.Bytes())
	if err != nil {
		return err
	}
	account := types.NewEmptyStateAccount()
	if len(blob) != 0 {
		if err := rlp.DecodeBytes(blob, &account); err != nil {
			return err
		}
	}
	st, err := loader.OpenStorageTrie(set.Root, addrHash, account.Root)
	if err != nil {
		return err
	}
	for key, val := range set.Storages[addrHash] {
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
	root, result := st.Commit(false)

	origin, err := types.FullAccount(set.Accounts[addrHash])
	if err != nil {
		return err
	}
	if root != origin.Root {
		return errors.New("failed to reset storage trie")
	}
	nodes.Merge(result)

	full, err := rlp.EncodeToBytes(origin)
	if err != nil {
		return err
	}
	return tr.Update(addrHash.Bytes(), full)
}

// Apply puts the state change to the state specified by the root to achieve
// the purpose of reverting all state changes. The trieLoader needs to provide
// the function of loading the specified trie, and after the state change is
// applied, the trie needs to return all the modified trie nodes.
func (set *Set) Apply(loader TrieLoader) (map[common.Hash]map[string]*trienode.Node, error) {
	tr, err := loader.OpenTrie(set.Root)
	if err != nil {
		return nil, err
	}
	nodes := trienode.NewMergedNodeSet()

	for addrHash, account := range set.Accounts {
		var err error
		if len(account) == 0 {
			err = set.deleteAccount(loader, tr, nodes, addrHash)
		} else {
			err = set.updateAccount(loader, tr, nodes, addrHash)
		}
		if err != nil {
			return nil, err
		}
	}
	root, result := tr.Commit(false)
	if root != set.Parent {
		return nil, errors.New("failed to reset state")
	}
	if err := nodes.Merge(result); err != nil {
		return nil, err
	}
	return nodes.Nodes(), nil
}
