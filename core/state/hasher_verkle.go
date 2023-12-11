// Copyright 2024 The go-ethereum Authors
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

package state

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

// verkleHasher implements Hasher interface, providing state hashing methods in
// the manner of Verkle-Tree.
type verkleHasher struct {
	root common.Hash
	trie *trie.VerkleTrie
}

// newVerkleHasher constructs the verkle hasher or returns an error if the
// associated verkle tree is not existent.
func newVerkleHasher(root common.Hash, db *triedb.Database, prefetcher *triePrefetcher) (*verkleHasher, error) {
	var (
		tr  Trie
		err error
	)
	if prefetcher != nil {
		// Bail out if the prefetcher is not matched with the associated state, or
		// it's a non-verkle trie prefetcher. Although we can silently ignore the
		// error and still fallback to load trie from database, but the mismatch
		// shouldn't happen in the first place, must be some programming errors.
		if prefetcher.root != root {
			return nil, errors.New("prefetcher is not matched with state")
		}
		if !prefetcher.verkle {
			return nil, errors.New("prefetcher trie type is not matched")
		}
		tr = prefetcher.trie(common.Hash{}, root)
	}
	if tr == nil {
		tr, err = trie.NewVerkleTrie(root, db, utils.NewPointCache(commitmentCacheItems))
		if err != nil {
			return nil, err
		}
	}
	return &verkleHasher{
		trie: tr.(*trie.VerkleTrie),
		root: root,
	}, nil
}

// UpdateAccount implements the Hasher interface, performing an account write to
// the verkle trie. It encodes the provided account object in the Verkle manner
// and then updates it in the verkle tree with provided address.
func (h *verkleHasher) UpdateAccount(address common.Address, account *types.StateAccount) error {
	return h.trie.UpdateAccount(address, account)
}

// DeleteAccount implements the Hasher interface, performing an account deletion
// from the verkle trie.
func (h *verkleHasher) DeleteAccount(address common.Address) error {
	return h.trie.DeleteAccount(address)
}

// UpdateContractCode implements the Hasher interface, performing a code write
// to the trie.
func (h *verkleHasher) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return h.trie.UpdateContractCode(address, codeHash, code)
}

// Hash implements the Hasher interface, returning the root hash of the verkle
// trie. It does not write to the database and can be used even if the verkle
// trie has no database backend.
func (h *verkleHasher) Hash() common.Hash {
	return h.trie.Hash()
}

// Commit implements the Hasher interface, collecting and returning all dirty
// nodes in the verkle trie.
func (h *verkleHasher) Commit() (common.Hash, *trienode.NodeSet, error) {
	return h.trie.Commit(false)
}

// Copy implements the Hasher interface, returning a deep-copied verkle hasher.
func (h *verkleHasher) Copy() Hasher {
	return &verkleHasher{
		root: h.root,
		trie: h.trie.Copy(),
	}
}

// StorageHasher implements the Hasher interface, constructing a storage hasher
// for the contract specified by address and storage root. Instead of creating
// another trie for hashing storage separately, verkle tree uses the global one
// for storage hashing as well.
func (h *verkleHasher) StorageHasher(address common.Address, root common.Hash) (StorageHasher, error) {
	return newVerkleStorageHasher(h.trie, address, root), nil
}

// verkleStorageHasher implements StorageHasher interface, providing storage
// hashing methods in the manner of Verkle-Tree.
type verkleStorageHasher struct {
	trie    *trie.VerkleTrie
	address common.Address
	root    common.Hash
}

// newVerkleStorageHasher constructs the storage merkle hasher with the given
// verkle tree.
func newVerkleStorageHasher(trie *trie.VerkleTrie, address common.Address, root common.Hash) *verkleStorageHasher {
	return &verkleStorageHasher{
		trie:    trie,
		address: address,
		root:    root,
	}
}

// UpdateStorage implements the StorageHasher interface, performing the storage
// write to the verkle trie.
func (h *verkleStorageHasher) UpdateStorage(address common.Address, key, value []byte) error {
	return h.trie.UpdateStorage(address, key, value)
}

// DeleteStorage implements the StorageHasher interface, performing the storage
// deletion from the verkle trie.
func (h *verkleStorageHasher) DeleteStorage(address common.Address, key []byte) error {
	return h.trie.DeleteStorage(address, key)
}

// Hash implements the StorageHasher interface, while it's a no-op function. As in
// verkle, all the states are hashed by a single trie. The hashing operation is
// skipped for contract storage and the original root is returned for backward-compatibility.
func (h *verkleStorageHasher) Hash() common.Hash {
	return h.root
}

// Commit implements the StorageHasher interface, while it's a no-op function. As in
// verkle, all the states are hashed by a single trie. The hashing operation is
// skipped for contract storage and the original root is returned for backward-compatibility.
func (h *verkleStorageHasher) Commit() (common.Hash, *trienode.NodeSet, error) {
	return h.root, nil, nil
}

// Copy implements the StorageHasher interface, returns a deep-copied storage hasher.
func (h *verkleStorageHasher) Copy(hasher Hasher) StorageHasher {
	vh, ok := hasher.(*verkleHasher)
	if !ok {
		panic(fmt.Errorf("unknown hasher copy verkle hasher, %T", hasher))
	}
	return newVerkleStorageHasher(vh.trie, h.address, h.root)
}
