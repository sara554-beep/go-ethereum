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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/merkle"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// merkleHasher implements Hasher interface, providing state hashing methods in
// the manner of Merkle-Patricia-Tree.
type merkleHasher struct {
	trie       *merkle.StateTrie
	root       common.Hash
	db         *triedb.Database
	prefetcher *triePrefetcher
}

// newMerkleHasher constructs the merkle hasher or returns an error if the
// associated merkle tree is not existent.
func newMerkleHasher(root common.Hash, db *triedb.Database, prefetcher *triePrefetcher) (*merkleHasher, error) {
	var (
		tr  Trie
		err error
	)
	if prefetcher != nil {
		// Bail out if the prefetcher is not matched with the associated state,
		// or it's a verkle trie prefetcher. Although we can silently ignore the
		// error and still fallback to load trie from database, but the mismatch
		// shouldn't happen in the first place, must be some programming errors.
		if prefetcher.root != root {
			return nil, errors.New("prefetcher is not matched with state")
		}
		if prefetcher.verkle {
			return nil, errors.New("prefetcher trie type is not matched")
		}
		tr = prefetcher.trie(common.Hash{}, root)
	}
	if tr == nil {
		tr, err = merkle.NewStateTrie(trie.StateTrieID(root), db)
		if err != nil {
			return nil, err
		}
	}
	return &merkleHasher{
		trie:       tr.(*merkle.StateTrie),
		root:       root,
		db:         db,
		prefetcher: prefetcher,
	}, nil
}

// UpdateAccount implements the Hasher interface, performing an account write to
// the merkle trie. It encodes the provided account object with RLP and then updates
// it in the merkle tree with provided address.
func (h *merkleHasher) UpdateAccount(address common.Address, account *types.StateAccount) error {
	return h.trie.UpdateAccount(address, account)
}

// DeleteAccount implements the Hasher interface, performing an account deletion
// from the merkle trie.
func (h *merkleHasher) DeleteAccount(address common.Address) error {
	return h.trie.DeleteAccount(address)
}

// UpdateContractCode implements the Hasher interface. It's a no-op function as
// contract code is not hashed by merkle hasher.
func (h *merkleHasher) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

// Hash implements the Hasher interface, returning the root hash of the merkle
// trie. It does not write to the database and can be used even if the merkle
// trie has no database backend.
func (h *merkleHasher) Hash() common.Hash {
	return h.trie.Hash()
}

// Commit implements the Hasher interface, collecting and returning all dirty
// nodes in the merkle trie, and replacing them with the associated node hash.
//
// The mutated tree leaves will also be collected as the reference between
// the main account trie and associated storage tries.
//
// The nodeset returned can be nil if the trie is clean(nothing to commit).
// Once the trie is committed, it's not usable anymore. A new trie must
// be created with new root and updated trie database for following usage.
func (h *merkleHasher) Commit() (common.Hash, *trienode.NodeSet, error) {
	return h.trie.Commit(true)
}

// Copy implements the Hasher interface, returning a deep-copied merkle hasher.
func (h *merkleHasher) Copy() Hasher {
	cpy := &merkleHasher{
		trie: h.trie.Copy(),
		root: h.root,
		db:   h.db,
	}
	if h.prefetcher != nil {
		cpy.prefetcher = h.prefetcher.copy()
	}
	return cpy
}

// StorageHasher implements the Hasher interface, constructing a second-layer
// storage hasher for the contract specified by address and storage root.
func (h *merkleHasher) StorageHasher(address common.Address, root common.Hash) (StorageHasher, error) {
	return newMerkleStorageHasher(h.root, address, root, h.db, h.prefetcher)
}

// merkleStorageHasher implements StorageHasher interface, providing storage
// hashing methods in the manner of Merkle-Patricia-Tree.
type merkleStorageHasher struct {
	trie *merkle.StateTrie
}

// newMerkleStorageHasher constructs the storage merkle hasher or returns an
// error if the associated merkle tree is not existent.
func newMerkleStorageHasher(stateRoot common.Hash, address common.Address, root common.Hash, db *triedb.Database, prefetcher *triePrefetcher) (*merkleStorageHasher, error) {
	var (
		tr       Trie
		err      error
		addrHash = crypto.Keccak256Hash(address.Bytes())
	)
	if prefetcher != nil {
		tr = prefetcher.trie(addrHash, root)
	}
	if tr == nil {
		tr, err = merkle.NewStateTrie(trie.StorageTrieID(stateRoot, addrHash, root), db)
		if err != nil {
			return nil, err
		}
	}
	return &merkleStorageHasher{trie: tr.(*merkle.StateTrie)}, nil
}

// UpdateStorage implements the StorageHasher interface, performing the storage
// write to the merkle trie.
func (h *merkleStorageHasher) UpdateStorage(address common.Address, key, value []byte) error {
	return h.trie.UpdateStorage(address, key, value)
}

// DeleteStorage implements the StorageHasher interface, performing the storage
// deletion from the merkle trie.
func (h *merkleStorageHasher) DeleteStorage(address common.Address, key []byte) error {
	return h.trie.DeleteStorage(address, key)
}

// Hash implements the StorageHasher interface, returning the root hash of the
// merkle trie. It does not write to the database and can be used even if the
// merkle trie has no database backend.
func (h *merkleStorageHasher) Hash() common.Hash {
	return h.trie.Hash()
}

// Commit implements the StorageHasher interface, collecting and returning all
// dirty nodes in the merkle trie, and replacing them with the associated node
// hash.
//
// The nodeset returned can be nil if the trie is clean(nothing to commit).
// Once the trie is committed, it's not usable anymore. A new trie must
// be created with new root and updated trie database for following usage.
func (h *merkleStorageHasher) Commit() (common.Hash, *trienode.NodeSet, error) {
	return h.trie.Commit(false)
}

// Copy implements the StorageHasher interface, returns a deep-copied storage hasher.
func (h *merkleStorageHasher) Copy(hasher Hasher) StorageHasher {
	return &merkleStorageHasher{
		trie: h.trie.Copy(),
	}
}
