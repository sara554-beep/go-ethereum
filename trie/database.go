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

package trie

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// Reader wraps the Node method of a backing trie reader.
type Reader interface {
	// Node retrieves the trie node blob with the provided trie identifier,
	// node path and the corresponding node hash. No error will be returned
	// if the node is not found.
	//
	// When looking up nodes in the account trie, 'owner' is the zero hash.
	// For contract storage trie nodes, 'owner' is the hash of the account
	// address that containing the storage.
	//
	// Don't modify the returned byte slice since it's not deep-copied and
	// still be referenced by database.
	Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}

// PreimageStore wraps the methods of a backing store for reading and writing
// trie node preimages.
type PreimageStore interface {
	// Preimage retrieves the preimage of the specified hash.
	Preimage(hash common.Hash) []byte

	// InsertPreimage commits a set of preimages along with their hashes.
	InsertPreimage(preimages map[common.Hash][]byte)
}

// Database wraps the methods of a backing trie store.
type Database interface {
	PreimageStore

	// ReaderAny returns a node reader associated with the specific state.
	// An error will be returned if the specified state is not available.
	ReaderAny(stateRoot common.Hash) (any, error)
}

// trieReader is a wrapper of the underlying node reader. It's not safe
// for concurrent usage.
type trieReader struct {
	owner  common.Hash
	reader Reader
	banned map[string]struct{} // Marker to prevent node from being accessed, for tests
}

// newTrieReader initializes the trie reader with the given node reader.
func newTrieReader(stateRoot, owner common.Hash, db Database) (*trieReader, error) {
	if stateRoot == (common.Hash{}) || stateRoot == types.EmptyRootHash {
		if stateRoot == (common.Hash{}) {
			log.Error("Zero state root hash!")
		}
		return &trieReader{owner: owner}, nil
	}
	ret, err := db.ReaderAny(stateRoot)
	if err != nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: stateRoot, err: err}
	}
	reader, ok := ret.(Reader)
	if !ok {
		return nil, errors.New("invalid trie database")
	}
	return &trieReader{owner: owner, reader: reader}, nil
}

// newEmptyReader initializes the pure in-memory reader. All read operations
// should be forbidden and returns the MissingNodeError.
func newEmptyReader() *trieReader {
	return &trieReader{}
}

// node retrieves the rlp-encoded trie node with the provided trie node
// information. An MissingNodeError will be returned in case the node is
// not found or any error is encountered.
func (r *trieReader) node(path []byte, hash common.Hash) ([]byte, error) {
	// Perform the logics in tests for preventing trie node access.
	if r.banned != nil {
		if _, ok := r.banned[string(path)]; ok {
			return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
		}
	}
	if r.reader == nil {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
	}
	blob, err := r.reader.Node(r.owner, path, hash)
	if err != nil || len(blob) == 0 {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path, err: err}
	}
	return blob, nil
}

// MerkleLoader implements triestate.TrieLoader for constructing tries.
type MerkleLoader struct {
	db Database
}

func NewMerkleTrieLoader(db Database) *MerkleLoader {
	return &MerkleLoader{db: db}
}

// OpenTrie opens the main account trie.
func (l *MerkleLoader) OpenTrie(root common.Hash) (*Trie, error) {
	return New(TrieID(root), l.db)
}

// OpenStorageTrie opens the storage trie of an account.
func (l *MerkleLoader) OpenStorageTrie(stateRoot common.Hash, addrHash, root common.Hash) (*Trie, error) {
	return New(StorageTrieID(stateRoot, addrHash, root), l.db)
}
