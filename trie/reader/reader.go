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

package reader

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// Reader is a wrapper of the underlying database reader for accessing trie
// nodes on demand. It's not safe for concurrent usage.
type Reader struct {
	owner  common.Hash
	reader database.NodeReader
	banned map[string]struct{} // Marker to prevent node from being accessed, for tests
}

// New initializes the trie reader.
func New(stateRoot, owner common.Hash, db database.NodeDatabase) (*Reader, error) {
	if stateRoot == (common.Hash{}) || stateRoot == types.EmptyRootHash {
		if stateRoot == (common.Hash{}) {
			log.Error("Zero state root hash!")
		}
		return &Reader{owner: owner}, nil
	}
	reader, err := db.NodeReader(stateRoot)
	if err != nil {
		return nil, &trie.MissingNodeError{Owner: owner, NodeHash: stateRoot, Err: err}
	}
	return &Reader{owner: owner, reader: reader}, nil
}

// NewEmpty initializes the pure in-memory reader. All read operations
// should be forbidden and returns the errors.MissingNodeError.
func NewEmpty() *Reader {
	return &Reader{}
}

// Node retrieves the encoded trie node with the provided trie node information.
// An MissingNodeError will be returned in case the node is not found or any error
// is encountered.
func (r *Reader) Node(path []byte, hash common.Hash) ([]byte, error) {
	// Perform the logics in tests for preventing trie node access.
	if r.banned != nil {
		if _, ok := r.banned[string(path)]; ok {
			return nil, &trie.MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
		}
	}
	// Short circuit if the database reader is not reachable.
	if r.reader == nil {
		return nil, &trie.MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
	}
	blob, err := r.reader.Node(r.owner, path, hash)
	if err != nil || len(blob) == 0 {
		return nil, &trie.MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path, Err: err}
	}
	return blob, nil
}

// Ban prevents the node with specific path being accessed, for testing.
func (r *Reader) Ban(path []byte) {
	if r.banned == nil {
		r.banned = make(map[string]struct{})
	}
	r.banned[string(path)] = struct{}{}
}

// Unban enables the node with specific path being accessed, for testing.
func (r *Reader) Unban(path []byte) {
	if r.banned == nil {
		return
	}
	if _, ok := r.banned[string(path)]; !ok {
		return
	}
	delete(r.banned, string(path))

	if len(r.banned) != 0 {
		return
	}
	r.banned = nil
}
