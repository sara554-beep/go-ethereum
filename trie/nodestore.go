// Copyright 2022 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// errUnexpectedNode is returned if the requested node with specified path is
// not hash matched or marked as deleted.
var errUnexpectedNode = errors.New("unexpected node")

// nodeStore is built on the underlying node database with an additional
// node cache. The dirty nodes will be cached here whenever trie commit
// is performed to make them accessible. Nodes are keyed by node path
// which is unique in the trie.
//
// nodeStore is not safe for concurrent use.
type nodeStore struct {
	db    *Database
	nodes map[string]*memoryNode
}

// readNode retrieves the node in canonical representation.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) readNode(owner common.Hash, hash common.Hash, path []byte) (node, error) {
	// Load the node from the local cache first.
	mn, ok := s.nodes[string(path)]
	if ok {
		if mn.hash == hash {
			return mn.obj(), nil
		}
		// Bubble up an error if the trie node is not hash matched.
		// It shouldn't happen at all.
		return nil, fmt.Errorf("%w %x!=%x(%x %v)", errUnexpectedNode, mn.hash, hash, owner, path)
	}
	// Load the node from the underlying database then
	if s.db == nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
	}
	n := s.db.node(hash)
	if n != nil {
		return n, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// readBlob retrieves the node in rlp-encoded representation.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) readBlob(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	// Load the node from the local cache first
	mn, ok := s.nodes[string(path)]
	if ok {
		if mn.hash == hash {
			return mn.rlp(), nil
		}
		// Bubble up an error if the trie node is not hash matched.
		// It shouldn't happen at all.
		return nil, fmt.Errorf("%w %x!=%x(%x %v)", errUnexpectedNode, mn.hash, hash, owner, path)
	}
	// Load the node from the underlying database then
	if s.db == nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
	}
	blob, err := s.db.Node(hash)
	if len(blob) > 0 {
		return blob, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: err}
}

// write inserts a dirty node into the store. It happens in trie commit procedure.
func (s *nodeStore) write(path string, node *memoryNode) {
	s.nodes[path] = node
}

// copy deep copies the nodeStore and returns an independent handler but with
// same content cached inside.
func (s *nodeStore) copy() *nodeStore {
	nodes := make(map[string]*memoryNode)
	for k, n := range s.nodes {
		nodes[k] = n
	}
	return &nodeStore{
		db:    s.db, // safe to copy directly.
		nodes: nodes,
	}
}

// size returns the total memory usage used by caching nodes internally.
func (s *nodeStore) size() common.StorageSize {
	var size common.StorageSize
	for k, n := range s.nodes {
		size += common.StorageSize(n.memorySize(len(k)))
	}
	return size
}

// newNodeStore initializes the nodeStore with the given node reader.
func newNodeStore(db *Database) (*nodeStore, error) {
	return &nodeStore{
		db:    db,
		nodes: make(map[string]*memoryNode),
	}, nil
}

// newMemoryStore initializes the pure in-memory store.
func newMemoryStore() *nodeStore {
	return &nodeStore{nodes: make(map[string]*memoryNode)}
}
