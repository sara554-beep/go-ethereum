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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// nodeStore is built on the underlying node reader with an additional
// node cache. The dirty nodes will be cached here whenever trie commit
// is performed to make them accessible.
//
// nodeStore is not safe for concurrent use.
type nodeStore struct {
	reader Reader
	nodes  map[string]*memoryNode

	// Marker to prevent node from being accessed, only used for testing
	banned map[string]struct{}
}

// read retrieves the trie node with given node hash and the node path.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) read(owner common.Hash, hash common.Hash, path []byte) (*memoryNode, error) {
	// Perform the logics in tests for preventing trie node access.
	storage := EncodeStorageKey(owner, path)
	if s.banned != nil {
		if _, ok := s.banned[string(storage)]; ok {
			return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
		}
	}
	// Load the node from the local cache first
	n, ok := s.nodes[string(storage)]
	if ok {
		// If the trie node is not hash matched, or marked as removed,
		// bubble up an error here. It shouldn't happen at all.
		if n.hash != hash {
			return nil, fmt.Errorf("%w %x!=%x(%x %v)", errUnexpectedNode, n.hash, hash, owner, path)
		}
		return n, nil
	}
	// Load the node from the underlying node reader then
	if s.reader == nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
	}
	n, err := s.reader.Node(storage, hash)
	if err != nil || n == nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: err}
	}
	return n, nil
}

// readNode retrieves the node in canonical representation.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) readNode(owner common.Hash, hash common.Hash, path []byte) (node, error) {
	node, err := s.read(owner, hash, path)
	if err != nil {
		return nil, err
	}
	return node.obj(), nil
}

// readBlob retrieves the node in rlp-encoded representation.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) readBlob(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	node, err := s.read(owner, hash, path)
	if err != nil {
		return nil, err
	}
	return node.rlp(), nil
}

// write puts a dirty node into the store. It happens after trie commit operation.
func (s *nodeStore) write(key []byte, node *memoryNode) {
	s.nodes[string(key)] = node
}

// copy deep copies the nodeStore and returns an independent handler but
// with same content cached inside.
func (s *nodeStore) copy() *nodeStore {
	nodes := make(map[string]*memoryNode)
	for k, n := range s.nodes {
		nodes[k] = n
	}
	return &nodeStore{
		reader: s.reader, // safe to copy directly.
		nodes:  nodes,
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
func newNodeStore(root common.Hash, db NodeReader) (*nodeStore, error) {
	return &nodeStore{
		reader: db.GetReader(root),
		nodes:  make(map[string]*memoryNode),
	}, nil
}

// newMemoryStore initializes the pure in-memory store.
func newMemoryStore() *nodeStore {
	return &nodeStore{nodes: make(map[string]*memoryNode)}
}
