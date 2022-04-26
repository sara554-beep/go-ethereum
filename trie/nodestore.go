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
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// nodeStore is built on the underlying in-memory database with an additional
// node cache. Once trie is committed, the dirty but not persisted nodes can
// be cached in the store.
type nodeStore struct {
	db    *Database
	nodes map[common.Hash]node
	lock  sync.RWMutex
}

// read retrieves the trie node with given node hash and the node path.
// Returns an MissingNodeError error if the node is not found.
func (s *nodeStore) read(owner common.Hash, hash common.Hash, path []byte) (node, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if n, ok := s.nodes[hash]; ok {
		return expandNode(hash.Bytes(), n), nil
	}
	if n := s.db.node(hash); n != nil {
		return n, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// readBlob retrieves the node in rlp-encoded representation.
func (s *nodeStore) readBlob(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if n, ok := s.nodes[hash]; ok {
		return nodeToBytes(n), nil
	}
	if blob, err := s.db.Node(hash); err == nil && len(blob) != 0 {
		return blob, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// commit accepts a batch of newly modified nodes and caches them in
// the local set. It happens after each commit operation.
func (s *nodeStore) commit(nodes []*commitNode) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, node := range nodes {
		s.nodes[node.hash] = node.slim
	}
}

// copy deep copies the nodeStore and returns an independent handler but
// with same content cached inside.
func (s *nodeStore) copy() *nodeStore {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes := make(map[common.Hash]node)
	for h, n := range s.nodes {
		nodes[h] = n
	}
	return &nodeStore{
		db:    s.db,
		nodes: nodes,
	}
}

// reset drops all cached nodes to clean up the nodeStore.
func (s *nodeStore) reset() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.nodes = make(map[common.Hash]node)
}

// newHashStore initializes the nodeStore with the given database.
func newNodeStore(db *Database) *nodeStore {
	return &nodeStore{
		db:    db,
		nodes: make(map[common.Hash]node),
	}
}
