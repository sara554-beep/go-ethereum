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
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// nodeReader wraps the necessary functions for trie to read the trie nodes
type nodeReader interface {
	// read retrieves the rlp-encoded trie node with given node hash and the
	// node path. Returns the node blob if found, otherwise, an MissingNodeError
	// error is expected.
	read(owner common.Hash, hash common.Hash, path []byte) ([]byte, error)
}

// snapReader is an implementation of nodeReader. It leverages the in-memory
// multi-layer structure to access the trie node based on the path.
type snapReader struct {
	snap snapshot // The base layer for retrieving trie node
}

// newSnapReader constructs the snapReader by given state identifier and
// in-memory database. If the corresponding state layer can't be found,
// return an MissingNodeError error then.
func newSnapReader(stateRoot common.Hash, owner common.Hash, db StateReader) (*snapReader, error) {
	var snap snapshot
	if stateRoot != (common.Hash{}) && stateRoot != emptyState {
		ret := db.Snapshot(stateRoot)
		if ret == nil {
			return nil, &MissingNodeError{NodeHash: stateRoot, Owner: owner}
		}
		snap = ret.(snapshot)
	}
	return &snapReader{snap: snap}, nil
}

// read retrieves the rlp-encoded trie node with given node hash and the
// node path. Returns the node blob if found, otherwise, an MissingNodeError
// error is expected.
func (s *snapReader) read(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	blob, err := s.snap.NodeBlob(EncodeStorageKey(owner, path), hash)
	if err != nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: err}
	}
	if len(blob) > 0 {
		return blob, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// hashReader is an implementation of nodeReader. It's the legacy version
// trie node reader which resolves the node by its hash.
type hashReader struct {
	db ethdb.Database
}

// newHashReader constructs the hashReader with the given raw database.
func newHashReader(db ethdb.Database) *hashReader {
	return &hashReader{db: db}
}

// read retrieves the rlp-encoded trie node with given node hash. Returns
// the node blob if found, otherwise, an MissingNodeError error is expected.
func (h *hashReader) read(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	blob := rawdb.ReadLegacyTrieNode(h.db, hash)
	if len(blob) != 0 {
		return blob, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// nodeStore is built on the underlying node reader with an additional
// node cache. Once trie is committed, the dirty but not persisted nodes
// can be cached in the store
type nodeStore struct {
	reader nodeReader
	nodes  map[string]*cachedNode
	origin map[string][]byte
	lock   sync.RWMutex
}

// read retrieves the rlp-encoded trie node with given node hash and the
// node path. Returns the node blob if found, otherwise, an MissingNodeError
// error is expected.
func (s *nodeStore) read(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Load the node from the cached dirty node set first
	storage := string(EncodeStorageKey(owner, path))
	n, exist := s.nodes[storage]
	if exist && n.hash == hash {
		return n.rlp(), nil
	}
	// Load the node from the underlying node reader then
	blob, err := s.reader.read(owner, hash, path)
	if err != nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: err}
	}
	if len(blob) == 0 {
		return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
	}
	s.origin[storage] = blob
	return blob, nil
}

// readByPath retrieves the trie node blob with given node storage key.
// It holds the assumption that the node with specified path must
// already be loaded from the underlying reader and cached internally.
// It's used to load the previous value of the node.
func (s *nodeStore) readByPath(storage string) []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.origin[storage]
}

// commit accepts a batch of newly modified nodes and caches them in
// the local set. It happens after each commit operation.
func (s *nodeStore) commit(nodes *nodeSet) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes.forEach(func(storage string, n *cachedNode) {
		s.nodes[storage] = n
		if n.node == nil {
			delete(s.origin, storage)
		} else {
			s.origin[storage] = n.rlp()
		}
	})
}

// copy deep copies the nodeStore and returns an independent handler but
// with same content cached inside.
func (s *nodeStore) copy() *nodeStore {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes := make(map[string]*cachedNode)
	for k, n := range s.nodes {
		nodes[k] = n
	}
	origin := make(map[string][]byte)
	for k, blob := range s.origin {
		origin[k] = blob
	}
	return &nodeStore{
		reader: s.reader,
		nodes:  nodes,
		origin: origin,
	}
}

// newSnapStore initializes the snap based nodeStore with the given multilayer
// trie nodes and the corresponding state identifier.
func newSnapStore(stateRoot common.Hash, owner common.Hash, db StateReader) (*nodeStore, error) {
	reader, err := newSnapReader(stateRoot, owner, db)
	if err != nil {
		return nil, err
	}
	return &nodeStore{
		reader: reader,
		nodes:  make(map[string]*cachedNode),
		origin: make(map[string][]byte),
	}, nil
}

// newHashStore initializes the hash based nodeStore with the given database.
func newHashStore(db ethdb.Database) *nodeStore {
	return &nodeStore{
		reader: newHashReader(db),
		nodes:  make(map[string]*cachedNode),
		origin: make(map[string][]byte),
	}
}
