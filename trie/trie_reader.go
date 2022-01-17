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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// nodeReadCacher wraps the necessary functions for trie to read/cache the
// trie nodes.
type nodeReadCacher interface {
	// Read retrieves the trie node with given node hash and the node path
	// Returns the node object if found, otherwise, an MissingNodeError
	// error is expected.
	Read(owner common.Hash, hash common.Hash, path []byte) (node, error)

	// ReadBlob retrieves the rlp-encoded trie node with given node hash and the
	// node path. Returns the node blob if found, otherwise, an MissingNodeError
	// error is expected.
	ReadBlob(owner common.Hash, hash common.Hash, path []byte) ([]byte, error)

	// ReadOrigin retrieves the rlp-encoded trie node with the specific node
	// path. It doesn't check if the node hash is matched or not.
	// The error is only returned if there is an internal failure. If the node
	// is found, return the RLP-encoded blob, otherwise return nil with no error.
	ReadOrigin(owner common.Hash, path []byte) ([]byte, error)

	// CacheNodes accepts a batch of newly modified nodes and caches them in
	// the local set.
	CacheNodes(nodes *nodeSet)
}

// snapReadCacher is an implementation of nodeReadCacher. It combines the path-based disk
// storage and the in-memory multi-layer structure, to access the trie node based
// on the node path.
type snapReadCacher struct {
	// snap it the base layer for retrieving trie state. It's initialised since
	// the trie creation. All subsequent new states introduced by the trie operation
	// can be accessed in the accumulated memory node set.
	snap snapshot

	// cache is the container for maintaining all un-persisted nodes since creation.
	// It acts as the auxiliary self-contained lookup and will only be released
	// when the trie itself is deallocated.
	cache *nodeSet

	// The previous loaded node
	origin     map[string]node
	originLock sync.RWMutex
}

// newSnapReadCacher constructs the snapReadCacher by given state identifier and in-memory database.
// If the corresponding in-disk state can't be found, return an error then.
func newSnapReadCacher(stateRoot common.Hash, owner common.Hash, db *Database) (*snapReadCacher, error) {
	var snap snapshot
	if stateRoot != (common.Hash{}) && stateRoot != emptyState {
		ret := db.Snapshot(stateRoot)
		if ret == nil {
			return nil, &MissingNodeError{NodeHash: stateRoot, Owner: owner}
		}
		snap = ret.(snapshot)
	}
	return &snapReadCacher{
		snap:   snap,
		cache:  newNodeSet(),
		origin: make(map[string]node),
	}, nil
}

// Read retrieves the trie node with given node hash and the node path
// Returns the node object if found, otherwise, an MissingNodeError
// error is expected.
func (s *snapReadCacher) Read(owner common.Hash, hash common.Hash, path []byte) (node, error) {
	// Always try to look up the node in temporary memory set in case they
	// are still kept in the memory.
	var storage = EncodeStorageKey(owner, path)
	if n, exist := s.cache.get(storage, hash); exist {
		return n, nil
	}
	// Lookup the node in multi-layer structure, cache it in memory if found.
	if s.snap != nil {
		node, err := s.snap.Node(storage, hash)
		if err != nil {
			return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: err}
		}
		if node == nil {
			return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path, err: errors.New("deleted")}
		}
		s.originLock.Lock()
		s.origin[string(storage)] = node
		s.originLock.Unlock()
		return node, nil
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// ReadBlob retrieves the rlp-encoded trie node with given node hash and the
// node path. Returns the node blob if found, otherwise, an MissingNodeError
// error is expected.
func (s *snapReadCacher) ReadBlob(owner common.Hash, hash common.Hash, path []byte) ([]byte, error) {
	// Always try to look up the node in temporary memory set in case they
	// are still kept in the memory.
	var storage = EncodeStorageKey(owner, path)
	if blob, exist := s.cache.getBlob(storage, hash); exist {
		return blob, nil
	}
	if s.snap != nil {
		blob, err := s.snap.NodeBlob(storage, hash)
		if err != nil {
			return nil, err
		}
		if len(blob) > 0 {
			return blob, nil
		}
	}
	return nil, &MissingNodeError{Owner: owner, NodeHash: hash, Path: path}
}

// ReadOrigin retrieves the rlp-encoded trie node with the specific node
// path. It doesn't check if the node hash is matched or not.
// The error is only returned if there is an internal failure. If the node
// is found, return the RLP-encoded blob, otherwise return nil with no error.
func (s *snapReadCacher) ReadOrigin(owner common.Hash, path []byte) ([]byte, error) {
	s.originLock.RLock()
	defer s.originLock.RUnlock()

	var storage = EncodeStorageKey(owner, path)
	if n, exist := s.origin[string(storage)]; exist {
		switch nn := n.(type) {
		case *fullNode:
			return rlp.EncodeToBytes(nn)
		case *shortNode:
			node := nn.copy()
			node.Key = hexToCompact(node.Key)
			return rlp.EncodeToBytes(node)
		}
	}
	return nil, nil
}

// CacheNodes accepts a batch of newly modified nodes and caches them in
// the local set.
func (s *snapReadCacher) CacheNodes(nodes *nodeSet) {
	s.cache.merge(nodes)
}
