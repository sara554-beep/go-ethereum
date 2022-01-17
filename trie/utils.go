// Copyright 2021 The go-ethereum Authors
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

// nodeSet is a set for containing batch of nodes in memory temporarily.
type nodeSet struct {
	lock  sync.RWMutex
	nodes map[string]*cachedNode // Set of dirty nodes, indexed by **storage** key
}

// newNodeSet initializes the node set.
func newNodeSet() *nodeSet {
	return &nodeSet{nodes: make(map[string]*cachedNode)}
}

// get retrieves the trie node in the set with **storage** format key.
// Note the returned value shouldn't be changed by callers.
func (set *nodeSet) get(storage []byte, hash common.Hash) (node, bool) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return nil, false
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		return node.obj(hash), true
	}
	return nil, false
}

// getBlob retrieves the encoded trie node in the set with **storage** format key.
// Note the returned value shouldn't be changed by callers.
func (set *nodeSet) getBlob(storage []byte, hash common.Hash) ([]byte, bool) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return nil, false
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		return node.rlp(), true
	}
	return nil, false
}

// getBlobByPath retrieves the encoded trie node in the set with **storage**
// format key. The node hash is not checked here.
// Note the returned value shouldn't be changed by callers.
func (set *nodeSet) getBlobByPath(storage []byte) ([]byte, bool) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return nil, false
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	if node, ok := set.nodes[string(storage)]; ok {
		return node.rlp(), true
	}
	return nil, false
}

// put stores the given state entry in the set. The given key should be encoded in
// the storage format. Note the val shouldn't be changed by caller later.
func (set *nodeSet) put(storage []byte, n node, size uint16, hash common.Hash) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return
	}
	set.lock.Lock()
	defer set.lock.Unlock()

	set.nodes[string(storage)] = &cachedNode{
		hash: hash,
		node: n,
		size: size,
	}
}

// del deletes the node from the nodeset with the given key and node hash.
// Note it's mainly used in testing!
func (set *nodeSet) del(storage []byte, hash common.Hash) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return
	}
	set.lock.Lock()
	defer set.lock.Unlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		delete(set.nodes, string(storage))
	}
}

// merge merges the dirty nodes from the other set. If there are two
// nodes with same key, then update with the node in other set.
func (set *nodeSet) merge(other *nodeSet) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil || other == nil {
		return
	}
	set.lock.Lock()
	defer set.lock.Unlock()

	other.lock.RLock()
	defer other.lock.RUnlock()

	for key, n := range other.nodes {
		set.nodes[key] = n
	}
}

// forEach iterates the dirty nodes in the set and executes the given function.
func (set *nodeSet) forEach(fn func(string, *cachedNode) error) error {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return nil
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	for key, n := range set.nodes {
		err := fn(key, n)
		if err != nil {
			return err
		}
	}
	return nil
}

// forEachBlob iterates the dirty nodes in the set and pass them in RLP-encoded format.
func (set *nodeSet) forEachBlob(fn func(string, []byte)) {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	for key, n := range set.nodes {
		fn(key, n.rlp())
	}
}

// len returns the items maintained in the set.
func (set *nodeSet) len() int {
	// Don't panic on uninitialized set, it's possible in testing.
	if set == nil {
		return 0
	}
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.nodes)
}

// CommitResult wraps the trie commit result in a single struct.
type CommitResult struct {
	Root common.Hash // The re-calculated trie root hash after commit

	// Nodes is the collection of newly updated and created nodes
	// since last commit. Nodes are indexed by **storage** key.
	Nodes     *nodeSet
	PreValues map[string][]byte
}

// CommitTo commits the tracked state diff into the given container.
func (result *CommitResult) CommitTo() map[string]*nodeWithPreValue {
	nodes := make(map[string]*nodeWithPreValue)
	result.Nodes.forEach(func(key string, n *cachedNode) error {
		nodes[key] = &nodeWithPreValue{
			cachedNode: n,
			pre:        result.PreValues[key],
		}
		return nil
	})
	return nodes
}

// NodeLen returns the number of contained nodes.
func (result *CommitResult) NodeLen() int {
	return result.Nodes.len()
}

// Merge merges the dirty nodes from the other set.
func (result *CommitResult) Merge(other *CommitResult) {
	result.Nodes.merge(other.Nodes)
	for key, pre := range other.PreValues {
		result.PreValues[key] = pre
	}
}

// NodeBlobs returns the rlp-encoded format of nodes
func (result *CommitResult) NodeBlobs() map[string][]byte {
	ret := make(map[string][]byte)
	result.Nodes.forEachBlob(func(k string, v []byte) {
		ret[k] = v
	})
	return ret
}

// NewResultFromDeletionSet constructs a commit result with the given deletion set.
func NewResultFromDeletionSet(keys [][]byte, preVals map[string][]byte) *CommitResult {
	updated := newNodeSet()
	for _, storage := range keys {
		updated.put(storage, nil, 0, common.Hash{})
	}
	return &CommitResult{
		Root:      common.Hash{},
		Nodes:     updated,
		PreValues: preVals,
	}
}
