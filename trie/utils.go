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

// nodeSet is a set for maintaining trie nodes.
type nodeSet struct {
	lock  sync.RWMutex
	nodes map[string]*cachedNode // indexed by **storage** key
}

// newNodeSet initializes the node set.
func newNodeSet() *nodeSet {
	return &nodeSet{nodes: make(map[string]*cachedNode)}
}

// get retrieves the trie node in the set with the given key.
func (set *nodeSet) get(storage []byte, hash common.Hash) (node, bool) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		return node.obj(), true
	}
	return nil, false
}

// getBlob retrieves the encoded trie node in the set with the given key.
func (set *nodeSet) getBlob(storage []byte, hash common.Hash) ([]byte, bool) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		return node.rlp(), true
	}
	return nil, false
}

// put stores the given state entry in the set. The given key should be encoded in
// the storage format.
func (set *nodeSet) put(storage []byte, n node, size uint16, hash common.Hash) {
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
	set.lock.Lock()
	defer set.lock.Unlock()

	if node, ok := set.nodes[string(storage)]; ok && node.hash == hash {
		delete(set.nodes, string(storage))
	}
}

// merge merges the nodes from the given set into the local one.
// If the node with same key is also present in the local set,
// then ignore the one from the other set.
func (set *nodeSet) merge(other *nodeSet) {
	set.lock.Lock()
	defer set.lock.Unlock()

	other.lock.RLock()
	defer other.lock.RUnlock()

	for key, n := range other.nodes {
		if _, present := set.nodes[key]; present {
			continue
		}
		set.nodes[key] = n
	}
}

// forEach iterates the nodes in the set and executes the given function.
func (set *nodeSet) forEach(fn func(string, *cachedNode)) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	for key, n := range set.nodes {
		fn(key, n)
	}
}

// forEachBlob iterates the nodes in the set and pass them to the given function
// in RLP-encoded format.
func (set *nodeSet) forEachBlob(fn func(string, []byte)) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	for key, n := range set.nodes {
		fn(key, n.rlp())
	}
}

// len returns the items maintained in the set.
func (set *nodeSet) len() int {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.nodes)
}

// copy deep copies the content cached inside and returns a new independent one.
func (set *nodeSet) copy() *nodeSet {
	nodes := make(map[string]*cachedNode)
	for key, node := range set.nodes {
		nodes[key] = node // node itself is read-only, safe for copy
	}
	return &nodeSet{nodes: nodes}
}

// tracer tracks the changes of trie nodes.
type tracer struct {
	lock   sync.RWMutex
	insert map[string]struct{}
	delete map[string]struct{}
}

// newTracer initializes state diff tracer.
func newTracer() *tracer {
	return &tracer{
		insert: make(map[string]struct{}),
		delete: make(map[string]struct{}),
	}
}

// onInsert tracks the newly inserted trie node. If it's already
// in the deletion set(resurrected node), then just wipe it from
// the deletion set as the "untouched".
func (t *tracer) onInsert(key []byte) {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, present := t.delete[string(key)]; present {
		delete(t.delete, string(key))
		return
	}
	t.insert[string(key)] = struct{}{}
}

// onDelete tracks the newly deleted trie node. If it's already
// in the addition set, then just wipe it from the addition set
// as the "untouched".
func (t *tracer) onDelete(key []byte) {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, present := t.insert[string(key)]; present {
		delete(t.insert, string(key))
		return
	}
	t.delete[string(key)] = struct{}{}
}

// insertList returns the tracked inserted trie nodes in list.
func (t *tracer) insertList() [][]byte {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return nil
	}
	t.lock.RLock()
	defer t.lock.RUnlock()

	var ret [][]byte
	for key := range t.insert {
		ret = append(ret, []byte(key))
	}
	return ret
}

// deleteList returns the tracked deleted trie nodes in list.
func (t *tracer) deleteList() [][]byte {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return nil
	}
	t.lock.RLock()
	defer t.lock.RUnlock()

	var ret [][]byte
	for key := range t.delete {
		ret = append(ret, []byte(key))
	}
	return ret
}

// deleteList returns the tracked inserted/deleted trie nodes in list.
func (t *tracer) reset() {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	t.insert = make(map[string]struct{})
	t.delete = make(map[string]struct{})
}

func (t *tracer) copy() *tracer {
	// Don't panic on uninitialized tracer, it's possible in testing.
	if t == nil {
		return nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	var (
		insert = make(map[string]struct{})
		delete = make(map[string]struct{})
	)
	for key := range t.insert {
		insert[key] = struct{}{}
	}
	for key := range t.delete {
		delete[key] = struct{}{}
	}
	return &tracer{
		insert: insert,
		delete: delete,
	}
}

// CommitResult wraps the trie commit result in a single struct.
type CommitResult struct {
	Root   common.Hash       // The re-calculated trie root hash after commit
	Dirty  *nodeSet          // The sef of dirty nodes since last commit operations
	Origin map[string][]byte // The corresponding pre-values of the dirty nodes
}

// Nodes returns the set of dirty nodes along with their previous value.
func (result *CommitResult) Nodes() map[string]*nodeWithPreValue {
	ret := make(map[string]*nodeWithPreValue)
	result.Dirty.forEach(func(key string, n *cachedNode) {
		ret[key] = &nodeWithPreValue{
			cachedNode: n,
			pre:        result.Origin[key],
		}
	})
	return ret
}

// NodeLen returns the number of contained nodes.
func (result *CommitResult) NodeLen() int {
	return result.Dirty.len()
}

// Merge merges the dirty nodes from the other set.
func (result *CommitResult) Merge(other *CommitResult) {
	result.Dirty.merge(other.Dirty)
	for key, pre := range other.Origin {
		if _, present := result.Origin[key]; present {
			continue
		}
		result.Origin[key] = pre
	}
}

// NodeBlobs returns the rlp-encoded format of nodes
func (result *CommitResult) NodeBlobs() map[string][]byte {
	ret := make(map[string][]byte)
	result.Dirty.forEachBlob(func(k string, v []byte) {
		ret[k] = v
	})
	return ret
}

// NewResultFromDeletionSet constructs a commit result with the given deletion set.
func NewResultFromDeletionSet(keys [][]byte, preVals map[string][]byte) *CommitResult {
	set := newNodeSet()
	for _, storage := range keys {
		set.put(storage, nil, 0, common.Hash{})
	}
	return &CommitResult{
		Root:   emptyRoot,
		Dirty:  set,
		Origin: preVals,
	}
}
