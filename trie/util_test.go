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
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	tiny = []struct{ k, v string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}
	nonAligned = []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	standard = []struct{ k, v string }{
		{string(randBytes(32)), "verb"},
		{string(randBytes(32)), "wookiedoo"},
		{string(randBytes(32)), "stallion"},
		{string(randBytes(32)), "horse"},
		{string(randBytes(32)), "coin"},
		{string(randBytes(32)), "puppy"},
		{string(randBytes(32)), "myothernodedata"},
	}
)

func TestTrieTracer(t *testing.T) {
	testTrieTracer(t, tiny)
	testTrieTracer(t, nonAligned)
	testTrieTracer(t, standard)
}

// Tests if the trie diffs are tracked correctly. Tracer should capture
// all non-leave dirty nodes, no matter the node is embedded or not.
func testTrieTracer(t *testing.T, vals []struct{ k, v string }) {
	db := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(db)

	// Determine all insertions are tracked
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	insertSet := copySet(trie.tracer.insert) // copy before commit
	deleteSet := copySet(trie.tracer.delete) // copy before commit
	root, nodes := trie.Commit(false)
	db.Update(NewWithNodeSet(nodes))

	seen := setKeys(iterNodes(db, root))
	if !compareSet(insertSet, seen) {
		t.Fatal("Unexpected insertion set")
	}
	if !compareSet(deleteSet, nil) {
		t.Fatal("Unexpected deletion set")
	}

	// Determine all deletions are tracked
	trie, _ = New(TrieID(root), db)
	for _, val := range vals {
		trie.Delete([]byte(val.k))
	}
	insertSet, deleteSet = copySet(trie.tracer.insert), copySet(trie.tracer.delete)
	if !compareSet(insertSet, nil) {
		t.Fatal("Unexpected insertion set")
	}
	if !compareSet(deleteSet, seen) {
		t.Fatal("Unexpected deletion set")
	}
}

// Test that after inserting a new batch of nodes and deleting them immediately,
// the trie tracer should be cleared normally as no operation happened.
func TestTrieTracerNoop(t *testing.T) {
	testTrieTracerNoop(t, tiny)
	testTrieTracerNoop(t, nonAligned)
	testTrieTracerNoop(t, standard)
}

func testTrieTracerNoop(t *testing.T, vals []struct{ k, v string }) {
	trie := NewEmpty(NewDatabase(rawdb.NewMemoryDatabase()))
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	for _, val := range vals {
		trie.Delete([]byte(val.k))
	}
	if len(trie.tracer.insert) != 0 {
		t.Fatal("Unexpected insertion set")
	}
	if len(trie.tracer.delete) != 0 {
		t.Fatal("Unexpected deletion set")
	}
}

// Test whether the original value of the loaded nodes are correctly recorded.
// Besides, this will also ensure the accessList won't be spammed because of
// trie iteration and proving.
func TestAccessList(t *testing.T) {
	testAccessList(t, tiny)
	testAccessList(t, nonAligned)
	testAccessList(t, standard)
}

func testAccessList(t *testing.T, vals []struct{ k, v string }) {
	db := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(db)
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	if len(trie.tracer.accessList) != 0 {
		t.Fatalf("Nothing should be tracked")
	}
	root, nodes := trie.Commit(false)
	db.Update(NewWithNodeSet(nodes))

	// Reload all nodes in trie
	trie, _ = New(TrieID(root), db)
	for _, val := range vals {
		trie.TryGet([]byte(val.k))
	}
	// Ensure all nodes are tracked by tracer with correct values,
	// which should be aligned with *non-embedded* trie nodes.
	seen := iterNodesWithHash(db, root)
	if !compareValueSet(trie.tracer.accessList, seen) {
		t.Fatal("Unexpected accessList")
	}

	// Re-open the trie and iterate the trie, ensure nothing will be tracked.
	// Iterator will not link any loaded nodes to trie.
	trie, _ = New(TrieID(root), db)
	prev := len(trie.tracer.accessList)
	iter := trie.NodeIterator(nil)
	for iter.Next(true) {
	}
	if len(trie.tracer.accessList) != prev {
		t.Fatalf("Nothing should be tracked")
	}

	// Re-open the trie and generate proof for entries, ensure nothing will
	// be tracked. Prover will not link any loaded nodes to trie.
	trie, _ = New(TrieID(root), db)
	prev = len(trie.tracer.accessList)
	for _, val := range vals {
		trie.Prove([]byte(val.k), 0, rawdb.NewMemoryDatabase())
	}
	if len(trie.tracer.accessList) != prev {
		t.Fatalf("Nothing should be tracked")
	}

	// Delete entries from trie, ensure all previous values are correct,
	// which should be aligned with *non-embedded* trie nodes.
	trie, _ = New(TrieID(root), db)
	for _, val := range vals {
		trie.TryDelete([]byte(val.k))
	}
	if !compareValueSet(trie.tracer.accessList, seen) {
		t.Fatal("Unexpected accessList")
	}
}

// Tests that nodes are correctly recorded when inserting or deleting nodes
// into the trie.
func TestNodeSet(t *testing.T) {
	testNodeSet(t, tiny)
	testNodeSet(t, nonAligned)
	testNodeSet(t, standard)
}

func testNodeSet(t *testing.T, vals []struct{ k, v string }) {
	db := NewDatabase(rawdb.NewMemoryDatabase())
	trie := NewEmpty(db)
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	root, set := trie.Commit(false)
	db.Update(NewWithNodeSet(set))

	nodes := iterNodesWithHash(db, root)
	dirty := make(map[string]struct{})
	for path := range set.nodes {
		dirty[path] = struct{}{}
	}
	if !compareSet(dirty, setKeys(nodes)) {
		t.Fatal("Unexpected nodeset")
	}
	if !compareValueSet(set.accessList, nil) {
		t.Fatal("Unexpected accessList")
	}

	// Delete entries from trie, ensure all values are detected
	trie, _ = New(TrieID(root), db)
	for _, val := range vals {
		trie.Delete([]byte(val.k))
	}
	root, set = trie.Commit(false)
	if root != types.EmptyRootHash {
		t.Fatalf("Invalid trie root %v", root)
	}
	dirty = make(map[string]struct{})
	for path := range set.nodes {
		dirty[path] = struct{}{}
	}
	if !compareSet(dirty, setKeys(nodes)) {
		t.Fatal("Unexpected nodeset")
	}
	if !compareValueSet(set.accessList, nodes) {
		t.Fatal("Unexpected accessList")
	}
}

// Tests whether the original tree node is correctly deleted after being embedded
// in its parent due to the smaller size of the original tree node.
func TestEmbedNode(t *testing.T) {
	var (
		db   = NewDatabase(rawdb.NewMemoryDatabase())
		trie = NewEmpty(db)
	)
	for _, val := range tiny {
		trie.Update([]byte(val.k), randBytes(32))
	}
	root, set := trie.Commit(false)
	db.Update(NewWithNodeSet(set))
	nodesA := iterNodesWithHash(db, root)

	trie, _ = New(TrieID(root), db)
	for _, val := range tiny {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	root, set = trie.Commit(false)
	db.Update(NewWithNodeSet(set))
	nodesB := iterNodesWithHash(db, root)

	// The nodes in old set but not in new set are the nodes
	// get removed from trie.
	for path, blob := range nodesA {
		if _, ok := nodesB[path]; ok {
			continue
		}
		n, ok := set.nodes[path]
		if !ok {
			t.Fatal("missing node")
		}
		if !n.isDeleted() {
			t.Fatal("unexpected node")
		}
		if !bytes.Equal(set.accessList[path], blob) {
			t.Fatal("unexpected accessList")
		}
	}
}

func compareSet(setA, setB map[string]struct{}) bool {
	if len(setA) != len(setB) {
		return false
	}
	for key := range setA {
		if _, ok := setB[key]; !ok {
			return false
		}
	}
	return true
}

func compareValueSet(setA, setB map[string][]byte) bool {
	if len(setA) != len(setB) {
		return false
	}
	for key, valA := range setA {
		valB, ok := setB[key]
		if !ok {
			return false
		}
		if !bytes.Equal(valA, valB) {
			return false
		}
	}
	return true
}

func iterNodes(db *Database, root common.Hash) map[string][]byte {
	var (
		trie, _ = New(TrieID(root), db)
		it      = trie.NodeIterator(nil)
		nodes   = make(map[string][]byte)
	)
	for it.Next(true) {
		if it.Leaf() {
			continue
		}
		nodes[string(it.Path())] = common.CopyBytes(it.NodeBlob())
	}
	return nodes
}

func iterNodesWithHash(db *Database, root common.Hash) map[string][]byte {
	var (
		trie, _ = New(TrieID(root), db)
		it      = trie.NodeIterator(nil)
		nodes   = make(map[string][]byte)
	)
	for it.Next(true) {
		if it.Hash() == (common.Hash{}) {
			continue
		}
		nodes[string(it.Path())] = common.CopyBytes(it.NodeBlob())
	}
	return nodes
}

func setKeys(set map[string][]byte) map[string]struct{} {
	keys := make(map[string]struct{})
	for k := range set {
		keys[k] = struct{}{}
	}
	return keys
}

func copySet(set map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{})
	for k := range set {
		copied[k] = struct{}{}
	}
	return copied
}
