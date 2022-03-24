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
	"reflect"

	"github.com/ethereum/go-ethereum/common"
)

// memoryNode is all the information we know about a single cached trie node
// in the memory.
type memoryNode struct {
	hash common.Hash // Node hash, computed by hashing rlp value, empty for deleted nodes
	size uint16      // Byte size of the useful cached data, 0 for deleted nodes
	node node        // Cached collapsed trie node, or raw rlp data, nil for deleted nodes
}

// memoryNodeSize is the raw size of a memoryNode data structure without any
// node data included. It's an approximate size, but should be a lot better
// than not counting them.
var memoryNodeSize = int(reflect.TypeOf(memoryNode{}).Size())

// rlp returns the raw rlp encoded blob of the cached trie node, either directly
// from the cache, or by regenerating it from the collapsed node.
func (n *memoryNode) rlp() []byte {
	if n.node == nil {
		return nil
	}
	if node, ok := n.node.(rawNode); ok {
		return node
	}
	return nodeToBytes(n.node)
}

// obj returns the decoded and expanded trie node, either directly from the cache,
// or by regenerating it from the rlp encoded blob.
func (n *memoryNode) obj() node {
	if n.node == nil {
		return nil
	}
	if node, ok := n.node.(rawNode); ok {
		return mustDecodeNode(n.hash[:], node)
	}
	return expandNode(n.hash[:], n.node)
}

// memorySize returns the total memory size used by this node.
func (n *memoryNode) memorySize(key int) int {
	return int(n.size) + memoryNodeSize + key
}

// nodeWithPrev wraps the memoryNode with the previous node value.
type nodeWithPrev struct {
	*memoryNode
	prev []byte // RLP-encoded previous value, nil means it's non-existent
}

// unwrap returns the internal memoryNode object.
func (n *nodeWithPrev) unwrap() *memoryNode {
	return n.memoryNode
}

// memorySize returns the total memory size used by this node. It overloads
// the function in memoryNode by counting the size of previous value as well.
func (n *nodeWithPrev) memorySize(key int) int {
	return n.memoryNode.memorySize(key) + len(n.prev)
}

// nodesWithOrder represents a collection of dirty nodes which includes
// newly-inserted and updated nodes. The modification order of all nodes
// is represented by order list.
type nodesWithOrder struct {
	order []string                 // the path list of dirty nodes, sort by insertion order
	nodes map[string]*nodeWithPrev // the map of dirty nodes, keyed by node path
}

// NodeSet contains all dirty nodes collected during the commit operation.
// Each node is keyed by path. It's not thread-safe to use.
type NodeSet struct {
	owner   common.Hash       // the identifier of the trie
	updates *nodesWithOrder   // the set of updated nodes(include inserted)
	deletes map[string][]byte // the map of deleted nodes, keyed by node
	leafs   []*leaf           // the list of dirty leafs
}

// NewNodeSet initializes an empty dirty node set.
func NewNodeSet(owner common.Hash) *NodeSet {
	return &NodeSet{
		owner: owner,
		updates: &nodesWithOrder{
			nodes: make(map[string]*nodeWithPrev),
		},
		deletes: make(map[string][]byte),
	}
}

// NewNodeSetWithDeletion initializes the nodeset with provided deletion set.
func NewNodeSetWithDeletion(owner common.Hash, paths [][]byte, prev [][]byte) *NodeSet {
	set := NewNodeSet(owner)
	for i, path := range paths {
		set.markDeleted(path, prev[i])
	}
	return set
}

// markUpdated marks the node as dirty(newly-inserted or updated) with provided
// node path, node object along with its previous value.
func (set *NodeSet) markUpdated(path []byte, node *memoryNode, prev []byte) {
	set.updates.order = append(set.updates.order, string(path))
	set.updates.nodes[string(path)] = &nodeWithPrev{
		memoryNode: node,
		prev:       prev,
	}
}

// markDeleted marks the node as deleted with provided path and previous value.
func (set *NodeSet) markDeleted(path []byte, prev []byte) {
	set.deletes[string(path)] = prev
}

// addLeaf collects the provided leaf node into set.
func (set *NodeSet) addLeaf(node *leaf) {
	set.leafs = append(set.leafs, node)
}

// Size returns the number of updated and deleted nodes contained in the set.
func (set *NodeSet) Size() (int, int) {
	return len(set.updates.order), len(set.deletes)
}

// Hashes returns the hashes of all updated nodes. TODO(rjl493456442) how can
// we get rid of it?
func (set *NodeSet) Hashes() []common.Hash {
	var ret []common.Hash
	for _, node := range set.updates.nodes {
		ret = append(ret, node.hash)
	}
	return ret
}

// MergedNodeSet represents a merged dirty node set for a group of tries.
type MergedNodeSet struct {
	nodes map[common.Hash]*NodeSet
}

// NewMergedNodeSet initializes an empty merged set.
func NewMergedNodeSet() *MergedNodeSet {
	return &MergedNodeSet{nodes: make(map[common.Hash]*NodeSet)}
}

// NewWithNodeSet constructs a merged nodeset with the provided single set.
func NewWithNodeSet(set *NodeSet) *MergedNodeSet {
	merged := NewMergedNodeSet()
	merged.Merge(set)
	return merged
}

// Merge merges the provided dirty nodes of a trie into the set. The assumption
// is held that no duplicated set belonging to the same trie will be merged twice.
func (set *MergedNodeSet) Merge(other *NodeSet) error {
	_, present := set.nodes[other.owner]
	if present {
		return fmt.Errorf("duplicated trie %x", other.owner)
	}
	set.nodes[other.owner] = other
	return nil
}
