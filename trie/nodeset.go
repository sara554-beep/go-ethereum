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

// NodeSet contains all dirty nodes collected during the commit operation.
// Each node is keyed by path. It's not thread-safe to use.
type NodeSet struct {
	owner common.Hash            // the identifier of the trie
	paths []string               // the path of dirty nodes, sort by insertion order
	nodes map[string]*memoryNode // the map of dirty nodes, keyed by node path
	leafs []*leaf                // the list of dirty leafs
}

// NewNodeSet initializes an empty dirty node set.
func NewNodeSet(owner common.Hash) *NodeSet {
	return &NodeSet{
		owner: owner,
		nodes: make(map[string]*memoryNode),
	}
}

// add caches node with provided path and node object.
func (set *NodeSet) add(path string, node *memoryNode) {
	set.paths = append(set.paths, path)
	set.nodes[path] = node
}

// addLeaf caches the provided leaf node.
func (set *NodeSet) addLeaf(node *leaf) {
	set.leafs = append(set.leafs, node)
}

// Len returns the number of dirty nodes contained in the set.
func (set *NodeSet) Len() int {
	return len(set.nodes)
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