// Copyright 2020 The go-ethereum Authors
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
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// leaf represents a trie leaf value
type leaf struct {
	val        []byte      // raw blob of leaf
	parent     []byte      // the key of parent node
	parentHash common.Hash // the hash of parent node
}

// committer is a type used for the trie Commit operation. The committer will
// cache all dirty nodes collected during the commit process and sort them in
// order from bottom to top.
type committer struct {
	owner    common.Hash
	keys     [][]byte      // the list of dirty node keys in order
	nodes    []*memoryNode // the list of dirty nodes in order
	leaves   []*leaf       // the list of leaf nodes
	embedded [][]byte      // the list of embedded node keys
}

// committers live in a global sync.Pool
var committerPool = sync.Pool{
	New: func() interface{} {
		return &committer{}
	},
}

// newCommitter creates a new committer or picks one from the pool.
func newCommitter(owner common.Hash) *committer {
	ret := committerPool.Get().(*committer)
	ret.owner = owner
	return ret
}

func returnCommitterToPool(h *committer) {
	h.owner = common.Hash{}
	h.keys = nil
	h.nodes = nil
	h.leaves = nil
	h.embedded = nil
	committerPool.Put(h)
}

// Commit collapses a node down into a hash node and inserts it into the database
func (c *committer) Commit(n node) (hashNode, error) {
	h, err := c.commit(nil, n)
	if err != nil {
		return nil, err
	}
	return h.(hashNode), nil
}

// commit collapses a node down into a hash node and inserts it into the database
func (c *committer) commit(path []byte, n node) (node, error) {
	// if this path is clean, use available cached data
	hash, dirty := n.cache()
	if hash != nil && !dirty {
		return hash, nil
	}
	// Commit children, then parent, and remove the dirty flag.
	switch cn := n.(type) {
	case *shortNode:
		// Commit child
		collapsed := cn.copy()

		// If the child is fullNode, recursively commit,
		// otherwise it can only be hashNode or valueNode.
		if _, ok := cn.Val.(*fullNode); ok {
			childV, err := c.commit(append(path, cn.Key...), cn.Val)
			if err != nil {
				return nil, err
			}
			collapsed.Val = childV
		}
		// The key needs to be copied, since we're delivering it to database
		collapsed.Key = hexToCompact(cn.Key)
		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn, nil
		}
		// The short node is embedded in its parent, track it.
		c.embedded = append(c.embedded, EncodeStorageKey(c.owner, path))
		return collapsed, nil
	case *fullNode:
		hashedKids, err := c.commitChildren(path, cn)
		if err != nil {
			return nil, err
		}
		collapsed := cn.copy()
		collapsed.Children = hashedKids

		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn, nil
		}
		// The full node is embedded in its parent, track it.
		c.embedded = append(c.embedded, EncodeStorageKey(c.owner, path))
		return collapsed, nil
	case hashNode:
		return cn, nil
	default:
		// nil, valuenode shouldn't be committed
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// commitChildren commits the children of the given fullnode
func (c *committer) commitChildren(path []byte, n *fullNode) ([17]node, error) {
	var children [17]node
	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child == nil {
			continue
		}
		// If it's the hashed child, save the hash value directly.
		// Note: it's impossible that the child in range [0, 15]
		// is a valueNode.
		if hn, ok := child.(hashNode); ok {
			children[i] = hn
			continue
		}
		// Commit the child recursively and store the "hashed" value.
		// Note the returned node can be some embedded nodes, so it's
		// possible the type is not hashNode.
		hashed, err := c.commit(append(path, byte(i)), child)
		if err != nil {
			return children, err
		}
		children[i] = hashed
	}
	// For the 17th child, it's possible the type is valuenode.
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children, nil
}

// store hashes the node n and if we have a storage layer specified, it writes
// the key/value pair to it and tracks any node->child references as well as any
// node->external trie references.
func (c *committer) store(path []byte, n node) node {
	// Larger nodes are replaced by their hash and stored in the database.
	var hash, _ = n.cache()

	// This was not generated - must be a small node stored in the parent.
	// In theory, we should check if the node is leaf here (embedded node
	// usually is leaf node). But small value(less than 32bytes) is not
	// our target(leaves in account trie only).
	if hash == nil {
		return n
	}
	// We have the hash already, estimate the RLP encoding-size of the node.
	// The size is used for mem tracking, does not need to be exact
	var (
		size  = estimateSize(n)
		key   = EncodeStorageKey(c.owner, path)
		nhash = common.BytesToHash(hash)
	)
	c.keys = append(c.keys, key)
	c.nodes = append(c.nodes, &memoryNode{
		hash: nhash,
		node: simplifyNode(n),
		size: uint16(size),
	})
	switch n := n.(type) {
	case *shortNode:
		// We don't check full node since in ethereum it's impossible
		// to store value in fullNode. The key length of leaves should
		// be exactly same.
		if val, ok := n.Val.(valueNode); ok {
			c.leaves = append(c.leaves, &leaf{
				val:        val,
				parent:     key,
				parentHash: nhash,
			})
		}
	}
	return hash
}

// estimateSize estimates the size of an rlp-encoded node, without actually
// rlp-encoding it (zero allocs). This method has been experimentally tried, and with a trie
// with 1000 leafs, the only errors above 1% are on small shortnodes, where this
// method overestimates by 2 or 3 bytes (e.g. 37 instead of 35)
func estimateSize(n node) int {
	switch n := n.(type) {
	case *shortNode:
		// A short node contains a compacted key, and a value.
		return 3 + len(n.Key) + estimateSize(n.Val)
	case *fullNode:
		// A full node contains up to 16 hashes (some nils), and a key
		s := 3
		for i := 0; i < 16; i++ {
			if child := n.Children[i]; child != nil {
				s += estimateSize(child)
			} else {
				s++
			}
		}
		return s
	case valueNode:
		return 1 + len(n)
	case hashNode:
		return 1 + len(n)
	default:
		panic(fmt.Sprintf("node type %T", n))
	}
}

// NodeSet contains all dirty nodes collected during the commit operation.
// It's not thread-safe to use.
type NodeSet struct {
	keys   [][]byte                     // the key list of updated nodes, sort by modification order
	dels   [][]byte                     // the key list of deleted nodes
	leaves []*leaf                      // the list of updated leaves
	nodes  map[string]*nodeWithPreValue // the map of all dirty nodes
}

// NewEmptyNodeSet initializes an empty dirty node set.
func NewEmptyNodeSet() *NodeSet {
	return &NodeSet{nodes: make(map[string]*nodeWithPreValue)}
}

// newNodeSet initializes the node set with the given information.
func newNodeSet(keys [][]byte, dels [][]byte, nodes map[string]*nodeWithPreValue, leaves []*leaf) *NodeSet {
	return &NodeSet{keys: keys, dels: dels, nodes: nodes, leaves: leaves}
}

// Len returns the number of dirty nodes contained in the set.
func (set *NodeSet) Len() int {
	return len(set.nodes)
}

// Merge merges the dirty nodes from the given set. Executing a block will
// generate several dirty node sets from different tries, and they need to
// be merged in order to keep the node order correctly.
// Note, we hold the assumption that there won't have two nodes with the same
// key, each node is unique in the whole 2-layer tries structure.
func (set *NodeSet) Merge(other *NodeSet) error {
	for key, node := range other.nodes {
		if _, present := set.nodes[key]; present {
			return errors.New("duplicated node")
		}
		set.nodes[key] = node
	}
	set.keys = append(set.keys, other.keys...)
	set.dels = append(set.dels, other.dels...)
	set.leaves = append(set.leaves, other.leaves...)
	return nil
}

// MergeDeletion adds a batch of deleted trie nodes into the node set.
// It's not safe to modify the vals after return.
// Note, we hold the assumption that there won't have two nodes with
// the same key, each node is unique in the whole 2-layer tries structure.
func (set *NodeSet) MergeDeletion(keys [][]byte, vals [][]byte) error {
	for i, key := range keys {
		if _, present := set.nodes[string(key)]; present {
			return errors.New("duplicated node")
		}
		set.nodes[string(key)] = &nodeWithPreValue{
			memoryNode: &memoryNode{
				node: nil,
				hash: common.Hash{},
				size: 0,
			},
			pre: vals[i], // panic if the length of keys and vals are not aligned.
		}
	}
	set.dels = append(set.dels, keys...)
	return nil
}

// ForEach iterates the dirty nodes contained and applies callback for them.
func (set *NodeSet) ForEach(callback func([]byte, []byte)) {
	for key, node := range set.nodes {
		callback([]byte(key), node.rlp())
	}
}
