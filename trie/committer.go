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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// leaf represents a trie leaf node
type leaf struct {
	blob   []byte      // raw blob of leaf
	parent common.Hash // the hash of parent node
}

// committer is the tool used for the trie Commit operation. The committer will
// capture all dirty nodes during the commit process and keep them cached in
// insertion order.
type committer struct {
	nodes       *NodeSet
	collectLeaf bool
}

// newCommitter creates a new committer or picks one from the pool.
func newCommitter(nodeset *NodeSet, collectLeaf bool) *committer {
	return &committer{
		nodes:       nodeset,
		collectLeaf: collectLeaf,
	}
}

// Commit collapses a node down into a hash node and returns it along with
// the modified nodeset.
func (c *committer) Commit(n node) hashNode {
	node, _ := c.commit(nil, n)
	return node.(hashNode)
}

// commit collapses a node down into a hash node and returns it.
func (c *committer) commit(path []byte, n node) (node, bool) {
	// if this path is clean, use available cached data
	hash, dirty := n.cache()
	if hash != nil && !dirty {
		return hash, true
	}
	// Commit children, then parent, and remove the dirty flag.
	switch cn := n.(type) {
	case *shortNode:
		// Commit child
		collapsed := cn.copy()

		// If the child is fullNode, recursively commit,
		// otherwise it can only be hashNode or valueNode.
		var children []common.Hash
		switch nn := cn.Val.(type) {
		case *fullNode:
			child, hashed := c.commit(append(path, cn.Key...), cn.Val)
			if hashed {
				children = append(children, common.BytesToHash(child.(hashNode)))
			}
			collapsed.Val = child
		case hashNode:
			children = append(children, common.BytesToHash(nn))
		}
		// The key needs to be copied, since we're adding it to the
		// modified nodeset.
		collapsed.Key = hexToCompact(cn.Key)
		hashedNode := c.store(path, collapsed, children)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn, true
		}
		return collapsed, false
	case *fullNode:
		hashedKids, children := c.commitChildren(path, cn)
		collapsed := cn.copy()
		collapsed.Children = hashedKids

		hashedNode := c.store(path, collapsed, children)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn, true
		}
		return collapsed, false
	case hashNode:
		return cn, true
	default:
		// nil, valuenode shouldn't be committed
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// commitChildren commits the children of the given fullnode
func (c *committer) commitChildren(path []byte, n *fullNode) ([17]node, []common.Hash) {
	var (
		children [17]node
		hashes   []common.Hash
	)
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
			hashes = append(hashes, common.BytesToHash(hn))
			continue
		}
		// Commit the child recursively and store the "hashed" value.
		// Note the returned node can be some embedded nodes, so it's
		// possible the type is not hashNode.
		child, hashed := c.commit(append(path, byte(i)), child)
		if hashed {
			hashes = append(hashes, common.BytesToHash(child.(hashNode)))
		}
		children[i] = child
	}
	// For the 17th child, it's possible the type is valuenode.
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children, hashes
}

// store hashes the node n and adds it to the modified nodeset. If leaf collection
// is enabled, leaf nodes will be tracked in the modified nodeset as well.
func (c *committer) store(path []byte, n node, children []common.Hash) node {
	// Larger nodes are replaced by their hash and stored in the database.
	var hash, _ = n.cache()

	// This was not generated - must be a small node stored in the parent.
	// In theory, we should check if the node is leaf here (embedded node
	// usually is leaf node). But small value (less than 32bytes) is not
	// our target (leaves in account trie only).
	if hash == nil {
		// The node is embedded in its parent, in other words, this node
		// will not be stored in the database independently, mark it as
		// deleted only if the node was existent in database before.
		if _, ok := c.nodes.accessList[string(path)]; ok {
			c.nodes.markDeleted(path)
		}
		return n
	}
	// We have the hash already, estimate the RLP encoding-size of the node.
	// The size is used for mem tracking, does not need to be exact
	var (
		nhash = common.BytesToHash(hash)
		mnode = &memoryNode{
			hash:     nhash,
			node:     nodeToBytes(n),
			children: children,
		}
	)
	// Collect the dirty node to nodeset for return.
	c.nodes.markUpdated(path, mnode)

	// Collect the corresponding leaf node if it's required. We don't check
	// full node since it's impossible to store value in fullNode. The key
	// length of leaves should be exactly same.
	if c.collectLeaf {
		if sn, ok := n.(*shortNode); ok {
			if val, ok := sn.Val.(valueNode); ok {
				c.nodes.addLeaf(&leaf{blob: val, parent: nhash})
			}
		}
	}
	return hash
}
