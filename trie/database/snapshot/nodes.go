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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package snapshot

import "github.com/ethereum/go-ethereum/common"

type memoryNode struct {
	blob []byte      // RLP-encoded node value, nil means it's deleted
	hash common.Hash // Node hash, computed by hashing rlp value, empty for deleted nodes
}

// memorySize returns the total memory size used by this node.
func (n *memoryNode) memorySize(pathlen int) int {
	return len(n.blob) + pathlen + common.HashLength
}

// isDeleted returns the indicator if the node is marked as deleted.
func (n *memoryNode) isDeleted() bool {
	return n.hash == (common.Hash{})
}

// nodeWithPrev wraps the trie node with the previous node value.
type nodeWithPrev struct {
	*memoryNode
	prev []byte // RLP-encoded previous value, nil means it's non-existent
}

// memorySize returns the total memory size used by this node.
func (n *nodeWithPrev) memorySize(pathlen int) int {
	return n.memoryNode.memorySize(pathlen) + len(n.prev)
}

// unwrap returns the internal memoryNode object.
func (n *nodeWithPrev) unwrap() *memoryNode {
	return n.memoryNode
}
