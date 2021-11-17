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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// diffLayer represents a collection of modifications made to the in-memory tries
// after running a block on top.
//
// The goal of a diff layer is to act as a journal, tracking recent modifications
// made to the state, that have not yet graduated into a semi-immutable state.
type diffLayer struct {
	parent snapshot // Parent snapshot modified by this one, never nil
	memory uint64   // Approximate guess as to how much memory we use

	root  common.Hash            // Root hash to which this snapshot diff belongs to
	rid   uint64                 // Corresponding reverse diff id
	stale uint32                 // Signals that the layer became stale (state progressed)
	nodes map[string]*cachedNode // Keyed trie nodes for retrieval, indexed by storage key
	lock  sync.RWMutex           // Lock used to protect parent and stale fields.
}

// newDiffLayer creates a new diff on top of an existing snapshot, whether that's a low
// level persistent database or a hierarchical diff already.
func newDiffLayer(parent snapshot, root common.Hash, rid uint64, nodes map[string]*cachedNode) *diffLayer {
	dl := &diffLayer{
		parent: parent,
		root:   root,
		rid:    rid,
		nodes:  nodes,
	}
	for key, node := range nodes {
		dl.memory += uint64(len(key) + int(node.size) + cachedNodeSize)
		triedbDirtyWriteMeter.Mark(int64(node.size))
	}
	triedbDiffLayerSizeMeter.Mark(int64(dl.memory))
	triedbDiffLayerNodesMeter.Mark(int64(len(nodes)))
	log.Debug("Created new diff layer", "nodes", len(nodes), "size", common.StorageSize(dl.memory))
	return dl
}

// Root returns the root hash of corresponding state.
func (dl *diffLayer) Root() common.Hash {
	return dl.root
}

// Parent returns the subsequent layer of a diff layer.
func (dl *diffLayer) Parent() snapshot {
	return dl.parent
}

// Stale return whether this layer has become stale (was flattened across) or if
// it's still live.
func (dl *diffLayer) Stale() bool {
	return atomic.LoadUint32(&dl.stale) != 0
}

// ID returns the id of associated reverse diff.
func (dl *diffLayer) ID() uint64 {
	return dl.rid
}

// Node retrieves the trie node associated with a particular key.
func (dl *diffLayer) Node(storage []byte, hash common.Hash) (node, error) {
	return dl.node(storage, hash, 0)
}

// node is the inner version of Node which counts the accessed layer depth.
func (dl *diffLayer) node(storage []byte, hash common.Hash, depth int) (node, error) {
	// If the layer was flattened into, consider it invalid (any live reference to
	// the original should be marked as unusable).
	if dl.Stale() {
		return nil, ErrSnapshotStale
	}
	// If the trie node is known locally, return it
	if n, ok := dl.nodes[string(storage)]; ok && n.hash == hash {
		triedbDirtyHitMeter.Mark(1)
		triedbDirtyNodeHitDepthHist.Update(int64(depth))
		triedbDirtyReadMeter.Mark(int64(n.size))

		// The trie node is marked as deleted, don't bother parent anymore.
		if n.node == nil {
			return nil, nil
		}
		return n.obj(hash), nil
	}
	// Trie node unknown to this diff, resolve from parent
	if diff, ok := dl.parent.(*diffLayer); ok {
		return diff.node(storage, hash, depth+1)
	}
	return dl.parent.Node(storage, hash)
}

// NodeBlob retrieves the trie node blob associated with a particular key.
func (dl *diffLayer) NodeBlob(storage []byte, hash common.Hash) ([]byte, error) {
	return dl.nodeBlob(storage, hash, 0)
}

// nodeBlob is the inner version of NodeBlob which counts the accessed layer depth.
func (dl *diffLayer) nodeBlob(storage []byte, hash common.Hash, depth int) ([]byte, error) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// If the layer was flattened into, consider it invalid (any live reference to
	// the original should be marked as unusable).
	if dl.Stale() {
		return nil, ErrSnapshotStale
	}
	// If the trie node is known locally, return it
	if n, ok := dl.nodes[string(storage)]; ok && n.hash == hash {
		triedbDirtyHitMeter.Mark(1)
		triedbDirtyNodeHitDepthHist.Update(int64(depth))
		triedbDirtyReadMeter.Mark(int64(n.size))

		// The trie node is marked as deleted, don't bother parent anymore.
		if n.node == nil {
			return nil, nil
		}
		return n.rlp(), nil
	}
	// Trie node unknown to this diff, resolve from parent
	if diff, ok := dl.parent.(*diffLayer); ok {
		return diff.nodeBlob(storage, hash, depth+1)
	}
	return dl.parent.NodeBlob(storage, hash)
}

// Update creates a new layer on top of the existing snapshot diff tree with
// the specified data items.
func (dl *diffLayer) Update(blockRoot common.Hash, id uint64, nodes map[string]*cachedNode) *diffLayer {
	return newDiffLayer(dl, blockRoot, id, nodes)
}

// persist persists the diff layer and all its parent diff layers to disk.
// The order should be strictly from bottom to top.
func (dl *diffLayer) persist(config *Config, newDiff chan uint64) snapshot {
	parent, ok := dl.parent.(*diffLayer)
	if ok {
		// Hold the lock to prevent any read operations until the new
		// parent is linked correctly.
		dl.lock.Lock()
		dl.parent = parent.persist(config, newDiff)
		dl.lock.Unlock()
	}
	return diffToDisk(dl, config, newDiff)
}
