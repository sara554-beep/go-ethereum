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
	"errors"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var (
	// defaultCacheSize is the default memory limitation of the disk cache
	// that aggregates the writes from above until it's flushed into the disk.
	// Do not increase the cache size arbitrarily, otherwise the system pause
	// time will increase when the database writes happen.
	defaultCacheSize = 64 * 1024 * 1024
)

// diskcache is a collection of dirty trie nodes to aggregate the disk
// write. It can act as an additional cache to avoid hitting disk too much.
// diskcache is not thread-safe, callers must manage concurrency issues
// by themselves.
type diskcache struct {
	layers uint64                 // The number of diff layers merged inside
	size   uint64                 // The approximate size of cached nodes
	limit  uint64                 // The maximum memory allowance in bytes for cache
	nodes  map[string]*memoryNode // The dirty node set, mapped by storage key
}

// newDiskcache initializes the dirty node cache with the given information.
func newDiskcache(limit int, nodes map[string]*memoryNode, layers uint64) *diskcache {
	// Don't panic for lazy callers.
	if nodes == nil {
		nodes = make(map[string]*memoryNode)
	}
	var size uint64
	for key, node := range nodes {
		size += uint64(node.memorySize(len(key)))
	}
	return &diskcache{layers: layers, nodes: nodes, size: size, limit: uint64(limit)}
}

// node retrieves the node with given storage key and hash.
func (cache *diskcache) node(storage []byte, hash common.Hash) (*memoryNode, error) {
	n, ok := cache.nodes[string(storage)]
	if !ok {
		return nil, nil
	}
	if n.hash != hash {
		owner, path := decodeStorageKey(storage)
		return nil, fmt.Errorf("%w %x!=%x(%x %v)", errUnexpectedNode, n.hash, hash, owner, path)
	}
	return n, nil
}

// commit merges the dirty node belonging to the bottom-most diff layer
// into the disk cache.
func (cache *diskcache) commit(nodes map[string]*memoryNode) *diskcache {
	var (
		delta          int64
		overwrites     int64
		overwriteSizes int64
	)
	for storage, node := range nodes {
		if orig, exist := cache.nodes[storage]; !exist {
			delta += int64(node.memorySize(len(storage)))
		} else {
			delta += int64(node.size) - int64(orig.size)
			overwrites += 1
			overwriteSizes += int64(orig.size)
		}
		cache.nodes[storage] = node
	}
	cache.updateSize(delta)
	cache.layers += 1
	triedbGCNodesMeter.Mark(overwrites)
	triedbGCSizeMeter.Mark(overwriteSizes)
	return cache
}

// revert applies the reverse diff to the disk cache.
func (cache *diskcache) revert(diff *reverseDiff) error {
	if cache.layers == 0 {
		return errStateUnrecoverable
	}
	cache.layers -= 1
	if cache.layers == 0 {
		cache.reset()
		return nil
	}
	var delta int64
	for _, state := range diff.States {
		cur, ok := cache.nodes[string(state.Key)]
		if !ok {
			owner, path := decodeStorageKey(state.Key)
			panic(fmt.Sprintf("non-existent node (%x %v)", owner, path))
		}
		if len(state.Val) == 0 {
			cache.nodes[string(state.Key)] = &memoryNode{
				node: nil,
				size: 0,
				hash: common.Hash{},
			}
			delta -= int64(cur.size)
		} else {
			cache.nodes[string(state.Key)] = &memoryNode{
				node: rawNode(state.Val),
				size: uint16(len(state.Val)),
				hash: crypto.Keccak256Hash(state.Val),
			}
			delta += int64(uint16(len(state.Val)) - cur.size)
		}
	}
	cache.updateSize(delta)
	return nil
}

// updateSize updates the total cache size by the given delta.
func (cache *diskcache) updateSize(delta int64) {
	size := int64(cache.size) + delta
	if size >= 0 {
		cache.size = uint64(size)
		return
	}
	log.Error("Negative disk cache size", "previous", common.StorageSize(cache.size), "diff", common.StorageSize(delta))
	cache.size = 0
}

// reset cleans up the disk cache.
func (cache *diskcache) reset() {
	cache.layers = 0
	cache.size = 0
	cache.nodes = make(map[string]*memoryNode)
}

// empty returns an indicator if diskcache contains any state transition inside.
func (cache *diskcache) empty() bool {
	return cache.layers == 0
}

// forEach iterates all the cached nodes and applies the given callback on them
func (cache *diskcache) forEach(callback func(key string, node *memoryNode)) {
	for storage, n := range cache.nodes {
		callback(storage, n)
	}
}

// setSize sets the cache size to the provided limit. Schedule flush operation
// if the current memory usage exceeds the new limit.
func (cache *diskcache) setSize(size int, db ethdb.KeyValueStore, clean *fastcache.Cache, diffid uint64) error {
	cache.limit = uint64(size)
	return cache.mayFlush(db, clean, diffid, false)
}

// mayFlush persists the in-memory dirty trie node into the disk if the predefined
// memory threshold is reached. Note, all data must be written to disk atomically.
func (cache *diskcache) mayFlush(db ethdb.KeyValueStore, clean *fastcache.Cache, diffid uint64, force bool) error {
	if cache.size <= cache.limit && !force {
		return nil
	}
	// Ensure the given reverse diff identifier is aligned
	// with the internal counter.
	head := rawdb.ReadReverseDiffHead(db)
	if head+cache.layers != diffid {
		return errors.New("invalid reverse diff id")
	}
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(int(cache.limit))
	)
	for storage, n := range cache.nodes {
		if n.node == nil {
			rawdb.DeleteTrieNode(batch, []byte(storage))
			continue
		}
		blob := n.rlp()
		rawdb.WriteTrieNode(batch, []byte(storage), blob)
		if clean != nil {
			clean.Set(n.hash.Bytes(), blob)
		}
	}
	rawdb.WriteReverseDiffHead(batch, diffid)
	if err := batch.Write(); err != nil {
		return err
	}
	triedbCommitSizeMeter.Mark(int64(batch.ValueSize()))
	triedbCommitNodesMeter.Mark(int64(len(cache.nodes)))
	triedbCommitTimeTimer.UpdateSince(start)

	log.Debug("Persisted uncommitted nodes",
		"nodes", len(cache.nodes),
		"size", common.StorageSize(batch.ValueSize()),
		"elapsed", common.PrettyDuration(time.Since(start)),
	)
	cache.reset()
	return nil
}
