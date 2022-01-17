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
	// dirtyMemoryLimit is the maximum size of the dirty cache that aggregates
	// the writes from above until it's flushed into the disk.
	dirtyMemoryLimit = uint64(256 * 1024 * 1024)
)

// dirtyCache is a collection of uncommitted dirty nodes to aggregate the disk
// write. It can also act as an additional cache to avoid hitting disk too much.
type dirtyCache struct {
	nodes map[string]*cachedNode // Uncommitted dirty nodes, indexed by storage key
	size  uint64                 // The approximate size of cached nodes
}

// newDirtyCache initializes the dirty node cache with the given node set.
func newDirtyCache(nodes map[string]*cachedNode) *dirtyCache {
	if nodes == nil {
		nodes = make(map[string]*cachedNode)
	}
	var size uint64
	for key, node := range nodes {
		size += uint64(len(key) + int(node.size) + cachedNodeSize)
	}
	return &dirtyCache{nodes: nodes, size: size}
}

// node retrieves the node with given storage key and hash.
func (cache *dirtyCache) node(storage []byte, hash common.Hash) (node, error) {
	n, ok := cache.nodes[string(storage)]
	if ok {
		if n.node == nil || n.hash != hash {
			owner, path := DecodeStorageKey(storage)
			return nil, fmt.Errorf("%w %x(%x %v)", errUnexpectedNode, hash, owner, path)
		}
		triedbDirtyHitMeter.Mark(1)
		triedbDirtyNodeHitDepthHist.Update(int64(128))
		triedbDirtyReadMeter.Mark(int64(n.size))
		return n.obj(hash), nil
	}
	return nil, nil
}

// nodeBlob retrieves the node blob with given storage key and hash.
func (cache *dirtyCache) nodeBlob(storage []byte, hash common.Hash) ([]byte, error) {
	n, ok := cache.nodes[string(storage)]
	if ok {
		if n.node == nil || n.hash != hash {
			owner, path := DecodeStorageKey(storage)
			return nil, fmt.Errorf("%w %x(%x %v)", errUnexpectedNode, hash, owner, path)
		}
		triedbDirtyHitMeter.Mark(1)
		triedbDirtyNodeHitDepthHist.Update(int64(128))
		triedbDirtyReadMeter.Mark(int64(n.size))
		return n.rlp(), nil
	}
	return nil, nil
}

// update merges the given nodes into the cache. This function should never be called
// simultaneously with other map accessors.
func (cache *dirtyCache) update(nodes map[string]*cachedNode) *dirtyCache {
	var diff int64
	for storage, n := range nodes {
		if prev, exist := cache.nodes[storage]; exist {
			diff += int64(n.size) - int64(prev.size)
		} else {
			diff += int64(int(n.size) + len(storage) + cachedNodeSize)
		}
		cache.nodes[storage] = n
	}
	if final := int64(cache.size) + diff; final < 0 {
		log.Error("Negative dirty cache size", "previous", cache.size, "diff", diff)
		cache.size = 0
	} else {
		cache.size = uint64(final)
	}
	return cache
}

// revert applies the reverse diff to the local dirty node set. This function
// should never be called simultaneously with other map accessors.
func (cache *dirtyCache) revert(diff *reverseDiff) error {
	for _, state := range diff.States {
		_, ok := cache.nodes[string(state.Key)]
		if !ok {
			owner, path := DecodeStorageKey(state.Key)
			return fmt.Errorf("non-existent node (%x %v)", owner, path)
		}
		if len(state.Val) == 0 {
			cache.nodes[string(state.Key)] = &cachedNode{
				node: nil,
				size: 0,
				hash: common.Hash{},
			}
		} else {
			cache.nodes[string(state.Key)] = &cachedNode{
				node: rawNode(state.Val),
				size: uint16(len(state.Val)),
				hash: crypto.Keccak256Hash(state.Val),
			}
		}
	}
	return nil
}

// flush persists the in-memory dirty trie node into the disk if the predefined
// memory threshold is reached. Depends on the given config, the additional legacy
// format node can be written as well (e.g. for archive node). Note, all data must
// be written to disk atomically.
// This function should never be called simultaneously with other map accessors.
func (cache *dirtyCache) flush(db ethdb.KeyValueStore, clean *fastcache.Cache, diffid uint64, force bool) error {
	if cache.size <= dirtyMemoryLimit && !force {
		return nil
	}
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(int(dirtyMemoryLimit))
	)
	for storage, n := range cache.nodes {
		if n.node == nil {
			rawdb.DeleteTrieNode(batch, []byte(storage))
			continue
		}
		blob := n.rlp()
		rawdb.WriteTrieNode(batch, []byte(storage), blob)
		if clean != nil {
			clean.Set(EncodeInternalKey([]byte(storage), n.hash), blob)
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
	cache.nodes, cache.size = make(map[string]*cachedNode), 0
	return nil
}
