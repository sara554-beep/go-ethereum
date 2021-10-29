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
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// diskLayer is a low level persistent snapshot built on top of a key-value store.
type diskLayer struct {
	diskdb ethdb.KeyValueStore // Key-value store containing the base snapshot
	cache  *fastcache.Cache    // Cache to avoid hitting the disk for direct access
	root   common.Hash         // Root hash of the base snapshot

	stale     bool                   // Signals that the layer became stale (state progressed)
	frozen    map[string]*cachedNode // Uncommitted dirty node set, it's read only, lock free for accessing
	committed chan struct{}          // Channel used to send signal the frozen set is flushed
	lock      sync.RWMutex           // Lock used to prevent frozen set and stale flag
}

// newDiskLayer creates a new disk layer based on the passing arguments.
// It can either be called during the trie.Database initialization, or
// when flattening happens.
func newDiskLayer(root common.Hash, dirty map[string]*cachedNode, cache *fastcache.Cache, diskdb ethdb.KeyValueStore, writeLegacy bool) *diskLayer {
	dl := &diskLayer{
		diskdb:    diskdb,
		cache:     cache,
		root:      root,
		frozen:    dirty,
		committed: make(chan struct{}),
	}
	go dl.commit(writeLegacy)
	return dl
}

// Root returns root hash of corresponding state.
func (dl *diskLayer) Root() common.Hash {
	return dl.root
}

// Parent always returns nil as there's no layer below the disk.
func (dl *diskLayer) Parent() snapshot {
	return nil
}

// Stale return whether this layer has become stale (was flattened across) or if
// it's still live.
func (dl *diskLayer) Stale() bool {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.stale
}

// waitCommit blocks if the frozen set is still be committing
func (dl *diskLayer) waitCommit() {
	dl.lock.RLock()
	frozen := dl.frozen
	dl.lock.RUnlock()

	if frozen == nil {
		return
	}
	<-dl.committed
}

// Node retrieves the trie node associated with a particular key.
// The given key must be the internal format node key.
func (dl *diskLayer) Node(key []byte) (node, error) {
	blob, err := dl.NodeBlob(key)
	if err != nil {
		return nil, err
	}
	if len(blob) == 0 {
		return nil, nil
	}
	_, hash := DecodeInternalKey(key)
	return mustDecodeNode(hash[:], blob), nil
}

// NodeBlob retrieves the trie node blob associated with a particular key.
// The given key must be the internal format node key.
func (dl *diskLayer) NodeBlob(key []byte) ([]byte, error) {
	if dl.Stale() {
		return nil, ErrSnapshotStale
	}
	// If we're in the disk layer, all diff layers missed
	triedbDirtyMissMeter.Mark(1)

	// Try to retrieve the trie node from the uncommitted
	// node set.
	dl.lock.RLock()
	frozen := dl.frozen
	dl.lock.RUnlock()

	if frozen != nil {
		node, ok := frozen[string(key)]
		if ok {
			triedbFrozenHitMeter.Mark(1)
			if node == nil {
				return nil, nil
			}
			blob := node.rlp()
			triedbFrozenReadMeter.Mark(int64(len(blob)))
			return blob, nil
		}
	}
	// Try to retrieve the trie node from the memory cache
	if dl.cache != nil {
		if blob, found := dl.cache.HasGet(nil, key); found {
			triedbCleanHitMeter.Mark(1)
			triedbCleanReadMeter.Mark(int64(len(blob)))
			return blob, nil
		}
		triedbCleanMissMeter.Mark(1)
	}
	path, hash := DecodeInternalKey(key)
	blob, nodeHash := rawdb.ReadTrieNode(dl.diskdb, path)
	if len(blob) == 0 || nodeHash != hash {
		blob = rawdb.ReadArchiveTrieNode(dl.diskdb, hash)
		if len(blob) != 0 {
			triedbFallbackHitMeter.Mark(1)
			triedbFallbackReadMeter.Mark(int64(len(blob)))
		}
	}
	if dl.cache != nil {
		dl.cache.Set(key, blob)
		triedbCleanWriteMeter.Mark(int64(len(blob)))
	}
	if len(blob) > 0 {
		return blob, nil
	}
	return nil, nil
}

func (dl *diskLayer) Update(blockHash common.Hash, nodes map[string]*cachedNode) *diffLayer {
	return newDiffLayer(dl, blockHash, nodes)
}

// commit flushes all dirty nodes in the frozen set to database in atomic way and
// release the frozen set afterward.
func (dl *diskLayer) commit(writeLegacy bool) {
	dl.lock.RLock()
	frozen := dl.frozen
	dl.lock.RUnlock()

	if frozen == nil {
		return
	}
	defer func(start time.Time) {
		triedbCommitTimeTimer.Update(time.Since(start))
	}(time.Now())

	// Push all updated accounts into the database.
	// TODO all the nodes belong to the same layer should be written
	// in atomic way. However a huge disk write should be avoid in the
	// first place. A balance needs to be found to ensure that the bottom
	// most layer is large enough to combine duplicated writes, and also
	// the big write can be avoided.
	var (
		totalSize int64
		start     = time.Now()
		nodes     = len(frozen)
		batch     = dl.diskdb.NewBatch()
	)
	for key, node := range frozen {
		var (
			blob       []byte
			path, hash = DecodeInternalKey([]byte(key))
		)
		if node == nil {
			rawdb.DeleteTrieNode(batch, path)
			if dl.cache != nil {
				dl.cache.Set([]byte(key), nil)
			}
		} else {
			blob = node.rlp()
			rawdb.WriteTrieNode(batch, path, blob)
			if writeLegacy {
				rawdb.WriteArchiveTrieNode(batch, hash, blob)
			}
			if dl.cache != nil {
				dl.cache.Set([]byte(key), blob)
			}
		}
		totalSize += int64(len(blob) + len(key))
	}
	triedbCommitSizeMeter.Mark(totalSize)
	triedbCommitNodesMeter.Mark(int64(len(frozen)))

	// Flush all the updates in the single db operation. Ensure the
	// disk layer transition is atomic.
	if err := batch.Write(); err != nil {
		log.Crit("Failed to write bottom dirty trie nodes", "err", err)
	}
	// Nuke out the frozen handler for releasing memory and send the
	// signal as well.
	dl.lock.Lock()
	dl.frozen = nil
	close(dl.committed)
	dl.lock.Unlock()

	log.Info("Persisted uncommitted nodes", "nodes", nodes, "size", common.StorageSize(totalSize), "elapsed", common.PrettyDuration(time.Since(start)))
}
