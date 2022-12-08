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
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// diskLayerSnapshot is the snapshot of diskLayer.
type diskLayerSnapshot struct {
	root    common.Hash      // Immutable, root hash of the base snapshot
	id      uint64           // Immutable, corresponding state id
	datadir string           // The directory path of the ephemeral database
	diskdb  ethdb.Database   // Key-value store for storing temporary state changes, needs to be erased later
	snap    ethdb.Snapshot   // Key-value store snapshot created since the diskLayer snapshot is built
	clean   *fastcache.Cache // Clean node cache to avoid hitting the disk for direct access
	stale   bool             // Signals that the layer became stale (state progressed)
	lock    sync.RWMutex     // Lock used to protect stale flag
}

func (dl *diskLayer) getSnapshot() (ethdb.Snapshot, ethdb.Database, string, error) {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, nil, "", errSnapshotStale
	}
	// Create a disk snapshot for read purposes. It's inherited from the live
	// database but has the isolation property since now. The snapshot must be
	// released afterwards otherwise it will block compaction.
	snap, err := dl.db.diskdb.NewSnapshot()
	if err != nil {
		return nil, nil, "", err
	}
	// Create a fresh new database for write purposes.
	datadir, err := os.MkdirTemp("", "")
	if err != nil {
		snap.Release()
		return nil, nil, "", err
	}
	db, err := rawdb.NewLevelDBDatabase(datadir, 16, 16, "", false)
	if err != nil {
		snap.Release()
		os.RemoveAll(datadir)
		return nil, nil, "", err
	}
	// Flush all cached nodes in disk cache into ephemeral database,
	// since the requested state may point to a garbage-collected
	// version embedded in disk cache. Note it can take a few seconds.
	batch := db.NewBatchWithSize(int(dl.dirty.limit))
	dl.dirty.forEach(func(owner common.Hash, path []byte, node *memoryNode) {
		// Deletions are ignored, it doesn't make any sense to
		// apply deletions against a fresh-new database.
		if node.isDeleted() {
			return
		}
		if owner == (common.Hash{}) {
			rawdb.WriteAccountTrieNode(batch, path, node.rlp())
		} else {
			rawdb.WriteStorageTrieNode(batch, owner, path, node.rlp())
		}
	})
	if err := batch.Write(); err != nil {
		snap.Release()
		os.RemoveAll(datadir)
		return nil, nil, "", err
	}
	return snap, db, datadir, nil
}

// GetSnapshot creates a disk layer snapshot and rewinds the snapshot
// to the specified state. In order to store the temporary mutations,
// the unique database namespace will be allocated for the snapshot,
// and it's expected to be released after the usage.
func (dl *diskLayer) GetSnapshot(root common.Hash) (*diskLayerSnapshot, error) {
	// Ensure the requested state is recoverable.
	root = convertEmpty(root)
	if !dl.db.Recoverable(root) {
		return nil, errStateUnrecoverable
	}
	if dl.db.freezer == nil {
		return nil, errStateUnrecoverable
	}
	// Obtain the live database snapshot and construct an ephemeral
	// database for both read/write purposes. The nodes in cache will
	// be flushed into newly created db if requested.
	snap, db, datadir, err := dl.getSnapshot()
	if err != nil {
		return nil, err
	}
	layer := &diskLayerSnapshot{
		root:    dl.root,
		id:      dl.id,
		datadir: datadir,
		diskdb:  db,
		snap:    snap,
		clean:   fastcache.New(16 * 1024 * 1024), // tiny cache
	}
	// Apply the reverse diffs with the given order.
	var (
		logged = time.Now()
		start  = time.Now()
		batch  = layer.diskdb.NewBatch()
	)
	for {
		h, err := loadTrieHistory(dl.db.freezer, layer.id)
		if err != nil {
			layer.Release()
			return nil, err
		}
		layer, err = layer.revert(batch, h)
		if err != nil {
			layer.Release()
			return nil, err
		}
		if time.Since(logged) > 8*time.Second {
			logged = time.Now()
			log.Info("Preparing database snapshot", "target", root, "elapsed", common.PrettyDuration(time.Since(start)))
		}
		if batch.ValueSize() > ethdb.IdealBatchSize || layer.Root() == root {
			if err := batch.Write(); err != nil {
				layer.Release()
				return nil, err
			}
			batch.Reset()
		}
		if layer.Root() == root {
			break
		}
	}
	log.Info("Prepared database snapshot", "target", root, "elapsed", common.PrettyDuration(time.Since(start)))
	return layer, nil
}

// Root returns root hash of corresponding state.
func (snap *diskLayerSnapshot) Root() common.Hash {
	return snap.root
}

// Parent always returns nil as there's no layer below the disk.
func (snap *diskLayerSnapshot) Parent() snapshot {
	return nil
}

// Stale return whether this layer has become stale (was flattened across) or if
// it's still live.
func (snap *diskLayerSnapshot) Stale() bool {
	snap.lock.RLock()
	defer snap.lock.RUnlock()

	return snap.stale
}

// ID returns the id of associated reverse diff.
func (snap *diskLayerSnapshot) ID() uint64 {
	return snap.id
}

// MarkStale sets the stale flag as true.
func (snap *diskLayerSnapshot) MarkStale() {
	snap.lock.Lock()
	defer snap.lock.Unlock()

	if snap.stale {
		panic("triedb disk layer is stale") // we've committed into the same base from two children, boom
	}
	snap.stale = true
}

// node retrieves the node with provided storage key and node hash. The returned
// node is in a wrapper through which callers can obtain the RLP-format or canonical
// node representation easily. No error will be returned if node is not found.
func (snap *diskLayerSnapshot) node(owner common.Hash, path []byte, hash common.Hash, depth int) (*memoryNode, error) {
	snap.lock.RLock()
	defer snap.lock.RUnlock()

	if snap.stale {
		return nil, errSnapshotStale
	}
	// Try to retrieve the trie node from the clean cache
	if blob := snap.clean.Get(nil, hash.Bytes()); len(blob) > 0 {
		return &memoryNode{node: rawNode(blob), hash: hash, size: uint16(len(blob))}, nil
	}
	// Firstly try to retrieve the trie node from the ephemeral
	// disk area or fallback to the live disk state if it's not
	// existent.
	var (
		nBlob   []byte
		nHash   common.Hash
		accTrie = owner == (common.Hash{})
	)
	if accTrie {
		nBlob, nHash = rawdb.ReadAccountTrieNode(snap.diskdb, path)
		if len(nBlob) == 0 {
			nBlob, nHash = rawdb.ReadAccountTrieNode(snap.snap, path)
		}
	} else {
		nBlob, nHash = rawdb.ReadStorageTrieNode(snap.diskdb, owner, path)
		if len(nBlob) == 0 {
			nBlob, nHash = rawdb.ReadStorageTrieNode(snap.snap, owner, path)
		}
	}
	if nHash != hash {
		return nil, fmt.Errorf("%w %x!=%x(%x %v)", errUnexpectedNode, nHash, hash, owner, path)
	}
	if len(nBlob) > 0 {
		snap.clean.Set(hash.Bytes(), nBlob)
	}
	return &memoryNode{node: rawNode(nBlob), hash: hash, size: uint16(len(nBlob))}, nil
}

// Node retrieves the trie node with the provided trie identifier, node path
// and the corresponding node hash. No error will be returned if the node is
// not found.
func (snap *diskLayerSnapshot) Node(owner common.Hash, path []byte, hash common.Hash) (node, error) {
	n, err := snap.node(owner, path, hash, 0)
	if err != nil || n == nil {
		return nil, err
	}
	return n.obj(), nil
}

// NodeBlob retrieves the RLP-encoded trie node blob with the provided trie
// identifier, node path and the corresponding node hash. No error will be
// returned if the node is not found.
func (snap *diskLayerSnapshot) NodeBlob(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	n, err := snap.node(owner, path, hash, 0)
	if err != nil || n == nil {
		return nil, err
	}
	return n.rlp(), nil
}

// Update returns a new diff layer on top with the given dirty node set.
func (snap *diskLayerSnapshot) Update(blockHash common.Hash, id uint64, nodes map[common.Hash]map[string]*nodeWithPrev) *diffLayer {
	return newDiffLayer(snap, blockHash, id, nodes)
}

// Journal it's not supported by diskLayer snapshot.
func (snap *diskLayerSnapshot) Journal(buffer *bytes.Buffer) error {
	return errors.New("not implemented")
}

// commit flushes the dirty nodes in bottom-most diff layer into
// disk. The nodes will be stored in an ephemeral disk area and
// will be erased once the snapshot itself is released.
func (snap *diskLayerSnapshot) commit(bottom *diffLayer) (*diskLayerSnapshot, error) {
	snap.lock.Lock()
	defer snap.lock.Unlock()

	// Mark the snapshot as stale before applying any mutations on top.
	snap.stale = true

	// Commit the dirty nodes in the diff layer.
	batch := snap.diskdb.NewBatch()
	for owner, subset := range bottom.nodes {
		accTrie := owner == (common.Hash{})
		for path, n := range subset {
			if n.isDeleted() {
				if accTrie {
					rawdb.DeleteAccountTrieNode(batch, []byte(path))
				} else {
					rawdb.DeleteStorageTrieNode(batch, owner, []byte(path))
				}
			} else {
				blob := n.rlp()
				if accTrie {
					rawdb.WriteAccountTrieNode(batch, []byte(path), blob)
				} else {
					rawdb.WriteStorageTrieNode(batch, owner, []byte(path), blob)
				}
				snap.clean.Set(n.hash.Bytes(), blob)
			}
		}
	}
	if err := batch.Write(); err != nil {
		return nil, err
	}
	return &diskLayerSnapshot{
		root:    bottom.root,
		id:      bottom.id,
		datadir: snap.datadir,
		diskdb:  snap.diskdb,
		snap:    snap.snap,
		clean:   snap.clean,
	}, nil
}

// revert applies the given reverse diff by reverting the disk layer
// and return a newly constructed disk layer.
func (snap *diskLayerSnapshot) revert(batch ethdb.Batch, diff *trieHistory) (*diskLayerSnapshot, error) {
	if diff.Root != snap.Root() {
		return nil, errUnexpectedTrieHistory
	}
	if snap.id == 0 {
		return nil, fmt.Errorf("%w: zero reverse diff id", errStateUnrecoverable)
	}
	// Mark the snapshot as stale before applying any mutations on top.
	snap.lock.Lock()
	defer snap.lock.Unlock()

	snap.stale = true
	diff.apply(batch)

	return &diskLayerSnapshot{
		root:    diff.Parent,
		id:      snap.id - 1,
		datadir: snap.datadir,
		diskdb:  snap.diskdb,
		snap:    snap.snap,
		clean:   snap.clean,
	}, nil
}

func (snap *diskLayerSnapshot) Release() {
	snap.snap.Release()        // release the read-only disk database snapshot.
	snap.clean.Reset()         // release the allocated memory space
	os.RemoveAll(snap.datadir) // nuke out the ephemeral key-value database
}
