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
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// diskLayerSnapshot is the snapshot of diskLayer.
type diskLayerSnapshot struct {
	root    common.Hash      // Immutable, root hash of the base snapshot
	diffid  uint64           // Immutable, corresponding reverse diff id
	datadir string           // The directory path of the ephemeral database
	diskdb  ethdb.Database   // Key-value store for storing temporary state changes, needs to be erased later
	snap    ethdb.Snapshot   // Key-value store snapshot created since the diskLayer snapshot is built
	clean   *fastcache.Cache // Clean node cache to avoid hitting the disk for direct access
	stale   bool             // Signals that the layer became stale (state progressed)
	lock    sync.RWMutex     // Lock used to protect stale flag
}

// GetSnapshot creates a disk layer snapshot and rewinds the snapshot
// to the specified state. In order to store the temporary mutations,
// the unique database namespace will be allocated for the snapshot,
// and it's expected to be released after the usage.
func (dl *diskLayer) GetSnapshot(root common.Hash, freezer *rawdb.Freezer) (*diskLayerSnapshot, error) {
	// Ensure the requested state is recoverable in the first place.
	id := rawdb.ReadReverseDiffLookup(dl.diskdb, convertEmpty(root))
	if id == nil {
		return nil, errStateUnrecoverable
	}
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	if dl.stale {
		return nil, errSnapshotStale
	}
	// Create a disk snapshot for read purposes. It's inherited from the live
	// database but has the isolation since now.
	sdb, err := dl.diskdb.NewSnapshot()
	if err != nil {
		return nil, err
	}
	// Create a fresh new database for write purposes. It's also isolated with
	// the live database. TODO(rjl493456442) it can take ~100ms to create a
	// database.
	datadir, err := os.MkdirTemp("", "")
	if err != nil {
		sdb.Release()
		return nil, err
	}
	ndb, err := rawdb.NewLevelDBDatabase(datadir, 16, 16, "", false)
	if err != nil {
		sdb.Release()
		return nil, err
	}
	// Flush all cached nodes in disk cache into ephemeral database,
	// since the requested state may point to a garbage-collected
	// version embedded in disk cache. Note it can take a few seconds.
	batch := ndb.NewBatchWithSize(int(dl.dirty.limit))
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
		return nil, err
	}
	// Construct the disk layer snapshot
	snap := &diskLayerSnapshot{
		root:    dl.root,
		diffid:  dl.diffid,
		datadir: datadir,
		diskdb:  ndb,
		snap:    sdb,
		clean:   fastcache.New(16 * 1024 * 1024), // tiny cache
	}
	// Apply the reverse diffs with the given order.
	for snap.diffid >= *id {
		diff, err := loadReverseDiff(freezer, snap.diffid)
		if err != nil {
			snap.Release()
			os.RemoveAll(datadir)
			return nil, err
		}
		snap, err = snap.revert(diff, snap.diffid)
		if err != nil {
			snap.Release()
			os.RemoveAll(datadir)
			return nil, err
		}
	}
	return snap, nil
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
	return snap.diffid
}

// MarkStale sets the stale flag as true.
func (snap *diskLayerSnapshot) MarkStale() {
	snap.lock.Lock()
	defer snap.lock.Unlock()

	if snap.stale == true {
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
		diffid:  bottom.diffid,
		datadir: snap.datadir,
		diskdb:  snap.diskdb,
		snap:    snap.snap,
		clean:   snap.clean,
	}, nil
}

// revert applies the given reverse diff by reverting the disk layer
// and return a newly constructed disk layer.
func (snap *diskLayerSnapshot) revert(diff *reverseDiff, diffid uint64) (*diskLayerSnapshot, error) {
	var (
		root  = snap.Root()
		batch = snap.diskdb.NewBatch()
	)
	if diff.Root != root {
		return nil, errUnmatchedReverseDiff
	}
	if diffid != snap.diffid {
		return nil, errUnmatchedReverseDiff
	}
	if snap.diffid == 0 {
		return nil, fmt.Errorf("%w: zero reverse diff id", errStateUnrecoverable)
	}
	// Mark the snapshot as stale before applying any mutations on top.
	snap.lock.Lock()
	defer snap.lock.Unlock()

	snap.stale = true

	diff.apply(batch)
	if err := batch.Write(); err != nil {
		log.Crit("Failed to write reverse diff", "err", err)
	}
	return &diskLayerSnapshot{
		root:    diff.Parent,
		diffid:  snap.diffid - 1,
		datadir: snap.datadir,
		diskdb:  snap.diskdb,
		snap:    snap.snap,
		clean:   snap.clean,
	}, nil
}

func (snap *diskLayerSnapshot) Release() {
	snap.snap.Release()        // release the disk database snapshot.
	snap.clean.Reset()         // release the held cache
	os.RemoveAll(snap.datadir) // nuke out the ephemeral database.
}
