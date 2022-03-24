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
	"runtime"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const (
	HashScheme = "hashScheme" // Identifier of hash based node scheme
	PathScheme = "pathScheme" // Identifier of path based node scheme
)

// NodeScheme describes the scheme for interacting nodes in disk.
type NodeScheme interface {
	// Name returns the identifier of node scheme.
	Name() string

	// HasTrieNode checks the trie node presence with the provided node key and
	// the associated node hash.
	HasTrieNode(db ethdb.KeyValueReader, key []byte, hash common.Hash) bool

	// ReadTrieNode retrieves the trie node from database with the provided node key
	// and the associated node hash.
	ReadTrieNode(db ethdb.KeyValueReader, key []byte, hash common.Hash) []byte

	// WriteTrieNode writes the trie node into database with the provided node key
	// and associated node hash.
	WriteTrieNode(db ethdb.KeyValueWriter, key []byte, hash common.Hash, node []byte)

	// DeleteTrieNode deletes the trie node from database with the provided node key
	// and associated node hash.
	DeleteTrieNode(db ethdb.KeyValueWriter, key []byte, hash common.Hash)

	// IsTrieNode returns an indicator if the given key is the key of trie node
	// according to the scheme.
	IsTrieNode(key []byte) (bool, []byte)
}

// Reader wraps the Node and NodeBlob method of a backing trie store.
type Reader interface {
	// Node retrieves the trie node associated with a particular trie node path
	// and the corresponding node hash. The returned node is in a wrapper through
	// which callers can obtain the RLP-format or canonical node representation
	// easily.
	// No error will be returned if the node is not found.
	Node(path []byte, hash common.Hash) (*memoryNode, error)

	// NodeBlob retrieves the RLP-encoded trie node blob associated with
	// a particular trie node path and the corresponding node hash.
	// No error will be returned if the node is not found.
	NodeBlob(path []byte, hash common.Hash) ([]byte, error)
}

// NodeReader warps all the necessary functions for accessing trie node.
type NodeReader interface {
	// GetReader returns a reader for accessing all trie nodes whose state root
	// is the specified root. Nil is returned in case the state is not available.
	GetReader(root common.Hash) Reader
}

// NodeWriter warps all the necessary functions for writing trie nodes into db.
type NodeWriter interface {
	// Update performs a state transition by committing dirty nodes contained
	// in the given set in order to update state from the specified parent to
	// the given root.
	Update(root common.Hash, parent common.Hash, nodes *NodeSet) error
}

// NodeDatabase wraps all the necessary functions for accessing and persisting
// nodes. It's implemented by Database and DatabaseSnapshot.
type NodeDatabase interface {
	NodeReader
	NodeWriter
}

// Config defines all necessary options for database.
type Config struct {
	Cache     int    // Memory allowance (MB) to use for caching trie nodes in memory
	Journal   string // Journal of clean cache to survive node restarts
	Preimages bool   // Flag whether the preimage of trie key is recorded
	ReadOnly  bool   // Flag whether the database is opened in read only mode.

	// Configs for experiment features
	Scheme     string // Disk scheme for reading/writing trie nodes, hash-based as default
	StateLimit uint64 // Number of recent blocks to maintain state history for
	DirtySize  int    // Maximum memory allowance (MB) for caching dirty nodes
}

// nodeBackend defines the methods needed to access/update trie nodes in
// different state scheme. It's implemented by hashDatabase and snapDatabase.
type nodeBackend interface {
	// GetReader returns a reader for accessing all trie nodes whose state root is
	// the specified root.
	GetReader(root common.Hash) Reader

	// Update performs a state transition from specified parent to root by committing
	// dirty nodes provided in the nodeset.
	Update(root common.Hash, parentRoot common.Hash, nodes *NodeSet) error

	// Commit writes all relevant trie nodes belonging to the specified state to disk.
	Commit(root common.Hash) error

	// Cap iteratively flushes old but still referenced trie nodes until the total
	// memory usage goes below the given threshold.
	Cap(limit common.StorageSize) error

	// IsEmpty returns an indicator if the node database is empty.
	IsEmpty() bool

	// Size returns the current storage size of the memory cache in front of the
	// persistent database layer.
	Size() common.StorageSize

	// Scheme returns the node scheme used in the database.
	Scheme() NodeScheme

	// Close closes the trie database and releases all held resources.
	Close() error
}

// Database is a multiple-layered structure for maintaining in-memory trie nodes.
// It consists of one persistent base layer backed by a key-value store, on top
// of which arbitrarily many in-memory diff layers are topped. The memory diffs
// can form a tree with branching, but the disk layer is singleton and common to
// all. If a reorg goes deeper than the disk layer, a batch of reverse diffs should
// be applied. The deepest reorg can be handled depends on the amount of reverse
// diffs tracked in the disk.
type Database struct {
	config   *Config          // Configuration for trie database.
	diskdb   ethdb.Database   // Persistent database to store the snapshot
	cleans   *fastcache.Cache // Megabytes permitted using for read caches
	preimage *preimageStore   // The store for caching preimages
	backend  nodeBackend      // The backend for managing trie nodes
}

// NewDatabase attempts to load an already existing snapshot from a persistent
// key-value store (with a number of memory layers from a journal). If the journal
// is not matched with the base persistent layer, all the recorded diff layers
// are discarded.
func NewDatabase(diskdb ethdb.Database, config *Config) *Database {
	var cleans *fastcache.Cache
	if config != nil && config.Cache > 0 {
		if config.Journal == "" {
			cleans = fastcache.New(config.Cache * 1024 * 1024)
		} else {
			cleans = fastcache.LoadFromFileOrNew(config.Journal, config.Cache*1024*1024)
		}
	}
	var preimage *preimageStore
	if config != nil && config.Preimages {
		preimage = newPreimageStore(diskdb)
	}
	db := &Database{
		config:   config,
		diskdb:   diskdb,
		cleans:   cleans,
		preimage: preimage,
	}
	if config != nil && config.Scheme == PathScheme {
		db.backend = openSnapDatabase(diskdb, cleans, config)
	} else {
		db.backend = openHashDatabase(diskdb, cleans)
	}
	return db
}

// GetReader returns a reader for accessing all trie nodes whose state root
// is the specified root. Nil is returned if the state is not existent.
func (db *Database) GetReader(blockRoot common.Hash) Reader {
	return db.backend.GetReader(blockRoot)
}

// Update performs a state transition by committing dirty nodes contained
// in the given set in order to update state from the specified parent to
// the specified root.
func (db *Database) Update(root common.Hash, parentRoot common.Hash, nodes *NodeSet) error {
	if db.preimage != nil {
		db.preimage.commit(false)
	}
	return db.backend.Update(root, parentRoot, nodes)
}

// Commit iterates over all the children of a particular node, writes them out
// to disk. As a side effect, all pre-images accumulated up to this point are
// also written.
func (db *Database) Commit(root common.Hash) error {
	if db.preimage != nil {
		db.preimage.commit(true)
	}
	return db.backend.Commit(root)
}

// Cap iteratively flushes old but still referenced trie nodes until the total
// memory usage goes below the given threshold.
func (db *Database) Cap(limit common.StorageSize) error {
	if db.preimage != nil {
		db.preimage.commit(false)
	}
	return db.backend.Cap(limit)
}

// Size returns the current storage size of the memory cache in front of the
// persistent database layer.
func (db *Database) Size() common.StorageSize {
	return db.backend.Size()
}

// IsEmpty returns an indicator if the node database is empty.
func (db *Database) IsEmpty() bool {
	return db.backend.IsEmpty()
}

// Scheme returns the node scheme used in the database.
func (db *Database) Scheme() NodeScheme {
	return db.backend.Scheme()
}

// Close closes the trie database and releases all held resources.
func (db *Database) Close() error {
	return db.backend.Close()
}

// Recover rollbacks the database to a specified historical point. The state is
// supported as the rollback destination only if it's canonical state and the
// corresponding reverse diffs are existent. It's only supported by snap database
// and it's noop for hash database.
func (db *Database) Recover(target common.Hash) error {
	if snapDB, ok := db.backend.(*snapDatabase); ok {
		return snapDB.Recover(target)
	}
	return nil // not supported
}

// Recoverable returns the indicator if the specified state is enabled to be
// recovered. It's only supported by snap database.
func (db *Database) Recoverable(root common.Hash) bool {
	if snapDB, ok := db.backend.(*snapDatabase); ok {
		return snapDB.Recoverable(root)
	}
	return false // not supported
}

// Reset wipes all available journal from the persistent database and discard
// all caches and diff layers. Using the given root to create a new disk layer.
// It's only supported by snap database and it's noop for hash database.
func (db *Database) Reset(root common.Hash) error {
	if snapDB, ok := db.backend.(*snapDatabase); ok {
		return snapDB.Reset(root)
	}
	return nil // not supported
}

// Journal commits an entire diff hierarchy to disk into a single journal entry.
// This is meant to be used during shutdown to persist the snapshot without
// flattening everything down (bad for reorgs).
func (db *Database) Journal(root common.Hash) error {
	if db.preimage != nil {
		db.preimage.commit(true)
	}
	if snapDB, ok := db.backend.(*snapDatabase); ok {
		return snapDB.Journal(root)
	}
	return nil // not supported
}

// SetCacheSize sets the dirty cache size to the provided value(in mega-bytes).
func (db *Database) SetCacheSize(size int) error {
	if snapDB, ok := db.backend.(*snapDatabase); ok {
		return snapDB.SetCacheSize(size)
	}
	return nil // not supported
}

// Reference adds a new reference from a parent node to a child node.
// This function is used to add reference between internal trie node
// and external node(e.g. storage trie root), all internal trie nodes
// are referenced together by database itself. It's only supported by
// hash-based database and it's a noop for others.
func (db *Database) Reference(root common.Hash) {
	if hashDB, ok := db.backend.(*hashDatabase); ok {
		hashDB.Reference(root)
	}
}

// Dereference removes an existing reference from a root node. It's only
// supported by hash-based database and it's a noop for others.
func (db *Database) Dereference(root common.Hash) {
	if hashDB, ok := db.backend.(*hashDatabase); ok {
		hashDB.Dereference(root)
	}
}

// DiskDB retrieves the persistent storage backing the trie database.
func (db *Database) DiskDB() ethdb.Database {
	return db.diskdb
}

// saveCache saves clean state cache to given directory path
// using specified CPU cores.
func (db *Database) saveCache(dir string, threads int) error {
	if db.cleans == nil {
		return nil
	}
	log.Info("Writing clean trie cache to disk", "path", dir, "threads", threads)

	start := time.Now()
	err := db.cleans.SaveToFileConcurrent(dir, threads)
	if err != nil {
		log.Error("Failed to persist clean trie cache", "error", err)
		return err
	}
	log.Info("Persisted the clean trie cache", "path", dir, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// SaveCache atomically saves fast cache data to the given dir using all
// available CPU cores.
func (db *Database) SaveCache(dir string) error {
	return db.saveCache(dir, runtime.GOMAXPROCS(0))
}

// SaveCachePeriodically atomically saves fast cache data to the given dir with
// the specified interval. All dump operation will only use a single CPU core.
func (db *Database) SaveCachePeriodically(dir string, interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			db.saveCache(dir, 1)
		case <-stopCh:
			return
		}
	}
}
