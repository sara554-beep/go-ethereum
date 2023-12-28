// Copyright 2023 The go-ethereum Authors
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

package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// NewDatabase creates a state database with the provided data sources.
func NewDatabase(codedb *CodeDB, triedb *trie.Database, snaps *snapshot.Tree) Database {
	return &cachingDB{
		codedb: codedb,
		triedb: triedb,
		snaps:  snaps,
	}
}

// NewDatabaseForTesting is similar to NewDatabase, but it sets up the different
// data sources using the same provided database with default config for testing.
func NewDatabaseForTesting(db ethdb.Database) Database {
	return NewDatabase(NewCodeDB(db), trie.NewDatabase(db, nil), nil)
}

// cachingDB is the implementation of Database interface, designed for providing
// functionalities to read and write states.
type cachingDB struct {
	codedb *CodeDB
	triedb *trie.Database
	snaps  *snapshot.Tree
}

// Reader implements Database interface, returning a reader of the specific state.
func (db *cachingDB) Reader(stateRoot common.Hash) (Reader, error) {
	return newReader(stateRoot, db.triedb, nil)
}

// Hasher implements Database interface, returning a hasher of the specific state.
func (db *cachingDB) Hasher(stateRoot common.Hash) (Hasher, error) {
	if db.triedb.IsVerkle() {
		return newVerkleHasher(db.triedb, stateRoot, nil)
	}
	return newMerkleHasher(db.triedb, stateRoot, nil)
}

// ReadCode implements CodeReader, retrieving a particular contract's code.
func (db *cachingDB) ReadCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	return db.codedb.ReadCode(address, codeHash)
}

// ReadCodeSize implements CodeReader, retrieving a particular contracts
// code's size.
func (db *cachingDB) ReadCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return db.codedb.ReadCodeSize(addr, codeHash)
}

// WriteCodes implements CodeWriter, writing the provided a list of contract
// codes into database.
func (db *cachingDB) WriteCodes(addresses []common.Address, hashes []common.Hash, codes [][]byte) error {
	return db.codedb.WriteCodes(addresses, hashes, codes)
}

// TrieDB returns the associated trie database.
func (db *cachingDB) TrieDB() *trie.Database {
	return db.triedb
}

// Snapshot returns the associated state snapshot, it may be nil if not configured.
func (db *cachingDB) Snapshot() *snapshot.Tree {
	return db.snaps
}

// Commit accepts the state changes made by execution and applies it to database.
func (db *cachingDB) Commit(originRoot, root common.Hash, block uint64, trans *Transition) error {
	// Short circuit if the state is not changed at all.
	if originRoot == root {
		return nil
	}
	// Flush the cached dirty contract codes into key-value store first.
	var (
		blobs     [][]byte
		hashes    []common.Hash
		addresses []common.Address
	)
	for _, item := range trans.Codes {
		blobs = append(blobs, item.Blob)
		hashes = append(hashes, item.Hash)
		addresses = append(addresses, item.Address)
	}
	if err := db.codedb.WriteCodes(addresses, hashes, blobs); err != nil {
		return err
	}
	// If snapshotting is enabled, update the snapshot tree with this new version.
	if db.snaps != nil && db.snaps.Snapshot(originRoot) != nil {
		if err := db.snaps.Update(root, originRoot, trans.Destructs, trans.Accounts, trans.Storages); err != nil {
			log.Warn("Failed to update snapshot tree", "from", originRoot, "to", root, "err", err)
			return err
		}
		// Keep 128 diff layers in the memory, persistent layer is 129th.
		// - head layer is paired with HEAD state
		// - head-1 layer is paired with HEAD-1 state
		// - head-127 layer(bottom-most diff layer) is paired with HEAD-127 state
		if err := db.snaps.Cap(root, 128); err != nil {
			log.Warn("Failed to cap snapshot tree", "root", root, "layers", 128, "err", err)
			return err
		}
	}
	// Update the trie database with new version.
	set := triestate.New(trans.AccountsOrigin, trans.StoragesOrigin, trans.StorageIncomplete)
	return db.triedb.Update(root, originRoot, block, trans.Nodes, set)
}
