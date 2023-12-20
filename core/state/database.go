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
	"errors"
	"fmt"

	"github.com/crate-crypto/go-ipa/banderwagon"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/ethereum/go-ethereum/trie/utils"
)

const (
	// commitmentSize is the size of commitment stored in cache.
	commitmentSize = banderwagon.UncompressedSize

	// Cache item granted for caching commitment results.
	commitmentCacheItems = 64 * 1024 * 1024 / (commitmentSize + common.AddressLength)
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
	return newReader(stateRoot, db.triedb, db.snaps)
}

// OpenTrie opens the main account trie.
func (db *cachingDB) OpenTrie(stateRoot common.Hash) (Trie, error) {
	if db.triedb.IsVerkle() {
		return trie.NewVerkleTrie(stateRoot, db.triedb, utils.NewPointCache(commitmentCacheItems))
	}
	return trie.NewStateTrie(trie.StateTrieID(stateRoot), db.triedb)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (Trie, error) {
	if db.triedb.IsVerkle() {
		return nil, errors.New("not supported")
	}
	return trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.triedb)
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
func (db *cachingDB) Commit(originRoot, root common.Hash, block uint64, update *Update) error {
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
	for _, item := range update.Codes {
		blobs = append(blobs, item.Blob)
		hashes = append(hashes, item.Hash)
		addresses = append(addresses, item.Address)
	}
	if err := db.codedb.WriteCodes(addresses, hashes, blobs); err != nil {
		return err
	}
	// If snapshotting is enabled and the snapshot of original state is also
	// available, update the snapshot tree with this new version.
	if db.snaps != nil && db.snaps.Snapshot(originRoot) != nil {
		if err := db.snaps.Update(root, originRoot, update.Destructs, update.Accounts, update.Storages); err != nil {
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
	set := triestate.New(update.AccountsOrigin, update.StoragesOrigin, update.StorageIncomplete)
	return db.triedb.Update(root, originRoot, block, update.Nodes, set)
}

// mustCopyTrie creates a deep-copied trie and panic if the trie is unknown.
func mustCopyTrie(tr Trie) Trie {
	switch t := tr.(type) {
	case *trie.StateTrie:
		return t.Copy()
	case *trie.VerkleTrie:
		return t.Copy()
	default:
		panic(fmt.Sprintf("Unknown trie type %T", tr))
	}
}
