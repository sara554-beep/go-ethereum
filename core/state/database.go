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
	"fmt"

	"github.com/crate-crypto/go-ipa/banderwagon"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
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

// snapReader is a wrapper over the state snapshot and implements the StateReader
// interface. It provides an efficient way to access state.
type snapReader struct {
	snap   snapshot.Snapshot
	hasher crypto.KeccakState
}

// newSnapReader constructs a snap reader based on the specific state root.
func newSnapReader(snaps *snapshot.Tree, root common.Hash) *snapReader {
	snap := snaps.Snapshot(root)
	if snap == nil {
		return nil
	}
	return &snapReader{
		snap:   snap,
		hasher: crypto.NewKeccakState(),
	}
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state. An error will be returned if the referenced snapshot
// is already stale or the snapshot is not fully constructed yet.
func (r *snapReader) Account(addr common.Address) (*types.StateAccount, error) {
	ret, err := r.snap.Account(crypto.HashData(r.hasher, addr.Bytes()))
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, nil
	}
	acct := &types.StateAccount{
		Nonce:    ret.Nonce,
		Balance:  ret.Balance,
		CodeHash: ret.CodeHash,
		Root:     common.BytesToHash(ret.Root),
	}
	if len(acct.CodeHash) == 0 {
		acct.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if acct.Root == (common.Hash{}) {
		acct.Root = types.EmptyRootHash
	}
	return acct, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state. An error will be returned if
// the referenced snapshot is already stale or the snapshot is not fully
// constructed yet.
func (r *snapReader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	addrHash := crypto.HashData(r.hasher, addr.Bytes())
	slotHash := crypto.HashData(r.hasher, key.Bytes())
	ret, err := r.snap.Storage(addrHash, slotHash)
	if err != nil {
		return common.Hash{}, err
	}
	if len(ret) == 0 {
		return common.Hash{}, nil
	}
	_, content, _, err := rlp.Split(ret)
	if err != nil {
		return common.Hash{}, err
	}
	var slot common.Hash
	slot.SetBytes(content)
	return slot, nil
}

// trieReader implements the StateReader interface, providing functions to access
// state from the referenced trie.
type trieReader struct {
	root     common.Hash             // State root which uniquely represent a state.
	db       *trie.Database          // Database for loading trie
	hasher   crypto.KeccakState      // Hasher for keccak256 hashing.
	mainTrie Trie                    // Main trie, resolved in constructor
	subTries map[common.Address]Trie // group of storage tries, cached when it's resolved.
}

// trieReader constructs a trie reader of the specific state.
func newTrieReader(root common.Hash, db *trie.Database) (*trieReader, error) {
	var (
		tr  Trie
		err error
	)
	if !db.IsVerkle() {
		tr, err = trie.NewStateTrie(trie.StateTrieID(root), db)
	} else {
		tr, err = trie.NewVerkleTrie(root, db, utils.NewPointCache(commitmentCacheItems))
	}
	if err != nil {
		return nil, err
	}
	return &trieReader{
		root:     root,
		db:       db,
		hasher:   crypto.NewKeccakState(),
		mainTrie: tr,
		subTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state.
func (r *trieReader) Account(addr common.Address) (*types.StateAccount, error) {
	return r.mainTrie.GetAccount(addr)
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state.
func (r *trieReader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	var (
		tr    Trie
		found bool
		slot  common.Hash
	)
	if r.db.IsVerkle() {
		tr = r.mainTrie
	} else {
		tr, found = r.subTries[addr]
		if !found {
			var err error
			tr, err = trie.NewStateTrie(trie.StorageTrieID(r.root, crypto.HashData(r.hasher, addr.Bytes()), root), r.db)
			if err != nil {
				return common.Hash{}, err
			}
			r.subTries[addr] = tr
		}
	}
	ret, err := tr.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	slot.SetBytes(ret)
	return slot, nil
}

// reader implements the StateReader interface, offering methods to access
// accounts and storage slots.
type reader struct {
	snap *snapReader // fast reader, might not available
	trie *trieReader // slow reader, always available
}

// newReader constructs a reader of the specific state.
func newReader(root common.Hash, db *trie.Database, snaps *snapshot.Tree) (*reader, error) {
	trie, err := newTrieReader(root, db) // mandatory trie reader
	if err != nil {
		return nil, err
	}
	var snap *snapReader
	if snaps != nil {
		snap = newSnapReader(snaps, root) // optional snapshot reader
	}
	return &reader{
		snap: snap,
		trie: trie,
	}, nil
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state.
func (r *reader) Account(addr common.Address) (*types.StateAccount, error) {
	if r.snap != nil {
		acct, err := r.snap.Account(addr)
		if err == nil {
			return acct, nil
		}
	}
	acct, err := r.trie.Account(addr)
	if err != nil {
		return nil, err
	}
	return acct, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state.
func (r *reader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	if r.snap != nil {
		slot, err := r.snap.Storage(addr, root, key)
		if err == nil {
			return slot, nil
		}
	}
	slot, err := r.trie.Storage(addr, root, key)
	if err != nil {
		return common.Hash{}, err
	}
	return slot, nil
}

// NewDatabase creates a state database with the provided data sources.
func NewDatabase(codedb *CodeDB, triedb *trie.Database, snaps *snapshot.Tree) Database {
	return &cachingDB{
		codedb: codedb,
		triedb: triedb,
		snaps:  snaps,
	}
}

// NewDatabaseForTesting is similar to NewDatabase, but it sets up the different
// data sources using the same provided database with default config, specifically
// intended for testing.
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

// StateReader constructs a reader for the specific state.
func (db *cachingDB) StateReader(stateRoot common.Hash) (StateReader, error) {
	return newReader(stateRoot, db.triedb, nil)
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *cachingDB) OpenTrie(root common.Hash) (Trie, error) {
	if db.triedb.IsVerkle() {
		return trie.NewVerkleTrie(root, db.triedb, utils.NewPointCache(commitmentCacheItems))
	}
	return trie.NewStateTrie(trie.StateTrieID(root), db.triedb)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	// In the verkle case, there is only one tree. But the two-tree structure
	// is hardcoded in the codebase. So we need to return the same trie in this
	// case.
	if db.triedb.IsVerkle() {
		return self, nil
	}
	return trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.triedb)
}

// CopyTrie returns an independent copy of the given trie.
func (db *cachingDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.StateTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
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

// TrieDB retrieves any intermediate trie-node caching layer.
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
	// Flush the cached dirty contract codes.
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
	// If snapshotting is enabled, update the snapshot tree with this new version
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
	// Update the trie database with new version
	set := triestate.New(trans.AccountsOrigin, trans.StoragesOrigin, trans.StorageIncomplete)
	return db.triedb.Update(root, originRoot, block, trans.Nodes, set)
}
