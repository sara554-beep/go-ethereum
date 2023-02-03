// Copyright 2014 The go-ethereum Authors
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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/trie"
)

type proofList [][]byte

func (n *proofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

func (n *proofList) Delete(key []byte) error {
	panic("not supported")
}

// StateDB structs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StateDB struct {
	*stateCore

	ts    *tries
	db    Database
	snaps *snapshot.Tree
	snap  snapshot.Snapshot
}

// New creates a new state from a given trie.
func New(root common.Hash, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	ts, err := newTries(root, db)
	if err != nil {
		return nil, err
	}
	var snap snapshot.Snapshot
	if snaps != nil {
		snap = snaps.Snapshot(root)
	}
	reader := newTrieAndSnap(root, db, ts, snap)
	core, err := newStateCore(root, reader)
	if err != nil {
		return nil, err
	}
	sdb := &StateDB{
		stateCore: core,
		ts:        ts,
		db:        db,
		snaps:     snaps,
		snap:      snap,
	}
	return sdb, nil
}

// setError remembers the first non-nil error it is called with.
func (db *StateDB) setError(err error) {
	if db.dbErr == nil {
		db.dbErr = err
	}
}

func (db *StateDB) Error() error {
	return db.dbErr
}

// GetProof returns the Merkle proof for a given account.
func (db *StateDB) GetProof(addr common.Address) ([][]byte, error) {
	return db.GetProofByHash(crypto.Keccak256Hash(addr.Bytes()))
}

// GetProofByHash returns the Merkle proof for a given account.
func (db *StateDB) GetProofByHash(addrHash common.Hash) ([][]byte, error) {
	var proof proofList
	err := db.ts.accountTrie.Prove(addrHash[:], 0, &proof)
	return proof, err
}

// GetStorageProof returns the Merkle proof for given storage slot.
func (db *StateDB) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	trie, err := db.StorageTrie(a)
	if err != nil {
		return nil, err
	}
	if trie == nil {
		return nil, errors.New("storage trie for requested address does not exist")
	}
	var proof proofList
	err = trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

// Database retrieves the low level database supporting the lower level trie ops.
func (db *StateDB) Database() Database {
	return db.db
}

// StorageTrie returns the storage trie of an account. The return value is a copy
// and is nil for non-existent accounts. An error will be returned if storage trie
// is existent but can't be loaded correctly.
func (db *StateDB) StorageTrie(addr common.Address) (Trie, error) {
	stateObject := db.getStateObject(addr)
	if stateObject == nil {
		return nil, nil
	}
	cpy := stateObject.deepCopy(db.stateCore)
	tr, err := db.db.OpenStorageTrie(db.originalRoot, cpy.addrHash, cpy.data.Root)
	if err != nil {
		return nil, err
	}
	if _, err := cpy.updateTrie(tr); err != nil {
		return nil, err
	}
	return tr, nil
}

//
// Setting, updating & deleting state object methods.
//

// updateStateObject writes the given object to the trie.
func (db *StateDB) updateStateObject(obj *stateObject) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { db.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Encode the account and update the account trie
	addr := obj.Address()
	if err := db.ts.accountTrie.TryUpdateAccount(addr, &obj.data); err != nil {
		db.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
	}

	// If state snapshotting is active, cache the data til commit. Note, this
	// update mechanism is not symmetric to the deletion, because whereas it is
	// enough to track account updates at commit time, deletions need tracking
	// at transaction boundary level to ensure we capture state clearing.
	if db.snap != nil {
		db.snapAccounts[obj.addrHash] = snapshot.SlimAccountRLP(obj.data.Nonce, obj.data.Balance, obj.data.Root, obj.data.CodeHash)
	}
}

// deleteStateObject removes the given object from the state trie.
func (db *StateDB) deleteStateObject(obj *stateObject) {
	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { db.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()
	if err := db.ts.accountTrie.TryDeleteAccount(addr); err != nil {
		db.setError(fmt.Errorf("deleteStateObject (%x) error: %v", addr[:], err))
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (db *StateDB) Copy() *StateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
		stateCore: db.stateCore.Copy(),
		ts:        db.ts.Copy(),
		db:        db.db,
		snaps:     db.snaps,
		snap:      db.snap,
	}
	state.stateCore.reader.(*trieAndSnap).update(state.ts)
	return state
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (db *StateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	db.Finalise(deleteEmptyObjects)

	// Although naively it makes sense to retrieve the account trie and then do
	// the contract storage and account updates sequentially, that short circuits
	// the account prefetcher. Instead, let's process all the storage updates
	// first, giving the account prefetches just a few more milliseconds of time
	// to pull useful data from disk.
	for addr := range db.stateObjectsPending {
		if obj := db.stateObjects[addr]; !obj.deleted {
			tr, err := db.ts.storageTrie(obj.addrHash, obj.data.Root)
			if err != nil {
				continue
			}
			obj.updateRoot(tr)
		}
	}
	for addr := range db.stateObjectsPending {
		if obj := db.stateObjects[addr]; obj.deleted {
			db.deleteStateObject(obj)
			db.AccountDeleted += 1
		} else {
			db.updateStateObject(obj)
			db.AccountUpdated += 1
		}
	}
	if len(db.stateObjectsPending) > 0 {
		db.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { db.AccountHashes += time.Since(start) }(time.Now())
	}
	return db.ts.accountTrie.Hash()
}

// Commit writes the state to the underlying in-memory trie database.
func (db *StateDB) Commit(deleteEmptyObjects bool) (common.Hash, error) {
	if db.dbErr != nil {
		return common.Hash{}, fmt.Errorf("commit aborted due to earlier error: %v", db.dbErr)
	}
	// Finalize any pending changes and merge everything into the tries
	db.IntermediateRoot(deleteEmptyObjects)

	// Commit objects to the trie, measuring the elapsed time
	var (
		accountTrieNodesUpdated int
		accountTrieNodesDeleted int
		storageTrieNodesUpdated int
		storageTrieNodesDeleted int
		nodes                   = trie.NewMergedNodeSet()
	)
	codeWriter := db.db.DiskDB().NewBatch()
	for addr := range db.stateObjectsDirty {
		if obj := db.stateObjects[addr]; !obj.deleted {
			// Write any contract code associated with the state object
			if obj.code != nil && obj.dirtyCode {
				rawdb.WriteCode(codeWriter, common.BytesToHash(obj.CodeHash()), obj.code)
				obj.dirtyCode = false
			}
			// Write any storage changes in the state object to its storage trie
			tr, err := db.ts.storageTrie(obj.addrHash, obj.data.Root)
			if err != nil {
				continue
			}
			set, err := obj.commitTrie(tr)
			if err != nil {
				return common.Hash{}, err
			}
			// Merge the dirty nodes of storage trie into global set
			if set != nil {
				if err := nodes.Merge(set); err != nil {
					return common.Hash{}, err
				}
				updates, deleted := set.Size()
				storageTrieNodesUpdated += updates
				storageTrieNodesDeleted += deleted
			}
		}
		// If the contract is destructed, the storage is still left in the
		// database as dangling data. Theoretically it's should be wiped from
		// database as well, but in hash-based-scheme it's extremely hard to
		// determine that if the trie nodes are also referenced by other storage,
		// and in path-based-scheme some technical challenges are still unsolved.
		// Although it won't affect the correctness but please fix it TODO(rjl493456442).
	}
	if len(db.stateObjectsDirty) > 0 {
		db.stateObjectsDirty = make(map[common.Address]struct{})
	}
	if codeWriter.ValueSize() > 0 {
		if err := codeWriter.Write(); err != nil {
			log.Crit("Failed to commit dirty codes", "error", err)
		}
	}
	// Write the account trie changes, measuring the amount of wasted time
	var start time.Time
	if metrics.EnabledExpensive {
		start = time.Now()
	}
	root, set, err := db.ts.accountTrie.Commit(true)
	if err != nil {
		return common.Hash{}, err
	}
	// Merge the dirty nodes of account trie into global set
	if set != nil {
		if err := nodes.Merge(set); err != nil {
			return common.Hash{}, err
		}
		accountTrieNodesUpdated, accountTrieNodesDeleted = set.Size()
	}
	if metrics.EnabledExpensive {
		db.AccountCommits += time.Since(start)

		accountUpdatedMeter.Mark(int64(db.AccountUpdated))
		storageUpdatedMeter.Mark(int64(db.StorageUpdated))
		accountDeletedMeter.Mark(int64(db.AccountDeleted))
		storageDeletedMeter.Mark(int64(db.StorageDeleted))
		accountTrieUpdatedMeter.Mark(int64(accountTrieNodesUpdated))
		accountTrieDeletedMeter.Mark(int64(accountTrieNodesDeleted))
		storageTriesUpdatedMeter.Mark(int64(storageTrieNodesUpdated))
		storageTriesDeletedMeter.Mark(int64(storageTrieNodesDeleted))
		db.AccountUpdated, db.AccountDeleted = 0, 0
		db.StorageUpdated, db.StorageDeleted = 0, 0
	}
	// If snapshotting is enabled, update the snapshot tree with this new version
	if db.snap != nil {
		start := time.Now()
		// Only update if there's a state transition (skip empty Clique blocks)
		if parent := db.snap.Root(); parent != root {
			if err := db.snaps.Update(root, parent, db.convertAccountSet(db.stateObjectsDestruct), db.snapAccounts, db.snapStorage); err != nil {
				log.Warn("Failed to update snapshot tree", "from", parent, "to", root, "err", err)
			}
			// Keep 128 diff layers in the memory, persistent layer is 129th.
			// - head layer is paired with HEAD state
			// - head-1 layer is paired with HEAD-1 state
			// - head-127 layer(bottom-most diff layer) is paired with HEAD-127 state
			if err := db.snaps.Cap(root, 128); err != nil {
				log.Warn("Failed to cap snapshot tree", "root", root, "layers", 128, "err", err)
			}
		}
		if metrics.EnabledExpensive {
			db.SnapshotCommits += time.Since(start)
		}
		db.snap, db.snapAccounts, db.snapStorage = nil, nil, nil
	}
	if len(db.stateObjectsDestruct) > 0 {
		db.stateObjectsDestruct = make(map[common.Address]struct{})
	}
	if root == (common.Hash{}) {
		root = emptyRoot
	}
	origin := db.originalRoot
	if origin == (common.Hash{}) {
		origin = emptyRoot
	}
	if root != origin {
		start := time.Now()
		if err := db.db.TrieDB().Update(nodes); err != nil {
			return common.Hash{}, err
		}
		db.originalRoot = root
		if metrics.EnabledExpensive {
			db.TrieDBCommits += time.Since(start)
		}
	}
	return root, nil
}

// convertAccountSet converts a provided account set from address keyed to hash keyed.
func (db *StateDB) convertAccountSet(set map[common.Address]struct{}) map[common.Hash]struct{} {
	ret := make(map[common.Hash]struct{})
	for addr := range set {
		obj, exist := db.stateObjects[addr]
		if !exist {
			ret[crypto.Keccak256Hash(addr[:])] = struct{}{}
		} else {
			ret[obj.addrHash] = struct{}{}
		}
	}
	return ret
}
