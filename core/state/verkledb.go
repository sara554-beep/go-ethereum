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
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// verkleReader implements the StateReader interface, offering methods to access
// accounts and storage slots in the verkle tree manner.
type verkleReader struct {
	// The associated state root, intended to uniquely represent a state.
	root common.Hash

	// The associated state snapshot, which may be nil if the snapshot is not
	// enabled, or partial functional which is still generating in the background.
	snap snapshot.Snapshot

	// The reusable hasher for keccak256 hashing.
	hasher crypto.KeccakState

	// The associated verkle trie, serves as a fallback for accessing states
	// if the snapshot is unavailable.
	trie Trie
}

// newMerkleReader constructs a merkle reader with specific state root.
func newVerkleReader(root common.Hash, db *verkleDB) (*verkleReader, error) {
	// Open the verkle trie, bail out if it's not available.
	t, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	// Opens the optional state snapshot, which can significantly improve
	// state read efficiency but may have limited functionality(not fully
	// generated).
	var snap snapshot.Snapshot
	if db.snaps != nil {
		snap = db.snaps.Snapshot(root)
	}
	return &verkleReader{
		root:   root,
		snap:   snap,
		hasher: crypto.NewKeccakState(),
		trie:   t,
	}, nil
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state.
func (r *verkleReader) Account(addr common.Address) (acct *types.StateAccount, err error) {
	// Try to read account from snapshot, which is more read-efficient.
	if r.snap != nil {
		ret, err := r.snap.Account(crypto.HashData(r.hasher, addr.Bytes()))
		if err == nil {
			if ret == nil {
				return nil, nil
			}
			acct = &types.StateAccount{
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
	}
	// If snapshot unavailable or reading from it failed, read account
	// from verkle tree as fallback.
	return r.trie.GetAccount(addr)
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state.
func (r *verkleReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	// Try to read storage slot from snapshot first, which is more read-efficient.
	if r.snap != nil {
		ret, err := r.snap.Storage(crypto.Keccak256Hash(addr.Bytes()), crypto.Keccak256Hash(key.Bytes()))
		if err == nil {
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
	}
	// If snapshot unavailable or reading from it failed, read storage slot
	// from verkle tree as fallback.
	ret, err := r.trie.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	var slot common.Hash
	slot.SetBytes(ret)
	return slot, nil
}

// NewVerkleDatabase creates a verkleDB instance with provided components.
func NewVerkleDatabase(codeDB CodeStore, trieDB *trie.Database, snaps *snapshot.Tree) Database {
	return &verkleDB{
		codeDB: codeDB,
		trieDB: trieDB,
		snaps:  snaps,
	}
}

// verkleDB is the implementation of Database interface, designed for providing
// functionalities to read and write states.
type verkleDB struct {
	snaps  *snapshot.Tree
	codeDB CodeStore
	trieDB *trie.Database
}

// StateScheme returns the state scheme used by the database.
func (db *verkleDB) StateScheme() string {
	return db.trieDB.Scheme()
}

// TreeScheme returns the tree scheme used by the database.
func (db *verkleDB) TreeScheme() string {
	return rawdb.VerkleTree
}

// StateReader constructs a reader for the specific state.
func (db *verkleDB) StateReader(stateRoot common.Hash) (StateReader, error) {
	return newVerkleReader(stateRoot, db)
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *verkleDB) OpenTrie(root common.Hash) (Trie, error) {
	panic("not implemented yet")
}

// OpenStorageTrie opens the storage trie of an account.
func (db *verkleDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (Trie, error) {
	panic("not implemented yet")
}

// CopyTrie returns an independent copy of the given trie.
func (db *verkleDB) CopyTrie(t Trie) Trie {
	panic("not implemented yet")
}

// ReadCode implements CodeReader, retrieving a particular contract's code.
func (db *verkleDB) ReadCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	return db.codeDB.ReadCode(address, codeHash)
}

// ReadCodeSize implements CodeReader, retrieving a particular contracts
// code's size.
func (db *verkleDB) ReadCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return db.codeDB.ReadCodeSize(addr, codeHash)
}

// WriteCodes implements CodeWriter, writing the provided a list of contract
// codes into database.
func (db *verkleDB) WriteCodes(addresses []common.Address, hashes []common.Hash, codes [][]byte) error {
	return db.codeDB.WriteCodes(addresses, hashes, codes)
}

// TrieDB returns the associated trie database.
func (db *verkleDB) TrieDB() *trie.Database {
	return db.trieDB
}

// Snapshot returns the associated state snapshot, it may be nil if not configured.
func (db *verkleDB) Snapshot() *snapshot.Tree {
	return db.snaps
}
