// Copyright 2017 The go-ethereum Authors
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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000

	// Cache size granted for caching clean code.
	codeCacheSize = 64 * 1024 * 1024
)

// Database wraps access to tries and contract code.
type Database interface {
	// OpenTrie opens the main account trie.
	OpenTrie(root common.Hash) (trie.Trie, error)

	// OpenStorageTrie opens the storage trie of an account.
	OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (trie.Trie, error)

	// CopyTrie returns an independent copy of the given trie.
	CopyTrie(trie.Trie) trie.Trie

	// ContractCode retrieves a particular contract's code.
	ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error)

	// DiskDB returns the underlying key-value disk database.
	DiskDB() ethdb.KeyValueStore

	// TrieDB returns the underlying trie database for managing trie nodes.
	TrieDB() *trie.Database
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use, but does not retain any recent trie nodes in memory. To keep some
// historical state in memory, use the NewDatabaseWithConfig constructor.
func NewDatabase(db ethdb.Database) Database {
	return NewDatabaseWithConfig(db, nil)
}

// NewDatabaseWithConfig creates a backing store for state. The returned database
// is safe for concurrent use and retains a lot of collapsed RLP trie nodes in a
// large memory cache.
func NewDatabaseWithConfig(db ethdb.Database, config *trie.Config) Database {
	triedb := trie.NewDatabase(db, config)
	if config != nil && config.Verkle {
		return &verkleDB{
			disk:          db,
			codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
			codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
			triedb:        triedb,
		}
	}
	return &merkleDB{
		disk:          db,
		codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
		codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		triedb:        triedb,
	}
}

// NewDatabaseWithNodeDB creates a state database with an already initialized node database.
func NewDatabaseWithNodeDB(db ethdb.Database, triedb *trie.Database) Database {
	if triedb.Config().Verkle {
		return &verkleDB{
			disk:          db,
			codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
			codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
			triedb:        triedb,
		}
	} else {
		return &merkleDB{
			disk:          db,
			codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
			codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
			triedb:        triedb,
		}
	}
	/*
			in case of verkle transition period, create the transDb with
		    both verkleDB and merkleDB inrolled.
	*/
}

func NewTransitionDB(db ethdb.Database, merkleTrieDB, verkleTrieDB *trie.Database) Database {
	return &transDB{
		vdb: &verkleDB{
			disk:          db,
			codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
			codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
			triedb:        verkleTrieDB,
		},
		mdb: &merkleDB{
			disk:          db,
			codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
			codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
			triedb:        merkleTrieDB,
		},
	}
}

type merkleDB struct {
	disk          ethdb.KeyValueStore
	codeSizeCache *lru.Cache[common.Hash, int]
	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	triedb        *trie.Database
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *merkleDB) OpenTrie(root common.Hash) (trie.Trie, error) {
	tr, err := trie.NewStateTrie(trie.StateTrieID(root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// OpenStorageTrie opens the storage trie of an account.
func (db *merkleDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (trie.Trie, error) {
	tr, err := trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// CopyTrie returns an independent copy of the given trie.
func (db *merkleDB) CopyTrie(t trie.Trie) trie.Trie {
	switch t := t.(type) {
	case *trie.StateTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ContractCode retrieves a particular contract's code.
func (db *merkleDB) ContractCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	code, _ := db.codeCache.Get(codeHash)
	if len(code) > 0 {
		return code, nil
	}
	code = rawdb.ReadCode(db.disk, codeHash)
	if len(code) > 0 {
		db.codeCache.Add(codeHash, code)
		db.codeSizeCache.Add(codeHash, len(code))
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeWithPrefix retrieves a particular contract's code. If the
// code can't be found in the cache, then check the existence with **new**
// db scheme.
func (db *merkleDB) ContractCodeWithPrefix(address common.Address, codeHash common.Hash) ([]byte, error) {
	code, _ := db.codeCache.Get(codeHash)
	if len(code) > 0 {
		return code, nil
	}
	code = rawdb.ReadCodeWithPrefix(db.disk, codeHash)
	if len(code) > 0 {
		db.codeCache.Add(codeHash, code)
		db.codeSizeCache.Add(codeHash, len(code))
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *merkleDB) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	if cached, ok := db.codeSizeCache.Get(codeHash); ok {
		return cached, nil
	}
	code, err := db.ContractCode(addr, codeHash)
	return len(code), err
}

// DiskDB returns the underlying key-value disk database.
func (db *merkleDB) DiskDB() ethdb.KeyValueStore {
	return db.disk
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *merkleDB) TrieDB() *trie.Database {
	return db.triedb
}

type verkleDB struct {
	disk          ethdb.KeyValueStore
	codeSizeCache *lru.Cache[common.Hash, int]
	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	triedb        *trie.Database
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *verkleDB) OpenTrie(root common.Hash) (trie.Trie, error) {
	panic("not implemented")
}

// OpenStorageTrie opens the storage trie of an account.
func (db *verkleDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (trie.Trie, error) {
	panic("not implemented")
}

// CopyTrie returns an independent copy of the given trie.
func (db *verkleDB) CopyTrie(t trie.Trie) trie.Trie {
	panic("not implemented")
}

// ContractCode retrieves a particular contract's code.
func (db *verkleDB) ContractCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	panic("not implemented")
}

// ContractCodeWithPrefix retrieves a particular contract's code. If the
// code can't be found in the cache, then check the existence with **new**
// db scheme.
func (db *verkleDB) ContractCodeWithPrefix(address common.Address, codeHash common.Hash) ([]byte, error) {
	panic("not implemented")
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *verkleDB) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	panic("not implemented")
}

// DiskDB returns the underlying key-value disk database.
func (db *verkleDB) DiskDB() ethdb.KeyValueStore {
	return db.disk
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *verkleDB) TrieDB() *trie.Database {
	return db.triedb
}

type transDB struct {
	mdb *merkleDB
	vdb *verkleDB

	isVerkle bool
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *transDB) OpenTrie(root common.Hash) (trie.Trie, error) {
	if !db.isVerkle {
		return db.mdb.OpenTrie(root)
	}
	return db.vdb.OpenTrie(root)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *transDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash) (trie.Trie, error) {
	if !db.isVerkle {
		return db.mdb.OpenStorageTrie(stateRoot, address, root)
	}
	return db.vdb.OpenStorageTrie(stateRoot, address, root)
}

// CopyTrie returns an independent copy of the given trie.
func (db *transDB) CopyTrie(t trie.Trie) trie.Trie {
	panic("not implemented")
}

// ContractCode retrieves a particular contract's code.
func (db *transDB) ContractCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if !db.isVerkle {
		return db.mdb.ContractCode(address, codeHash)
	}
	return db.vdb.ContractCode(address, codeHash)
}

// ContractCodeWithPrefix retrieves a particular contract's code. If the
// code can't be found in the cache, then check the existence with **new**
// db scheme.
func (db *transDB) ContractCodeWithPrefix(address common.Address, codeHash common.Hash) ([]byte, error) {
	if !db.isVerkle {
		return db.mdb.ContractCodeWithPrefix(address, codeHash)
	}
	return db.vdb.ContractCodeWithPrefix(address, codeHash)
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *transDB) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	if !db.isVerkle {
		return db.mdb.ContractCodeSize(addr, codeHash)
	}
	return db.vdb.ContractCodeSize(addr, codeHash)
}

// DiskDB returns the underlying key-value disk database.
func (db *transDB) DiskDB() ethdb.KeyValueStore {
	panic("not implemented")
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *transDB) TrieDB() *trie.Database {
	panic("not implemented")
}

// EnableVerkle is invoked only when the verkle is activated. The whole verkle
// transition is supposed to be finished in advance.
func (db *transDB) EnableVerkle() {
	db.isVerkle = true
}
