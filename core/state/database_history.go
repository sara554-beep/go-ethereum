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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// NewHistoryDatabase creates a state database with the provided data sources.
func NewHistoryDatabase(codedb *CodeDB, triedb *triedb.Database) *HistoryDB {
	return &HistoryDB{
		codedb: codedb,
		triedb: triedb,
	}
}

// HistoryDB is the implementation of Database interface, designed for providing
// functionalities to read and write states.
type HistoryDB struct {
	codedb *CodeDB
	triedb *triedb.Database
}

// Reader implements Database interface, returning a reader of the specific state.
func (db *HistoryDB) Reader(stateRoot common.Hash) (Reader, error) {
	return newArchiveReader(stateRoot, db.triedb)
}

// Hasher implements Database interface, returning a hasher of the specific state.
func (db *HistoryDB) Hasher(stateRoot common.Hash) (Hasher, error) {
	return nil, errors.New("hashing is not supported")
}

// StorageDeleter implements Database interface, returning a storage deleter of
// the specific state.
func (db *HistoryDB) StorageDeleter(stateRoot common.Hash) (StorageDeleter, error) {
	return nil, errors.New("storage deleting is not supported")
}

// ReadCode implements CodeReader, retrieving a particular contract's code.
func (db *HistoryDB) ReadCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	return db.codedb.ReadCode(address, codeHash)
}

// ReadCodeSize implements CodeReader, retrieving a particular contracts
// code's size.
func (db *HistoryDB) ReadCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return db.codedb.ReadCodeSize(addr, codeHash)
}

// WriteCodes implements CodeWriter, writing the provided a list of contract
// codes into database.
func (db *HistoryDB) WriteCodes(addresses []common.Address, hashes []common.Hash, codes [][]byte) error {
	return nil
}

// TrieDB returns the associated trie database.
func (db *HistoryDB) TrieDB() *triedb.Database {
	return db.triedb
}

// Commit accepts the state changes made by execution and applies it to database.
func (db *HistoryDB) Commit(originRoot, root common.Hash, block uint64, update *Update, nodes *trienode.MergedNodeSet) error {
	return errors.New("state committing is not supported")
}
