// Copyright 2020 The go-ethereum Authors
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

package rawdb

import (
	"bytes"
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"time"
)

// ReadPreimage retrieves a single preimage of the provided hash.
func ReadPreimage(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, _ := db.Get(preimageKey(hash))
	return data
}

// WritePreimages writes the provided set of preimages to the database.
func WritePreimages(db ethdb.KeyValueWriter, preimages map[common.Hash][]byte) {
	for hash, preimage := range preimages {
		if err := db.Put(preimageKey(hash), preimage); err != nil {
			log.Crit("Failed to store trie preimage", "err", err)
		}
	}
	preimageCounter.Inc(int64(len(preimages)))
	preimageHitCounter.Inc(int64(len(preimages)))
}

// ReadCode retrieves the contract code of the provided code hash.
func ReadCode(db ethdb.KeyValueReader, hash common.Hash) []byte {
	// Try with the legacy code scheme first, if not then try with current
	// scheme. Since most of the code will be found with legacy scheme.
	//
	// todo(rjl493456442) change the order when we forcibly upgrade the code
	// scheme with snapshot.
	data, _ := db.Get(hash[:])
	if len(data) != 0 {
		return data
	}
	return ReadCodeWithPrefix(db, hash)
}

// ReadCodeWithPrefix retrieves the contract code of the provided code hash.
// The main difference between this function and ReadCode is this function
// will only check the existence with latest scheme(with prefix).
func ReadCodeWithPrefix(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, _ := db.Get(codeKey(hash))
	return data
}

// WriteCode writes the provided contract code database.
func WriteCode(db ethdb.KeyValueWriter, hash common.Hash, code []byte) {
	if err := db.Put(codeKey(hash), code); err != nil {
		log.Crit("Failed to store contract code", "err", err)
	}
}

// DeleteCode deletes the specified contract code from the database.
func DeleteCode(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(codeKey(hash)); err != nil {
		log.Crit("Failed to delete contract code", "err", err)
	}
}

// ReadTrieNode retrieves the trie node of the provided hash.
func ReadTrieNode(db ethdb.KeyValueReader, key []byte) []byte {
	data, _ := db.Get(trieNodeKey(key))
	return data
}

// WriteTrieNode writes the provided trie node into the database.
func WriteTrieNode(db ethdb.KeyValueWriter, key []byte, node []byte) {
	if err := db.Put(trieNodeKey(key), node); err != nil {
		log.Crit("Failed to store trie node", "err", err)
	}
}

// DeleteTrieNode deletes the specified trie node from the database.
func DeleteTrieNode(db ethdb.KeyValueWriter, key []byte) {
	if err := db.Delete(trieNodeKey(key)); err != nil {
		log.Crit("Failed to delete trie node", "err", err)
	}
}

func ReadTrieNodesWithPrefix(db ethdb.KeyValueStore, path []byte, filterFn func([]byte) bool) ([][]byte, [][]byte, int, time.Duration, time.Duration) {
	var (
		keys  [][]byte
		vals  [][]byte
		count int
		start = time.Now()
	)
	// Construct the key prefix of start point.
	prefix, length := trieNodePrefix(path)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	newElasped := time.Since(start)

	start = time.Now()
	for it.Next() {
		count += 1
		if key := it.Key(); len(key) == len(prefix)+common.HashLength+1 {
			if filterFn(it.Key()[length:]) {
				continue
			}
			keys = append(keys, common.CopyBytes(it.Key()[length:]))
			vals = append(vals, common.CopyBytes(it.Value()))
		}
	}
	iterElapsed := time.Since(start)
	return keys, vals, count, newElasped, iterElapsed
}

// ReadCommitRecord retrieves the state update of the provided hash.
func ReadCommitRecord(db ethdb.KeyValueReader, number uint64, hash common.Hash) []byte {
	data, _ := db.Get(commitRecordKey(number, hash))
	return data
}

// WriteCommitRecord writes the provided state update into the database.
func WriteCommitRecord(db ethdb.KeyValueWriter, number uint64, hash common.Hash, val []byte) {
	if err := db.Put(commitRecordKey(number, hash), val); err != nil {
		log.Crit("Failed to store state update", "err", err)
	}
}

// DeleteCommitRecord deletes the specified state update from the database.
func DeleteCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash) {
	if err := db.Delete(commitRecordKey(number, hash)); err != nil {
		log.Crit("Failed to delete commit record", "err", err)
	}
}

// ReadAllCommitRecords retrieves all the commit record objects at the certain
// range where from is included while to is excluded.
func ReadAllCommitRecords(db ethdb.Iteratee, from uint64, to uint64) ([]uint64, []common.Hash) {
	var (
		numbers []uint64
		hashes  []common.Hash
		start   = commitRecordKey(from, common.Hash{})
		end     = commitRecordKey(to, common.Hash{})
	)
	it := db.NewIterator(nil, start)
	defer it.Release()

	for it.Next() {
		if bytes.Compare(it.Key(), end) >= 0 {
			break
		}
		if key := it.Key(); len(key) == len(commitRecordPrefix)+8+common.HashLength {
			numbers = append(numbers, binary.BigEndian.Uint64(key[len(commitRecordPrefix):len(commitRecordPrefix)+8]))
			hashes = append(hashes, common.BytesToHash(key[len(key)-common.HashLength:]))
		}
	}
	return numbers, hashes
}
