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
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
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
	// Try with the prefixed code scheme first, if not then try with legacy
	// scheme.
	data := ReadCodeWithPrefix(db, hash)
	if len(data) != 0 {
		return data
	}
	data, _ = db.Get(hash.Bytes())
	return data
}

// ReadCodeWithPrefix retrieves the contract code of the provided code hash.
// The main difference between this function and ReadCode is this function
// will only check the existence with latest scheme(with prefix).
func ReadCodeWithPrefix(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, _ := db.Get(codeKey(hash))
	return data
}

// HasCode checks if the contract code corresponding to the
// provided code hash is present in the db.
func HasCode(db ethdb.KeyValueReader, hash common.Hash) bool {
	// Try with the prefixed code scheme first, if not then try with legacy
	// scheme.
	if ok := HasCodeWithPrefix(db, hash); ok {
		return true
	}
	ok, _ := db.Has(hash.Bytes())
	return ok
}

// HasCodeWithPrefix checks if the contract code corresponding to the
// provided code hash is present in the db. This function will only check
// presence using the prefix-scheme.
func HasCodeWithPrefix(db ethdb.KeyValueReader, hash common.Hash) bool {
	ok, _ := db.Has(codeKey(hash))
	return ok
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

// ReadReverseDiff retrieves the state reverse diff with the given associated
// identifier. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadReverseDiff(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(freezerReverseDiffTable, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadReverseDiffHash retrieves the state root corresponding to the specified
// reverse diff. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadReverseDiffHash(db ethdb.AncientReaderOp, id uint64) common.Hash {
	blob, err := db.Ancient(freezerReverseDiffHashTable, id-1)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(blob)
}

// WriteReverseDiff writes the provided reverse diff to database. Calculate the
// real position of reverse diff in freezer by minus one since the first reverse
// diff is started from one(zero for empty state).
func WriteReverseDiff(db ethdb.AncientWriter, id uint64, blob []byte, state common.Hash) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(freezerReverseDiffTable, id-1, blob)
		op.AppendRaw(freezerReverseDiffHashTable, id-1, state.Bytes())
		return nil
	})
}

// ReadReverseDiffLookup retrieves the reverse diff id with the given associated
// state root. Return nil if it's not existent.
func ReadReverseDiffLookup(db ethdb.KeyValueReader, root common.Hash) *uint64 {
	data, err := db.Get(reverseDiffLookupKey(root))
	if err != nil || len(data) == 0 {
		return nil
	}
	id := binary.BigEndian.Uint64(data)
	return &id
}

// WriteReverseDiffLookup writes the provided reverse diff lookup to database.
func WriteReverseDiffLookup(db ethdb.KeyValueWriter, root common.Hash, id uint64) {
	var buff [8]byte
	binary.BigEndian.PutUint64(buff[:], id)
	if err := db.Put(reverseDiffLookupKey(root), buff[:]); err != nil {
		log.Crit("Failed to store reverse diff lookup", "err", err)
	}
}

// DeleteReverseDiffLookup deletes the specified reverse diff lookup from the database.
func DeleteReverseDiffLookup(db ethdb.KeyValueWriter, root common.Hash) {
	if err := db.Delete(reverseDiffLookupKey(root)); err != nil {
		log.Crit("Failed to delete reverse diff lookup", "err", err)
	}
}

// ReadReverseDiffHead retrieves the number of the latest reverse diff from
// the database.
func ReadReverseDiffHead(db ethdb.KeyValueReader) uint64 {
	data, _ := db.Get(ReverseDiffHeadKey)
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// WriteReverseDiffHead stores the number of the latest reverse diff id
// into database.
func WriteReverseDiffHead(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Put(ReverseDiffHeadKey, encodeBlockNumber(number)); err != nil {
		log.Crit("Failed to store the head reverse diff id", "err", err)
	}
}

// ReadTrieJournal retrieves the serialized in-memory trie node diff layers saved at
// the last shutdown. The blob is expected to be max a few 10s of megabytes.
func ReadTrieJournal(db ethdb.KeyValueReader) []byte {
	data, _ := db.Get(triesJournalKey)
	return data
}

// WriteTrieJournal stores the serialized in-memory trie node diff layers to save at
// shutdown. The blob is expected to be max a few 10s of megabytes.
func WriteTrieJournal(db ethdb.KeyValueWriter, journal []byte) {
	if err := db.Put(triesJournalKey, journal); err != nil {
		log.Crit("Failed to store tries journal", "err", err)
	}
}

// DeleteTrieJournal deletes the serialized in-memory trie node diff layers saved at
// the last shutdown
func DeleteTrieJournal(db ethdb.KeyValueWriter) {
	if err := db.Delete(triesJournalKey); err != nil {
		log.Crit("Failed to remove tries journal", "err", err)
	}
}
