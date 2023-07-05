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

// ReadStateID retrieves the state id with the provided state root.
func ReadStateID(db ethdb.KeyValueReader, root common.Hash) (uint64, bool) {
	data, err := db.Get(stateIDKey(root))
	if err != nil || len(data) == 0 {
		return 0, false
	}
	return binary.BigEndian.Uint64(data), true
}

// WriteStateID writes the provided state lookup to database.
func WriteStateID(db ethdb.KeyValueWriter, root common.Hash, id uint64) {
	var buff [8]byte
	binary.BigEndian.PutUint64(buff[:], id)
	if err := db.Put(stateIDKey(root), buff[:]); err != nil {
		log.Crit("Failed to store state ID", "err", err)
	}
}

// DeleteStateID deletes the specified state lookup from the database.
func DeleteStateID(db ethdb.KeyValueWriter, root common.Hash) {
	if err := db.Delete(stateIDKey(root)); err != nil {
		log.Crit("Failed to delete state ID", "err", err)
	}
}

// ReadPersistentStateID retrieves the id of the disk state from the database.
func ReadPersistentStateID(db ethdb.KeyValueReader) uint64 {
	data, _ := db.Get(persistentStateIDKey)
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// WritePersistentStateID stores the id of the disk state into database.
func WritePersistentStateID(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Put(persistentStateIDKey, encodeBlockNumber(number)); err != nil {
		log.Crit("Failed to store the persistent state ID", "err", err)
	}
}

// ReadStateIndex retrieves the state index with the provided owner and state hash.
func ReadStateIndex(db ethdb.KeyValueReader, owner common.Hash, state common.Hash) []byte {
	data, err := db.Get(stateIndexKey(owner, state))
	if err != nil || len(data) == 0 {
		return nil
	}
	return data
}

// WriteStateIndex writes the provided state lookup to database.
func WriteStateIndex(db ethdb.KeyValueWriter, owner common.Hash, state common.Hash, data []byte) {
	if err := db.Put(stateIndexKey(owner, state), data); err != nil {
		log.Crit("Failed to store state index", "err", err)
	}
}

// DeleteStateIndex deletes the specified state lookup from the database.
func DeleteStateIndex(db ethdb.KeyValueWriter, owner common.Hash, state common.Hash) {
	if err := db.Delete(stateIndexKey(owner, state)); err != nil {
		log.Crit("Failed to delete state index", "err", err)
	}
}

// ReadStateIndexBlock retrieves the state index with the provided owner and state hash.
func ReadStateIndexBlock(db ethdb.KeyValueReader, owner common.Hash, state common.Hash, id uint32) []byte {
	data, err := db.Get(stateIndexBlockKey(owner, state, id))
	if err != nil || len(data) == 0 {
		return nil
	}
	return data
}

// WriteStateIndexBlock writes the provided state lookup to database.
func WriteStateIndexBlock(db ethdb.KeyValueWriter, owner common.Hash, state common.Hash, id uint32, data []byte) {
	if err := db.Put(stateIndexBlockKey(owner, state, id), data); err != nil {
		log.Crit("Failed to store state index", "err", err)
	}
}

// DeleteStateIndexBlock deletes the specified state lookup from the database.
func DeleteStateIndexBlock(db ethdb.KeyValueWriter, owner common.Hash, state common.Hash, id uint32) {
	if err := db.Delete(stateIndexBlockKey(owner, state, id)); err != nil {
		log.Crit("Failed to delete state index", "err", err)
	}
}

// ReadTrieJournal retrieves the serialized in-memory trie node diff layers saved at
// the last shutdown. The blob is expected to be max a few 10s of megabytes.
func ReadTrieJournal(db ethdb.KeyValueReader) []byte {
	data, _ := db.Get(trieJournalKey)
	return data
}

// WriteTrieJournal stores the serialized in-memory trie node diff layers to save at
// shutdown. The blob is expected to be max a few 10s of megabytes.
func WriteTrieJournal(db ethdb.KeyValueWriter, journal []byte) {
	if err := db.Put(trieJournalKey, journal); err != nil {
		log.Crit("Failed to store tries journal", "err", err)
	}
}

// DeleteTrieJournal deletes the serialized in-memory trie node diff layers saved at
// the last shutdown
func DeleteTrieJournal(db ethdb.KeyValueWriter) {
	if err := db.Delete(trieJournalKey); err != nil {
		log.Crit("Failed to remove tries journal", "err", err)
	}
}

// WriteStateHistory writes the provided state history to database. Calculate the
// real position of state history in freezer by minus one since the first state
// history is started from one(zero for empty state).
func WriteStateHistory(db ethdb.AncientWriter, id uint64, meta []byte, accountIndex []byte, storageIndex []byte, accounts []byte, storages []byte) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(stateHistoryMeta, id-1, meta)
		op.AppendRaw(stateHistoryAccountIndex, id-1, accountIndex)
		op.AppendRaw(stateHistoryStorageIndex, id-1, storageIndex)
		op.AppendRaw(stateHistoryAccountData, id-1, accounts)
		op.AppendRaw(stateHistoryStorageData, id-1, storages)
		return nil
	})
}

// ReadStateHistoryMeta retrieves the metadata corresponding to the specified
// state history. Calculate the real position of state history in freezer by minus
// one since the first state history is started from one(zero for empty state).
func ReadStateHistoryMeta(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryMeta, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateHistoryMetaList retrieves a batch of meta objects with the specified
// start position and count. Calculate the real position of state history in freezer
// by minus one since the first state history is started from one(zero for empty
// state).
func ReadStateHistoryMetaList(db ethdb.AncientReaderOp, start uint64, count uint64) ([][]byte, error) {
	return db.AncientRange(stateHistoryMeta, start-1, count, 0)
}

// ReadStateAccountIndex retrieves the state root corresponding to the specified
// state history. Calculate the real position of state history in freezer by minus
// one since the first state history is started from one(zero for empty state).
func ReadStateAccountIndex(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryAccountIndex, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateStorageIndex retrieves the state root corresponding to the specified
// state history. Calculate the real position of state history in freezer by minus
// one since the first state history is started from one(zero for empty state).
func ReadStateStorageIndex(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryStorageIndex, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateAccountHistory retrieves the state root corresponding to the specified
// state history. Calculate the real position of state history in freezer by minus
// one since the first state history is started from one(zero for empty state).
func ReadStateAccountHistory(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryAccountData, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateStorageHistory retrieves the state root corresponding to the specified
// state history. Calculate the real position of state history in freezer by minus
// one since the first state history is started from one(zero for empty state).
func ReadStateStorageHistory(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryStorageData, id-1)
	if err != nil {
		return nil
	}
	return blob
}

func ReadStateHistoryList(db ethdb.AncientReaderOp, start uint64, count uint64) ([][]byte, [][]byte, [][]byte, [][]byte, [][]byte, error) {
	metaList, err := db.AncientRange(stateHistoryMeta, start-1, count, 0)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	aIndexList, err := db.AncientRange(stateHistoryAccountIndex, start-1, count, 0)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	sIndexList, err := db.AncientRange(stateHistoryStorageIndex, start-1, count, 0)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	aDataList, err := db.AncientRange(stateHistoryAccountData, start-1, count, 0)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	sDataList, err := db.AncientRange(stateHistoryStorageData, start-1, count, 0)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return metaList, aIndexList, sIndexList, aDataList, sDataList, nil
}
