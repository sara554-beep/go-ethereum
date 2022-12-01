package rawdb

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// WriteStateHistory writes the provided reverse diff to database. Calculate the
// real position of reverse diff in freezer by minus one since the first reverse
// diff is started from one(zero for empty state).
func WriteStateHistory(db ethdb.AncientWriter, id uint64, aIndex []byte, sIndex []byte, aData []byte, sData []byte) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(stateHistoryAccountIndex, id-1, aIndex)
		op.AppendRaw(stateHistoryStorageIndex, id-1, sIndex)
		op.AppendRaw(stateHistoryAccountData, id-1, aData)
		op.AppendRaw(stateHistoryStorageData, id-1, sData)
		return nil
	})
}

// ReadStateAccountIndex retrieves the state root corresponding to the specified
// reverse diff. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadStateAccountIndex(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryAccountIndex, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateStorageIndex retrieves the state root corresponding to the specified
// reverse diff. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadStateStorageIndex(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryStorageIndex, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateAccountData retrieves the state root corresponding to the specified
// reverse diff. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadStateAccountData(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryAccountData, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadStateStorageData retrieves the state root corresponding to the specified
// reverse diff. Calculate the real position of reverse diff in freezer by minus
// one since the first reverse diff is started from one(zero for empty state).
func ReadStateStorageData(db ethdb.AncientReaderOp, id uint64) []byte {
	blob, err := db.Ancient(stateHistoryStorageData, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// ReadAccountIndex retrieves the account trie node and the associated node
// hash with the specified node path.
func ReadAccountIndex(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, err := db.Get(accountIndexNodeKey(hash))
	if err != nil {
		return nil
	}
	return data
}

// HasAccountIndex checks the account trie node presence with the specified
// node path and the associated node hash.
func HasAccountIndex(db ethdb.KeyValueReader, hash common.Hash) bool {
	data, err := db.Get(accountIndexNodeKey(hash))
	if err != nil {
		return false
	}
	return len(data) != 0
}

// WriteAccountIndex writes the provided account trie node into database.
func WriteAccountIndex(db ethdb.KeyValueWriter, hash common.Hash, data []byte) {
	if err := db.Put(accountIndexNodeKey(hash), data); err != nil {
		log.Crit("Failed to store account trie node", "err", err)
	}
}

// DeleteAccountIndex deletes the specified account trie node from the database.
func DeleteAccountIndex(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(accountIndexNodeKey(hash)); err != nil {
		log.Crit("Failed to delete account trie node", "err", err)
	}
}

// ReadStorageIndex retrieves the account trie node and the associated node
// hash with the specified node path.
func ReadStorageIndex(db ethdb.KeyValueReader, accountHash, hash common.Hash) []byte {
	data, err := db.Get(storageIndexNodeKey(accountHash, hash))
	if err != nil {
		return nil
	}
	return data
}

// HasStorageIndex checks the account trie node presence with the specified
// node path and the associated node hash.
func HasStorageIndex(db ethdb.KeyValueReader, accountHash, hash common.Hash) bool {
	data, err := db.Get(storageIndexNodeKey(accountHash, hash))
	if err != nil {
		return false
	}
	return len(data) != 0
}

// WriteStorageIndex writes the provided account trie node into database.
func WriteStorageIndex(db ethdb.KeyValueWriter, accountHash, hash common.Hash, data []byte) {
	if err := db.Put(storageIndexNodeKey(accountHash, hash), data); err != nil {
		log.Crit("Failed to store account trie node", "err", err)
	}
}

// DeleteStorageIndex deletes the specified account trie node from the database.
func DeleteStorageIndex(db ethdb.KeyValueWriter, accountHash, hash common.Hash) {
	if err := db.Delete(storageIndexNodeKey(accountHash, hash)); err != nil {
		log.Crit("Failed to delete account trie node", "err", err)
	}
}
