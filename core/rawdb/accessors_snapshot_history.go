package rawdb

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// WriteStateHistory writes the provided state history to database. Calculate the
// real position of state history in freezer by minus one since the first state
// history is started from one(zero for empty state).
func WriteStateHistory(db ethdb.AncientWriter, id uint64, accountIndex []byte, storageIndex []byte, accounts []byte, storages []byte) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(stateHistoryAccountIndex, id-1, accountIndex)
		op.AppendRaw(stateHistoryStorageIndex, id-1, storageIndex)
		op.AppendRaw(stateHistoryAccountData, id-1, accounts)
		op.AppendRaw(stateHistoryStorageData, id-1, storages)
		return nil
	})
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

// ReadAccountIndex retrieves the account index and the associated node
// hash with the specified node path.
func ReadAccountIndex(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, err := db.Get(accountIndexNodeKey(hash))
	if err != nil {
		return nil
	}
	return data
}

// HasAccountIndex checks the account index presence with the specified
// node path and the associated node hash.
func HasAccountIndex(db ethdb.KeyValueReader, hash common.Hash) bool {
	data, err := db.Get(accountIndexNodeKey(hash))
	if err != nil {
		return false
	}
	return len(data) != 0
}

// WriteAccountIndex writes the provided account index into database.
func WriteAccountIndex(db ethdb.KeyValueWriter, hash common.Hash, data []byte) {
	if err := db.Put(accountIndexNodeKey(hash), data); err != nil {
		log.Crit("Failed to store account index", "err", err)
	}
}

// DeleteAccountIndex deletes the specified account index from the database.
func DeleteAccountIndex(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(accountIndexNodeKey(hash)); err != nil {
		log.Crit("Failed to delete account index", "err", err)
	}
}

// ReadStorageIndex retrieves the storage index and the associated node
// hash with the specified node path.
func ReadStorageIndex(db ethdb.KeyValueReader, accountHash, storageHash common.Hash) []byte {
	data, err := db.Get(storageIndexNodeKey(accountHash, storageHash))
	if err != nil {
		return nil
	}
	return data
}

// HasStorageIndex checks the storage index presence with the specified
// node path and the associated node hash.
func HasStorageIndex(db ethdb.KeyValueReader, accountHash, storageHash common.Hash) bool {
	data, err := db.Get(storageIndexNodeKey(accountHash, storageHash))
	if err != nil {
		return false
	}
	return len(data) != 0
}

// WriteStorageIndex writes the provided storage index into database.
func WriteStorageIndex(db ethdb.KeyValueWriter, accountHash, storageHash common.Hash, data []byte) {
	if err := db.Put(storageIndexNodeKey(accountHash, storageHash), data); err != nil {
		log.Crit("Failed to store storage index", "err", err)
	}
}

// DeleteStorageIndex deletes the specified storage index from the database.
func DeleteStorageIndex(db ethdb.KeyValueWriter, accountHash, storageHash common.Hash) {
	if err := db.Delete(storageIndexNodeKey(accountHash, storageHash)); err != nil {
		log.Crit("Failed to delete storage index", "err", err)
	}
}
