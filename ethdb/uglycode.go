package ethdb

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// IterateReferences returns all node reference entries belongs to the
// given node.
func IterateReferences(db *LDBDatabase, hash common.Hash) [][]byte {
	lvldb := db.LDB()
	iter := lvldb.NewIterator(util.BytesPrefix(hash.Bytes()), nil)
	defer iter.Release()

	var references [][]byte
	for iter.Next() {
		if len(iter.Key()) == common.HashLength {
			continue
		}
		references = append(references, common.CopyBytes(iter.Key()[common.HashLength:]))
	}
	return references
}
