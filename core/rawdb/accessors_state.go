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
	"github.com/ethereum/go-ethereum/crypto"
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

// ReadTrieNode retrieves the trie node and the associated node hash of
// the provided node key.
func ReadTrieNode(db ethdb.KeyValueReader, key []byte) ([]byte, common.Hash) {
	data, err := db.Get(trieNodeKey(key))
	if err != nil {
		return nil, common.Hash{}
	}
	return data, crypto.Keccak256Hash(data) // TODO use hasher pool to reduce allocation
}

// WriteTrieNode writes the provided trie node database.
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

// ReadArchiveTrieNode retrieves the archive trie node with the given
// associated node hash.
func ReadArchiveTrieNode(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, err := db.Get(hash.Bytes())
	if err != nil {
		return nil
	}
	return data
}

// WriteArchiveTrieNode writes the provided archived trie node to database.
func WriteArchiveTrieNode(db ethdb.KeyValueWriter, hash common.Hash, node []byte) {
	if err := db.Put(hash.Bytes(), node); err != nil {
		log.Crit("Failed to store archived trie node", "err", err)
	}
}

// DeleteArchiveTrieNode deletes the specified archived trie node from the database.
func DeleteArchiveTrieNode(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(hash.Bytes()); err != nil {
		log.Crit("Failed to delete archived trie node", "err", err)
	}
}

// ReadShadowTrieNode retrieves the shadowy trie node with the given
// associated node hash. The additional flag is returned to represent
// the node is non-existent or just has empty value.
func ReadShadowTrieNode(db ethdb.KeyValueReader, id []byte, key []byte) ([]byte, common.Hash, bool) {
	data, err := db.Get(shadowTrieNodeKey(id, key))
	if err != nil {
		return nil, common.Hash{}, false
	}
	return data, crypto.Keccak256Hash(data), true
}

// WriteShadowTrieNode writes the provided shadowy trie node to database.
func WriteShadowTrieNode(db ethdb.KeyValueWriter, id []byte, key []byte, node []byte) {
	if err := db.Put(shadowTrieNodeKey(id, key), node); err != nil {
		log.Crit("Failed to store archived trie node", "err", err)
	}
}

// DeleteShadowTrieNodes deletes all the shadowy trie nodes under the given namespace
// from the disk.
func DeleteShadowTrieNodes(db ethdb.KeyValueStore, id []byte) {
	var (
		batch = db.NewBatch()
		iter  = db.NewIterator(append(ShadowTrieNodePrefix, id...), nil)
	)
	for iter.Next() {
		batch.Delete(iter.Key())
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				log.Crit("Failed to delete shadow nodes", "err", err)
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil {
		log.Crit("Failed to delete shadow nodes", "err", err)
	}
	iter.Release()
}

// ReadReverseDiff retrieves the state reverse diff with the given associated
// block hash and number.
func ReadReverseDiff(db ethdb.AncientReader, id uint64) []byte {
	blob, err := db.Ancient(reverseDiffFreezer, freezerReverseDiffTable, id-1)
	if err != nil {
		return nil
	}
	return blob
}

// WriteReverseDiff writes the provided reverse diff to database.
func WriteReverseDiff(db ethdb.AncientWriter, id uint64, blob []byte) {
	db.ModifyAncients(reverseDiffFreezer, func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(freezerReverseDiffTable, id-1, blob)
		return nil
	})
}

// DeleteReverseDiff deletes the specified reverse diff from the database.
func DeleteReverseDiff(db ethdb.KeyValueWriter, id uint64) {
	//if err := db.Delete(ReverseDiffKey(id)); err != nil {
	//	log.Crit("Failed to delete reverse diff", "err", err)
	//}
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

// ReadReverseDiffHead retrieves the number of latest reverse diff from
// the database.
func ReadReverseDiffHead(db ethdb.KeyValueReader) *uint64 {
	data, _ := db.Get(ReverseDiffHeadKey)
	if len(data) != 8 {
		return nil
	}
	number := binary.BigEndian.Uint64(data)
	return &number
}

// WriteReverseDiffHead stores the number of latest reverse diff id
// into database.
func WriteReverseDiffHead(db ethdb.KeyValueWriter, number uint64) {
	if err := db.Put(ReverseDiffHeadKey, encodeBlockNumber(number)); err != nil {
		log.Crit("Failed to store the head reverse diff id", "err", err)
	}
}
