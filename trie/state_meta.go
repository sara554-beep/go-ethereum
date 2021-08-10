// Copyright 2021 The go-ethereum Authors
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

package trie

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// CommitRecord represents a diff set for each Commit operation(trie.Database)
// which occurs regularly at a certain time interval. It will flush out all the
// dirty nodes compared with the latest flushed state so that it can be regarded
// as the state update. The flushed trie node keys can be used as the indicator
// for deriving a list of stale trie nodes in the same path scope and these stale
// trie nodes can be pruned later from the disk.
type CommitRecord struct {
	db     ethdb.KeyValueStore
	hash   common.Hash
	number uint64

	// The key list of the flushed dirty trie nodes. They are used to derive
	// the stale node keys for pruning purposes. All the dirty trie nodes are
	// flushed from the bottom to top. The keys here are also sorted in this order.
	Keys [][]byte

	// The key list of the stale trie nodes which can be deleted from the disk
	// later if the Commit operation has enough confirmation(prevent deep reorg).
	//
	// Note the key of the trie node is not trivial(around 100 bytes in average),
	// but most of them have the shared key prefix and compressed zero bytes. The
	// optimization can be applied here in order to improve the space efficiency.
	// All the key in the deletion set should be unique.
	//
	// Export fields for RLP encoding/decoding.
	DeletionSet [][]byte
	PartialKeys [][]byte
	CleanKeys   [][]byte

	// Test hooks
	onDeletionSet func([][]byte) // Hooks used for exposing the generated deletion set
}

func newCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash) *CommitRecord {
	return &CommitRecord{
		db:     db,
		hash:   hash,
		number: number,
	}
}

func readCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash) (*CommitRecord, error) {
	blob := rawdb.ReadCommitRecord(db, number, hash, false)
	if len(blob) == 0 {
		return nil, errors.New("non-existent record")
	}
	var object CommitRecord
	if err := rlp.DecodeBytes(blob, &object); err != nil {
		return nil, err
	}
	object.db = db
	object.number = number
	object.hash = hash
	return &object, nil
}

func (record *CommitRecord) add(key []byte) {
	if len(key) != 2*common.HashLength+1 && len(key) != 3*common.HashLength+1 {
		log.Warn("Invalid key length", "len", len(key), "key", key)
		return // It should never happen
	}
	record.Keys = append(record.Keys, key)
}

type genstack struct {
	owner   common.Hash
	bottoms [][]byte
}

func (stack *genstack) push(path []byte) [][]byte {
	var dropped [][]byte
	var filtered = stack.bottoms[:0]
	for _, p := range stack.bottoms {
		if bytes.HasPrefix(p, path) {
			dropped = append(dropped, p)
		} else {
			filtered = append(filtered, p)
		}
	}
	stack.bottoms = filtered
	stack.bottoms = append(stack.bottoms, path)
	return dropped
}

func (record *CommitRecord) finalize(partialKeys [][]byte, cleanKeys [][]byte) (int, int, bool, error) {
	var (
		// Statistic
		iterated     uint64
		filtered     uint64
		read         uint64
		startTime    = time.Now()
		logged       time.Time
		newDuration  time.Duration
		iterDuration time.Duration
	)
	for index, key := range record.Keys {
		owner, path, hash := DecodeNodeKey(key)
		if time.Since(logged) > time.Second*8 {
			log.Info("Iterating database", "iterated", iterated, "read", read,
				"newDuration", common.PrettyDuration(newDuration), "iterDuration", common.PrettyDuration(iterDuration),
				"keyIndex", index, "remaining", len(record.Keys)-index, "elasped", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
		keys, _, count, newElapsed, iterElapsed := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, path), func(key []byte) bool {
			atomic.AddUint64(&iterated, 1)
			o, p, h := DecodeNodeKey(key)
			if !bytes.Equal(path, p) {
				return true
			}
			if o != owner {
				return true
			}
			if h == hash {
				return true
			}
			return false
		})

		read += uint64(count)
		newDuration += newElapsed
		iterDuration += iterElapsed

		for _, key := range keys {
			record.DeletionSet = append(record.DeletionSet, key)
		}
	}
	var (
		blob []byte
		err  error
		ok   bool
	)
	if len(record.DeletionSet) != 0 {
		record.PartialKeys = partialKeys
		record.CleanKeys = cleanKeys
		blob, err = rlp.EncodeToBytes(record)
		if err != nil {
			return 0, 0, false, err
		}
		log.Info("Try to persist commit record", "number", record.number, "hash", record.hash)
		rawdb.WriteCommitRecord(record.db, record.number, record.hash, blob)
		ok = true
	}
	log.Info("Written commit metadata", "key", len(record.Keys), "part", len(record.PartialKeys), "clean", len(record.CleanKeys), "stale", len(record.DeletionSet),
		"filter", filtered, "average", float64(iterated)/float64(len(record.Keys)), "metasize", len(blob), "elasped", common.PrettyDuration(time.Since(startTime)))

	if record.onDeletionSet != nil {
		record.onDeletionSet(record.DeletionSet)
	}
	record.DeletionSet, record.Keys, record.PartialKeys, record.CleanKeys = nil, nil, nil, nil
	return int(iterated), int(filtered), ok, nil
}

func (record *CommitRecord) deleteStale(db *Database, remove func(*CommitRecord, ethdb.KeyValueStore)) error {
	var (
		startTime = time.Now()
		batch     = record.db.NewBatch()
		tries     []*traverser // Individual trie traversers for liveness checks
		checks    uint64
		refed     uint64
		deleted   uint64
	)
	db.lock.RLock()
	for key := range db.dirties[metaRoot].children {
		owner, path, hash := DecodeNodeKey([]byte(key))
		if owner != (common.Hash{}) {
			log.Crit("Invalid root node", "owner", owner.Hex(), "path", path, "hash", hash.Hex())
		}
		if len(path) != 0 {
			log.Crit("Invalid root node", "owner", owner.Hex(), "path", path, "hash", hash.Hex())
		}
		tries = append(tries, &traverser{
			db:    db,
			state: &traverserState{hash: hash, node: hashNode(hash[:])},
		})
	}
	log.Info("Setup live trie traverses", "number", len(db.dirties[metaRoot].children))
	for index, key := range record.DeletionSet {
		owner, path, hash := DecodeNodeKey(key)
		// Iterate over all the live tries and check node liveliness
		crosspath := path
		if owner != (common.Hash{}) {
			crosspath = append(append(keybytesToHex(owner[:]), 0xff), crosspath...)
		}
		var skip bool
		for _, trie := range tries {
			checks += 1
			if trie.live(owner, hash, crosspath) {
				skip = true
				break
			}
		}
		if skip {
			refed += 1
			continue
		}
		if blob := rawdb.ReadTrieNode(record.db, key); len(blob) == 0 {
			log.Info("The deleted key is not present", "key", key)
			continue
		}
		rawdb.DeleteTrieNode(batch, key)
		atomic.AddUint64(&deleted, 1)

		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				panic("failed to write batch")
			}
			batch.Reset()
		}
		if index%50000 == 0 {
			log.Info("Pruning stale trie nodes", "checks", checks, "deleted", index, "elasped", common.PrettyDuration(time.Since(startTime)))
		}
	}
	db.lock.RUnlock()
	remove(record, record.db)
	if err := batch.Write(); err != nil {
		return err
	}
	log.Info("Pruned stale trie nodes", "number", record.number, "hash", record.hash, "checks", checks, "referenced", refed, "deleted", deleted, "elapsed", common.PrettyDuration(time.Since(startTime)))
	return nil
}

type commitRecordsByNumber []*CommitRecord

func (t commitRecordsByNumber) Len() int           { return len(t) }
func (t commitRecordsByNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t commitRecordsByNumber) Less(i, j int) bool { return t[i].number < t[j].number }
