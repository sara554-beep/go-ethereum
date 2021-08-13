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
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// StateRecord represents a record for each commit operation(trie.Database)
// which includes the keys of the written dirty nodes and the relevant meta
// data. Commit operation writes all the trie nodes in the memory of the state
// to the database. These nodes can be regarded as an update to the state,
// so that the historical trie nodes that have the same path as the written
// node can be replaced and become stale.
type StateRecord struct {
	db ethdb.KeyValueStore

	// Meta info of the committed state
	Hash   common.Hash // The block hash of the committed state
	Number uint64      // The block number of the committed state
	Root   common.Hash // The account trie root of the committed state

	// The key list of the flushed dirty trie nodes. They can be used for
	// deriving the stale node keys for pruning purpooses. All the dirty
	// trie nodes are flushed from the bottom to top. The keys here are
	// also sorted in this order.
	// TODO: the keys can be stored in more efficient format.
	Keys [][]byte
}

func newCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash, root common.Hash) *StateRecord {
	return &StateRecord{
		db:     db,
		Hash:   hash,
		Number: number,
		Root:   root,
	}
}

func loadCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash) (*StateRecord, error) {
	blob := rawdb.ReadCommitRecord(db, number, hash)
	if len(blob) == 0 {
		return nil, errors.New("non-existent record")
	}
	var object StateRecord
	if err := rlp.DecodeBytes(blob, &object); err != nil {
		return nil, err
	}
	object.db = db
	return &object, nil
}

func (record *StateRecord) add(key []byte) {
	if len(key) != accountTrieNodekeyLength && len(key) != storageTrieNodeKeyLength {
		panic(fmt.Sprintf("invalid key(%d) %v", len(key), key))
	}
	record.Keys = append(record.Keys, key)
}

func (record *StateRecord) save() error {
	if len(record.Keys) == 0 {
		return nil
	}
	// Insert the key into keyset in reverse order, it's a key step
	// since the trie nodes are flushed from the bottom to top, so
	// the deletion can be done in reverse order.
	for i := len(record.Keys)/2 - 1; i >= 0; i-- {
		opp := len(record.Keys) - 1 - i
		record.Keys[i], record.Keys[opp] = record.Keys[opp], record.Keys[i]
	}
	blob, err := rlp.EncodeToBytes(record)
	if err != nil {
		return err
	}
	rawdb.WriteCommitRecord(record.db, record.Number, record.Hash, blob)
	log.Info("Committed state record", "number", record.Number, "hash", record.Hash.Hex(), "keys", len(record.Keys))

	record.Keys = nil // Release the keys
	return nil
}

// process is the function for deriving stale nodes and make the deletion.
// Since the entire proceduce can take a few minutes, so this function be
// be interrupted and resumed in order to not block the chain activities.
func (record *StateRecord) process(db *Database, genesis map[string]struct{}, noprune []common.Hash, remove func(*StateRecord, ethdb.KeyValueStore)) error {
	var (
		tries []*traverser
		batch = record.db.NewBatch()
		stats = newPruningStats(record.Number, record.Hash, len(record.Keys))
	)
	db.lock.RLock()

	// Initialises the trie traversers for liveness check, ensure all the
	// deleted trie nodes are not referenced by the target tries anymore.
	for key := range db.dirties[metaRoot].children {
		_, _, hash := DecodeNodeKey([]byte(key))
		tries = append(tries, &traverser{
			db:    db,
			state: &traverserState{hash: hash, node: hashNode(hash[:])},
		})
	}
	for _, hash := range noprune {
		tries = append(tries, &traverser{
			db:    db,
			state: &traverserState{hash: hash, node: hashNode(hash[:])},
		})
	}
	log.Info("Initialised tries for liveness check", "number", len(tries), "live", len(db.dirties[metaRoot].children), "noprune", len(noprune))

	// Iterate the database for retrieveing the stale nodes in the same path
	// scope and ensure they are not referenced by any target tries.
	for index, key := range record.Keys {
		stats.report("Processing state record", index, false)

		owner, path, hash := DecodeNodeKey(key)
		stales := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, path), func(key []byte) bool {
			if _, ok := genesis[string(key)]; ok {
				return true
			}
			keyOwner, keyPath, keyHash := DecodeNodeKey(key)
			return !bytes.Equal(path, keyPath) || owner != keyOwner || hash == keyHash
		})
		stats.read += len(stales)

		for _, stale := range stales {
			keyOwner, keyPath, keyHash := DecodeNodeKey(stale)
			crosspath := keyPath
			if keyOwner != (common.Hash{}) {
				crosspath = append(append(keybytesToHex(keyOwner[:]), 0xff), crosspath...)
			}
			var skip bool
			for _, trie := range tries {
				if trie.live(keyOwner, keyHash, crosspath, stats) {
					skip = true
					break
				}
			}
			if skip {
				stats.referenced += 1
				continue
			}
			if blob := rawdb.ReadTrieNode(record.db, stale); len(blob) == 0 {
				log.Info("The deleted key is not present", "key", stale)
				continue
			}
			rawdb.DeleteTrieNode(batch, stale)
			stats.deleted += 1

			if batch.ValueSize() > ethdb.IdealBatchSize {
				if err := batch.Write(); err != nil {
					panic("failed to write batch")
				}
				batch.Reset()
			}
		}
	}
	db.lock.RUnlock()

	remove(record, record.db)
	if err := batch.Write(); err != nil {
		return err
	}
	stats.report("Processed state record", len(record.Keys), true)
	return nil
}

type pruningStats struct {
	number uint64
	hash   common.Hash
	start  time.Time
	last   time.Time

	// General statistics
	keys       int // The count of keys in the state record
	read       int // The count of loaded trie nodes from the disk
	referenced int // The count of referenced trie nodes
	deleted    int // The count of deleted trie nodes

	// Liveness check statistics
	clean int // The count of nodes hit in the clean cache
	dirty int // The count of nodes hit in the dirty cache
	disk  int // The count of nodes hit in the database
	miss  int // The count of missing nodes
}

func newPruningStats(number uint64, hash common.Hash, keys int) *pruningStats {
	return &pruningStats{
		number: number,
		hash:   hash,
		start:  time.Now(),
		keys:   keys,
	}
}

func (stats *pruningStats) report(msg string, processed int, force bool) {
	if !force && time.Since(stats.last) < time.Second*8 {
		return
	}
	var ctx []interface{}
	ctx = append(ctx, []interface{}{
		"number", stats.number, "hash", stats.hash,
		"diskread", stats.read, "referenced", stats.referenced, "deleted", stats.deleted,
		"clean", stats.clean, "dirty", stats.dirty, "disk", stats.disk, "miss", stats.miss,
		"progress", float64(processed) * 100 / float64(stats.keys),
		"elasped", common.PrettyDuration(time.Since(stats.start)),
	}...)
	log.Info(msg, ctx...)
}

type commitRecordsByNumber []*StateRecord

func (t commitRecordsByNumber) Len() int           { return len(t) }
func (t commitRecordsByNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t commitRecordsByNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }
