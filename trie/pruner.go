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
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// resurrectionMarker is a delete-preventing marker of a trie node. It contains
// a list of commit record identifiers to prevent deletion by the corresponding
// pruning operation of these commit records.
//
// This design is to solve the problem of state resurrection, e.g.
// - node A is written in the commit operation X
// - node A then is tagged as stale in the commit opeation Y, so that it's in the
//   deletion set of Y
// - node A is resurrected in the commit operation Z and re-written into the
//   disk again.
// In this case if we process the commit record Y after flushing the node A, the
// live node A will be deleted by mistake.
//
// So before flushing trie nodes into the disk, the presence in deletion set will
// be checked and if so the relevant commit record id will be saved in the marker
// so that the corresponding commit record pruning operation can't delete this node.
//
// e.g. for node A, the no-deletion marker contain these records:
// [<number = 1, hash = xxx>, <number = 100, hash = yyy>], the node A won't be deleted
// by pruning operation of <n=1, hash=xxx> and <n=100, hash=yyy>, but it can be deleted
// by other pruning operations.
//
// If the marker doesn't contain any commit record id, the relevant database entry
// should be removed.
type resurrectionMarker struct {
	Numbers []uint64
	Hashes  []common.Hash
}

func readResurrectionMarker(reader ethdb.KeyValueReader, key []byte) (*resurrectionMarker, error) {
	blob := rawdb.ReadResurrectionMarker(reader, key)
	if len(blob) == 0 {
		return nil, nil
	}
	var marker resurrectionMarker
	if err := rlp.DecodeBytes(blob, &marker); err != nil {
		return nil, err
	}
	return &marker, nil
}

func updateResurrectionMarker(reader ethdb.KeyValueReader, writer ethdb.KeyValueWriter, key []byte, number uint64, hash common.Hash, add bool) error {
	var marker resurrectionMarker
	blob := rawdb.ReadResurrectionMarker(reader, key)
	if len(blob) != 0 {
		if err := rlp.DecodeBytes(blob, &marker); err != nil {
			return err
		}
	}
	// Sanity checks the presense or nonexistence of the entry. TODO(rjl493456442)
	if add {
		marker.Numbers = append(marker.Numbers, number)
		marker.Hashes = append(marker.Hashes, hash)
	} else {
		for i, h := range marker.Hashes {
			if h == hash && marker.Numbers[i] == number {
				marker.Numbers = append(marker.Numbers[:i], marker.Numbers[i+1:]...)
				marker.Hashes = append(marker.Hashes[:i], marker.Hashes[i+1:]...)
				break
			}
		}
	}
	if len(marker.Hashes) != 0 {
		blob, err := rlp.EncodeToBytes(marker)
		if err != nil {
			return err
		}
		rawdb.WriteResurrectionMarker(writer, key, blob)
	} else {
		rawdb.DeleteResurrectionMarker(writer, key)
	}
	return nil
}

// commitRecord represents a diff set for each Commit operation(trie.Database)
// which occurs regularly at a certain time interval. It will flush out all the
// dirty nodes compared with the latest flushed state so that it can be regarded
// as the state update. The flushed trie node keys can be used as the marker for
// deriving a list of stale trie nodes in the same path scope and these stale
// trie nodes can be pruned later from the disk.
type commitRecord struct {
	db     ethdb.KeyValueStore
	hash   common.Hash
	number uint64

	// The key list of the flushed dirty trie nodes. They are used to derive
	// the stale node keys for pruning purposes. All the dirty trie nodes are
	// flushed from the bottom to top. The keys here are also sorted in this order.
	keys [][]byte

	// The bloom filter of the stale node keys(a.k.a deletion set)
	bloom *keybloom

	// The key list of the stale trie nodes which can be deleted from the disk
	// later if the Commit operation has enough confirmation(prevent deep reorg).
	//
	// Note the key of the trie node is not trivial(around 100 bytes at most),
	// but most of them have the shared key prefix. The optimization can be applied
	// here in order to improve the space efficiency.
	//
	// Export fields for RLP encoding/decoding.
	DeletionSet [][]byte

	// Test hooks
	onDeletionSet func([][]byte) // Hooks used for exposing the generated deletion set
}

func newCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash) *commitRecord {
	return &commitRecord{
		db:     db,
		hash:   hash,
		number: number,
	}
}

func loadCommitRecord(number uint64, hash common.Hash, val []byte) (*commitRecord, error) {
	var object commitRecord
	if err := rlp.DecodeBytes(val, &object); err != nil {
		return nil, err
	}
	object.initBloom()
	object.DeletionSet = nil
	object.number = number
	object.hash = hash
	return &object, nil
}

func (record *commitRecord) add(key []byte) {
	if len(key) <= common.HashLength {
		log.Warn("Invalid key length", "len", len(key), "key", key)
		return // It should never happen
	}
	record.keys = append(record.keys, key)
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

func (record *commitRecord) finalize(noDelete *keybloom) error {
	// Derive the stale node keys from the added list as the deletion set.
	var (
		stack    *genstack
		filtered int
	)
	for _, key := range record.keys {
		owner, path, hash := DecodeNodeKey(key)

		// Scope changed, reset the stack context
		if stack != nil && stack.owner != owner {
			stack = nil
		}
		if stack == nil {
			stack = &genstack{owner: owner}
		}
		// Delete all other nodes with same node path
		keys, _ := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, path), func(key []byte) bool {
			if noDelete.contain(key) {
				filtered += 1
				return true
			}
			o, p, h := DecodeNodeKey(key)
			if o != owner {
				return true
			}
			if h == hash {
				return true
			}
			if !bytes.Equal(path, p) {
				return true
			}
			return false
		})
		for _, key := range keys {
			record.DeletionSet = append(record.DeletionSet, key)
		}
		// Push the path and pop all the children path, delete all intermidate nodes.
		children := stack.push(path)
		for _, child := range children {
			for i := len(path); i < len(child)-1; i++ {
				innerPath := append(path, child[len(path):i+1]...)
				keys, _ := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, innerPath), func(key []byte) bool {
					if noDelete.contain(key) {
						filtered += 1
						return true
					}
					o, p, _ := DecodeNodeKey(key)
					if o != owner {
						return true
					}
					if !bytes.Equal(innerPath, p) {
						return true
					}
					return false
				})
				for _, key := range keys {
					record.DeletionSet = append(record.DeletionSet, key)
				}
			}
		}
	}
	var (
		blob []byte
		err  error
	)
	if len(record.DeletionSet) != 0 {
		blob, err = rlp.EncodeToBytes(record)
		if err != nil {
			return err
		}
		rawdb.WriteCommitRecord(record.db, record.number, record.hash, blob)
	}
	record.initBloom()
	log.Info("Written commit metadata", "key", len(record.keys), "stale", len(record.DeletionSet), "filter", filtered, "metasize", len(blob))

	if record.onDeletionSet != nil {
		record.onDeletionSet(record.DeletionSet)
	}
	record.DeletionSet = nil
	record.keys = nil
	return nil
}

// initBloom initializes the bloom filter with the key set.
func (record *commitRecord) initBloom() {
	size := len(record.DeletionSet)
	if size == 0 {
		size = 1 // 0 size bloom filter is not allowed
	}
	bloom := newOptimalKeyBloom(uint64(size), 0.01)
	for _, key := range record.DeletionSet {
		bloom.add(key)
	}
	record.bloom = bloom
}

// contain reports if the given key is in the deletion set.
// It only checks the bloom filter but never check the real
// deletion set since the false-positive is totally fine.
func (record *commitRecord) contain(key []byte) bool {
	if record.bloom == nil {
		return true // In theory it shouldn't happen
	}
	return record.bloom.contain(key)
}

type commitRecordsByNumber []*commitRecord

func (t commitRecordsByNumber) Len() int           { return len(t) }
func (t commitRecordsByNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t commitRecordsByNumber) Less(i, j int) bool { return t[i].number < t[j].number }

type commitRecordManager struct {
	db      ethdb.KeyValueStore
	records []*commitRecord

	// statistic
	checked   uint64
	contained uint64
}

func newCommitRecordManager(db ethdb.KeyValueStore) *commitRecordManager {
	numbers, hashes, blobs := rawdb.ReadAllCommitRecords(db, 0, math.MaxUint64)
	var records []*commitRecord
	for i := 0; i < len(numbers); i++ {
		record, err := loadCommitRecord(numbers[i], hashes[i], blobs[i])
		if err != nil {
			rawdb.DeleteCommitRecord(db, numbers[i], hashes[i]) // Corrupted record can be just discarded
			continue
		}
		records = append(records, record)
	}
	sort.Sort(commitRecordsByNumber(records))
	return &commitRecordManager{db: db, records: records}
}

func (manager *commitRecordManager) addRecord(record *commitRecord) {
	manager.records = append(manager.records, record)
	sort.Sort(commitRecordsByNumber(manager.records))
	log.Info("Total commit records", len(manager.records), "totalchecked", manager.checked, "existence", manager.contained)
}

func (manager *commitRecordManager) invalidateDeletion(key []byte, writer ethdb.KeyValueWriter) error {
	var found bool
	for _, record := range manager.records {
		if record.contain(key) {
			err := updateResurrectionMarker(manager.db, writer, key, record.number, record.hash, true)
			if err != nil {
				return err
			}
			found = true
		}
	}
	manager.checked += 1
	if found {
		manager.contained += 1
	}
	return nil
}
