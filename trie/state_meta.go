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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
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

	// The bloom filter of the stale node keys(a.k.a deletion set)
	bloom *keybloom

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

func loadCommitRecord(db ethdb.KeyValueStore, number uint64, hash common.Hash, val []byte) (*CommitRecord, error) {
	var object CommitRecord
	if err := rlp.DecodeBytes(val, &object); err != nil {
		return nil, err
	}
	object.initBloom()
	object.DeletionSet = nil
	object.db = db
	object.number = number
	object.hash = hash
	return &object, nil
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

func (record *CommitRecord) finalize(noDelete *keybloom, partialKeys [][]byte, cleanKeys [][]byte) (int, int, bool, error) {
	var (
		// Statistic
		iterated             uint64
		filtered             uint64
		deletedWithSamePath  int
		deletedWithInnerPath int

		//stack     *genstack
		startTime = time.Now()

		logged time.Time
	)
	for index, key := range record.Keys {
		// Scope changed, reset the stack context
		owner, path, hash := DecodeNodeKey(key)
		//if stack != nil && stack.owner != owner {
		//	stack = nil
		//}
		//if stack == nil {
		//	stack = &genstack{owner: owner}
		//}
		if time.Since(logged) > time.Second*8 {
			log.Info("Iterating database", "iterated", iterated, "keyIndex", index, "remaining", len(record.Keys)-index, "elasped", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
		keys, _ := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, path), func(key []byte) bool {
			atomic.AddUint64(&iterated, 1)
			if noDelete.contain(key) {
				atomic.AddUint64(&filtered, 1)
				return true
			}
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
		for _, key := range keys {
			record.DeletionSet = append(record.DeletionSet, key)
		}
		deletedWithSamePath += len(keys)

		// Push the path and pop all the children path, delete all intermidate nodes.
		//children := stack.push(path)
		//for _, child := range children {
		//	for i := len(path); i < len(child)-1; i++ {
		//		innerPath := append(path, child[len(path):i+1]...)
		//		keys, _ := rawdb.ReadTrieNodesWithPrefix(record.db, encodeNodePath(owner, innerPath), func(key []byte) bool {
		//			atomic.AddUint64(&iterated, 1)
		//			if noDelete.contain(key) {
		//				atomic.AddUint64(&filtered, 1)
		//				return true
		//			}
		//			o, p, _ := DecodeNodeKey(key)
		//			if !bytes.Equal(innerPath, p) {
		//				return true
		//			}
		//			if o != owner {
		//				return true
		//			}
		//			return false
		//		})
		//		for _, key := range keys {
		//			record.DeletionSet = append(record.DeletionSet, key)
		//		}
		//		deletedWithInnerPath += len(keys)
		//	}
		//}
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
	log.Info("Initialize bloom filter for commit record")
	record.initBloom()
	log.Info("Written commit metadata", "key", len(record.Keys), "part", len(record.PartialKeys), "clean", len(record.CleanKeys), "stale", len(record.DeletionSet),
		"samepath", deletedWithSamePath, "innerpath", deletedWithInnerPath, "filter", filtered,
		"average", float64(iterated)/float64(len(record.Keys)), "metasize", len(blob), "bloomSize", common.StorageSize(float64(record.bloom.m())), "elasped", common.PrettyDuration(time.Since(startTime)))

	if record.onDeletionSet != nil {
		record.onDeletionSet(record.DeletionSet)
	}
	record.DeletionSet, record.Keys, record.PartialKeys, record.CleanKeys = nil, nil, nil, nil
	return int(iterated), int(filtered), ok, nil
}

// initBloom initializes the bloom filter with the key set.
func (record *CommitRecord) initBloom() {
	size := len(record.DeletionSet)
	if size == 0 {
		size = 1 // 0 size bloom filter is not allowed
	}
	bloom := newOptimalKeyBloom(uint64(size), maxFalsePositiveRate)
	for _, key := range record.DeletionSet {
		bloom.add(key)
	}
	record.bloom = bloom
}

// contain reports if the given key is in the deletion set.
// It only checks the bloom filter but never check the real
// deletion set since the false-positive is totally fine.
func (record *CommitRecord) contain(key []byte) bool {
	if record.bloom == nil {
		return true // In theory it shouldn't happen
	}
	return record.bloom.contain(key)
}

func (record *CommitRecord) deleteStale(remove func(*CommitRecord, ethdb.KeyValueStore)) error {
	var (
		startTime = time.Now()
		batch     = record.db.NewBatch()
		//mDeleter  = newMarkerWriter(record.db)

		// statistic
		resurrected uint64
		noDeletion  uint64
		deleted     uint64

		//threads = make(chan struct{}, runtime.NumCPU())
		//lock    sync.Mutex
		//wg      sync.WaitGroup
	)
	//for i := 0; i < runtime.NumCPU(); i++ {
	//	threads <- struct{}{}
	//}
	for _, key := range record.DeletionSet {
		//<-threads
		//wg.Add(1)
		//go func(key []byte) {
		//	defer func() {
		//		threads <- struct{}{}
		//		wg.Done()
		//	}()
		// Read the resurrection marker is expensive since most of
		// the disk reads are for the non-existent data. But it's a
		// background operation so the low efficiency is fine here.
		blob := rawdb.ReadResurrectionMarker(record.db, key)
		if len(blob) != 0 {
			atomic.AddUint64(&resurrected, 1)
			var marker ResurrectionMarker
			if err := rlp.DecodeBytes(blob, &marker); err != nil {
				panic("Failed to decode marker")
			}
			// Skip the deletion if the preventing-deletion marker
			// is present, and remove this marker as well.
			if ok, _ := marker.has(record.number, record.hash); ok {
				atomic.AddUint64(&noDeletion, 1)
				//if err := mDeleter.remove(key, record.number, record.hash); err != nil {
				//	return err
				//}
				//return err
				continue
			}
		}
		if blob := rawdb.ReadTrieNode(record.db, key); len(blob) == 0 {
			log.Info("The deleted key is not present", "key", key)
			//return nil
			continue
		}
		//lock.Lock()
		rawdb.DeleteTrieNode(batch, key)
		atomic.AddUint64(&deleted, 1)
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				panic("failed to write batch")
			}
			batch.Reset()
		}
		//lock.Unlock()
		//}(key)
	}
	//wg.Wait()
	// Delete the commit record itself before the markers
	remove(record, record.db)
	//if err := mDeleter.flush(batch); err != nil {
	//	return err
	//}
	if err := batch.Write(); err != nil {
		return err
	}
	log.Info("Pruned stale trie nodes", "number", record.number, "hash", record.hash, "deleted", deleted, "resurrected", resurrected, "nodeletion", noDeletion, "elasped", common.PrettyDuration(time.Since(startTime)))
	return nil
}

type commitRecordsByNumber []*CommitRecord

func (t commitRecordsByNumber) Len() int           { return len(t) }
func (t commitRecordsByNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t commitRecordsByNumber) Less(i, j int) bool { return t[i].number < t[j].number }

// ResurrectionMarker is a delete-preventing marker of a trie node. It contains
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
// be written and if so the relevant commit record id will be saved in the marker
// to prevent deletion from the relevant pruning operation. The overhead for checking
// deletion set is acceptable since only the bloom filters are written.
//
// e.g. for node A, the no-deletion marker contain these records:
// [<number = 1, hash = xxx>, <number = 100, hash = yyy>], the node A won't be deleted
// by pruning operation of <n=1, hash=xxx> and <n=100, hash=yyy>, but it can be deleted
// by other pruning operations.
//
// If the marker doesn't contain any commit record id, the relevant database entry
// should be removed.
type ResurrectionMarker struct {
	Numbers []uint64
	Hashes  []common.Hash
}

func (m ResurrectionMarker) has(number uint64, hash common.Hash) (bool, int) {
	for i := 0; i < len(m.Numbers); i++ {
		if m.Numbers[i] == number && m.Hashes[i] == hash {
			return true, i
		}
	}
	return false, 0
}

func (m ResurrectionMarker) inRange(number uint64, hash common.Hash) bool {
	var numbers []int
	for i := 0; i < len(m.Numbers); i++ {
		numbers = append(numbers, int(m.Numbers[i]))
	}
	sort.Ints(numbers)
	if len(numbers) > 0 && uint64(numbers[0]) < number && uint64(numbers[len(numbers)-1]) > number {
		return true
	}
	return false
}

// markerWriter is the marker cacher for writing a batch of resurrection marker
// updates into the database. Since the basic flow for updating resurrection marker
// is: load the existent content, append the new record and write it back. The
// underlying database batch doesn't support read uncommitted data so it should
// be handled by ourselves.
type markerWriter struct {
	reader ethdb.KeyValueReader           // Database handler for read operation
	cached map[string]*ResurrectionMarker // Uncommitted markers
	lock   sync.Mutex
}

func newMarkerWriter(reader ethdb.KeyValueReader) *markerWriter {
	return &markerWriter{
		reader: reader,
		cached: make(map[string]*ResurrectionMarker),
	}
}

func (writer *markerWriter) add(key []byte, number uint64, hash common.Hash, partial bool) error {
	writer.lock.Lock()
	defer writer.lock.Unlock()

	var marker ResurrectionMarker
	if m := writer.cached[string(key)]; m != nil {
		marker = *m
	} else {
		blob := rawdb.ReadResurrectionMarker(writer.reader, key)
		if len(blob) != 0 {
			err := rlp.DecodeBytes(blob, &marker)
			if err != nil {
				return err
			}
		}
	}
	if ok, _ := marker.has(number, hash); ok {
		owner, path, hash := DecodeNodeKey(key)
		log.Warn("Duplicated commit record", "number", number, "hash", hash, "key", hexutil.Encode(key), "owner", owner.Hex(), "path", path, "hash", hash.Hex(), "partial", partial)
		return nil
	}
	marker.Numbers = append(marker.Numbers, number)
	marker.Hashes = append(marker.Hashes, hash)
	writer.cached[string(key)] = &marker
	return nil
}

func (writer *markerWriter) remove(key []byte, number uint64, hash common.Hash) error {
	writer.lock.Lock()
	defer writer.lock.Unlock()

	var marker ResurrectionMarker
	if m := writer.cached[string(key)]; m != nil {
		marker = *m
	} else {
		blob := rawdb.ReadResurrectionMarker(writer.reader, key)
		if len(blob) != 0 {
			err := rlp.DecodeBytes(blob, &marker)
			if err != nil {
				return err
			}
		}
	}
	ok, index := marker.has(number, hash)
	if !ok {
		log.Warn("Non-existent commit record", "number", number, "hash", hash)
		return nil
	}
	if len(marker.Numbers) == 1 {
		writer.cached[string(key)] = nil
	} else {
		marker.Numbers = append(marker.Numbers[:index], marker.Numbers[index+1:]...)
		marker.Hashes = append(marker.Hashes[:index], marker.Hashes[index+1:]...)
		writer.cached[string(key)] = &marker
	}
	return nil
}

func (writer *markerWriter) flush(w ethdb.KeyValueWriter) error {
	writer.lock.Lock()
	defer writer.lock.Unlock()

	for k, m := range writer.cached {
		if m == nil {
			rawdb.DeleteResurrectionMarker(w, []byte(k))
			continue
		}
		blob, err := rlp.EncodeToBytes(m)
		if err != nil {
			return err
		}
		rawdb.WriteResurrectionMarker(w, []byte(k), blob)
	}
	writer.cached = make(map[string]*ResurrectionMarker)
	return nil
}
