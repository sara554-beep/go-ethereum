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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type PrunerConfig struct {
	Enabled        bool
	GenesisSet     map[string]struct{}
	MaximumRecords int
	Signal         chan uint64
	IsCanonical    func(uint64, common.Hash) bool
}

type pruner struct {
	config      PrunerConfig
	db          ethdb.KeyValueStore
	mwriter     *markerWriter
	partialKeys [][]byte
	bloom       *keybloom
	current     *CommitRecord
	records     []*CommitRecord
	minRecord   uint64
	recordLock  sync.Mutex

	signal   chan uint64
	pauseCh  chan chan struct{}
	resumeCh chan chan struct{}
	wg       sync.WaitGroup
	closeCh  chan struct{}

	// statistic
	written     uint64 // Counter for the total written trie nodes
	iterated    uint64 // Counter for the total itearated trie nodes
	filtered    uint64 // Counter for the total filtered trie nodes for deletion
	resurrected uint64 // Counter for the total resurrected trie nodes
}

func newPruner(config PrunerConfig, db ethdb.KeyValueStore) *pruner {
	numbers, hashes, blobs := rawdb.ReadAllCommitRecords(db, 0, math.MaxUint64, false)
	var records []*CommitRecord
	for i := 0; i < len(numbers); i++ {
		record, err := loadCommitRecord(db, numbers[i], hashes[i], blobs[i])
		if err != nil {
			log.Info("Commit record is corrupted", "number", numbers[i], "hash", hashes[i].Hex())
			rawdb.DeleteCommitRecord(db, numbers[i], hashes[i]) // Corrupted record can be just discarded
			continue
		}
		records = append(records, record)
	}
	sort.Sort(commitRecordsByNumber(records))

	minRecord := uint64(math.MaxUint64)
	if len(records) > 0 {
		minRecord = records[0].number
	}
	pruner := &pruner{
		config:    config,
		db:        db,
		mwriter:   newMarkerWriter(db),
		bloom:     newOptimalKeyBloom(commitBloomSize, maxFalsePositiveRate),
		records:   records,
		minRecord: minRecord,
		signal:    config.Signal,
		pauseCh:   make(chan chan struct{}),
		resumeCh:  make(chan chan struct{}),
		closeCh:   make(chan struct{}),
	}
	pruner.checkRecords()

	pruner.wg.Add(1)
	go pruner.loop()

	var ctx []interface{}
	ctx = append(ctx, "enabled", config.Enabled, "maxtasks", pruner.config.MaximumRecords)
	if len(records) > 0 {
		ctx = append(ctx, "records", len(pruner.records), "oldest", pruner.minRecord)
	}
	log.Info("Initialized state pruner", ctx...)
	return pruner
}

// checkRecords is a debug function used to ensure the commits are sorted correctly.
func (p *pruner) checkRecords() {
	if len(p.records) <= 1 {
		return
	}
	for i := 0; i < len(p.records)-1; i++ {
		current, next := p.records[i], p.records[i+1]
		if current.number >= next.number {
			log.Crit("Invalid commit record", "index", i, "curnumber", current.number, "curhash", current.hash, "nextnumber", next.number, "nexthash", next.hash)
		}
	}
}

func (p *pruner) close() {
	close(p.closeCh)
	p.wg.Wait()
}

func (p *pruner) commitStart(number uint64, hash common.Hash) {
	if !p.config.Enabled {
		return
	}
	if p.current != nil {
		log.Crit("The current commit record is not nil")
	}
	p.recordLock.Lock()
	live := len(p.records)
	p.recordLock.Unlock()

	if live >= p.config.MaximumRecords {
		log.Info("Too many accumulated records", "number", live, "threshold", p.config.MaximumRecords)
		return
	}
	p.current = newCommitRecord(p.db, number, hash)
	log.Info("Start commit operation", "number", number, "hash", hash.Hex())
}

func (p *pruner) addKey(key []byte, partial bool) error {
	// Invalidate the previous deletion if the given key is in the
	// deletion set. It's done no matter the pruning is enabled or not.
	for _, record := range p.records {
		if record.contain(key) {
			err := p.mwriter.add(key, record.number, record.hash, partial)
			if err != nil {
				return err
			}
			p.resurrected += 1
		}
	}
	p.written += 1

	// If the pruning is disabled, or there are too many un-processed pruning
	// tasks remained, skip the tracking .
	if !p.config.Enabled {
		return nil
	}
	if p.current == nil {
		return nil
	}
	// Track all the written keys in the bloom, and put it in the commit set
	// if it's passed by the Commit operation.
	p.bloom.add(key)
	if !partial {
		p.current.add(key)
	} else {
		p.partialKeys = append(p.partialKeys, key)
	}
	return nil
}

func (p *pruner) commitEnd() error {
	if !p.config.Enabled {
		return nil
	}
	if p.current == nil {
		return nil
	}
	for key := range p.config.GenesisSet {
		p.bloom.add([]byte(key))
	}
	iterated, filtered, err := p.current.finalize(p.bloom, p.partialKeys)
	if err != nil {
		return err
	}
	p.iterated += uint64(iterated)
	p.filtered += uint64(filtered)

	p.recordLock.Lock()
	p.records = append(p.records, p.current)
	sort.Sort(commitRecordsByNumber(p.records))
	p.minRecord = p.records[0].number
	p.checkRecords()
	p.recordLock.Unlock()

	log.Info("Commit operation is finished", "number", p.current.number, "hash", p.current.hash.Hex())

	p.current = nil
	p.bloom = newOptimalKeyBloom(commitBloomSize, maxFalsePositiveRate)
	p.partialKeys = nil
	log.Info("Added new record", "live", len(p.records), "written", p.written, "iterated", p.iterated, "filtered", p.filtered, "resurrected", p.resurrected)
	return nil
}

// flushMarker flushes out all cached marker updates into the given database writer
func (p *pruner) flushMarker(writer ethdb.KeyValueWriter) error {
	return p.mwriter.flush(writer)
}

func (p *pruner) filterRecords(number uint64) []*CommitRecord {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	var ret []*CommitRecord
	for _, r := range p.records {
		if r.number < number {
			ret = append(ret, r)
		}
	}
	return ret
}

func (p *pruner) removeRecord(record *CommitRecord, db ethdb.KeyValueStore) {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	var found bool
	for i, r := range p.records {
		if r.number == record.number && r.hash == record.hash {
			p.records = append(p.records[:i], p.records[i+1:]...)
			sort.Sort(commitRecordsByNumber(p.records))
			found = true
		}
	}
	if !found {
		log.Crit("Failed to delete non-existent commit record", "number", record.number, "hash", record.hash.Hex())
	}
	p.checkRecords()
	rawdb.DeleteCommitRecord(db, record.number, record.hash)
}

func (p *pruner) resume() {
	ch := make(chan struct{})
	p.resumeCh <- ch
	<-ch
}

func (p *pruner) pause() {
	ch := make(chan struct{})
	p.pauseCh <- ch
	<-ch
}

func (p *pruner) pruning(records []*CommitRecord, done chan struct{}, interrupt *uint64) {
	defer close(done)

	for _, r := range records {
		if atomic.LoadUint64(interrupt) == 1 {
			log.Info("Pruning operation is interrupted")
			return
		}
		if !p.config.IsCanonical(r.number, r.hash) {
			p.removeRecord(r, p.db)
			log.Info("Filtered out side commit record", "number", r.number, "hash", r.hash)
			continue
		}
		record, err := readCommitRecord(p.db, r.number, r.hash)
		if err != nil {
			p.removeRecord(r, p.db)
			log.Info("Filtered out corrupted commit record", "number", r.number, "hash", r.hash, "err", err)
			continue
		}
		record.deleteStale(p.removeRecord)
	}
}

// loop is the pruner background goroutine that waits for pruning targets
func (p *pruner) loop() {
	defer p.wg.Done()

	var (
		paused    bool          // Flag if the pruning is allowed
		done      chan struct{} // Non-nil if background unindexing or reindexing routine is active.
		interrupt *uint64       // Indicator for notifying pause signal
	)
	for {
		select {
		case number := <-p.signal:
			if paused {
				log.Info("Pruner is paused", "number", number)
				continue
			}
			if done != nil {
				log.Info("Pruner is running", "number", number)
				continue
			}
			if number < minBlockConfirms {
				continue
			}
			ret := p.filterRecords(number - uint64(minBlockConfirms))
			if len(ret) == 0 {
				continue
			}
			done, interrupt = make(chan struct{}), new(uint64)
			go p.pruning(ret, done, interrupt)

		case ch := <-p.pauseCh:
			if paused {
				log.Crit("Duplicated suspend operations")
			}
			paused = true
			if interrupt != nil && atomic.LoadUint64(interrupt) == 0 {
				atomic.StoreUint64(interrupt, 1)
				startTime := time.Now()
				log.Info("Wait the pruning operation to be finished")
				<-done
				log.Info("The pruning operation is finished", "elapsed", common.PrettyDuration(time.Since(startTime)))
			}
			ch <- struct{}{}

		case ch := <-p.resumeCh:
			if !paused {
				log.Crit("Invalid resume operation")
			}
			paused = false
			if interrupt != nil && atomic.LoadUint64(interrupt) == 0 {
				log.Crit("Pruner is paused but the pruning operation is running")
			}
			ch <- struct{}{}

		case <-done:
			done, interrupt = nil, nil
			log.Info("Finish pruning task, wait next")

		case <-p.closeCh:
			return
		}
	}
}
