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
	"fmt"
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
	GenesisSet     map[string]struct{}
	MaximumRecords int
	Signal         chan uint64
	IsCanonical    func(uint64, common.Hash) bool
}

type pruner struct {
	config     PrunerConfig
	db         *Database
	current    *StateRecord
	records    []*StateRecord
	minRecord  uint64
	recordLock sync.Mutex

	signal   chan uint64
	pauseCh  chan chan struct{}
	resumeCh chan chan struct{}
	wg       sync.WaitGroup
	closeCh  chan struct{}

	// statistic
	written uint64 // Counter for the total written trie nodes
}

func newPruner(config PrunerConfig, triedb *Database) *pruner {
	numbers, hashes := rawdb.ReadAllCommitRecords(triedb.diskdb, 0, math.MaxUint64)
	var records []*StateRecord
	for i := 0; i < len(numbers); i++ {
		record, err := loadCommitRecord(triedb.diskdb, numbers[i], hashes[i])
		if err != nil {
			panic(fmt.Sprintf("failed to decode commit record %v", err))
		}
		records = append(records, record)
	}
	sort.Sort(commitRecordsByNumber(records))

	minRecord := uint64(math.MaxUint64)
	if len(records) > 0 {
		minRecord = records[0].Number
	}
	pruner := &pruner{
		config:    config,
		db:        triedb,
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
	ctx = append(ctx, "maxtasks", pruner.config.MaximumRecords)
	if len(records) > 0 {
		ctx = append(ctx, "records", len(pruner.records), "oldest", pruner.minRecord)
	}
	log.Info("Initialized state pruner", ctx...)
	return pruner
}

func (p *pruner) noprunes() []common.Hash {
	var hashes []common.Hash
	for i, record := range p.records {
		if i == 0 {
			continue
		}
		hashes = append(hashes, record.Root)
	}
	return hashes
}

// checkRecords is a debug function used to ensure the commits are sorted correctly.
func (p *pruner) checkRecords() {
	if len(p.records) <= 1 {
		return
	}
	for i := 0; i < len(p.records)-1; i++ {
		current, next := p.records[i], p.records[i+1]
		if current.Number >= next.Number {
			log.Crit("Invalid commit record", "index", i, "curnumber", current.Number, "curhash", current.Hash,
				"nextnumber", next.Number, "nexthash", next.Hash)
		}
	}
}

func (p *pruner) close() {
	close(p.closeCh)
	p.wg.Wait()
}

func (p *pruner) commitStart(number uint64, hash common.Hash, root common.Hash) {
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
	p.current = newCommitRecord(p.db.diskdb, number, hash, root)
	log.Info("Start commit operation", "number", number, "hash", hash.Hex())
}

func (p *pruner) addKey(key []byte, partial bool) error {
	p.written += 1
	if !partial && p.current != nil {
		p.current.add(key)
		if len(p.current.Keys)%100_000 == 0 {
			log.Info("Added keys to current set", "len", len(p.current.Keys))
		}
	}
	return nil
}

func (p *pruner) commitEnd() error {
	if p.current == nil {
		return nil
	}
	if err := p.current.save(); err != nil {
		return err
	}

	p.recordLock.Lock()
	p.records = append(p.records, p.current)
	sort.Sort(commitRecordsByNumber(p.records))
	p.minRecord = p.records[0].Number
	p.checkRecords()
	p.recordLock.Unlock()

	p.current = nil
	log.Info("Added new record", "live", len(p.records), "written", p.written)
	return nil
}

func (p *pruner) filterRecords(number uint64) []*StateRecord {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	var ret []*StateRecord
	for _, r := range p.records {
		if r.Number < number {
			ret = append(ret, r)
		}
	}
	return ret
}

func (p *pruner) removeRecord(record *StateRecord, db ethdb.KeyValueStore) {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	var found bool
	for i, r := range p.records {
		if r.Number == record.Number && r.Hash == record.Hash {
			p.records = append(p.records[:i], p.records[i+1:]...)
			sort.Sort(commitRecordsByNumber(p.records))
			found = true
		}
	}
	if !found {
		log.Crit("Failed to delete non-existent commit record", "number", record.Number, "hash", record.Hash.Hex())
	}
	p.checkRecords()
	rawdb.DeleteCommitRecord(db, record.Number, record.Hash)
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

func (p *pruner) pruning(records []*StateRecord, done chan struct{}, interrupt *uint64) {
	defer close(done)

	for _, r := range records {
		if atomic.LoadUint64(interrupt) == 1 {
			log.Info("Pruning operation is interrupted")
			return
		}
		if !p.config.IsCanonical(r.Number, r.Hash) {
			p.removeRecord(r, p.db.diskdb)
			log.Info("Filtered out side commit record", "number", r.Number, "hash", r.Hash)
			continue
		}
		record, err := loadCommitRecord(p.db.diskdb, r.Number, r.Hash)
		if err != nil {
			p.removeRecord(r, p.db.diskdb)
			log.Info("Filtered out corrupted commit record", "number", r.Number, "hash", r.Hash, "err", err)
			continue
		}
		record.process(p.db, p.config.GenesisSet, p.noprunes(), p.removeRecord)
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
			log.Info("Start pruning", "task", len(ret))
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
