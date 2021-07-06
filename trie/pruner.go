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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type PrunerConfig struct {
	Enabled     bool
	GenesisSet  map[string]struct{}
	IsCanonical func(uint64, common.Hash) bool
}

type pruner struct {
	config     PrunerConfig
	db         ethdb.KeyValueStore
	mwriter    *markerWriter
	bloom      *keybloom
	current    *commitRecord
	records    []*commitRecord
	minRecord  uint64
	recordLock sync.Mutex

	signal   chan uint64
	pauseCh  chan chan struct{}
	resumeCh chan chan struct{}
	wg       sync.WaitGroup
	closeCh  chan struct{}

	// statistic
	checked   uint64
	contained uint64
}

func newPruner(config PrunerConfig, db ethdb.KeyValueStore) *pruner {
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
		signal:    make(chan uint64),
		pauseCh:   make(chan chan struct{}),
		resumeCh:  make(chan chan struct{}),
		closeCh:   make(chan struct{}),
	}
	pruner.wg.Add(1)
	go pruner.loop()

	var ctx []interface{}
	ctx = append(ctx, "enabled", config.Enabled)
	if len(records) > 0 {
		ctx = append(ctx, "records", len(pruner.records), "oldest", pruner.minRecord)
	}
	log.Info("Initialized state pruner", ctx)
	return pruner
}

func (p *pruner) close() {
	close(p.closeCh)
	p.wg.Wait()
}

func (p *pruner) commitStart(number uint64, hash common.Hash) {
	if !p.config.Enabled {
		return
	}
	p.current = newCommitRecord(p.db, number, hash)
}

func (p *pruner) addKey(key []byte, partial bool) error {
	// Invalidate the previous deletion if the given key is in the
	// deletion set. It's done no matter the pruning is enabled or not.
	var found bool
	for _, record := range p.records {
		if record.contain(key) {
			err := p.mwriter.add(key, record.number, record.hash)
			if err != nil {
				return err
			}
			found = true
		}
	}
	p.checked += 1
	if found {
		p.contained += 1
	}
	// If pruning is disabled, unnecessary to track the written keys here.
	if !p.config.Enabled {
		return nil
	}
	p.bloom.add(key)
	if !partial {
		p.current.add(key)
	}
	return nil
}

func (p *pruner) commitEnd() error {
	if !p.config.Enabled {
		return nil
	}
	for key := range p.config.GenesisSet {
		p.bloom.add([]byte(key))
	}
	if err := p.current.finalize(p.bloom); err != nil {
		return err
	}
	p.recordLock.Lock()
	p.records = append(p.records, p.current)
	sort.Sort(commitRecordsByNumber(p.records))
	p.minRecord = p.records[0].number
	p.recordLock.Unlock()

	p.current = nil
	p.bloom = newOptimalKeyBloom(commitBloomSize, maxFalsePositiveRate)
	log.Info("Added new record", "live", len(p.records), "totalchecked", p.checked, "existence", p.contained)
	return nil
}

// flushMarker flushes out all cached marker updates into the given database writer
func (p *pruner) flushMarker(writer ethdb.KeyValueWriter) error {
	return p.mwriter.flush(writer)
}

func (p *pruner) recordsLT(number uint64) []*commitRecord {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	var ret []*commitRecord
	for _, r := range p.records {
		if r.number < number {
			ret = append(ret, r)
		}
	}
	return ret
}

func (p *pruner) removeRecord(record *commitRecord, writer ethdb.KeyValueWriter) {
	p.recordLock.Lock()
	defer p.recordLock.Unlock()

	for i, r := range p.records {
		if r.number == record.number && r.hash == record.hash {
			p.records = append(p.records[:i], p.records[i+1:]...)
			sort.Sort(commitRecordsByNumber(p.records))
		}
	}
	rawdb.DeleteCommitRecord(writer, record.number, record.hash)
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

func (p *pruner) pruning(records []*commitRecord, done chan struct{}, cancel chan chan struct{}) {
	defer func() { done <- struct{}{} }()

	for _, r := range records {
		select {
		case signal := <-cancel:
			signal <- struct{}{}
			return
		default:
		}
		if p.config.IsCanonical(r.number, r.hash) {
			p.removeRecord(r, p.db)
			continue
		}
		record, err := readCommitRecord(p.db, r.number, r.hash)
		if err != nil {
			p.removeRecord(r, p.db)
			continue
		}
		record.deleteStale(p.removeRecord)
	}
}

// loop is the pruner background goroutine that waits for pruning targets
func (p *pruner) loop() {
	defer p.wg.Done()

	var (
		paused bool               // Flag if the pruning is allowed
		done   chan struct{}      // Non-nil if background unindexing or reindexing routine is active.
		cancel chan chan struct{} // Channel for notifying pause signal
	)
	for {
		select {
		case number := <-p.signal:
			if paused {
				continue
			}
			if done != nil {
				continue
			}
			if number < minBlockConfirms {
				continue
			}
			ret := p.recordsLT(number - uint64(minBlockConfirms))
			if len(ret) == 0 {
				continue
			}
			done, cancel = make(chan struct{}), make(chan chan struct{})
			go p.pruning(ret, done, cancel)

		case ch := <-p.pauseCh:
			paused = true

			s := make(chan struct{})
			cancel <- s
			<-s
			ch <- struct{}{}

		case ch := <-p.resumeCh:
			paused = false
			ch <- struct{}{}

		case <-done:
			done, cancel = nil, nil

		case <-p.closeCh:
			return
		}
	}
}
