// Copyright 2023 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/

package history

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const historyReadBatch = 1000 // The batch size for reading state history

type stateRange struct {
	tail uint64
	head uint64
}

type Indexer struct {
	disk    ethdb.KeyValueStore
	freezer *rawdb.ResettableFreezer
	head    uint64
	rangeCh chan stateRange
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewIndexer(disk ethdb.KeyValueStore, freezer *rawdb.ResettableFreezer, head uint64) *Indexer {
	i := &Indexer{
		disk:    disk,
		freezer: freezer,
		head:    head,
		rangeCh: make(chan stateRange),
		closeCh: make(chan struct{}),
	}
	i.wg.Add(1)
	go i.loop()
	return i
}

func (i *Indexer) Close() {
	select {
	case <-i.closeCh:
		return
	default:
		close(i.closeCh)
		i.wg.Wait()
	}
}

func (i *Indexer) Notify(tail uint64, head uint64) error {
	select {
	case <-i.closeCh:
		return errors.New("closed")
	case i.rangeCh <- stateRange{tail: tail, head: head}:
	}
	return nil
}

func (i *Indexer) process(w *writer, h *History, id uint64) error {
	for _, account := range h.accountList {
		w.addAccount(account, id)
		for _, slot := range h.storageList[account] {
			w.addSlot(account, slot, id)
		}
	}
	return w.finish(i.disk, false, id)
}

func (i *Indexer) next() (uint64, error) {
	tail, err := i.freezer.Tail()
	if err != nil {
		return 0, err
	}
	head := rawdb.ReadStateIndexHead(i.disk)

	// Case (a), there is no state index present in disk,
	// start indexing from the first history element.
	if head == nil {
		return tail + 1, nil // +1 to convert to state id
	}
	// Case (b), a part of state histories have been indexed,
	// continue from the last indexed one.
	if *head >= tail {
		return *head + 1, nil // +1 as the id of next one
	}
	// Case (c), history gap is detected, discard the gaped
	// history segment, shift to the first available history
	// element as the new tail.
	log.Info("History gap detected, discard old segment", "oldHead", *head, "newHead", tail+1)
	return tail + 1, nil
}

func (i *Indexer) run(done chan struct{}, head uint64, interrupt *atomic.Int32) {
	defer close(done)

	begin, err := i.next()
	if err != nil {
		log.Error("Failed to find next state to index", "err", err)
		return
	}
	// TODO what if head is lower than the index head. It can
	// happen if the entire state history freezer is reset.
	//if begin > head {
	//
	//}
	log.Info("Start indexing", "begin", begin, "head", head)
	var (
		cur    = begin
		w      = newWriter()
		start  = time.Now()
		logged = time.Now()
	)
	for cur <= head {
		batch := head - cur + 1
		if batch > historyReadBatch {
			batch = historyReadBatch
		}
		s := time.Now()
		result, err := ReadBatch(i.freezer, cur, batch)
		if err != nil {
			log.Error("Failed to read history", "err", err)
			return
		}
		log.Info("Read history", "number", len(result), "elapsed", common.PrettyDuration(time.Since(s)))

		for _, h := range result {
			if err := i.process(w, h, cur); err != nil {
				log.Error("Failed to index history", "err", err)
				return
			}
			cur += 1

			if time.Since(logged) > time.Second*8 {
				logged = time.Now()

				var (
					left  = head - cur
					done  = cur - begin
					speed = done/uint64(time.Since(start)/time.Millisecond+1) + 1 // +1s to avoid division by zero
				)
				// Override the ETA if larger than the largest until now
				eta := time.Duration(left/speed) * time.Millisecond
				log.Info("Indexing state history", "processed", cur-begin+1, "remain", head-cur, "eta", common.PrettyDuration(eta))
			}
		}
		// Check interruption signal and abort process if it's fired
		if interrupt != nil {
			if signal := interrupt.Load(); signal != 0 {
				if err := w.finish(i.disk, true, cur-1); err != nil {
					log.Error("Failed to flush index", "err", err)
				}
				log.Info("State indexing interrupted")
				return
			}
		}
	}
	if err := w.finish(i.disk, true, head); err != nil {
		log.Error("Failed to flush index", "err", err)
	}
	log.Info("Indexed state history", "from", begin, "to", head, "elapsed", common.PrettyDuration(time.Since(start)))
}

func (i *Indexer) loop() {
	defer i.wg.Done()

	// Launch background indexing thread
	done, interrupt := make(chan struct{}), new(atomic.Int32)
	go i.run(done, i.head, interrupt)

	for {
		select {
		case r := <-i.rangeCh:
			if r.head <= i.head {
				continue // system very wrong
			}
			i.head = r.head

			if done == nil {
				done, interrupt = make(chan struct{}), new(atomic.Int32)
				go i.run(done, i.head, interrupt)
			}
		case <-done:
			done, interrupt = nil, nil

		case <-i.closeCh:
			if done != nil {
				interrupt.Store(1)
				log.Info("Waiting background indexer to exit")
				<-done
			}
			return
		}
	}
}
