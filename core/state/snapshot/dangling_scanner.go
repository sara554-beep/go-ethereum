// Copyright 2022 The go-ethereum Authors
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

package snapshot

import (
	"bytes"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type danglingScanner struct {
	db          ethdb.KeyValueStore
	interrupt   chan chan struct{}
	done        chan struct{}
	wg          sync.WaitGroup
	lastAccount []byte

	// statistics
	danglingSlots    int
	danglingStorages int
	duration         time.Duration
}

func newDanglingScanner(db ethdb.KeyValueStore) *danglingScanner {
	return &danglingScanner{
		db:        db,
		interrupt: make(chan chan struct{}),
		done:      make(chan struct{}),
	}
}

func (scanner *danglingScanner) detect(abort chan chan *generatorStats) chan *generatorStats {
	scanner.wg.Add(1)
	go func() {
		scanner.run()
		scanner.wg.Done()
	}()

	select {
	case sig := <-abort:
		ch := make(chan struct{})
		scanner.interrupt <- ch
		<-ch
		return sig
	case <-scanner.done:
		return nil
	}
}

func (scanner *danglingScanner) run() error {
	defer func(start time.Time) {
		scanner.duration += time.Since(start)
	}(time.Now())

	var (
		lastVerified []byte
		lastDangling []byte
		batch        = scanner.db.NewBatch()
		count        int
	)
	it := rawdb.NewKeyLengthIterator(scanner.db.NewIterator(rawdb.SnapshotStoragePrefix, scanner.lastAccount), len(rawdb.SnapshotStoragePrefix)+2*common.HashLength)
	defer it.Release()

	for it.Next() {
		// Check the interruption signal with reasonable interval.
		if count%100_000 == 0 {
			select {
			case signal := <-scanner.interrupt:
				// For sake of simplification, it's fine to use the latest
				// verified(non-dangling) account as the termination flag.
				scanner.lastAccount = lastVerified
				close(signal)
				return batch.Write()
			default:
			}
		}
		count += 1

		key := it.Key()
		account := key[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength]
		storage := key[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]

		// Skip unnecessary checks for verified storage.
		if bytes.Equal(account, lastVerified) {
			continue
		}
		data := rawdb.ReadAccountSnapshot(scanner.db, common.BytesToHash(account))
		if len(data) != 0 {
			lastVerified = common.CopyBytes(account)
			continue
		}
		if !bytes.Equal(account, lastDangling) {
			lastDangling = common.CopyBytes(account)
			scanner.danglingStorages += 1
		}
		rawdb.DeleteStorageSnapshot(batch, common.BytesToHash(account), common.BytesToHash(storage))
		scanner.danglingSlots += 1

		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil {
		return err
	}
	close(scanner.done)
	return nil
}
