// Copyright 2019 The go-ethereum Authors
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
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256Hash(nil)
)

// generatorStats is a collection of statistics gathered by the snapshot generator
// for logging purposes.
type generatorStats struct {
	lock     sync.RWMutex       // Lock to protect concurrency issue.
	wiping   chan struct{}      // Notification channel if wiping is in progress
	origin   uint64             // Origin prefix where generation started
	start    time.Time          // Timestamp when generation started
	accounts uint64             // Number of accounts indexed
	slots    uint64             // Number of storage slots indexed
	size     common.StorageSize // Account and storage slot size
}

// progess adds the latest generation progress to counter.
func (gs *generatorStats) progress(accounts uint64, slots uint64, size common.StorageSize) {
	gs.lock.Lock()
	defer gs.lock.Unlock()

	gs.accounts += accounts
	gs.slots += slots
	gs.size += size
}

// log creates an contextual log with the given message and the context pulled
// from the internally maintained statistics.
func (gs *generatorStats) log(msg string, root common.Hash, marker []byte) {
	gs.lock.RLock()
	defer gs.lock.RUnlock()

	var ctx []interface{}
	if root != (common.Hash{}) {
		ctx = append(ctx, []interface{}{"root", root}...)
	}
	// Figure out whether we're after or within an account
	account, storages, err := decodeMarker(marker)
	if err != nil {
		log.Crit("Failed to decode marker", "marker", marker)
	}
	if len(account) > 0 {
		ctx = append(ctx, []interface{}{"account", common.BytesToHash(marker)}...)
	}
	if len(storages) > 0 {
		sortByteArrays(storages)
		from, to := storages[0], storages[len(storages)-1]
		fromAccount, fromSlot := from[:common.HashLength], from[common.HashLength:]
		toAccount, toSlot := to[:common.HashLength], to[common.HashLength:]
		ctx = append(ctx, []interface{}{
			"storages", len(storages),
			"range", fmt.Sprintf("%x(%x) - %x(%x)", fromAccount, fromSlot, toAccount, toSlot),
		}...)
	}
	// Add the usual measurements
	ctx = append(ctx, []interface{}{
		"accounts", gs.accounts,
		"slots", gs.slots,
		"size", gs.size,
		"elapsed", common.PrettyDuration(time.Since(gs.start)),
	}...)
	// Calculate the estimated indexing time based on current stats
	if len(account) > 0 {
		if done := binary.BigEndian.Uint64(account[:8]) - gs.origin; done > 0 {
			left := math.MaxUint64 - binary.BigEndian.Uint64(account[:8])

			speed := done/uint64(time.Since(gs.start)/time.Millisecond+1) + 1 // +1s to avoid division by zero
			ctx = append(ctx, []interface{}{
				"eta", common.PrettyDuration(time.Duration(left/speed) * time.Millisecond),
			}...)
		}
	}
	log.Info(msg, ctx...)
}

// generateSnapshot regenerates a brand new snapshot based on an existing state
// database and head block asynchronously. The snapshot is returned immediately
// and generation is continued in the background until done.
func generateSnapshot(diskdb ethdb.KeyValueStore, triedb *trie.Database, cache int, root common.Hash, wiper chan struct{}) *diskLayer {
	// Wipe any previously existing snapshot from the database if no wiper is
	// currently in progress.
	if wiper == nil {
		wiper = wipeSnapshot(diskdb, true)
	}
	// Create a new disk layer with an initialized state marker at zero
	rawdb.WriteSnapshotRoot(diskdb, root)

	base := &diskLayer{
		diskdb:     diskdb,
		triedb:     triedb,
		root:       root,
		cache:      fastcache.New(cache * 1024 * 1024),
		genMarker:  []byte{}, // Initialized but empty!
		genPending: make(chan struct{}),
		genAbort:   make(chan chan *generatorStats),
	}
	go base.generate(&generatorStats{wiping: wiper, start: time.Now()})
	return base
}

// storageGeneration represents a task for generating the snapshot of
// the specific storage.
type storageGeneration struct {
	iter    *trie.Iterator
	account common.Hash
	marker  []byte
	batch   ethdb.Batch
	stats   *generatorStats
}

// generateStorages is a helper function of generate. It accepts all derived
// tasks for generating storage snapshot. This function will try to spin up
// several go-routines for executing the accumulated tasks concurrently, but
// the concurrency is limited.
//
// If the signal from stop channel is received, all running tasks will be aborted.
// The execution progress markers will be sent back via out channel. Note we expect
// the out channel has at least 1 slot available so that sending won't be blocked.
func (dl *diskLayer) generateStorages(in chan *storageGeneration, out chan [][]byte, stop chan chan struct{}) {
	var (
		running int
		limit   = runtime.NumCPU()
		done    = make(chan struct{})
		stopRun = make(chan struct{})

		markerLock sync.Mutex
		markers    [][]byte
	)
	if cap(out) < 1 {
		panic("require buffered channel")
	}
	run := func(task *storageGeneration, stop chan struct{}) {
		var ctx []interface{}
		ctx = append(ctx, []interface{}{"account", task.account}...)
		if task.marker != nil {
			ctx = append(ctx, []interface{}{"marker", common.Bytes2Hex(task.marker)}...)
		}
		log.Info("Start running storage task", ctx)

		iter, batch, marker := task.iter, task.batch, task.marker
		for iter.Next() {
			var abort bool
			select {
			case <-stop:
				abort = true
			default:
			}
			if marker == nil || !bytes.Equal(marker, iter.Key) {
				rawdb.WriteStorageSnapshot(batch, task.account, common.BytesToHash(iter.Key), iter.Value)
			}
			if batch.ValueSize() > ethdb.IdealBatchSize || abort {
				if batch.ValueSize() > 0 {
					if err := batch.Write(); err != nil {
						log.Crit("Failed to write snapshot", "error", err)
					}
					batch.Reset()

					markerLock.Lock()
					markers = append(markers, append(task.account.Bytes(), iter.Key...))
					markerLock.Unlock()
				}
				if abort {
					return
				}
			}
			marker = nil
		}
		// Commit the last batch. In this case we don't need to store
		// the progress marker seems the storage trie is fully iterated.
		if batch.ValueSize() > 0 {
			if err := batch.Write(); err != nil {
				log.Crit("Failed to write snapshot", "error", err)
			}
			batch.Reset()
		}
	}
	for {
		select {
		case task := <-in:
			// No more tasks, wait and exit
			if task == nil {
				for running > 0 {
					<-done
					running -= 1
				}
				if len(markers) != 0 {
					log.Crit("Invalid generation markers, should finish all", "marker", markers)
				}
				out <- nil
				return
			}
			// Try to schedule up one more go-rountine for new task.
			running += 1
			go run(task, stopRun)

			for running >= limit {
				<-done
				running -= 1
			}
		case <-done:
			running -= 1

		case ch := <-stop:
			close(stopRun)
			for running > 0 {
				<-done
				running -= 1
			}
			out <- markers
			close(ch)
			return
		}
	}
}

// generate is a background thread that iterates over the state and storage tries,
// constructing the state snapshot. All the arguments are purely for statistics
// gethering and logging, since the method surfs the blocks as they arrive, often
// being restarted.
func (dl *diskLayer) generate(stats *generatorStats) {
	// If a database wipe is in operation, wait until it's done
	if stats.wiping != nil {
		stats.log("Wiper running, state snapshotting paused", common.Hash{}, dl.genMarker)
		select {
		// If wiper is done, resume normal mode of operation
		case <-stats.wiping:
			stats.wiping = nil
			stats.start = time.Now()

		// If generator was aborted during wipe, return
		case abort := <-dl.genAbort:
			abort <- stats
			return
		}
	}
	// Create an account and state iterator pointing to the current generator marker
	accTrie, err := trie.NewSecure(dl.root, dl.triedb)
	if err != nil {
		// The account trie is missing (GC), surf the chain until one becomes available
		stats.log("Trie missing, state snapshotting paused", dl.root, dl.genMarker)

		abort := <-dl.genAbort
		abort <- stats
		return
	}
	stats.log("Resuming state snapshot generation", dl.root, dl.genMarker)

	var (
		accMarker    []byte
		storeMarkers [][]byte
	)
	if len(dl.genMarker) > 0 { // []byte{} is the start
		accMarker, storeMarkers, err = decodeMarker(dl.genMarker)
		if err != nil {
			log.Crit("Failed to decode marker", "error", err)
		}
	}
	// Setup the go-routine for processing storage tasks in the background
	var (
		taskIn   = make(chan *storageGeneration)
		taskStop = make(chan chan struct{})
		taskOut  = make(chan [][]byte, 1)
	)
	go dl.generateStorages(taskIn, taskOut, taskStop)

	// If there are remaining storage tasks, resume them.
	for _, marker := range storeMarkers {
		acct, slot := marker[:common.HashLength], marker[common.HashLength:]

		// The resumed storage task MUST have the corresponding
		// account persisted. Otherwise, it's really wrong.
		blob := rawdb.ReadAccountSnapshot(dl.diskdb, common.BytesToHash(acct))
		if len(blob) == 0 {
			log.Crit("Failed to read snapshot account", "account", common.BytesToHash(acct))
		}
		account, err := FullAccount(blob)
		if err != nil {
			log.Crit("Failed to decode snapshot account", "account", common.BytesToHash(acct), "error", err)
		}
		storeTrie, err := trie.NewSecure(common.BytesToHash(account.Root), dl.triedb)
		if err != nil {
			log.Crit("Storage trie inaccessible for snapshot generation", "err", err)
		}
		taskIn <- &storageGeneration{
			iter:    trie.NewIterator(storeTrie.NodeIterator(slot)),
			account: common.BytesToHash(acct),
			marker:  slot,
			batch:   dl.diskdb.NewBatch(),
			stats:   stats,
		}
	}
	accIt := trie.NewIterator(accTrie.NodeIterator(accMarker))
	batch := dl.diskdb.NewBatch()

	// Iterate from the previous marker and continue generating the state snapshot
	logged := time.Now()
	for accIt.Next() {
		// Retrieve the current account and flatten it into the internal format
		accountHash := common.BytesToHash(accIt.Key)

		var acc struct {
			Nonce    uint64
			Balance  *big.Int
			Root     common.Hash
			CodeHash []byte
		}
		if err := rlp.DecodeBytes(accIt.Value, &acc); err != nil {
			log.Crit("Invalid account encountered during snapshot creation", "err", err)
		}
		data := SlimAccountRLP(acc.Nonce, acc.Balance, acc.Root, acc.CodeHash)

		// If the account is not yet in-progress, write it out
		if accMarker == nil || !bytes.Equal(accountHash[:], accMarker) {
			rawdb.WriteAccountSnapshot(batch, accountHash, data)
			stats.progress(1, 0, common.StorageSize(1+common.HashLength+len(data)))
		}
		// If we've exceeded our batch allowance or termination was requested, flush to disk
		var abort chan *generatorStats
		select {
		case abort = <-dl.genAbort:
		default:
		}
		if batch.ValueSize() > ethdb.IdealBatchSize || abort != nil {
			// Only write and set the marker if we actually did something useful
			if batch.ValueSize() > 0 {
				if err := batch.Write(); err != nil {
					log.Crit("Failed to write snapshot", "error", err)
				}
				batch.Reset()
			}
			if abort != nil {
				stats.log("Aborting state snapshot generation", dl.root, accountHash[:])

				ch := make(chan struct{})
				taskStop <- ch
				<-ch

				dl.lock.Lock()
				marker, err := encodeMarker(accountHash.Bytes(), <-taskOut)
				if err != nil {
					log.Crit("Failed to encode marker", "error", err)
				}
				dl.genMarker = marker
				dl.lock.Unlock()

				abort <- stats
				return
			}
		}
		// If the account is in-progress, continue where we left off (otherwise iterate all)
		if acc.Root != emptyRoot {
			// If the storage task is already sent, don't send it again.
			var skip bool
			if accMarker != nil {
				for _, storage := range storeMarkers {
					if bytes.Equal(accMarker, storage[:common.HashLength]) {
						skip = true
						break
					}
				}
			}
			if !skip {
				storeTrie, err := trie.NewSecure(acc.Root, dl.triedb)
				if err != nil {
					log.Crit("Storage trie inaccessible for snapshot generation", "err", err)
				}
				taskIn <- &storageGeneration{
					iter:    trie.NewIterator(storeTrie.NodeIterator(nil)),
					account: accountHash,
					marker:  nil,
					batch:   dl.diskdb.NewBatch(),
					stats:   stats,
				}
			}
		}
		if time.Since(logged) > 8*time.Second {
			stats.log("Generating state snapshot", dl.root, accIt.Key)
			logged = time.Now()
		}
		// Some account processed, unmark the marker
		accMarker = nil
	}
	// Commit the last batch, the state trie is fully iterated
	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			log.Crit("Failed to write snapshot", "error", err)
		}
	}
	// State trie is fully iterated, wait the storage generators
	taskIn <- nil
	<-taskOut

	log.Info("Generated state snapshot", "accounts", stats.accounts, "slots", stats.slots,
		"storage", stats.size, "elapsed", common.PrettyDuration(time.Since(stats.start)))

	dl.lock.Lock()
	dl.genMarker = nil
	close(dl.genPending)
	dl.lock.Unlock()

	// Someone will be looking for us, wait it out
	abort := <-dl.genAbort
	abort <- nil
}
