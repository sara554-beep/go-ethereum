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

package fetcher

import (
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

const (
	txAnnounceLimit     = 4096            // Maximum number of unique transaction a peer may have announced
	txFetchTimeout      = 5 * time.Second // Maximum allotted time to return an explicitly requested transaction
	MaxTransactionFetch = 192             // Maximum transaction number can be fetched in one request.
)

// txAnnounce is the hash notification of the availability of a new transaction
// in the network.
type txAnnounce struct {
	hash   common.Hash // Hash of the transaction being announced
	origin string      // Identifier of the peer originating the notification
	time   time.Time   // Timestamp of the announcement

	fetchTxs func([]common.Hash) error
}

// txInject represents a new arrived transaction which we can
// import directly.
type txInject struct {
	origin string
	txs    []*types.Transaction
}

// TxFetcher is responsible for retrieving new transaction based
// on the announcement.
type TxFetcher struct {
	notify chan *txAnnounce
	inject chan *txInject
	quit   chan struct{}

	// Announce states
	announces map[string]int                // Per peer transaction announce counts to prevent memory exhaustion
	announced map[common.Hash][]*txAnnounce // Announced transactions, scheduled for fetching
	fetching  map[common.Hash]*txAnnounce   // Announced transactions, currently fetching

	// Callbacks
	hasTx    func(common.Hash) bool             // Retrieves a tx from the local chain or local txpool
	addTxs   func([]*types.Transaction) []error // Insert a batch of transactions into local txloop
	dropPeer func(string)                       // Drop the specified peer

	// Hooks
	importTxsHook func([]*types.Transaction) // Hook which is called when a batch of transactions are imported.
	dropHook      func(string)               // Hook which is called when a peer is dropped
}

// NewTxFetcher creates a transaction fetcher to retrieve transaction
// based on hash announcements.
func NewTxFetcher(hasTx func(common.Hash) bool, addTxs func([]*types.Transaction) []error, dropPeer func(string)) *TxFetcher {
	return &TxFetcher{
		notify:    make(chan *txAnnounce),
		inject:    make(chan *txInject),
		quit:      make(chan struct{}),
		announces: make(map[string]int),
		announced: make(map[common.Hash][]*txAnnounce),
		fetching:  make(map[common.Hash]*txAnnounce),
		hasTx:     hasTx,
		addTxs:    addTxs,
		dropPeer:  dropPeer,
	}
}

// Notify announces the fetcher of the potential availability of a
// new transaction in the network.
func (f *TxFetcher) Notify(peer string, hash common.Hash, time time.Time, fetchTxs func([]common.Hash) error) error {
	announce := &txAnnounce{
		hash:     hash,
		time:     time,
		origin:   peer,
		fetchTxs: fetchTxs,
	}
	select {
	case f.notify <- announce:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// EnqueueTxs imports a batch of received transaction into fetcher.
func (f *TxFetcher) EnqueueTxs(peer string, txs []*types.Transaction) error {
	op := &txInject{
		origin: peer,
		txs:    txs,
	}
	select {
	case f.inject <- op:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and block fetches until termination requested.
func (f *TxFetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *TxFetcher) Stop() {
	close(f.quit)
}

func (f *TxFetcher) loop() {
	fetchTimer := time.NewTimer(0)

	for {
		// Clean up any expired transaction fetches
		for hash, announce := range f.fetching {
			if time.Since(announce.time) > txFetchTimeout {
				delete(f.fetching, hash)
			}
		}
		select {
		case anno := <-f.notify:
			count := f.announces[anno.origin] + 1
			if count > txAnnounceLimit {
				break
			}
			// All is well, schedule the annouce if transaction is not yet downloading
			if _, ok := f.fetching[anno.hash]; ok {
				break
			}
			f.announces[anno.origin] = count
			f.announced[anno.hash] = append(f.announced[anno.hash], anno)

			if len(f.announced) == 1 {
				f.reschedule(fetchTimer)
			}
		case <-fetchTimer.C:
			// At least one block's timer ran out, check for needing retrieval
			request := make(map[string][]common.Hash)

			for hash, announces := range f.announced {
				if time.Since(announces[0].time) > arriveTimeout-gatherSlack {
					// Pick a random peer to retrieve from, reset all others
					announce := announces[rand.Intn(len(announces))]
					f.forgetHash(hash)

					// If the block still didn't arrive, queue for fetching
					if !f.hasTx(hash) {
						request[announce.origin] = append(request[announce.origin], hash)
						f.fetching[hash] = announce
					}
				}
			}
			// Send out all block header requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled transactions", "peer", peer, "txs", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				fetchTxs, hashes := f.fetching[hashes[0]].fetchTxs, hashes

				go func() {
					for {
						if len(hashes) > MaxTransactionFetch {
							fetchTxs(hashes[:MaxTransactionFetch])
							hashes = hashes[MaxTransactionFetch:]
						} else {
							fetchTxs(hashes)
							return
						}
					}
				}()
			}
			// Schedule the next fetch if blocks are still pending
			f.reschedule(fetchTimer)
		case op := <-f.inject:
			// The transaction from both eth63 or eth64 peer will arrive here.
			var drop bool
			errs := f.addTxs(op.txs)
			for i, err := range errs {
				if err == core.ErrInvalidSender {
					drop = true
				}
				f.forgetHash(op.txs[i].Hash())
			}
			if f.importTxsHook != nil {
				f.importTxsHook(op.txs)
			}
			// Drop the peer if some transaction is failed to
			// signature verification. We can regard this peer
			// is DDoS us by feeding lots of random hashes.
			if drop {
				f.dropPeer(op.origin)
				if f.dropHook != nil {
					f.dropHook(op.origin)
				}
			}
		case <-f.quit:
			return
		}
	}
}

// rescheduleFetch resets the specified fetch timer to the next blockAnnounce timeout.
func (f *TxFetcher) reschedule(fetch *time.Timer) {
	// Short circuit if no transactions are announced
	if len(f.announced) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.announced {
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	fetch.Reset(arriveTimeout - time.Since(earliest))
}

func (f *TxFetcher) forgetHash(hash common.Hash) {
	// Remove all pending announces and decrement DOS counters
	for _, announce := range f.announced[hash] {
		f.announces[announce.origin]--
		if f.announces[announce.origin] <= 0 {
			delete(f.announces, announce.origin)
		}
	}
	delete(f.announced, hash)
	delete(f.fetching, hash)
}
