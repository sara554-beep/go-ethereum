// Copyright 2020 The go-ethereum Authors
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

package ethflare

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/time/rate"
)

// recentnessCutoff is the number of blocks after which a block is considered
// unavailable for state retrievals.
const recentnessCutoff = 64

// backend represents an Ethereum node with the ethflare RPC API implemented and
// exposed via WebSockets (we need notification support for new heads).
type backend struct {
	id     string
	conn   *rpc.Client
	client *ethclient.Client

	headSub  ethereum.Subscription
	headCh   chan *types.Header
	headFeed *event.Feed

	// Status of backend
	headers map[common.Hash]*types.Header // Set of recent headers across all mini-forks
	recents *prque.Prque                  // Priority queue for evicting stale headers
	states  map[common.Hash]int           // Set of state roots available for tiling
	limiter *rate.Limiter                 // Rate limit to provent adding too much pressure

	logger  log.Logger
	lock    sync.RWMutex
	closeCh chan struct{}
}

// newBackend takes a live websocket connection to a backend and starts to monitor
// its chain progression and optionally request chain and state data.
func newBackend(id string, conn *rpc.Client, ratelimit uint64) (*backend, error) {
	client := ethclient.NewClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sink := make(chan *types.Header, 256)
	sub, err := client.SubscribeNewHead(ctx, sink)
	if err != nil {
		return nil, err
	}
	backend := &backend{
		id:       id,
		conn:     conn,
		client:   client,
		headSub:  sub,
		headCh:   sink,
		headFeed: new(event.Feed),
		headers:  make(map[common.Hash]*types.Header),
		recents:  prque.New(nil),
		states:   make(map[common.Hash]int),
		limiter:  rate.NewLimiter(rate.Limit(ratelimit), 10),
		logger:   log.New("id", id),
		closeCh:  make(chan struct{}),
	}
	return backend, nil
}

func (b *backend) start() {
	go b.loop()
}

func (b *backend) stop() {
	close(b.closeCh)
	b.headSub.Unsubscribe()
}

// loop keeps exhausting the head header announcement channel, maintaining the
// current fork tree as seen by the backing node.
func (b *backend) loop() {
	var updating chan struct{}

	for {
		select {
		case head := <-b.headCh:
			// New head announced, update the fork tree if we're not currently updating
			if updating == nil {
				updating = make(chan struct{})
				go func() {
					if err := b.update(head); err != nil {
						b.logger.Warn("Failed to update to new head", "err", err)
					} else {
						b.logger.Info("Updated to new head", "number", head.Number, "hash", head.Hash(), "root", head.Root)
						b.headFeed.Send(head)
					}
					updating <- struct{}{}
				}()
			}

		case <-updating:
			updating = nil

		case <-b.headSub.Err():
			// Subscription died, terminate the loop
			return

		case <-b.closeCh:
			// Backend is closed, terminate the loop
			return
		}
	}
}

// update extends the currently maintained fork tree of this backing node with a
// new head and it's progression towards already known blocks.
func (b *backend) update(head *types.Header) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.updateAll(head, 0)
}

// updateAll is the internal version update which assumes the lock is already held.
// updateAll recursively updates all parent information of given header, terminates
// if the depth is too high.
func (b *backend) updateAll(head *types.Header, depth int) error {
	// If the header is known already known, bail out
	hash := head.Hash()
	if _, ok := b.headers[hash]; ok {
		return nil
	}
	// If the parent lookup reached the limit without hitting a recently announced
	// head, the entire parent chain needs to be discarded since there's no way to
	// know if associated state is present or not (node reboot)
	if depth >= recentnessCutoff {
		return nil
	}
	// Otherwise track all parents first, then the new head
	if head.Number.Uint64() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		parent, err := b.client.HeaderByHash(ctx, head.ParentHash)
		if err != nil {
			return err
		}
		if err := b.updateAll(parent, depth+1); err != nil {
			b.logger.Warn("Rejecting uncertain state block", "number", parent.Number, "hash", parent.Hash(), "err", err)
			if depth > 0 { // Head is surely available, it was just announced
				return errors.New("couldn't prove state availability")
			}
		}
	}
	// All parents tracked, add the new head too
	b.logger.Debug("Tracking new block", "number", head.Number, "hash", hash)
	b.recents.Push(hash, -head.Number.Int64())
	b.headers[hash] = head
	b.states[head.Root]++
	if b.states[head.Root] == 1 {
		b.logger.Debug("Tracking new state", "root", head.Root)
	}
	// Since state is pruned, untrack anything older than the cutoff
	for !b.recents.Empty() {
		if item, prio := b.recents.Peek(); -prio <= head.Number.Int64()-recentnessCutoff {
			var (
				hash   = item.(common.Hash)
				header = b.headers[hash]
			)
			b.logger.Debug("Untracking old block", "number", header.Number, "hash", hash)

			delete(b.headers, hash)
			b.recents.PopItem()

			b.states[header.Root]--
			if b.states[header.Root] == 0 {
				b.logger.Debug("Untracking old state", "root", header.Root)
				delete(b.states, header.Root)
			}
			continue
		}
		break
	}
	return nil
}

// subscribeNewHead subscribes to new chain head events to act as triggers for
// the task tiler.
func (b *backend) subscribeNewHead(sink chan *types.Header) event.Subscription {
	return b.headFeed.Subscribe(sink)
}

// sendRequest checks the rate limiter first and then executes the request function.
func (b *backend) sendRequest(ctx context.Context, request func()) error {
	if err := b.limiter.Wait(ctx); err != nil {
		return err
	}
	request()
	return nil
}

// hasState checks whether a state is available from this backend.
// If the state is too old, then regard it as unavailable.
func (b *backend) hasState(root common.Hash) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, ok := b.states[root]
	return ok
}

// hasBlock checks whether a block is available from this backend.
// If the block is too old, return false also.
func (b *backend) hasBlock(hash common.Hash) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if _, ok := b.headers[hash]; ok {
		return true
	}
	return false
}

func (b *backend) getTile(hash common.Hash) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := b.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	b.logger.Trace("Fetching state tile", "hash", hash)

	var result [][]byte
	start := time.Now()
	err := b.conn.CallContext(ctx, &result, "cdn_tile", hash, 16, 256, 2)
	if err != nil {
		return nil, err
	}
	if err != nil {
		b.logger.Trace("Failed to fetch state tile", "hash", hash, "error", err)
	} else {
		b.logger.Trace("State tile fetched", "hash", hash, "nodes", len(result), "elapsed", time.Since(start))
	}
	return result, err
}

func (b *backend) getBlockByHash(hash common.Hash) (*types.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := b.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	b.logger.Trace("Fetching block header", "hash", hash)

	block, err := b.client.BlockByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (b *backend) getReceiptByHash(hash common.Hash) (*types.Receipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := b.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	b.logger.Trace("Fetching receipts", "hash", hash)

	receipt, err := b.client.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

type backendSet struct {
	lock sync.RWMutex
	set  map[string]*backend
}

func newBackendSet() *backendSet {
	return &backendSet{set: make(map[string]*backend)}
}

func (set *backendSet) addBackend(id string, b *backend) error {
	set.lock.Lock()
	defer set.lock.Unlock()

	if _, ok := set.set[id]; ok {
		return errors.New("duplicated backend")
	}
	set.set[id] = b
	return nil
}

func (set *backendSet) removeBackend(id string) error {
	set.lock.Lock()
	defer set.lock.Unlock()

	if _, ok := set.set[id]; !ok {
		return errors.New("non-existent backend")
	}
	delete(set.set, id)
	return nil
}

func (set *backendSet) hasState(root common.Hash) (string, bool) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	// Totally random check, no priority grant.
	for id, backend := range set.set {
		if backend.hasState(root) {
			return id, true
		}
	}
	return "", false
}

func (set *backendSet) hasBlock(hash common.Hash) string {
	set.lock.RLock()
	defer set.lock.RUnlock()

	var (
		index  int
		random string
		target = rand.Intn(len(set.set))
	)
	for id, backend := range set.set {
		if backend.hasBlock(hash) {
			return id
		}
		if index == target {
			random = id
		} else {
			index += 1
		}
	}
	// Nobody has the block, return a random backend.
	// It's very hard to determine whether the block
	// is not existent or too old(mostly it's too old).
	return random
}

func (set *backendSet) backend(id string) *backend {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return set.set[id]
}

// forEach iterates the whole backend set and applies callback on each of them.
// Stop iteration when the callback returns false
func (set *backendSet) forEach(callback func(id string, backend *backend) bool) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	if callback == nil {
		return
	}
	for id, backend := range set.set {
		if !callback(id, backend) {
			return
		}
	}
}
