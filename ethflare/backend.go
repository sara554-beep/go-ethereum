package main

import (
	"context"
	"errors"
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
)

// recentnessCutoff is the number of blocks after which a block is considered
// unavailable for state retrievals.
const recentnessCutoff = 128

// Backend represents an Ethereum node with the ethflare RPC API implemented and
// exposed via WebSockets (we need notification support for new heads).
type Backend struct {
	id     string
	conn   *rpc.Client
	client *ethclient.Client

	headSub  ethereum.Subscription
	headCh   chan *types.Header
	headFeed *event.Feed

	headers map[common.Hash]*types.Header // Set of recent headers across all mini-forks
	recents *prque.Prque                  // Priority queue for evicting stale headers
	states  map[common.Hash]int           // Set of state roots available for tiling

	logger log.Logger
	lock   sync.RWMutex
}

// NewBackend takes a live websocket connection to a backend and starts to monitor
// its chain progression and optionally request chain and state data.
func NewBackend(id string, conn *rpc.Client) (*Backend, error) {
	client := ethclient.NewClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sink := make(chan *types.Header, 256)
	sub, err := client.SubscribeNewHead(ctx, sink)
	if err != nil {
		return nil, err
	}
	backend := &Backend{
		id:       id,
		conn:     conn,
		client:   client,
		headSub:  sub,
		headCh:   sink,
		headFeed: new(event.Feed),
		headers:  make(map[common.Hash]*types.Header),
		recents:  prque.New(nil),
		states:   make(map[common.Hash]int),
		logger:   log.New("id", id),
	}
	go backend.loop()
	return backend, nil
}

// loop keeps exhausting the head header announcement channel, maintaining the
// current fork tree as seen by the backing node.
func (b *Backend) loop() {
	var updating chan struct{}

	for {
		select {
		case head := <-b.headCh:
			// New head announced, update the fork tree if we're not currently updating
			if updating == nil {
				updating = make(chan struct{})
				go func() {
					if err := b.update(head, 0); err != nil {
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
			// Subscription died, terminat the loop
			return
		}
	}
}

// update extends the currently maintained fork tree of this backing node with a
// new head and it's progression towards already known blocks.
func (b *Backend) update(head *types.Header, depth int) error {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// If the header is known already known, bail out
	hash := head.Hash()
	if _, ok := b.headers[hash]; ok {
		return nil
	}
	// If the parent lookup reached the limit without hitting a recently announced
	// head, the entire parent chain needs to be discarded since there's no way to
	// know if associated state is present or not (node reboot)
	if depth >= recentnessCutoff {
		return errors.New("exceeded recentness custoff threshold")
	}
	// Otherwise track all parents first, then the new head
	if head.Number.Uint64() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		parent, err := b.client.HeaderByHash(ctx, head.ParentHash)
		if err != nil {
			return err
		}
		if err := b.update(parent, depth+1); err != nil {
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

// SubscribeNewHead subscribes to new chain head events to act as triggers for
// the task scheduler.
func (b *Backend) SubscribeNewHead(sink chan *types.Header) event.Subscription {
	return b.headFeed.Subscribe(sink)
}

// Availability returns the set of headers that are considered live by this node.
func (b *Backend) Availability(cutoff uint64) []*types.Header {
	b.lock.RLock()
	defer b.lock.RUnlock()

	live := make([]*types.Header, 0, len(b.headers))
	for _, header := range b.headers {
		if header.Number.Uint64() > cutoff {
			live = append(live, header)
		}
	}
	return live
}

// Available checks whether a state is available from this backend.
func (b *Backend) Available(root common.Hash) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, ok := b.states[root]
	return ok
}
