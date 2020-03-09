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
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// headAnnounce is a new-head event tagged with the origin backend.
type headAnnounce struct {
	origin *backend
	head   *types.Header
}

// statistic is a set of stats for logging.
type statistic struct {
	storageTask     uint32 // Total number of allocated storage trie task
	stateTask       uint32 // Total number of allocated state trie task
	dropEntireTile  uint32 // Counter for dropping entire fetched tile to dedup
	dropPartialTile uint32 // Counter for dropping partial fetched tile
	duplicateTask   uint32 // Counter of allocated but duplicated task
	emptyTask       uint32 // Counter of allocated but empty task
	failures        uint32 // Counter of tile task failures
}

// tileQuery represents a request for asking whether a specified
// tile is indexed.
type tileQuery struct {
	hash   common.Hash
	result chan tileAnswer
}

// tileAnswer represents an answer for tile querying.
type tileAnswer struct {
	tile  *tileInfo
	state common.Hash
}

// tiler is responsible for keeping track of pending state and chain crawl
// jobs and distributing them to backends that have the dataset available.
type tiler struct {
	db            ethdb.Database
	addBackendCh  chan *backend
	remBackendCh  chan *backend
	newHeadCh     chan *headAnnounce
	newTileCh     chan *tileDelivery
	tileQueryCh   chan *tileQuery
	closeCh       chan struct{}
	removeBackend func(string) error
	stat          *statistic
	wg            sync.WaitGroup
}

// newTiler creates a tile crawl tiler.
func newTiler(db ethdb.Database, removeBackend func(string) error) *tiler {
	s := &tiler{
		db:            db,
		addBackendCh:  make(chan *backend),
		remBackendCh:  make(chan *backend),
		newHeadCh:     make(chan *headAnnounce),
		newTileCh:     make(chan *tileDelivery),
		tileQueryCh:   make(chan *tileQuery),
		removeBackend: removeBackend,
		closeCh:       make(chan struct{}),
		stat:          &statistic{},
	}
	return s
}

func (t *tiler) start() {
	t.wg.Add(2)
	go t.loop()
	go t.logger()
}

func (t *tiler) stop() {
	close(t.closeCh)
	t.wg.Wait()
}

// loop is the main event loop of the tiler, receiving various events and
// acting on them.
func (t *tiler) loop() {
	defer t.wg.Done()

	var (
		ids      []string
		backends = make(map[string]*backend)
		heads    = make(map[string]*types.Header)
		assigned = make(map[string]struct{})

		generator *generator
		pivot     *types.Header
		latest    *types.Header
		tileDB    = newTileDatabase(t.db)
	)

	for {
		// Assign new tasks to backends for tile indexing
		if generator != nil {
			for id, backend := range backends {
				if _, ok := assigned[id]; ok {
					continue
				}
				task := generator.assignTasks(id)
				if task == (common.Hash{}) {
					continue
				}
				assigned[id] = struct{}{}

				// Better solution, using a worker pool instead
				// of creating routines infinitely.
				go func() {
					nodes, err := backend.getTile(task)
					t.newTileCh <- &tileDelivery{
						origin: backend,
						hash:   task,
						nodes:  nodes,
						err:    err,
					}
				}()
			}
			if generator.pending() == 0 && pivot != nil {
				tileDB.commit(pivot.Root)
				log.Info("Tiles indexed", "number", pivot.Number, "hash", pivot.Hash(), "state", pivot.Root)

				pivot = nil
			}
		}
		// Handle any events, not much to do if nothing happens
		select {
		case <-t.closeCh:
			return

		case b := <-t.addBackendCh:
			if _, ok := backends[b.id]; ok {
				b.logger.Error("backend already registered in tiler")
				continue
			}
			b.logger.Info("backend registered into tiler")
			go t.muxHeadEvents(b)
			backends[b.id] = b
			ids = append(ids, b.id)

		case b := <-t.remBackendCh:
			if _, ok := backends[b.id]; !ok {
				b.logger.Error("backend not registered in tiler")
				continue
			}
			b.logger.Info("backend unregistered from tiler")
			delete(backends, b.id)

			for index, id := range ids {
				if id == b.id {
					ids = append(ids[:index], ids[index+1:]...)
					break
				}
			}
			if t.removeBackend != nil {
				t.removeBackend(b.id)
			}

		case announce := <-t.newHeadCh:
			heads[announce.origin.id] = announce.head
			if latest == nil || announce.head.Number.Uint64() > latest.Number.Uint64() {
				latest = announce.head
			}
			callback := func(leaf []byte, parent common.Hash) error {
				var obj state.Account
				if err := rlp.Decode(bytes.NewReader(leaf), &obj); err != nil {
					return err
				}
				generator.addTask(obj.Root, 64, parent, nil) // Spin up more sub tasks for storage trie
				return nil
			}
			// If it's first fired or newer state is available,
			// create generator to generate crawl tasks
			if pivot == nil || pivot.Number.Uint64()+recentnessCutoff <= announce.head.Number.Uint64() {
				pivot = announce.head
				generator = newGenerator(pivot.Root, tileDB, t.stat)
				generator.addTask(pivot.Root, 0, common.Hash{}, callback)
			}

		case tile := <-t.newTileCh:
			delete(assigned, tile.origin.id)
			generator.process(tile, ids)

		case req := <-t.tileQueryCh:
			tile, state := generator.getTile(req.hash)
			req.result <- tileAnswer{
				tile:  tile,
				state: state,
			}
		}
	}
}

// muxHeadEvents registers a watcher for new chain head events on the backend and
// multiplexes the events into a common channel for the tiler to handle.
func (t *tiler) muxHeadEvents(b *backend) {
	heads := make(chan *types.Header)

	sub := b.subscribeNewHead(heads)
	defer sub.Unsubscribe()

	for {
		select {
		case head := <-heads:
			b.logger.Trace("new header", "hash", head.Hash(), "number", head.Number)
			t.newHeadCh <- &headAnnounce{origin: b, head: head}
		case err := <-sub.Err():
			b.logger.Warn("backend head subscription failed", "err", err)
			t.remBackendCh <- b
			return
		}
	}
}

// registerBackend starts tracking a new Ethereum backend to delegate tasks to.
func (t *tiler) registerBackend(b *backend) {
	t.addBackendCh <- b
}

func (t *tiler) hasTile(root common.Hash) tileAnswer {
	result := make(chan tileAnswer, 1)
	select {
	case t.tileQueryCh <- &tileQuery{
		hash:   root,
		result: result,
	}:
		return <-result
	case <-t.closeCh:
		return tileAnswer{}
	}
}

// logger is a helper loop to print internal statistic with a fixed time interval.
func (t *tiler) logger() {
	defer t.wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			log.Info("Tiler progress", "state", atomic.LoadUint32(&t.stat.stateTask), "storage", atomic.LoadUint32(&t.stat.storageTask),
				"drop", atomic.LoadUint32(&t.stat.dropEntireTile), "droppart", atomic.LoadUint32(&t.stat.dropPartialTile),
				"duplicate", atomic.LoadUint32(&t.stat.duplicateTask), "empty", atomic.LoadUint32(&t.stat.emptyTask), "failure", atomic.LoadUint32(&t.stat.failures))
		}
	}
}
