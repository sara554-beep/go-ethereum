package main

import (
	"github.com/ethereum/go-ethereum/core/types"
)

// headAnnounce is a new-head event tagged with the origin backend.
type headAnnounce struct {
	origin *Backend
	head   *types.Header
}

// Scheduler is responsible for keeping track of pending state and chain crawl
// jobs and distributing them to backends that have the dataset available.
type Scheduler struct {
	addBackendCh chan *Backend
	remBackendCh chan *Backend
	newHeadCh    chan *headAnnounce
	newTileCh    chan *tileDelivery
}

// NewScheduler creates a tile crawl scheduler.
func NewScheduler() *Scheduler {
	s := &Scheduler{
		addBackendCh: make(chan *Backend),
		remBackendCh: make(chan *Backend),
		newHeadCh:    make(chan *headAnnounce),
		newTileCh:    make(chan *tileDelivery),
	}
	go s.loop()

	return s
}

// loop is the main event loop of the scheduler, receiving various events and
// acting on them.
func (s *Scheduler) loop() {
	var (
		backends = make(map[string]*Backend)
		progress = make(map[string]struct{})
		heads    = make(map[string]*types.Header)

		tiler = NewTiler(s.newTileCh)

		tiles    []*tileDelivery
		updating chan struct{}
	)
	for {
		// Handle any events, not much to do if nothing happens
		select {
		case b := <-s.addBackendCh:
			if _, ok := backends[b.id]; ok {
				b.logger.Error("Backend already registered in scheduler")
				continue
			}
			b.logger.Info("Backend registered into scheduler")
			go s.muxHeadEvents(b)
			backends[b.id] = b

		case b := <-s.remBackendCh:
			if _, ok := backends[b.id]; !ok {
				b.logger.Error("Backend not registered in scheduler")
				continue
			}
			b.logger.Info("Backend unregistered from scheduler")
			delete(backends, b.id)

		case announce := <-s.newHeadCh:
			heads[announce.origin.id] = announce.head
			progress[announce.origin.id] = struct{}{}

		case tile := <-s.newTileCh:
			tiles = append(tiles, tile)

		case <-updating:
			updating = nil
		}
		// Something happened. If we're not running an update loop, ping the tiler
		if updating == nil {
			updating = make(chan struct{})

			// Create a copy of the backends and latest announced heads
			backendsCopy := make(map[string]*Backend, len(backends))
			for id, b := range backends {
				backendsCopy[id] = b
			}
			headsCopy := make(map[string]*types.Header, len(heads))
			for id, header := range heads {
				headsCopy[id] = header
			}
			progressOld, tilesOld := progress, tiles
			progress, tiles = make(map[string]struct{}), nil

			go func() {
				tiler.Update(backendsCopy, headsCopy, progressOld, tilesOld)
				updating <- struct{}{}
			}()
		}
	}
}

// muxHeadEvents registers a watcher for new chain head events on the backend and
// multiplexes the events into a common channel for the scheduler to handle.
func (s *Scheduler) muxHeadEvents(b *Backend) {
	heads := make(chan *types.Header)

	sub := b.SubscribeNewHead(heads)
	defer sub.Unsubscribe()

	for {
		select {
		case head := <-heads:
			s.newHeadCh <- &headAnnounce{origin: b, head: head}
		case err := <-sub.Err():
			b.logger.Warn("Backend head subscription failed", "err", err)
			s.remBackendCh <- b
			return
		}
	}
}

// RegisterBackend starts tracking a new Ethereum backend to delegate tasks to.
func (s *Scheduler) RegisterBackend(b *Backend) {
	s.addBackendCh <- b
}
