package main

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
)

type tileInfo struct {
	depth uint8
	size  common.StorageSize
	refs  []common.Hash
}

// tileContext is a pending tile retrieval task to hold the contextual infos.
// The task may be reowned by blocks more recent than the origin.
type tileContext struct {
	block uint64      // Block number to reschedule into upon failure
	root  common.Hash // State root within siblings to reschedule into upon failure
	depth uint8       // Depth tracker to allow reporting per-depth statistics
}

// tileDelivery is a tile creation event tagged with the origin backend.
type tileDelivery struct {
	origin *Backend    // Backend that should be marked idle on delivery
	hash   common.Hash // Trie node (tile root) to reschedule upon failure
	nodes  [][]byte    // Delivered tile data upon success
	err    error       // Encountered error upon failure
}

// Tiler is responsible for keeping track of pending state-tile crawl jobs
// and distributing them to backends that have the dataset available.
type Tiler struct {
	// is used to deliver tiles from individual backends
	sink chan *tileDelivery

	// taskset is the crawl tasks, grouped first by block numbers, then by root hash,
	// mapping finally to the depth where this tile is rooted at.
	taskset map[uint64]map[common.Hash]map[common.Hash]uint8

	// tileset contains metadata about all the tracked state tiles that are used
	// for both crawling as well as pruning.
	tileset map[common.Hash]*tileInfo

	// pending contains the currently pending tile retrieval tasks with enough
	// contextual metadata to reschedule it in case of failure.
	pending map[common.Hash]*tileContext

	// assigned tracks the backends currnetly assigned to some task.
	assigned map[string]bool
}

// NewTiler creates a state-tile crawl scheduler.
func NewTiler(sink chan *tileDelivery) *Tiler {
	return &Tiler{
		sink:     sink,
		taskset:  make(map[uint64]map[common.Hash]map[common.Hash]uint8),
		tileset:  make(map[common.Hash]*tileInfo),
		pending:  make(map[common.Hash]*tileContext),
		assigned: make(map[string]bool),
	}
}

// Update modifies the tiler's internal state by discarding all crawl tasks that
// became stale, adds any new tasks originating from head announcements or tile
// deliveries and issues the crawl request to suitable idle backends.
func (t *Tiler) Update(backends map[string]*Backend, heads map[string]*types.Header, progress map[string]struct{}, tiles []*tileDelivery) {
	// Discard stale tasks and schedule new ones
	cutoff, latest := t.dropStaleTasks(heads)
	t.schedNewTasks(backends, progress, cutoff)

	// Iterate over any recently delivered tiles and integrate them into the tasksets
	for _, tile := range tiles {
		// First up, mark the backend as idle
		t.assigned[tile.origin.id] = false

		context := t.pending[tile.hash]
		delete(t.pending, tile.hash)

		// If tile retrieval failed or nothing returned, reschedule it
		if tile.err != nil || len(tile.nodes) == 0 {
			t.taskset[context.block][context.root][tile.hash] = context.depth
			continue
		}
		// Mark the tile as crawled and available
		t.tileset[tile.hash] = &tileInfo{
			depth: context.depth,
		}
		var storage common.StorageSize
		for _, node := range tile.nodes {
			storage += common.StorageSize(len(node))
		}
		t.tileset[tile.hash].size = storage

		// Decode the nodes in the tile and continue expansion to newly discovered ones
		included := make(map[common.Hash]struct{})
		for _, node := range tile.nodes {
			included[crypto.Keccak256Hash(node)] = struct{}{}
		}
		depths := map[common.Hash]uint8{
			crypto.Keccak256Hash(tile.nodes[0]): context.depth,
		}
		for _, node := range tile.nodes {
			trie.IterateRefs(node, func(path []byte, child common.Hash) error {
				depths[child] = depths[crypto.Keccak256Hash(node)] + uint8(len(path))
				if _, ok := included[child]; !ok {
					t.tileset[tile.hash].refs = append(t.tileset[tile.hash].refs, child)
					if queue, ok := t.taskset[context.block]; ok {
						queue[context.root][tile.hash] = depths[child]
					}
				}
				return nil
			})
		}
	}
	// Iterate over the idle backends and attempt to assign crawl tasks to them
	for id, b := range backends {
		if !t.assigned[id] {
			// Found an idle backend, find the next most urgent task needing crawling
			for block := latest; block > cutoff; block-- {
				for root, tasks := range t.taskset[block] {
					// If there are no tasks or the node doesn't have the state, skip
					if len(tasks) == 0 {
						continue
					}
					if !b.Available(root) {
						continue
					}
					// Yay, we found a task for this node, assign it (we don't care about the
					// order, we need to crawl everything anyway).
					var (
						task  common.Hash
						depth uint8
					)
					for key, val := range tasks {
						task, depth = key, val
						break
					}
					delete(tasks, task)

					// If the tile was retrieved in the mean time, skip it
					if _, ok := t.tileset[task]; ok {
						continue
					}
					// If the tile is being actively retrieved, reown it but also skip it
					if pend, ok := t.pending[task]; ok {
						if pend.block < block {
							pend.block, pend.root = block, root
						}
						continue
					}
					// Start a tile fetch in the background and deliver the result or failure
					// to the tiler sink.
					t.pending[task] = &tileContext{block: block, root: root, depth: depth}
					t.assigned[id] = true

					go t.fetchTile(b, task)
					break
				}
				// If something was assigned, skip to the next backend
				if t.assigned[id] {
					break
				}
			}
		}
	}
	// If any backend went missing, drop the assignment tracking
	for id := range t.assigned {
		if _, ok := backends[id]; !ok {
			delete(t.assigned, id)
		}
	}
}

// dropStaleTasks calculates the ideal header range, discarding tasks below the
// cutoff threshold based on the best available backend.
func (t *Tiler) dropStaleTasks(heads map[string]*types.Header) (uint64, uint64) {
	// Calculate the idea header range
	var (
		cutoff uint64
		latest uint64
	)
	for _, head := range heads {
		if number := head.Number.Uint64(); number > latest {
			latest = number
		}
	}
	if latest > recentnessCutoff {
		cutoff = latest - recentnessCutoff
	}
	// Drop all tasks below the cutoff threshold
	for number := range t.taskset {
		if number <= cutoff {
			var tasks int
			for _, subtasks := range t.taskset[number] {
				tasks += len(subtasks)
			}
			log.Warn("Block stale, dropping crawl tasks", "number", number, "tasks", tasks)
			delete(t.taskset, number)
		}
	}
	return cutoff, latest
}

// schedNewTasks retrieves the entire set of available headers for any backends
// that just progressed and schedules a crawl task for the root hash.
func (t *Tiler) schedNewTasks(backends map[string]*Backend, progress map[string]struct{}, cutoff uint64) {
	for id := range progress {
		// Skip any progressed backends that quit since
		b := backends[id]
		if b == nil {
			continue
		}
		// Retrieve all the available headers above the cutoff and schedule their state
		for _, header := range b.Availability(cutoff) {
			number := header.Number.Uint64()

			// Skip any tiles already crawled or currently crawling (reown if newer)
			if _, ok := t.tileset[header.Root]; ok {
				continue
			}
			if pend, ok := t.pending[header.Root]; ok {
				if pend.block < number {
					pend.block, pend.root = number, header.Root
				}
				continue
			}
			// Not yet crawled, deduplicate and schedule it if unique
			if _, ok := t.taskset[number]; !ok {
				t.taskset[number] = make(map[common.Hash]map[common.Hash]uint8)
			}
			if _, ok := t.taskset[number][header.Root]; !ok {
				t.taskset[number][header.Root] = make(map[common.Hash]uint8)
			}
			if _, ok := t.taskset[number][header.Root][header.Root]; !ok {
				log.Info("Scheduled new crawl task set", "number", number, "hash", header.Hash(), "root", header.Root)
				t.taskset[number][header.Root][header.Root] = 0
			}
		}
	}
}

// fetchTile attempts to retrieve a state trie tile from a remote backend.
func (t *Tiler) fetchTile(b *Backend, hash common.Hash) {
	// Make sure the tile retrieval doesn't hang indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fetch the tile and construct a delivery report out of it
	b.logger.Trace("Fetching state tile", "block", hash)
	start := time.Now()

	result := &tileDelivery{
		origin: b,
		hash:   hash,
	}
	result.err = b.conn.CallContext(ctx, &result.nodes, "cdn_tile", hash, 16, 256, 2)

	if result.err != nil {
		b.logger.Trace("Failed to fetch state tile", "block", hash)
	} else {
		b.logger.Trace("State tile fetched", "block", hash, "nodes", len(result.nodes), "elapsed", time.Since(start))
	}
	// Eiter way, deliver the results to the tiler
	t.sink <- result
}
