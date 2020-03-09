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
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
)

// emptyRoot is the known root hash of an empty trie.
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// leafCallback is a callback type invoked when a trie operation reaches a leaf
// node. It's used by state sync and commit to allow handling external references
// between account and storage tries.
type leafCallback func(leaf []byte, parent common.Hash) error

// tileRequest contains all context info of a tile request.
type tileRequest struct {
	hash     common.Hash    // Node hash of tile
	depth    uint8          // Depth tracker to allow reporting per-depth statistics
	parents  []*tileRequest // Parent tile requests referencing this entry (notify all upon completion)
	deps     int            // The number of children tiles referenced by it
	attempts map[string]struct{}
	onLeaf   leafCallback // Callback applied on leave nodes to expand more tasks
}

// tileDelivery is a tile creation event tagged with the origin backend.
type tileDelivery struct {
	origin *backend      // backend that should be marked idle on delivery
	hash   common.Hash   // Trie node (tile root) to reschedule upon failure
	nodes  [][]byte      // Delivered tile data upon success
	hashes []common.Hash // Hashes of included nodes, derived later
	refs   []common.Hash // Tile references, derived later
	err    error         // Encountered error upon failure
}

// generator is responsible for keeping track of pending state-tile crawl jobs
// and distributing them to backends that have the dataset available.
type generator struct {
	state      common.Hash
	requests   map[common.Hash]*tileRequest
	deliveries map[common.Hash]*tileDelivery
	queue      *prque.Prque
	database   *tileDatabase
	stat       *statistic
}

// newGenerator creates a state-tile crawl tiler.
func newGenerator(state common.Hash, database *tileDatabase, stat *statistic) *generator {
	return &generator{
		state:      state,
		requests:   make(map[common.Hash]*tileRequest),
		deliveries: make(map[common.Hash]*tileDelivery),
		database:   database,
		queue:      prque.New(nil),
		stat:       stat,
	}
}

func (g *generator) hasTile(hash common.Hash) (bool, common.Hash) {
	if g.deliveries[hash] != nil {
		return true, g.state // It's in memory, return target state hash
	}
	has, state := g.database.has(hash)

	// If no associated state is returned, use the latest
	if has && state == (common.Hash{}) {
		state = g.state
	}
	return has, state
}

func (g *generator) getTile(hash common.Hash) (*tileInfo, common.Hash) {
	if tile := g.deliveries[hash]; tile != nil {
		req := g.requests[hash]
		return &tileInfo{
			Depth:  req.depth,
			Nodes:  uint16(len(tile.nodes)),
			Hashes: tile.hashes,
			Refs:   tile.refs,
		}, g.state // It's in memory, return target state hash
	}
	tile, state := g.database.get(hash)

	// If no associated state is returned, use the latest
	if tile != nil && state == (common.Hash{}) {
		state = g.state
	}
	return tile, state
}

func (g *generator) addTask(hash common.Hash, depth uint8, parent common.Hash, onLeaf leafCallback) {
	// Short circuit if the tile is empty
	if hash == emptyRoot {
		atomic.AddUint32(&g.stat.emptyTask, 1)
		return
	}
	// Short circuit if the tile is already known
	if has, _ := g.hasTile(hash); has {
		atomic.AddUint32(&g.stat.duplicateTask, 1)
		return
	}
	// Assemble the new sub-tile sync request
	req := &tileRequest{
		hash:     hash,
		depth:    depth,
		attempts: make(map[string]struct{}),
		onLeaf:   onLeaf,
	}
	// If this sub-trie has a designated parent, link them together
	if parent != (common.Hash{}) {
		ancestor := g.requests[parent]
		if ancestor == nil {
			panic(fmt.Sprintf("sub-tile ancestor not found: %x", parent))
		}
		ancestor.deps++
		req.parents = append(req.parents, ancestor)
	}
	g.schedule(req)

	if onLeaf != nil {
		atomic.AddUint32(&g.stat.stateTask, 1)
	} else {
		atomic.AddUint32(&g.stat.storageTask, 1)
	}
}

// schedule inserts a new tile retrieval request into the fetch queue. If there
// is already a pending request for this node, the new request will be discarded
// and only a parent reference added to the old one.
func (g *generator) schedule(req *tileRequest) {
	// If we're already requesting this node, add a new reference and stop
	if old, ok := g.requests[req.hash]; ok {
		old.parents = append(old.parents, req.parents...)
		return
	}
	// Schedule the request for future retrieval
	g.queue.Push(req.hash, int64(req.depth))
	g.requests[req.hash] = req
	log.Trace("Add new task", "hash", req.hash)
}

func (g *generator) assignTasks(backend string) common.Hash {
	var (
		depths  []uint8
		poplist []common.Hash
	)
	defer func() {
		for i := 0; i < len(poplist); i++ {
			g.queue.Push(poplist[i], int64(depths[i]))
		}
	}()
	for !g.queue.Empty() {
		hash := g.queue.PopItem().(common.Hash)
		request := g.requests[hash]
		if _, ok := request.attempts[backend]; ok {
			poplist, depths = append(poplist, hash), append(depths, request.depth)
			continue
		}
		request.attempts[backend] = struct{}{}
		return hash
	}
	return common.Hash{} // No more task available
}

func (g *generator) pending() int {
	return len(g.requests)
}

// process injects the retrieved tile and expands more sub tasks from the
// tile references.
func (g *generator) process(delivery *tileDelivery, backends []string) error {
	if g.requests[delivery.hash] == nil {
		return errors.New("non-existent request")
	}
	request := g.requests[delivery.hash]

	// If tile retrieval failed or nothing returned, reschedule it
	if delivery.err != nil || len(delivery.nodes) == 0 {
		// Check whether there still exists some available backends to retry.
		for _, backend := range backends {
			if _, ok := request.attempts[backend]; !ok {
				g.queue.Push(request.hash, int64(request.depth))
				return nil
			}
		}
		// If we already try all backends to fetch it, discard it
		// silently. It's ok if the tile is still referenced by state,
		// we can retrieve it later.
		atomic.AddUint32(&g.stat.failures, 1)
		return nil
	}
	// Decode the nodes in the tile and continue expansion to newly discovered ones
	var (
		removed     []int
		removedRefs = make(map[common.Hash]bool)
		included    = make(map[common.Hash]struct{})
	)
	// Eliminate the intermediate nodes and their children if they are already tiled.
	for index, node := range delivery.nodes {
		hash := crypto.Keccak256Hash(node)
		has, _ := g.hasTile(hash)
		if has || removedRefs[hash] {
			// If the first node is also eliminated, it means the whole tile
			// is retrieved by other means, discard the whole response.
			//
			// todo why the task will be created in the first place?
			if index == 0 {
				atomic.AddUint32(&g.stat.dropEntireTile, 1)
				delete(g.requests, request.hash)
				g.commitParent(request)
				return nil
			}
			removed = append(removed, index)
			trie.IterateRefs(node, func(path []byte, child common.Hash) error {
				removedRefs[child] = true
				return nil
			}, nil)
			continue
		}
		included[hash] = struct{}{}
	}
	// If any intermediate nodes are the root of another tile, remove it.
	//
	// todo if the simplified tile is too small(e.g. less than 8 nodes),
	// merge it to it's parent.
	var deleted int
	for index := range removed {
		delivery.nodes = append(delivery.nodes[:index-deleted], delivery.nodes[index-deleted+1:]...)
		deleted += 1
	}
	if len(removed) > 0 {
		atomic.AddUint32(&g.stat.dropPartialTile, 1)
	}
	// Expand more children tasks, the first node should never be eliminated
	depths := map[common.Hash]uint8{
		crypto.Keccak256Hash(delivery.nodes[0]): request.depth,
	}
	var children []*tileRequest
	for _, node := range delivery.nodes {
		trie.IterateRefs(node, func(path []byte, child common.Hash) error {
			depths[child] = depths[crypto.Keccak256Hash(node)] + uint8(len(path))
			if _, ok := included[child]; !ok {
				delivery.refs = append(delivery.refs, child)

				// Add the ref as the task if it's still not crawled.
				if has, _ := g.hasTile(child); !has {
					children = append(children, &tileRequest{
						hash:     child,
						depth:    depths[child],
						parents:  []*tileRequest{request},
						attempts: make(map[string]struct{}),
						onLeaf:   request.onLeaf,
					})
					if request.onLeaf != nil {
						atomic.AddUint32(&g.stat.stateTask, 1)
					} else {
						atomic.AddUint32(&g.stat.storageTask, 1)
					}
				}
			}
			return nil
		}, func(path []byte, node []byte) error {
			if request.onLeaf != nil {
				request.onLeaf(node, request.hash)
			}
			return nil
		})
	}
	log.Trace("Delivered tile", "hash", request.hash, "nodes", len(delivery.nodes), "reference", len(delivery.refs))

	for hash := range included {
		delivery.hashes = append(delivery.hashes, hash)
	}
	// We still need to check whether deps is zero or not.
	// Sub task may be created via callback.
	if len(children) == 0 && request.deps == 0 {
		g.commit(request, delivery)
		return nil
	}
	request.deps += len(children)
	g.deliveries[request.hash] = delivery

	for _, child := range children {
		g.schedule(child)
	}
	return nil
}

// commit finalizes a retrieval request and stores it into the membatch. If any
// of the referencing parent requests complete due to this commit, they are also
// committed themselves.
func (g *generator) commit(req *tileRequest, delivery *tileDelivery) error {
	var storage common.StorageSize
	for _, node := range delivery.nodes {
		storage += common.StorageSize(len(node))
	}
	g.database.insert(req.hash, req.depth, storage, len(delivery.nodes), delivery.hashes, delivery.refs)
	delete(g.deliveries, req.hash)
	delete(g.requests, req.hash)

	// Check all parents for completion
	return g.commitParent(req)
}

func (g *generator) commitParent(req *tileRequest) error {
	// Check all parents for completion
	for _, parent := range req.parents {
		parent.deps--
		if parent.deps == 0 {
			if err := g.commit(parent, g.deliveries[parent.hash]); err != nil {
				return err
			}
		}
	}
	return nil
}
