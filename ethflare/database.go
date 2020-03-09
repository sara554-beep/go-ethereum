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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

const commitThreshold = 1024

var (
	headStateKey = []byte("LatestState") // headStateKey tracks the latest committed state root
	tilePrefix   = []byte("-t")          // tilePrefix + tilehash => tile
)

type tileInfo struct {
	Depth  uint8
	Nodes  uint16
	Hashes []common.Hash
	Refs   []common.Hash

	flushPrev common.Hash // Previous node in the flush-list
	flushNext common.Hash // Next node in the flush-list
}

type diffLayer struct {
	parent *diffLayer                // Pointer to parent layer, nil if it's the deepest
	state  common.Hash               // Empty means the tile is not completed yet(still crawling)
	tiles  map[common.Hash]*tileInfo // Tile set, linked by the insertion order

	oldest common.Hash // Oldest tracked node, flush-list head
	newest common.Hash // Newest tracked node, flush-list tail
}

func (diff *diffLayer) flush(db *tileDatabase) (*diffLayer, int, error) {
	if diff.parent != nil {
		parent, committed, err := diff.parent.flush(db)
		if err != nil {
			return parent, committed, err
		}
		diff.parent = parent // may be set to nil if parent is flushed
		return diff, committed, err
	}
	batch, nodes := db.db.NewBatch(), len(diff.tiles)
	for h, tile := range diff.tiles {
		enc, err := rlp.EncodeToBytes(tile)
		if err != nil {
			return nil, 0, err
		}
		batch.Put(append(tilePrefix, h.Bytes()...), enc)
	}
	if diff.state != (common.Hash{}) {
		batch.Put(headStateKey, diff.state.Bytes()) // Commit state root to represent the whole state is tiled.
		delete(db.diffset, diff.state)              // Only complete layer will be registered into set
		diff.state = common.Hash{}
	}
	if err := batch.Write(); err != nil {
		return nil, 0, err
	}
	diff.tiles = make(map[common.Hash]*tileInfo)
	diff.oldest, diff.newest = common.Hash{}, common.Hash{}
	return nil, nodes, nil
}

func (diff *diffLayer) cap(db *tileDatabase, needDrop int) (*diffLayer, int, error) {
	var done int
	if diff.parent != nil {
		parent, committed, err := diff.parent.cap(db, needDrop)
		if err != nil {
			return parent, committed, err
		}
		diff.parent = parent // may be set to nil if parent is flushed
		if needDrop <= committed {
			return diff, committed, nil
		}
		needDrop -= committed
		done = committed
	}
	batch, nodes := db.db.NewBatch(), len(diff.tiles)

	// Keep committing nodes from the flush-list until we're below allowance
	oldest := diff.oldest
	for needDrop > 0 && oldest != (common.Hash{}) {
		tile := diff.tiles[oldest]
		enc, err := rlp.EncodeToBytes(tile)
		if err != nil {
			return nil, 0, err
		}
		batch.Put(append(tilePrefix, oldest.Bytes()...), enc)
		delete(diff.tiles, oldest)

		oldest = tile.flushNext
		needDrop -= 1
	}
	diff.oldest = oldest

	if diff.state != (common.Hash{}) && len(diff.tiles) == 0 {
		batch.Put(headStateKey, diff.state.Bytes()) // Commit state root to represent the whole state is tiled.
		delete(db.diffset, diff.state)              // Only complete layer will be registered into set
		diff.state = common.Hash{}
	}
	if err := batch.Write(); err != nil {
		return nil, 0, err
	}
	if len(diff.tiles) == 0 {
		diff.tiles = make(map[common.Hash]*tileInfo)
		diff.oldest, diff.newest = common.Hash{}, common.Hash{}
		return nil, done + nodes, nil
	}
	return diff, done + nodes - len(diff.tiles), nil
}

func (diff *diffLayer) has(hash common.Hash) bool {
	t, _ := diff.get(hash)
	return t != nil
}

func (diff *diffLayer) get(hash common.Hash) (*tileInfo, common.Hash) {
	// Search in this layer first.
	if tile := diff.tiles[hash]; tile != nil {
		return tile, diff.state
	}
	// Not found, search in parent layer
	if diff.parent != nil {
		return diff.parent.get(hash)
	}
	// Not found in deepest layer, return false
	return nil, common.Hash{}
}

type tileDatabase struct {
	db      ethdb.Database
	current *diffLayer
	diffset map[common.Hash]*diffLayer
	nodes   int
	latest  common.Hash // Pointer to latest registered layer

	// Statistic
	totalSize  common.StorageSize
	totalNodes int
}

func newTileDatabase(db ethdb.Database) *tileDatabase {
	return &tileDatabase{
		db:      db,
		diffset: make(map[common.Hash]*diffLayer),
	}
}

func (db *tileDatabase) insert(hash common.Hash, depth uint8, size common.StorageSize, nodes int, hashmap map[common.Hash]struct{}, refs []common.Hash) {
	// Allocate a new diff layer if necessary
	if db.current == nil {
		db.current = &diffLayer{tiles: make(map[common.Hash]*tileInfo)}
	}
	var hashes []common.Hash
	for hash := range hashmap {
		hashes = append(hashes, hash)
	}
	db.current.tiles[hash] = &tileInfo{
		Depth:  depth,
		Nodes:  uint16(nodes),
		Hashes: hashes,
		Refs:   refs,
	}
	// Update the flush-list endpoints
	if db.current.oldest == (common.Hash{}) {
		db.current.oldest, db.current.newest = hash, hash
	} else {
		db.current.tiles[db.current.newest].flushNext, db.current.newest = hash, hash
	}
	db.totalSize += size
	db.totalNodes += nodes

	db.nodes += 1
	if db.nodes > commitThreshold {
		diff, committed, err := db.current.cap(db, commitThreshold/2)
		if err != nil {
			return
		}
		db.current = diff
		db.nodes -= committed
		log.Info("Committed tiles", "number", committed)
	}
	log.Trace("Inserted tile", "hash", hash, "size", size, "nodes", nodes)
}

func (db *tileDatabase) commit(root common.Hash) error {
	// Short circult if the state transition is empty.
	// e.g. empty blocks created by clique engine.
	if db.current == nil {
		return nil
	}
	db.current.state = root
	db.diffset[root] = db.current
	db.latest = root

	if len(db.diffset) > 12 {
		_, committed, err := db.current.flush(db)
		if err != nil {
			return err
		}
		db.nodes -= committed
	}
	db.current = nil
	log.Info("Committed state", "root", root, "number", db.nodes, "size", db.totalSize, "nodes", db.totalNodes)
	return nil
}

func (db *tileDatabase) has(hash common.Hash) bool {
	layer := db.current
	if layer == nil && db.latest != (common.Hash{}) {
		layer = db.diffset[db.latest]
	}
	if layer != nil && layer.has(hash) {
		return true
	}
	has, err := db.db.Has(append(tilePrefix, hash.Bytes()...))
	if has && err == nil {
		return true
	}
	return false // Not found
}

func (db *tileDatabase) get(hash common.Hash) (*tileInfo, common.Hash) {
	layer := db.current
	if layer == nil && db.latest != (common.Hash{}) {
		layer = db.diffset[db.latest]
	}
	if layer != nil {
		if tile, state := layer.get(hash); tile != nil {
			return tile, state // state can be empty if it's not complete
		}
	}
	enc, err := db.db.Get(append(tilePrefix, hash.Bytes()...))
	if len(enc) > 0 && err == nil {
		var tile tileInfo
		if err := rlp.DecodeBytes(enc, &tile); err != nil {
			return nil, common.Hash{}
		}
		head, _ := db.db.Get(headStateKey)
		if head == nil {
			return &tile, common.BytesToHash(head) // Complete disk layer
		}
		return &tile, common.Hash{} // Uncomplete disk layer
	}
	return nil, common.Hash{} // Not found
}
