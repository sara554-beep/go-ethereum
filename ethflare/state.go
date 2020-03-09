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

package ethflare

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"
)

const (
	// tileTarget is the target number of trie nodes to place into each tile. The
	// actual count might be smaller for leaf tiles, or larger to ensure proper
	// tile barriers.
	tileTarget = 16

	// tileLimit is the maximum number of trie nodes to place into each tile, above
	// which the tile is forcefully split, even if that means breaking barriers.
	tileLimit = 256

	// tileBarrier is the trie depth multiplier where trie nodes need to end to
	// ensure that mutating tries still reuse the same non-mutated tiles.
	//
	// The number 2 was chosen experimentally, but the rationalization behind it is
	// that nodes close to the root will be very dense. The worst case where the nodes
	// are filled, a barrier of 3 would result in about 16^3 = 4096 hash pointers in
	// the leaves, which is 128KB + internal pointers + nodes + boilerplate. Depending
	// on trie shape, this can grow to even larger values, becoming useless, especially
	// at the root, so 2 seems to be a limit. A barrier of 2 produced about 8KB tiles
	// in our experiments.
	tileBarrier = 2
)

// serveState is responsible for serving HTTP requests for state data.
func (s *Server) serveState(w http.ResponseWriter, r *http.Request) {
	// TODO(karalabe): the non-defaults are for benchmarking, get rid when finalized
	cutTileTarget := int64(tileTarget)
	if target, ok := r.URL.Query()["target"]; ok {
		cutTileTarget, _ = strconv.ParseInt(target[0], 0, 64)
	}
	curTileLimit := int64(tileLimit)
	if limit, ok := r.URL.Query()["limit"]; ok {
		curTileLimit, _ = strconv.ParseInt(limit[0], 0, 64)
	}
	curTileBarrier := int64(tileBarrier)
	if barrier, ok := r.URL.Query()["barrier"]; ok {
		curTileBarrier, _ = strconv.ParseInt(barrier[0], 0, 64)
	}
	// Decode the root of the subtrie tile we should return
	root, err := hexutil.Decode(shift(&r.URL.Path))
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid state root: %v", err), http.StatusBadRequest)
		return
	}
	if len(root) != common.HashLength {
		http.Error(w, fmt.Sprintf("invalid state root: length %d != %d", len(root), common.HashLength), http.StatusBadRequest)
		return
	}
	hash := common.BytesToHash(root)
	// Validate the request, ensure the requested tile is still available and fresh.
	answer := s.tiler.hasTile(hash)
	if answer.tile == nil {
		http.Error(w, fmt.Sprintf("non-existent state root %x", hash), http.StatusBadRequest)
		return
	}
	bid, has := s.backends.hasState(answer.state)
	if !has {
		// Special case, the tile is referenced by a stale state
		// while it's already unavailable. Return error here anyway.
		http.Error(w, fmt.Sprintf("non-existent state root %x", hash), http.StatusBadRequest)
		return
	}
	nodes, err := s.backends.backend(bid).getTile(hash)
	if err != nil {
		http.Error(w, fmt.Sprintf("non-existent state root %x", hash), http.StatusBadRequest)
		return
	}
	// todo adjust the shape of tile based on the index info
	replyAndCache(w, nodes)
	log.Debug("Served state request", "root", hash, "target", cutTileTarget, "limit", curTileLimit, "barrier", curTileBarrier)
}
