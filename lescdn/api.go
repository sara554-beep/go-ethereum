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

package lescdn

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

// PrivateLightAPI provides an API to access the LES light server or light client.
type LESCDNAPI struct {
	chain *core.BlockChain
}

// NewPrivateLightAPI creates a new LES service API.
func NewLESCDNAPI(chain *core.BlockChain) *LESCDNAPI {
	return &LESCDNAPI{chain: chain}
}

func (api *LESCDNAPI) Tile(hash common.Hash, target, limit, barrier int) ([][]byte, error) {
	// Do a breadth-first expansion to collect a fixed size tile
	triedb := api.chain.StateCache().TrieDB()

	nodes, refset, cutset, err := makeIdealTile(triedb, hash, target, barrier)
	if err != nil {
		return nil, err
	}
	// If our cutset nodes won't result in meaningful tiles (they reach the leaves),
	// merge all of them into the current tile to avoid creating millions of subtiles.
	var (
		merged []common.Hash
		merges [][]byte
	)
	for !cutset.Empty() {
		// Fetch the deepest cutset node and merge in if it's a leaf
		hash := cutset.PopItem().(common.Hash)

		subnodes, _, subcutset, err := makeIdealTile(triedb, hash, target, barrier)
		if err != nil {
			return nil, err
		}
		if subcutset.Empty() {
			merged = append(merged, hash)
			merges = append(merges, subnodes...)
			continue
		}
		// Deepest cutset node produces non-leaf tile, don't bother with shallower node
		break
	}
	// If the final tile became huge, it means we packed in too many leaves due to
	// tile mergers. Shave off the nodes that caused tile mergers in the first place.
	if len(nodes)+len(merges) > limit {
		for _, drop := range merged {
			for i, refs := range refset {
				if _, ok := refs[drop]; ok {
					nodes = append(nodes[:i], nodes[i+1:]...)
					refset = append(refset[:i], refset[i+1:]...)
					break
				}
			}
		}
	} else {
		nodes = append(nodes, merges...)
	}
	return nodes, nil
}
