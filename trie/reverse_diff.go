// Copyright 2021 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package trie

import (
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

// stateDiff represents a reverse change of a state data. The value refers to the
// content before the change is applied.
type stateDiff struct {
	Key []byte // Storage format node key
	Val []byte // RLP-encoded node blob, nil means the node is previously non-existent
}

// reverseDiff represents a set of state diffs belong to the same block. The root
// and number refer to the corresponding state root and block number.
type reverseDiff struct {
	root   common.Hash
	number uint64
	states []stateDiff
}

func loadReverseDiff(db ethdb.KeyValueReader, number uint64, hash common.Hash) (*reverseDiff, error) {
	blob := rawdb.ReadReverseDiff(db, number, hash)
	if len(blob) == 0 {
		return nil, errors.New("reverse diff not found")
	}
	var states []stateDiff
	if err := rlp.DecodeBytes(blob, &states); err != nil {
		return nil, err
	}
	return &reverseDiff{
		root:   hash,
		number: number,
		states: states,
	}, nil
}

// storeAndPrunedReverseDiff extracts the reverse state diff by the passed
// bottom-most diff layer and its parent, stores the diff set into the disk
// and prunes the over-old diffs as well.
// This function will panic if it's called for non-bottom-most diff layer.
func storeAndPrunedReverseDiff(dl *diffLayer, limit uint64) error {
	defer func(start time.Time) {
		triedbReverseDiffTimeTimer.Update(time.Since(start))
	}(time.Now())

	var (
		base   = dl.parent.(*diskLayer)
		states []stateDiff
	)
	for key, node := range dl.nodes {
		pre, err := base.NodeBlob([]byte(key), node.hash)
		if err != nil {
			return err
		}
		states = append(states, stateDiff{
			Key: []byte(key),
			Val: pre,
		})
	}
	blob, err := rlp.EncodeToBytes(states)
	if err != nil {
		return err
	}
	rawdb.WriteReverseDiff(base.diskdb, dl.number, dl.root, blob)
	triedbReverseDiffSizeMeter.Mark(int64(len(blob)))

	// Prune the reverse diffs if they are too old
	if dl.number < limit {
		return nil
	}
	var (
		start uint64
		end   = dl.number - limit
		batch = base.diskdb.NewBatch()
	)
	for {
		numbers, hashes := rawdb.ReadReverseDiffsBelow(base.diskdb, start, end, 10240)
		if len(numbers) == 0 {
			break
		}
		for i := 0; i < len(numbers); i++ {
			rawdb.DeleteReverseDiff(batch, numbers[i], hashes[i])
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
		start = numbers[len(numbers)-1] + 1
	}
	if err := batch.Write(); err != nil {
		return err
	}
	return nil
}
