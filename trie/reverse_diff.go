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
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

type stateDiff struct {
	Key []byte // Storage format node key
	Val []byte // RLP-encoded node blob, nil means the node is null previously
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