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
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	reverseDiffVersion = uint64(0) // Initial version of reverse diff structure
)

// stateDiff represents a reverse change of a state data. The value refers to the
// content before the change is applied.
type stateDiff struct {
	Key []byte // Storage format node key
	Val []byte // RLP-encoded node blob, nil means the node is previously non-existent
}

// reverseDiff represents a set of state diffs belong to the same block. All the
// reverse-diffs in disk are linked with each other by a unique id(8byte integer),
// the head reverse-diff will be pruned in order to control the storage size.
type reverseDiff struct {
	Version uint64      // The version tag of stored reverse diff
	Parent  common.Hash // The corresponding state root of parent block
	Root    common.Hash // The corresponding state root which these diffs belong to
	States  []stateDiff // The list of state changes
}

// loadReverseDiff reads and decodes the reverse diff by the given id.
func loadReverseDiff(db ethdb.Database, id uint64) (*reverseDiff, error) {
	blob := rawdb.ReadReverseDiff(db, id)
	if len(blob) == 0 {
		return nil, errors.New("reverse diff not found")
	}
	var diff reverseDiff
	if err := rlp.DecodeBytes(blob, &diff); err != nil {
		return nil, err
	}
	if diff.Version != reverseDiffVersion {
		return nil, fmt.Errorf("%w want %d got %d", errors.New("unexpected reverse diff version"), reverseDiffVersion, diff.Version)
	}
	return &diff, nil
}

// storeReverseDiff extracts the reverse state diff by the passed bottom-most
// diff layer. After storing the corresponding reverse diffs, it will also prune
// the stale reverse diffs from the disk by the given limit.
// This function will panic if it's called for non-bottom-most diff layer.
func storeReverseDiff(dl *diffLayer, limit uint64) error {
	var (
		startTime = time.Now()
		base      = dl.Parent().(*diskLayer)
		states    []stateDiff
	)
	for key := range dl.nodes {
		pre, _ := rawdb.ReadTrieNode(base.diskdb, []byte(key))
		states = append(states, stateDiff{
			Key: []byte(key),
			Val: pre,
		})
	}
	diff := &reverseDiff{
		Version: reverseDiffVersion,
		Parent:  base.root,
		Root:    dl.root,
		States:  states,
	}
	blob, err := rlp.EncodeToBytes(diff)
	if err != nil {
		return err
	}
	// The reverse diff object and the lookup are stored in two different
	// places, so there is no atomicity guarantee. It's possible that reverse
	// diff object is written but lookup is not, vice versa. So double-check
	// the presence when using the reverse diff.
	rawdb.WriteReverseDiff(base.diskdb, dl.rid, blob, base.root) // RID -> Parent State && RID -> Reverse diff
	rawdb.WriteReverseDiffLookup(base.diskdb, base.root, dl.rid) // Parent State -> RID
	triedbReverseDiffSizeMeter.Mark(int64(len(blob)))

	// Prune stale reverse diffs if necessary
	logCtx := []interface{}{
		"id", dl.rid,
		"size", common.StorageSize(len(blob)),
	}
	if dl.rid > limit {
		oldTail, err := base.diskdb.Tail(rawdb.ReverseDiffFreezer)
		if err == nil {
			batch := base.diskdb.NewBatch()
			newTail := dl.rid - limit
			for i := oldTail; i < newTail; i++ {
				// The rid is added with 1, because reverse diff is encoded from
				// 1 in Geth, while encoded from 0 in freezer, the i here refers
				// to the index in freezer.
				hash := rawdb.ReadReverseDiffHash(base.diskdb, i+1)
				if hash != (common.Hash{}) {
					rawdb.DeleteReverseDiffLookup(batch, hash)
				}
			}
			if err := batch.Write(); err != nil {
				return err
			}
			base.diskdb.TruncateTail(rawdb.ReverseDiffFreezer, newTail)
			logCtx = append(logCtx, "pruned", newTail-oldTail)
		}
	}
	duration := time.Since(startTime)
	triedbReverseDiffTimeTimer.Update(duration)
	logCtx = append(logCtx, "elapsed", common.PrettyDuration(duration))
	log.Debug("Stored the reverse diff", logCtx...)
	return nil
}

// truncateReverseDiffHistory applies the head truncation with the given
// parameter. Hold the fact that the reverse diff history can already be
// truncated from the tail, which means the lowest available head will be
// the current tail. So always return the new head after the truncation.
func truncateReverseDiffHistory(db ethdb.Database, items uint64) uint64 {
	n, err := db.Ancients(rawdb.ReverseDiffFreezer)
	if err != nil {
		return 0 // ancient store is not supported
	}
	db.TruncateHead(rawdb.ReverseDiffFreezer, items)
	nItems, _ := db.Ancients(rawdb.ReverseDiffFreezer)
	rawdb.WriteReverseDiffHead(db, nItems)
	log.Debug("Truncated reverse diff history from head", "request", items, "rewound", nItems, "origin", n)
	return nItems
}

// repairReverseDiff is called when database is constructed. It ensures reverse diff
// history is aligned with disk layer, or do the necessary repair instead.
func repairReverseDiff(db ethdb.Database, diskroot common.Hash) uint64 {
	// Nothing expected, clean the entire reverse diff history
	head := rawdb.ReadReverseDiffHead(db)
	if head == 0 {
		return truncateReverseDiffHistory(db, 0)
	}
	// Align the reverse diff history and stored reverse diff head.
	rdiffs, err := db.Ancients(rawdb.ReverseDiffFreezer)
	if err == nil && rdiffs > 0 {
		// Note error can return if the freezer functionality
		// is disabled(testing). Don't panic for it.
		switch {
		case rdiffs == head:
			// reverse diff freezer is continuous with disk layer,
			// nothing to do here.
		case rdiffs > head:
			// reverse diff freezer is dangling, truncate the extra
			// diffs.
			head = truncateReverseDiffHistory(db, head)
			log.Info("Truncate dangling reverse diff freezer", "stored", head, "rdiffs", rdiffs)
		default:
			// disk layer is higher than reverse diff, the gap between
			// the disk layer and reverse diff freezer is NOT fixable.
			// truncate the entire reverse diff history.
			head = truncateReverseDiffHistory(db, 0)
			log.Info("Truncate entire reverse diff freezer", "stored", head, "rdiffs", rdiffs)
		}
	}
	// Ensure the head reverse diff matches with the disk layer,
	// otherwise invalidate the entire reverse diff list.
	if head != 0 {
		diff, err := loadReverseDiff(db, head)
		if err != nil || diff.Root != diskroot {
			head = truncateReverseDiffHistory(db, 0)
			log.Info("Truncate unmatched reverse diff freezer", "head", head)
		}
	}
	return head
}
