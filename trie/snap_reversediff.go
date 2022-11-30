// Copyright 2022 The go-ethereum Authors
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
	"bytes"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// Reverse diff records the state changes involved in executing a corresponding block.
// The state can be reverted to the previous status by applying reverse diff. Reverse
// diff is the guarantee that Geth can perform state rollback, for purposes of deep
// reorg, historical state tracing and so on.
//
// Each state transition will generate a corresponding reverse diff (Note that not every
// block has a reverse diff, for example, in the Clique network, if two adjacent blocks
// have no state change, then the second block has no reverse diff). Each reverse diff
// will have an id as its unique identifier. The id is a monotonically increasing number
// starting from 1(0 for empty state).
//
// The reverse diff will be written to disk (ancient store) when the corresponding diff
// layer is merged into the disk layer. At the same time, Geth can prune the oldest reverse
// diffs according to config.
//
//                                    block of disk state    block of disk layer    block of diff layer
//                                         /                              /                /
//    +--------------+--------------+--------------+--------------+----------------+--------------+
//    |   block 1    |      ...     |    block n   |      ...     |     block m    |  block m+1   |
//    +--------------+--------------+--------------+--------------+----------------+--------------+
//                           |             |                              |
//                    earliest rdiff    rdiff n           ...       latest rdiff m
//
//
// How does state rollback work? For example, if Geth wants to roll back its state to the state
// of block n, it first needs to check whether the reverse diff x corresponding to the state of
// block n exists. If so, all reverse diffs from the latest reverse diff to the reverse diff x
// will be applied in turn (x is included).
//
// Reverse diff structure:
//                                  +-----------------------+
//                                ->|     Reverse diff n    |
//                            ---/  +-----------------------+
//            +-------+   ---/
//            |   n   |--\                                       (Ancient store)
//            +-------+   ---\
//                            ---\  +-----------------------+
//                                ->|  Destination state S  |
//                                  +-----------------------+
//
//
//           +-----------------------+          +-------+
//           |  Destination state S  |--------->|   n   |       (Key-value store)
//           +-----------------------+          +-------+
//
// The state should be rewound to destination state S after applying the reverse diff n.

// reverseDiffVersion is the initial version of reverse diff structure.
const reverseDiffVersion = uint8(0)

// stateDiff represents a reverse change of a state data. The prev refers to the
// content before the change is applied.
type stateDiff struct {
	Path []byte // Path of node inside of the trie
	Prev []byte // RLP-encoded node blob, nil means the node is previously non-existent
}

// stateDiffs represents a list of state diffs belong to a single contract
// or the main account trie.
type stateDiffs struct {
	Owner  common.Hash // Identifier of contract or empty for main account trie
	States []stateDiff // The list of state diffs
}

// reverseDiff represents a set of state diffs belong to the same block. All the
// reverse-diffs in disk are linked with each other by a unique id(8byte integer),
// the tail(oldest) reverse-diff will be pruned in order to control the storage
// size.
type reverseDiff struct {
	Version uint64       // The version tag of stored reverse diff
	Parent  common.Hash  // The corresponding state root of parent block
	Root    common.Hash  // The corresponding state root which these diffs belong to
	States  []stateDiffs // The list of state changes
}

func (diff *reverseDiff) encode() ([]byte, error) {
	var buf = new(bytes.Buffer)
	buf.WriteByte(reverseDiffVersion)
	if err := rlp.Encode(buf, diff); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (diff *reverseDiff) decode(blob []byte) error {
	if len(blob) < 1 {
		return fmt.Errorf("no version tag")
	}
	switch blob[0] {
	case reverseDiffVersion:
		var dec reverseDiff
		if err := rlp.DecodeBytes(blob[1:], &dec); err != nil {
			return err
		}
		diff.Parent, diff.Root, diff.States = dec.Parent, dec.Root, dec.States
		return nil
	default:
		return fmt.Errorf("unknown reverse diff version %d", blob[0])
	}
}

// apply writes the reverse diff into the provided database batch.
func (diff *reverseDiff) apply(batch ethdb.Batch) {
	for _, entry := range diff.States {
		accTrie := entry.Owner == (common.Hash{})
		for _, state := range entry.States {
			if len(state.Prev) > 0 {
				if accTrie {
					rawdb.WriteAccountTrieNode(batch, state.Path, state.Prev)
				} else {
					rawdb.WriteStorageTrieNode(batch, entry.Owner, state.Path, state.Prev)
				}
			} else {
				if accTrie {
					rawdb.DeleteAccountTrieNode(batch, state.Path)
				} else {
					rawdb.DeleteStorageTrieNode(batch, entry.Owner, state.Path)
				}
			}
		}
	}
}

// loadReverseDiff reads and decodes the reverse diff by the given id.
func loadReverseDiff(freezer *rawdb.Freezer, id uint64) (*reverseDiff, error) {
	blob := rawdb.ReadReverseDiff(freezer, id)
	if len(blob) == 0 {
		return nil, fmt.Errorf("reverse diff not found %d", id)
	}
	var dec reverseDiff
	if err := dec.decode(blob); err != nil {
		return nil, err
	}
	return &dec, nil
}

// storeReverseDiff constructs the reverse state diff for the passed bottom-most
// diff layer. After storing the corresponding reverse diff, it will also prune
// the stale reverse diffs from the disk with the given threshold.
// This function will panic if it's called for non-bottom-most diff layer.
func storeReverseDiff(freezer *rawdb.Freezer, dl *diffLayer, limit uint64) error {
	var (
		start = time.Now()
		base  = dl.Parent().(*diskLayer)
		enc   = &reverseDiff{Parent: base.root, Root: dl.Root()}
	)
	for owner, subset := range dl.nodes {
		entry := stateDiffs{Owner: owner}
		for path, n := range subset {
			entry.States = append(entry.States, stateDiff{
				Path: []byte(path),
				Prev: n.prev,
			})
		}
		enc.States = append(enc.States, entry)
	}
	blob, err := enc.encode()
	if err != nil {
		return err
	}
	// The reverse diff object and the lookup are stored in two different
	// places, so there is no atomicity guarantee. It's possible that reverse
	// diff object is written but lookup is not, vice versa. So double-check
	// the presence when using the reverse diff.
	rawdb.WriteReverseDiff(freezer, dl.diffid, blob, base.root)

	// All stored lookups are identified by the **unique** state root. It's
	// impossible that in the same chain blocks at different height have the
	// same root. And only reverse diffs of canonical chain will be persisted,
	// so we can get the conclusion the state root here is unique.
	rawdb.WriteReverseDiffLookup(base.diskdb, base.root, dl.diffid)
	triedbReverseDiffSizeMeter.Mark(int64(len(blob)))

	logs := []interface{}{
		"id", dl.diffid,
		"nodes", len(dl.nodes),
		"size", common.StorageSize(len(blob)),
	}
	// Prune stale reverse diffs if necessary
	if limit != 0 && dl.diffid > limit {
		pruned, err := truncateFromTail(freezer, base.diskdb, dl.diffid-limit)
		if err != nil {
			return err
		}
		logs = append(logs, "pruned", pruned, "limit", limit)
	}
	duration := time.Since(start)
	triedbReverseDiffTimeTimer.Update(duration)
	logs = append(logs, "elapsed", common.PrettyDuration(duration))

	log.Debug("Stored the reverse diff", logs...)
	return nil
}

// truncateFromHead removes the extra reverse diff from the head with the
// given parameters. If the passed database is a non-freezer database,
// nothing to do here.
func truncateFromHead(freezer *rawdb.Freezer, disk ethdb.Database, nhead uint64) (int, error) {
	var (
		batch    = disk.NewBatch()
		ohead, _ = freezer.Ancients()
	)
	for id := ohead; id > nhead; id-- {
		hash := rawdb.ReadReverseDiffHash(freezer, id)
		if hash != (common.Hash{}) {
			rawdb.DeleteReverseDiffLookup(batch, hash)
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return 0, err
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	if err := freezer.TruncateHead(nhead); err != nil {
		return 0, err
	}
	return int(ohead - nhead), nil
}

// truncateFromTail removes the extra reverse diff from the tail with the
// given parameters. If the passed database is a non-freezer database,
// nothing to do here.
func truncateFromTail(freezer *rawdb.Freezer, disk ethdb.Database, ntail uint64) (int, error) {
	var (
		batch    = disk.NewBatch()
		otail, _ = freezer.Tail()
	)
	for id := otail + 1; id <= ntail; id++ {
		hash := rawdb.ReadReverseDiffHash(freezer, id)
		if hash != (common.Hash{}) {
			rawdb.DeleteReverseDiffLookup(batch, hash)
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return 0, err
			}
			batch.Reset()
		}
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	if err := freezer.TruncateTail(ntail); err != nil {
		return 0, err
	}
	return int(ntail - otail), nil
}

// truncateDiffs is called when database is constructed. It ensures reverse diff
// history is aligned with disk layer, and truncates the extra diffs from the
// freezer. The given target indicates the id of last item wants to be reserved.
func truncateDiffs(freezer *rawdb.Freezer, disk ethdb.Database, target uint64) {
	pruned, err := truncateFromHead(freezer, disk, target)
	if err != nil {
		log.Crit("Failed to truncate extra reverse diffs", "err", err)
	}
	if pruned != 0 {
		log.Info("Truncated extra reverse diff", "number", pruned, "head", target)
	}
}
