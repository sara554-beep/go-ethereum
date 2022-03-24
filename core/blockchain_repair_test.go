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

// Tests that abnormal program termination (i.e.crash) and restart doesn't leave
// the database in some strange state with gaps in the chain, nor with block data
// dangling in the future.

package core

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
)

// Tests a recovery for a short canonical chain where a recent block was already
// committed to disk and then the process crashed. In this case we expect the full
// chain to be rolled back to the committed block, but the chain data itself left
// in the database for replaying.
func TestShortRepairHashBased(t *testing.T) { testShortRepairHashBased(t, false, trie.HashScheme) }
func TestShortRepairWithSnapshotsHashBased(t *testing.T) {
	testShortRepairHashBased(t, true, trie.HashScheme)
}
func TestShortRepairPathBased(t *testing.T) { testShortRepairHashBased(t, false, trie.PathScheme) }
func TestShortRepairWithSnapshotsPathBased(t *testing.T) {
	testShortRepairHashBased(t, true, trie.PathScheme)
}

func testShortRepairHashBased(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 8,
		expSidechainBlocks: 0,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain where the fast sync pivot point was
// already committed, after which the process crashed. In this case we expect the full
// chain to be rolled back to the committed block, but the chain data itself left in
// the database for replaying.
func TestShortSnapSyncedRepairHashBased(t *testing.T) {
	testShortSnapSyncedRepair(t, false, trie.HashScheme)
}
func TestShortSnapSyncedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortSnapSyncedRepair(t, true, trie.HashScheme)
}
func TestShortSnapSyncedRepair(t *testing.T) { testShortSnapSyncedRepair(t, false, trie.PathScheme) }
func TestShortSnapSyncedRepairWithSnapshots(t *testing.T) {
	testShortSnapSyncedRepair(t, true, trie.PathScheme)
}

func testShortSnapSyncedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 0,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain where the fast sync pivot point was
// not yet committed, but the process crashed. In this case we expect the chain to
// detect that it was fast syncing and not delete anything, since we can just pick
// up directly where we left off.
func TestShortSnapSyncingRepairHashBased(t *testing.T) {
	testShortSnapSyncingRepair(t, false, trie.HashScheme)
}
func TestShortSnapSyncingRepairWithSnapshotsHashBased(t *testing.T) {
	testShortSnapSyncingRepair(t, true, trie.HashScheme)
}
func TestShortSnapSyncingRepairPathBased(t *testing.T) {
	testShortSnapSyncingRepair(t, false, trie.PathScheme)
}
func TestShortSnapSyncingRepairWithSnapshotsPathBased(t *testing.T) {
	testShortSnapSyncingRepair(t, true, trie.PathScheme)
}

func testShortSnapSyncingRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//
	// Frozen: none
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 0,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where a
// recent block was already committed to disk and then the process crashed. In this
// test scenario the side chain is below the committed block. In this case we expect
// the canonical chain to be rolled back to the committed block, but the chain data
// itself left in the database for replaying.
func TestShortOldForkedRepairHashBased(t *testing.T) {
	testShortOldForkedRepair(t, false, trie.HashScheme)
}
func TestShortOldForkedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortOldForkedRepair(t, true, trie.HashScheme)
}
func TestShortOldForkedRepairPathBased(t *testing.T) {
	testShortOldForkedRepair(t, false, trie.PathScheme)
}
func TestShortOldForkedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortOldForkedRepair(t, true, trie.PathScheme)
}

func testShortOldForkedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 8,
		expSidechainBlocks: 3,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where
// the fast sync pivot point was already committed to disk and then the process
// crashed. In this test scenario the side chain is below the committed block. In
// this case we expect the canonical chain to be rolled back to the committed block,
// but the chain data itself left in the database for replaying.
func TestShortOldForkedSnapSyncedRepairHashBased(t *testing.T) {
	testShortOldForkedSnapSyncedRepair(t, false, trie.HashScheme)
}
func TestShortOldForkedSnapSyncedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortOldForkedSnapSyncedRepair(t, true, trie.HashScheme)
}
func TestShortOldForkedSnapSyncedRepairPathBased(t *testing.T) {
	testShortOldForkedSnapSyncedRepair(t, false, trie.PathScheme)
}
func TestShortOldForkedSnapSyncedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortOldForkedSnapSyncedRepair(t, true, trie.PathScheme)
}

func testShortOldForkedSnapSyncedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 3,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where
// the fast sync pivot point was not yet committed, but the process crashed. In this
// test scenario the side chain is below the committed block. In this case we expect
// the chain to detect that it was fast syncing and not delete anything, since we
// can just pick up directly where we left off.
func TestShortOldForkedSnapSyncingRepairHashBased(t *testing.T) {
	testShortOldForkedSnapSyncingRepair(t, false, trie.HashScheme)
}
func TestShortOldForkedSnapSyncingRepairWithSnapshotsHashBased(t *testing.T) {
	testShortOldForkedSnapSyncingRepair(t, true, trie.HashScheme)
}
func TestShortOldForkedSnapSyncingRepairPathBased(t *testing.T) {
	testShortOldForkedSnapSyncingRepair(t, false, trie.PathScheme)
}
func TestShortOldForkedSnapSyncingRepairWithSnapshotsPathBased(t *testing.T) {
	testShortOldForkedSnapSyncingRepair(t, true, trie.PathScheme)
}

func testShortOldForkedSnapSyncingRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen: none
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 3,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where a
// recent block was already committed to disk and then the process crashed. In this
// test scenario the side chain reaches above the committed block. In this case we
// expect the canonical chain to be rolled back to the committed block, but the
// chain data itself left in the database for replaying.
func TestShortNewlyForkedRepairHashBased(t *testing.T) {
	testShortNewlyForkedRepair(t, false, trie.HashScheme)
}
func TestShortNewlyForkedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortNewlyForkedRepair(t, true, trie.HashScheme)
}
func TestShortNewlyForkedRepairPathBased(t *testing.T) {
	testShortNewlyForkedRepair(t, false, trie.PathScheme)
}
func TestShortNewlyForkedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortNewlyForkedRepair(t, true, trie.PathScheme)
}

func testShortNewlyForkedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    6,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 8,
		expSidechainBlocks: 6,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where
// the fast sync pivot point was already committed to disk and then the process
// crashed. In this test scenario the side chain reaches above the committed block.
// In this case we expect the canonical chain to be rolled back to the committed
// block, but the chain data itself left in the database for replaying.
func TestShortNewlyForkedSnapSyncedRepairHashBased(t *testing.T) {
	testShortNewlyForkedSnapSyncedRepair(t, false, trie.HashScheme)
}
func TestShortNewlyForkedSnapSyncedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortNewlyForkedSnapSyncedRepair(t, true, trie.HashScheme)
}
func TestShortNewlyForkedSnapSyncedRepairPathBased(t *testing.T) {
	testShortNewlyForkedSnapSyncedRepair(t, false, trie.PathScheme)
}
func TestShortNewlyForkedSnapSyncedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortNewlyForkedSnapSyncedRepair(t, true, trie.PathScheme)
}

func testShortNewlyForkedSnapSyncedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    6,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 6,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a shorter side chain, where
// the fast sync pivot point was not yet committed, but the process crashed. In
// this test scenario the side chain reaches above the committed block. In this
// case we expect the chain to detect that it was fast syncing and not delete
// anything, since we can just pick up directly where we left off.
func TestShortNewlyForkedSnapSyncingRepairHashBased(t *testing.T) {
	testShortNewlyForkedSnapSyncingRepair(t, false, trie.HashScheme)
}
func TestShortNewlyForkedSnapSyncingRepairWithSnapshotsHashBased(t *testing.T) {
	testShortNewlyForkedSnapSyncingRepair(t, true, trie.HashScheme)
}
func TestShortNewlyForkedSnapSyncingRepairPathBased(t *testing.T) {
	testShortNewlyForkedSnapSyncingRepair(t, false, trie.PathScheme)
}
func TestShortNewlyForkedSnapSyncingRepairWithSnapshotsPathBased(t *testing.T) {
	testShortNewlyForkedSnapSyncingRepair(t, true, trie.PathScheme)
}

func testShortNewlyForkedSnapSyncingRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Frozen: none
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    6,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 6,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a longer side chain, where a
// recent block was already committed to disk and then the process crashed. In this
// case we expect the canonical chain to be rolled back to the committed block, but
// the chain data itself left in the database for replaying.
func TestShortReorgedRepairHashBased(t *testing.T) { testShortReorgedRepair(t, false, trie.HashScheme) }
func TestShortReorgedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortReorgedRepair(t, true, trie.HashScheme)
}
func TestShortReorgedRepairPathBased(t *testing.T) { testShortReorgedRepair(t, false, trie.PathScheme) }
func TestShortReorgedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortReorgedRepair(t, true, trie.PathScheme)
}

func testShortReorgedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    10,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 8,
		expSidechainBlocks: 10,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a longer side chain, where
// the fast sync pivot point was already committed to disk and then the process
// crashed. In this case we expect the canonical chain to be rolled back to the
// committed block, but the chain data itself left in the database for replaying.
func TestShortReorgedSnapSyncedRepairHashBased(t *testing.T) {
	testShortReorgedSnapSyncedRepair(t, false, trie.HashScheme)
}
func TestShortReorgedSnapSyncedRepairWithSnapshotsHashBased(t *testing.T) {
	testShortReorgedSnapSyncedRepair(t, true, trie.HashScheme)
}
func TestShortReorgedSnapSyncedRepairPathBased(t *testing.T) {
	testShortReorgedSnapSyncedRepair(t, false, trie.PathScheme)
}
func TestShortReorgedSnapSyncedRepairWithSnapshotsPathBased(t *testing.T) {
	testShortReorgedSnapSyncedRepair(t, true, trie.PathScheme)
}

func testShortReorgedSnapSyncedRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Frozen: none
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    10,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 10,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a short canonical chain and a longer side chain, where
// the fast sync pivot point was not yet committed, but the process crashed. In
// this case we expect the chain to detect that it was fast syncing and not delete
// anything, since we can just pick up directly where we left off.
func TestShortReorgedSnapSyncingRepairHashBased(t *testing.T) {
	testShortReorgedSnapSyncingRepair(t, false, trie.HashScheme)
}
func TestShortReorgedSnapSyncingRepairWithSnapshotsHashBased(t *testing.T) {
	testShortReorgedSnapSyncingRepair(t, true, trie.HashScheme)
}
func TestShortReorgedSnapSyncingRepairPathBased(t *testing.T) {
	testShortReorgedSnapSyncingRepair(t, false, trie.PathScheme)
}
func TestShortReorgedSnapSyncingRepairWithSnapshotsPathBased(t *testing.T) {
	testShortReorgedSnapSyncingRepair(t, true, trie.PathScheme)
}

func testShortReorgedSnapSyncingRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Frozen: none
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in leveldb:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10
	//
	// Expected head header    : C8
	// Expected head fast block: C8
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    8,
		sidechainBlocks:    10,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 8,
		expSidechainBlocks: 10,
		expFrozen:          0,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where a recent
// block - newer than the ancient limit - was already committed to disk and then
// the process crashed. In this case we expect the chain to be rolled back to the
// committed block, with everything afterwads kept as fast sync data.
func TestLongShallowRepairHashBased(t *testing.T) { testLongShallowRepair(t, false, trie.HashScheme) }
func TestLongShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongShallowRepair(t, true, trie.HashScheme)
}
func TestLongShallowRepairPathBased(t *testing.T) { testLongShallowRepair(t, false, trie.PathScheme) }
func TestLongShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongShallowRepair(t, true, trie.PathScheme)
}

func testLongShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where a recent
// block - older than the ancient limit - was already committed to disk and then
// the process crashed. In this case we expect the chain to be rolled back to the
// committed block, with everything afterwads deleted.
func TestLongDeepRepairHashBased(t *testing.T) { testLongDeepRepair(t, false, trie.HashScheme) }
func TestLongDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongDeepRepair(t, true, trie.HashScheme)
}
func TestLongDeepRepairPathBased(t *testing.T) { testLongDeepRepair(t, false, trie.PathScheme) }
func TestLongDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongDeepRepair(t, true, trie.PathScheme)
}

func testLongDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where the fast
// sync pivot point - newer than the ancient limit - was already committed, after
// which the process crashed. In this case we expect the chain to be rolled back
// to the committed block, with everything afterwads kept as fast sync data.
func TestLongSnapSyncedShallowRepairHashBased(t *testing.T) {
	testLongSnapSyncedShallowRepair(t, false, trie.HashScheme)
}
func TestLongSnapSyncedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongSnapSyncedShallowRepair(t, true, trie.HashScheme)
}
func TestLongSnapSyncedShallowRepairPathBased(t *testing.T) {
	testLongSnapSyncedShallowRepair(t, false, trie.PathScheme)
}
func TestLongSnapSyncedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongSnapSyncedShallowRepair(t, true, trie.PathScheme)
}

func testLongSnapSyncedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where the fast
// sync pivot point - older than the ancient limit - was already committed, after
// which the process crashed. In this case we expect the chain to be rolled back
// to the committed block, with everything afterwads deleted.
func TestLongSnapSyncedDeepRepairHashBased(t *testing.T) {
	testLongSnapSyncedDeepRepair(t, false, trie.HashScheme)
}
func TestLongSnapSyncedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongSnapSyncedDeepRepair(t, true, trie.HashScheme)
}
func TestLongSnapSyncedDeepRepairPathBased(t *testing.T) {
	testLongSnapSyncedDeepRepair(t, false, trie.PathScheme)
}
func TestLongSnapSyncedDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongSnapSyncedDeepRepair(t, true, trie.PathScheme)
}

func testLongSnapSyncedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where the fast
// sync pivot point - older than the ancient limit - was not yet committed, but the
// process crashed. In this case we expect the chain to detect that it was fast
// syncing and not delete anything, since we can just pick up directly where we
// left off.
func TestLongSnapSyncingShallowRepairHashBased(t *testing.T) {
	testLongSnapSyncingShallowRepair(t, false, trie.HashScheme)
}
func TestLongSnapSyncingShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongSnapSyncingShallowRepair(t, true, trie.HashScheme)
}
func TestLongSnapSyncingShallowRepairPathBased(t *testing.T) {
	testLongSnapSyncingShallowRepair(t, false, trie.PathScheme)
}
func TestLongSnapSyncingShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongSnapSyncingShallowRepair(t, true, trie.PathScheme)
}

func testLongSnapSyncingShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks where the fast
// sync pivot point - newer than the ancient limit - was not yet committed, but the
// process crashed. In this case we expect the chain to detect that it was fast
// syncing and not delete anything, since we can just pick up directly where we
// left off.
func TestLongSnapSyncingDeepRepairHashBased(t *testing.T) {
	testLongSnapSyncingDeepRepair(t, false, trie.HashScheme)
}
func TestLongSnapSyncingDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongSnapSyncingDeepRepair(t, true, trie.HashScheme)
}
func TestLongSnapSyncingDeepRepairPathBased(t *testing.T) {
	testLongSnapSyncingDeepRepair(t, false, trie.PathScheme)
}
func TestLongSnapSyncingDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongSnapSyncingDeepRepair(t, true, trie.PathScheme)
}

func testLongSnapSyncingDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected in leveldb:
	//   C8)->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24
	//
	// Expected head header    : C24
	// Expected head fast block: C24
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    0,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 24,
		expSidechainBlocks: 0,
		expFrozen:          9,
		expHeadHeader:      24,
		expHeadFastBlock:   24,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where a recent block - newer than the ancient limit - was already
// committed to disk and then the process crashed. In this test scenario the side
// chain is below the committed block. In this case we expect the chain to be
// rolled back to the committed block, with everything afterwads kept as fast
// sync data; the side chain completely nuked by the freezer.
func TestLongOldForkedShallowRepairHashBased(t *testing.T) {
	testLongOldForkedShallowRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedShallowRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedShallowRepair(t *testing.T) {
	testLongOldForkedShallowRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedShallowRepairWithSnapshots(t *testing.T) {
	testLongOldForkedShallowRepair(t, true, trie.PathScheme)
}

func testLongOldForkedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where a recent block - older than the ancient limit - was already
// committed to disk and then the process crashed. In this test scenario the side
// chain is below the committed block. In this case we expect the canonical chain
// to be rolled back to the committed block, with everything afterwads deleted;
// the side chain completely nuked by the freezer.
func TestLongOldForkedDeepRepairHashBased(t *testing.T) {
	testLongOldForkedDeepRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedDeepRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedDeepRepair(t *testing.T) {
	testLongOldForkedDeepRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedDeepRepairWithSnapshots(t *testing.T) {
	testLongOldForkedDeepRepair(t, true, trie.PathScheme)
}

func testLongOldForkedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - newer than the ancient limit -
// was already committed to disk and then the process crashed. In this test scenario
// the side chain is below the committed block. In this case we expect the chain
// to be rolled back to the committed block, with everything afterwads kept as
// fast sync data; the side chain completely nuked by the freezer.
func TestLongOldForkedSnapSyncedShallowRepairHashBased(t *testing.T) {
	testLongOldForkedSnapSyncedShallowRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedSnapSyncedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedSnapSyncedShallowRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedSnapSyncedShallowRepairPathBased(t *testing.T) {
	testLongOldForkedSnapSyncedShallowRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedSnapSyncedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongOldForkedSnapSyncedShallowRepair(t, true, trie.PathScheme)
}

func testLongOldForkedSnapSyncedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was already committed to disk and then the process crashed. In this test scenario
// the side chain is below the committed block. In this case we expect the canonical
// chain to be rolled back to the committed block, with everything afterwads deleted;
// the side chain completely nuked by the freezer.
func TestLongOldForkedSnapSyncedDeepRepairHashBased(t *testing.T) {
	testLongOldForkedSnapSyncedDeepRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedSnapSyncedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedSnapSyncedDeepRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedSnapSyncedDeepRepairPathBased(t *testing.T) {
	testLongOldForkedSnapSyncedDeepRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedSnapSyncedDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongOldForkedSnapSyncedDeepRepair(t, true, trie.PathScheme)
}

func testLongOldForkedSnapSyncedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was not yet committed, but the process crashed. In this test scenario the side
// chain is below the committed block. In this case we expect the chain to detect
// that it was fast syncing and not delete anything. The side chain is completely
// nuked by the freezer.
func TestLongOldForkedSnapSyncingShallowRepairHashBased(t *testing.T) {
	testLongOldForkedSnapSyncingShallowRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedSnapSyncingShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedSnapSyncingShallowRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedSnapSyncingShallowRepairPathBased(t *testing.T) {
	testLongOldForkedSnapSyncingShallowRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedSnapSyncingShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongOldForkedSnapSyncingShallowRepair(t, true, trie.PathScheme)
}

func testLongOldForkedSnapSyncingShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was not yet committed, but the process crashed. In this test scenario the side
// chain is below the committed block. In this case we expect the chain to detect
// that it was fast syncing and not delete anything. The side chain is completely
// nuked by the freezer.
func TestLongOldForkedSnapSyncingDeepRepairHashBased(t *testing.T) {
	testLongOldForkedSnapSyncingDeepRepair(t, false, trie.HashScheme)
}
func TestLongOldForkedSnapSyncingDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongOldForkedSnapSyncingDeepRepair(t, true, trie.HashScheme)
}
func TestLongOldForkedSnapSyncingDeepRepairPathBased(t *testing.T) {
	testLongOldForkedSnapSyncingDeepRepair(t, false, trie.PathScheme)
}
func TestLongOldForkedSnapSyncingDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongOldForkedSnapSyncingDeepRepair(t, true, trie.PathScheme)
}

func testLongOldForkedSnapSyncingDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected in leveldb:
	//   C8)->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24
	//
	// Expected head header    : C24
	// Expected head fast block: C24
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    3,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 24,
		expSidechainBlocks: 0,
		expFrozen:          9,
		expHeadHeader:      24,
		expHeadFastBlock:   24,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where a recent block - newer than the ancient limit - was already
// committed to disk and then the process crashed. In this test scenario the side
// chain is above the committed block. In this case we expect the chain to be
// rolled back to the committed block, with everything afterwads kept as fast
// sync data; the side chain completely nuked by the freezer.
func TestLongNewerForkedShallowRepairHashBased(t *testing.T) {
	testLongNewerForkedShallowRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedShallowRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedShallowRepairPathBased(t *testing.T) {
	testLongNewerForkedShallowRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedShallowRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where a recent block - older than the ancient limit - was already
// committed to disk and then the process crashed. In this test scenario the side
// chain is above the committed block. In this case we expect the canonical chain
// to be rolled back to the committed block, with everything afterwads deleted;
// the side chain completely nuked by the freezer.
func TestLongNewerForkedDeepRepairHashBased(t *testing.T) {
	testLongNewerForkedDeepRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedDeepRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedDeepRepairPathBased(t *testing.T) {
	testLongNewerForkedDeepRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedDeepRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - newer than the ancient limit -
// was already committed to disk and then the process crashed. In this test scenario
// the side chain is above the committed block. In this case we expect the chain
// to be rolled back to the committed block, with everything afterwads kept as fast
// sync data; the side chain completely nuked by the freezer.
func TestLongNewerForkedSnapSyncedShallowRepairHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncedShallowRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncedShallowRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncedShallowRepairPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncedShallowRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedSnapSyncedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncedShallowRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedSnapSyncedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was already committed to disk and then the process crashed. In this test scenario
// the side chain is above the committed block. In this case we expect the canonical
// chain to be rolled back to the committed block, with everything afterwads deleted;
// the side chain completely nuked by the freezer.
func TestLongNewerForkedSnapSyncedDeepRepairHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncedDeepRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncedDeepRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncedDeepRepairPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncedDeepRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedSnapSyncedDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncedDeepRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedSnapSyncedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was not yet committed, but the process crashed. In this test scenario the side
// chain is above the committed block. In this case we expect the chain to detect
// that it was fast syncing and not delete anything. The side chain is completely
// nuked by the freezer.
func TestLongNewerForkedSnapSyncingShallowRepairHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncingShallowRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncingShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncingShallowRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncingShallowRepairPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncingShallowRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedSnapSyncingShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncingShallowRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedSnapSyncingShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a shorter
// side chain, where the fast sync pivot point - older than the ancient limit -
// was not yet committed, but the process crashed. In this test scenario the side
// chain is above the committed block. In this case we expect the chain to detect
// that it was fast syncing and not delete anything. The side chain is completely
// nuked by the freezer.
func TestLongNewerForkedSnapSyncingDeepRepairHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncingDeepRepair(t, false, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncingDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongNewerForkedSnapSyncingDeepRepair(t, true, trie.HashScheme)
}
func TestLongNewerForkedSnapSyncingDeepRepairPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncingDeepRepair(t, false, trie.PathScheme)
}
func TestLongNewerForkedSnapSyncingDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongNewerForkedSnapSyncingDeepRepair(t, true, trie.PathScheme)
}

func testLongNewerForkedSnapSyncingDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected in leveldb:
	//   C8)->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24
	//
	// Expected head header    : C24
	// Expected head fast block: C24
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    12,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 24,
		expSidechainBlocks: 0,
		expFrozen:          9,
		expHeadHeader:      24,
		expHeadFastBlock:   24,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer side
// chain, where a recent block - newer than the ancient limit - was already committed
// to disk and then the process crashed. In this case we expect the chain to be
// rolled back to the committed block, with everything afterwads kept as fast sync
// data. The side chain completely nuked by the freezer.
func TestLongReorgedShallowRepairHashBased(t *testing.T) {
	testLongReorgedShallowRepair(t, false, trie.HashScheme)
}
func TestLongReorgedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedShallowRepair(t, true, trie.HashScheme)
}
func TestLongReorgedShallowRepairPathBased(t *testing.T) {
	testLongReorgedShallowRepair(t, false, trie.PathScheme)
}
func TestLongReorgedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongReorgedShallowRepair(t, true, trie.PathScheme)
}

func testLongReorgedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer side
// chain, where a recent block - older than the ancient limit - was already committed
// to disk and then the process crashed. In this case we expect the canonical chains
// to be rolled back to the committed block, with everything afterwads deleted. The
// side chain completely nuked by the freezer.
func TestLongReorgedDeepRepairHashBased(t *testing.T) {
	testLongReorgedDeepRepair(t, false, trie.HashScheme)
}
func TestLongReorgedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedDeepRepair(t, true, trie.HashScheme)
}
func TestLongReorgedDeepRepairPathBased(t *testing.T) {
	testLongReorgedDeepRepair(t, false, trie.PathScheme)
}
func TestLongReorgedDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongReorgedDeepRepair(t, true, trie.PathScheme)
}

func testLongReorgedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : none
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         nil,
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer
// side chain, where the fast sync pivot point - newer than the ancient limit -
// was already committed to disk and then the process crashed. In this case we
// expect the chain to be rolled back to the committed block, with everything
// afterwads kept as fast sync data. The side chain completely nuked by the
// freezer.
func TestLongReorgedSnapSyncedShallowRepairHashBased(t *testing.T) {
	testLongReorgedSnapSyncedShallowRepair(t, false, trie.HashScheme)
}
func TestLongReorgedSnapSyncedShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedSnapSyncedShallowRepair(t, true, trie.HashScheme)
}
func TestLongReorgedSnapSyncedShallowRepairPathBased(t *testing.T) {
	testLongReorgedSnapSyncedShallowRepair(t, false, trie.PathScheme)
}
func TestLongReorgedSnapSyncedShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongReorgedSnapSyncedShallowRepair(t, true, trie.PathScheme)
}

func testLongReorgedSnapSyncedShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer
// side chain, where the fast sync pivot point - older than the ancient limit -
// was already committed to disk and then the process crashed. In this case we
// expect the canonical chains to be rolled back to the committed block, with
// everything afterwads deleted. The side chain completely nuked by the freezer.
func TestLongReorgedSnapSyncedDeepRepairHashBased(t *testing.T) {
	testLongReorgedSnapSyncedDeepRepair(t, false, trie.HashScheme)
}
func TestLongReorgedSnapSyncedDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedSnapSyncedDeepRepair(t, true, trie.HashScheme)
}
func TestLongReorgedSnapSyncedDeepRepairPathBased(t *testing.T) {
	testLongReorgedSnapSyncedDeepRepair(t, false, trie.PathScheme)
}
func TestLongReorgedSnapSyncedDeepRepairWithSnapshotsPathased(t *testing.T) {
	testLongReorgedSnapSyncedDeepRepair(t, true, trie.PathScheme)
}

func testLongReorgedSnapSyncedDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G, C4
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4
	//
	// Expected in leveldb: none
	//
	// Expected head header    : C4
	// Expected head fast block: C4
	// Expected head block     : C4
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        4,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 4,
		expSidechainBlocks: 0,
		expFrozen:          5,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer
// side chain, where the fast sync pivot point - newer than the ancient limit -
// was not yet committed, but the process crashed. In this case we expect the
// chain to detect that it was fast syncing and not delete anything, since we
// can just pick up directly where we left off.
func TestLongReorgedSnapSyncingShallowRepairHashBased(t *testing.T) {
	testLongReorgedSnapSyncingShallowRepair(t, false, trie.HashScheme)
}
func TestLongReorgedSnapSyncingShallowRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedSnapSyncingShallowRepair(t, true, trie.HashScheme)
}
func TestLongReorgedSnapSyncingShallowRepairPathBased(t *testing.T) {
	testLongReorgedSnapSyncingShallowRepair(t, false, trie.PathScheme)
}
func TestLongReorgedSnapSyncingShallowRepairWithSnapshotsPathBased(t *testing.T) {
	testLongReorgedSnapSyncingShallowRepair(t, true, trie.PathScheme)
}

func testLongReorgedSnapSyncingShallowRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2
	//
	// Expected in leveldb:
	//   C2)->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18
	//
	// Expected head header    : C18
	// Expected head fast block: C18
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    18,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 18,
		expSidechainBlocks: 0,
		expFrozen:          3,
		expHeadHeader:      18,
		expHeadFastBlock:   18,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

// Tests a recovery for a long canonical chain with frozen blocks and a longer
// side chain, where the fast sync pivot point - older than the ancient limit -
// was not yet committed, but the process crashed. In this case we expect the
// chain to detect that it was fast syncing and not delete anything, since we
// can just pick up directly where we left off.
func TestLongReorgedSnapSyncingDeepRepairHashBased(t *testing.T) {
	testLongReorgedSnapSyncingDeepRepair(t, false, trie.HashScheme)
}
func TestLongReorgedSnapSyncingDeepRepairWithSnapshotsHashBased(t *testing.T) {
	testLongReorgedSnapSyncingDeepRepair(t, true, trie.HashScheme)
}
func TestLongReorgedSnapSyncingDeepRepairPathBased(t *testing.T) {
	testLongReorgedSnapSyncingDeepRepair(t, false, trie.PathScheme)
}
func TestLongReorgedSnapSyncingDeepRepairWithSnapshotsPathBased(t *testing.T) {
	testLongReorgedSnapSyncingDeepRepair(t, true, trie.PathScheme)
}

func testLongReorgedSnapSyncingDeepRepair(t *testing.T, snapshots bool, scheme string) {
	// Chain:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24 (HEAD)
	//   └->S1->S2->S3->S4->S5->S6->S7->S8->S9->S10->S11->S12->S13->S14->S15->S16->S17->S18->S19->S20->S21->S22->S23->S24->S25->S26
	//
	// Frozen:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Commit: G
	// Pivot : C4
	//
	// CRASH
	//
	// ------------------------------
	//
	// Expected in freezer:
	//   G->C1->C2->C3->C4->C5->C6->C7->C8
	//
	// Expected in leveldb:
	//   C8)->C9->C10->C11->C12->C13->C14->C15->C16->C17->C18->C19->C20->C21->C22->C23->C24
	//
	// Expected head header    : C24
	// Expected head fast block: C24
	// Expected head block     : G
	testRepair(t, &rewindTest{
		canonicalBlocks:    24,
		sidechainBlocks:    26,
		freezeThreshold:    16,
		commitBlock:        0,
		pivotBlock:         uint64ptr(4),
		expCanonicalBlocks: 24,
		expSidechainBlocks: 0,
		expFrozen:          9,
		expHeadHeader:      24,
		expHeadFastBlock:   24,
		expHeadBlock:       0,
	}, snapshots, scheme)
}

func testRepair(t *testing.T, tt *rewindTest, snapshots bool, scheme string) {
	// It's hard to follow the test case, visualize the input
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	// fmt.Println(tt.dump(true))

	// Create a temporary persistent database
	datadir := t.TempDir()

	db, err := rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "", false)
	if err != nil {
		t.Fatalf("Failed to create persistent database: %v", err)
	}
	defer db.Close() // Might double close, should be fine

	// Initialize a fresh chain
	var (
		genesis, _ = (&Genesis{BaseFee: big.NewInt(params.InitialBaseFee)}).Commit(db, scheme)
		engine     = ethash.NewFullFaker()
		config     = &CacheConfig{
			TrieCleanLimit: 256,
			TrieDirtyLimit: 256,
			TrieTimeLimit:  5 * time.Minute,
			SnapshotLimit:  0, // Disable snapshot by default
			NodeScheme:     scheme,
		}
	)
	defer engine.Close()
	if snapshots {
		config.SnapshotLimit = 256
		config.SnapshotWait = true
	}
	chain, err := NewBlockChain(db, config, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create chain: %v", err)
	}
	// If sidechain blocks are needed, make a light chain and import it
	var sideblocks types.Blocks
	if tt.sidechainBlocks > 0 {
		sideblocks, _ = GenerateChain(params.TestChainConfig, genesis, engine, rawdb.NewMemoryDatabase(), tt.sidechainBlocks, func(i int, b *BlockGen) {
			b.SetCoinbase(common.Address{0x01})
		})
		if _, err := chain.InsertChain(sideblocks); err != nil {
			t.Fatalf("Failed to import side chain: %v", err)
		}
	}
	canonblocks, _ := GenerateChain(params.TestChainConfig, genesis, engine, rawdb.NewMemoryDatabase(), tt.canonicalBlocks, func(i int, b *BlockGen) {
		b.SetCoinbase(common.Address{0x02})
		b.SetDifficulty(big.NewInt(1000000))
	})
	if _, err := chain.InsertChain(canonblocks[:tt.commitBlock]); err != nil {
		t.Fatalf("Failed to import canonical chain start: %v", err)
	}
	if tt.commitBlock > 0 {
		if err := chain.triedb.Commit(canonblocks[tt.commitBlock-1].Root()); err != nil {
			t.Fatalf("Failed to flush trie state: %v", err)
		}
		if snapshots {
			if err := chain.snaps.Cap(canonblocks[tt.commitBlock-1].Root(), 0); err != nil {
				t.Fatalf("Failed to flatten snapshots: %v", err)
			}
		}
	}
	if _, err := chain.InsertChain(canonblocks[tt.commitBlock:]); err != nil {
		t.Fatalf("Failed to import canonical chain tail: %v", err)
	}
	// Force run a freeze cycle
	type freezer interface {
		Freeze(threshold uint64) error
		Ancients() (uint64, error)
	}
	db.(freezer).Freeze(tt.freezeThreshold)

	// Set the simulated pivot block
	if tt.pivotBlock != nil {
		rawdb.WriteLastPivotNumber(db, *tt.pivotBlock)
	}
	// Pull the plug on the database, simulating a hard crash
	chain.triedb.Close()
	db.Close()

	// Start a new blockchain back up and see where the repair leads us
	db, err = rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "", false)
	if err != nil {
		t.Fatalf("Failed to reopen persistent database: %v", err)
	}
	defer db.Close()

	newChain, err := NewBlockChain(db, config, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to recreate chain: %v", err)
	}
	defer newChain.Stop()

	// Iterate over all the remaining blocks and ensure there are no gaps
	verifyNoGaps(t, newChain, true, canonblocks)
	verifyNoGaps(t, newChain, false, sideblocks)
	verifyCutoff(t, newChain, true, canonblocks, tt.expCanonicalBlocks)
	verifyCutoff(t, newChain, false, sideblocks, tt.expSidechainBlocks)

	if head := newChain.CurrentHeader(); head.Number.Uint64() != tt.expHeadHeader {
		t.Errorf("Head header mismatch: have %d, want %d", head.Number, tt.expHeadHeader)
	}
	if head := newChain.CurrentFastBlock(); head.NumberU64() != tt.expHeadFastBlock {
		t.Errorf("Head fast block mismatch: have %d, want %d", head.NumberU64(), tt.expHeadFastBlock)
	}
	if head := newChain.CurrentBlock(); head.NumberU64() != tt.expHeadBlock {
		t.Errorf("Head block mismatch: have %d, want %d", head.NumberU64(), tt.expHeadBlock)
	}
	if frozen, err := db.(freezer).Ancients(); err != nil {
		t.Errorf("Failed to retrieve ancient count: %v\n", err)
	} else if int(frozen) != tt.expFrozen {
		t.Errorf("Frozen block count mismatch: have %d, want %d", frozen, tt.expFrozen)
	}
}

// TestIssue23496 tests scenario described in https://github.com/ethereum/go-ethereum/pull/23496#issuecomment-926393893
// Credits to @zzyalbert for finding the issue.
//
// Local chain owns these blocks:
// G  B1  B2  B3  B4
// B1: state committed
// B2: snapshot disk layer
// B3: state committed
// B4: head block
//
// Crash happens without fully persisting snapshot and in-memory states,
// chain rewinds itself to the B1 (skip B3 in order to recover snapshot)
// In this case the snapshot layer of B3 is not created because of existent
// state.
func TestIssue23496HashBased(t *testing.T) { testIssue23496(t, trie.HashScheme) }
func TestIssue23496PathBased(t *testing.T) { testIssue23496(t, trie.PathScheme) }

func testIssue23496(t *testing.T, scheme string) {
	// It's hard to follow the test case, visualize the input
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	// Create a temporary persistent database
	datadir := t.TempDir()

	db, err := rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "", false)
	if err != nil {
		t.Fatalf("Failed to create persistent database: %v", err)
	}
	defer db.Close() // Might double close, should be fine

	// Initialize a fresh chain
	var (
		genesis, _ = (&Genesis{BaseFee: big.NewInt(params.InitialBaseFee)}).Commit(db, scheme)
		engine     = ethash.NewFullFaker()
	)
	chain, err := NewBlockChain(db, DefaultCacheConfigWithScheme(scheme), params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create chain: %v", err)
	}
	blocks, _ := GenerateChain(params.TestChainConfig, genesis, engine, rawdb.NewMemoryDatabase(), 4, func(i int, b *BlockGen) {
		b.SetCoinbase(common.Address{0x02})
		b.SetDifficulty(big.NewInt(1000000))
	})

	// Insert block B1 and commit the state into disk
	if _, err := chain.InsertChain(blocks[:1]); err != nil {
		t.Fatalf("Failed to import canonical chain start: %v", err)
	}
	chain.triedb.Commit(blocks[0].Root())

	// Insert block B2 and commit the snapshot into disk
	if _, err := chain.InsertChain(blocks[1:2]); err != nil {
		t.Fatalf("Failed to import canonical chain start: %v", err)
	}
	if err := chain.snaps.Cap(blocks[1].Root(), 0); err != nil {
		t.Fatalf("Failed to flatten snapshots: %v", err)
	}

	// Insert block B3 and commit the state into disk
	if _, err := chain.InsertChain(blocks[2:3]); err != nil {
		t.Fatalf("Failed to import canonical chain start: %v", err)
	}
	chain.triedb.Commit(blocks[2].Root())

	// Insert the remaining blocks
	if _, err := chain.InsertChain(blocks[3:]); err != nil {
		t.Fatalf("Failed to import canonical chain tail: %v", err)
	}

	// Pull the plug on the database, simulating a hard crash
	chain.triedb.Close()
	db.Close()

	// Start a new blockchain back up and see where the repair leads us
	db, err = rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "", false)
	if err != nil {
		t.Fatalf("Failed to reopen persistent database: %v", err)
	}
	defer db.Close()

	chain, err = NewBlockChain(db, DefaultCacheConfigWithScheme(scheme), params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to recreate chain: %v", err)
	}
	defer chain.Stop()

	if head := chain.CurrentHeader(); head.Number.Uint64() != uint64(4) {
		t.Errorf("Head header mismatch: have %d, want %d", head.Number, 4)
	}
	if head := chain.CurrentFastBlock(); head.NumberU64() != uint64(4) {
		t.Errorf("Head fast block mismatch: have %d, want %d", head.NumberU64(), uint64(4))
	}
	expHead := uint64(1)
	if scheme == trie.PathScheme {
		expHead = uint64(2)
	}
	if head := chain.CurrentBlock(); head.NumberU64() != expHead {
		t.Errorf("Head block mismatch: have %d, want %d", head.NumberU64(), expHead)
	}

	// Reinsert B2-B4
	if _, err := chain.InsertChain(blocks[1:]); err != nil {
		t.Fatalf("Failed to import canonical chain tail: %v", err)
	}
	if head := chain.CurrentHeader(); head.Number.Uint64() != uint64(4) {
		t.Errorf("Head header mismatch: have %d, want %d", head.Number, 4)
	}
	if head := chain.CurrentFastBlock(); head.NumberU64() != uint64(4) {
		t.Errorf("Head fast block mismatch: have %d, want %d", head.NumberU64(), uint64(4))
	}
	if head := chain.CurrentBlock(); head.NumberU64() != uint64(4) {
		t.Errorf("Head block mismatch: have %d, want %d", head.NumberU64(), uint64(4))
	}
	if layer := chain.Snapshots().Snapshot(blocks[2].Root()); layer == nil {
		t.Error("Failed to regenerate the snapshot of known state")
	}
}
