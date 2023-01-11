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

package snap

import "github.com/ethereum/go-ethereum/metrics"

var (
	triedbCleanHitMeter   = metrics.NewRegisteredMeter("trie/snap/clean/hit", nil)
	triedbCleanMissMeter  = metrics.NewRegisteredMeter("trie/snap/clean/miss", nil)
	triedbCleanReadMeter  = metrics.NewRegisteredMeter("trie/snap/clean/read", nil)
	triedbCleanWriteMeter = metrics.NewRegisteredMeter("trie/snap/clean/write", nil)

	triedbDirtyHitMeter   = metrics.NewRegisteredMeter("trie/snap/dirty/hit", nil)
	triedbDirtyMissMeter  = metrics.NewRegisteredMeter("trie/snap/dirty/miss", nil)
	triedbDirtyReadMeter  = metrics.NewRegisteredMeter("trie/snap/dirty/read", nil)
	triedbDirtyWriteMeter = metrics.NewRegisteredMeter("trie/snap/dirty/write", nil)

	triedbDirtyNodeHitDepthHist = metrics.NewRegisteredHistogram("trie/snap/dirty/depth", nil, metrics.NewExpDecaySample(1028, 0.015))

	triedbCommitTimeTimer  = metrics.NewRegisteredTimer("trie/snap/commit/time", nil)
	triedbCommitNodesMeter = metrics.NewRegisteredMeter("trie/snap/commit/nodes", nil)
	triedbCommitSizeMeter  = metrics.NewRegisteredMeter("trie/snap/commit/size", nil)

	triedbGCNodesMeter = metrics.NewRegisteredMeter("trie/snap/gc/nodes", nil)
	triedbGCSizeMeter  = metrics.NewRegisteredMeter("trie/snap/gc/size", nil)

	triedbDiffLayerSizeMeter  = metrics.NewRegisteredMeter("trie/snap/diff/size", nil)
	triedbDiffLayerNodesMeter = metrics.NewRegisteredMeter("trie/snap/diff/nodes", nil)

	triedbTrieHistoryTimeMeter = metrics.NewRegisteredTimer("trie/snap/triehistory/time", nil)
	triedbTrieHistorySizeMeter = metrics.NewRegisteredMeter("trie/snap/triehistory/size", nil)

	triedbHitAccessListMeter = metrics.NewRegisteredMeter("trie/snap/triehistory/prev/accessList", nil)
	triedbHitDatabaseMeter   = metrics.NewRegisteredMeter("trie/snap/triehistory/prev/database", nil)
)
