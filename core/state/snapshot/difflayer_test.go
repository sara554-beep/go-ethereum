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

package snapshot

import (
	"bytes"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func copyDestructs(destructs map[common.Hash]struct{}) map[common.Hash]struct{} {
	copy := make(map[common.Hash]struct{})
	for hash := range destructs {
		copy[hash] = struct{}{}
	}
	return copy
}

func copyAccounts(accounts map[common.Hash][]byte) map[common.Hash][]byte {
	copy := make(map[common.Hash][]byte)
	for hash, blob := range accounts {
		copy[hash] = blob
	}
	return copy
}

func copyStorage(storage map[common.Hash]map[common.Hash][]byte) map[common.Hash]map[common.Hash][]byte {
	copy := make(map[common.Hash]map[common.Hash][]byte)
	for accHash, slots := range storage {
		copy[accHash] = make(map[common.Hash][]byte)
		for slotHash, blob := range slots {
			copy[accHash][slotHash] = blob
		}
	}
	return copy
}

func emptyLayer() *diskLayer {
	return &diskLayer{
		diskdb: memorydb.New(),
		cache:  fastcache.New(500 * 1024),
	}
}

// BenchmarkSearch checks how long it takes to find a non-existing key
// BenchmarkSearch-6   	  200000	     10481 ns/op (1K per layer)
// BenchmarkSearch-6   	  200000	     10760 ns/op (10K per layer)
// BenchmarkSearch-6   	  100000	     17866 ns/op
//
// BenchmarkSearch-6   	  500000	      3723 ns/op (10k per layer, only top-level RLock()
func BenchmarkSearch(b *testing.B) {
	// First, we set up 128 diff layers, with 1K items each
	fill := func(parent snapshot) *diffLayer {
		var (
			destructs = make(map[common.Hash]struct{})
			accounts  = make(map[common.Hash][]byte)
			storage   = make(map[common.Hash]map[common.Hash][]byte)
		)
		for i := 0; i < 10000; i++ {
			accounts[randomHash()] = randomAccount()
		}
		return newDiffLayer(parent, common.Hash{}, parent.ID()+1, destructs, accounts, storage)
	}
	var layer snapshot
	layer = emptyLayer()
	for i := 0; i < 128; i++ {
		layer = fill(layer)
	}
	key := crypto.Keccak256Hash([]byte{0x13, 0x38})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		layer.AccountRLP(key)
	}
}

// BenchmarkSearchSlot checks how long it takes to find a non-existing key
// - Number of layers: 128
// - Each layers contains the account, with a couple of storage slots
// BenchmarkSearchSlot-6   	  100000	     14554 ns/op
// BenchmarkSearchSlot-6   	  100000	     22254 ns/op (when checking parent root using mutex)
// BenchmarkSearchSlot-6   	  100000	     14551 ns/op (when checking parent number using atomic)
// With bloom filter:
// BenchmarkSearchSlot-6   	 3467835	       351 ns/op
func BenchmarkSearchSlot(b *testing.B) {
	// First, we set up 128 diff layers, with 1K items each
	accountKey := crypto.Keccak256Hash([]byte{0x13, 0x37})
	storageKey := crypto.Keccak256Hash([]byte{0x13, 0x37})
	accountRLP := randomAccount()
	fill := func(parent snapshot) *diffLayer {
		var (
			destructs = make(map[common.Hash]struct{})
			accounts  = make(map[common.Hash][]byte)
			storage   = make(map[common.Hash]map[common.Hash][]byte)
		)
		accounts[accountKey] = accountRLP

		accStorage := make(map[common.Hash][]byte)
		for i := 0; i < 5; i++ {
			value := make([]byte, 32)
			rand.Read(value)
			accStorage[randomHash()] = value
			storage[accountKey] = accStorage
		}
		return newDiffLayer(parent, common.Hash{}, parent.ID()+1, destructs, accounts, storage)
	}
	var layer snapshot
	layer = emptyLayer()
	for i := 0; i < 128; i++ {
		layer = fill(layer)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		layer.Storage(accountKey, storageKey)
	}
}

// This test writes ~324M of diff layers to disk, spread over
// - 128 individual layers,
// - each with 200 accounts
// - containing 200 slots
//
// BenchmarkJournal-6   	       1	1471373923 ns/ops
// BenchmarkJournal-6   	       1	1208083335 ns/op // bufio writer
func BenchmarkJournal(b *testing.B) {
	fill := func(parent snapshot) *diffLayer {
		var (
			destructs = make(map[common.Hash]struct{})
			accounts  = make(map[common.Hash][]byte)
			storage   = make(map[common.Hash]map[common.Hash][]byte)
		)
		for i := 0; i < 200; i++ {
			accountKey := randomHash()
			accounts[accountKey] = randomAccount()

			accStorage := make(map[common.Hash][]byte)
			for i := 0; i < 200; i++ {
				value := make([]byte, 32)
				rand.Read(value)
				accStorage[randomHash()] = value
			}
			storage[accountKey] = accStorage
		}
		return newDiffLayer(parent, common.Hash{}, parent.ID()+1, destructs, accounts, storage)
	}
	layer := snapshot(emptyLayer())
	for i := 1; i < 128; i++ {
		layer = fill(layer)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		layer.Journal(new(bytes.Buffer))
	}
}
