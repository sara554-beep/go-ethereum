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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package snap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

// randomHash generates a random blob of data and returns it as a hash.
func randomHash() common.Hash {
	var hash common.Hash
	if n, err := rand.Read(hash[:]); n != common.HashLength || err != nil {
		panic(err)
	}
	return hash
}

func TestGeneration(t *testing.T) {
	var (
		db     = rawdb.NewMemoryDatabase()
		triedb = trie.NewDatabase(db)

		accounts       = make(map[common.Hash][]common.Hash)
		storageEntries = make(map[common.Hash]entrySlice)
		source         = make(map[common.Hash]common.Hash)
	)
	// Lower the trigger
	generationTrigger = 1024

	for i := 0; i < 256; i++ {
		tr, entries := makeStorageTrieWithSeed(4, uint64(i), triedb)
		root := tr.Hash()
		storageEntries[root] = entries
		triedb.Commit(root, false, nil)

		for j := 0; j < 4; j++ {
			accountHash := randomHash()
			accounts[root] = append(accounts[root], accountHash)
			source[accountHash] = root
		}
	}

	// Feed the scheduler in random order.
	sche := newStorageSnapScheduler(triedb, db)
	for account, root := range source {
		sche.tryAdd(root, account)
	}
	sche.close()

	// Verify the result
	for root, accts := range accounts {
		entrie := storageEntries[root]
		for _, acct := range accts {
			iter := db.NewIterator(append(rawdb.SnapshotStoragePrefix, acct.Bytes()...), nil)
			var index int
			for iter.Next() {
				key, val := iter.Key(), iter.Value()
				if index >= len(entrie) {
					t.Error("Extra storage snap detected", "key", key, "val", val, "len", len(entrie))
					continue
				}
				if !bytes.Equal(val, entrie[index].v) {
					t.Error("Invalid storage snap detected", "key", key, "val", val)
					continue
				}
				index += 1
			}
			if index < len(entrie) {
				t.Error("Miss storage snaps")
			}
			iter.Release()
		}
	}
}
