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

package trie

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
)

func fillDB() (*Database, []uint64, []common.Hash, [][]string, [][][]byte) {
	var (
		db      = NewDatabase(rawdb.NewMemoryDatabase(), nil)
		numbers []uint64
		roots   []common.Hash

		testKeys [][]string
		testVals [][][]byte
	)
	// First, we set up 128 diff layers, with 3K items each
	fill := func(parentHash common.Hash, parentNumber uint64) (common.Hash, []string, [][]byte) {
		var (
			keys  []string
			vals  [][]byte
			nodes = make(map[string]*cachedNode)
		)
		for i := 0; i < 3000; i++ {
			var (
				storage []byte
				val     *cachedNode
			)
			r := rand.Intn(3)
			if r == 0 {
				// Creation
				storage = EncodeStorageKey(common.Hash{}, randomHash().Bytes())
				val = randomNode()
			} else if r == 1 {
				// Modification
				if parentNumber == 0 {
					continue
				}
				pkeys := testKeys[parentNumber-1]
				if len(pkeys) == 0 {
					continue
				}
				storage = []byte(pkeys[rand.Intn(len(pkeys))])
				val = randomNode()
			} else {
				// Deletion
				if parentNumber == 0 {
					continue
				}
				pkeys, pvals := testKeys[parentNumber-1], testVals[parentNumber-1]
				if len(pkeys) == 0 {
					continue
				}
				index := rand.Intn(len(pkeys))
				if len(pvals[index]) == 0 {
					continue
				}
				storage = []byte(pkeys[index])
				val = randomEmptyNode(crypto.Keccak256Hash(pvals[index]))
			}
			// Don't add duplicate updates
			if _, ok := nodes[string(storage)]; ok {
				continue
			}
			nodes[string(storage)] = val
			keys = append(keys, string(storage))
			vals = append(vals, common.CopyBytes(val.rlp()))
		}
		hash := randomHash()
		db.Update(hash, parentHash, nodes)
		db.Cap(hash, 128)

		numbers = append(numbers, parentNumber+1)
		roots = append(roots, hash)
		return hash, keys, vals
	}
	// Construct a database with enough reverse diffs stored
	var (
		keys   []string
		vals   [][]byte
		parent common.Hash
	)
	for i := 0; i < 2*128; i++ {
		parent, keys, vals = fill(parent, uint64(i))
		testKeys = append(testKeys, keys)
		testVals = append(testVals, vals)
	}
	return db, numbers, roots, testKeys, testVals
}

func TestDatabaseRollback(t *testing.T) {
	var (
		db, numbers, roots, testKeys, testVals = fillDB()
		dl                                     = db.disklayer()
		diskIndex                              int
	)
	for diskIndex = 0; diskIndex < len(roots); diskIndex++ {
		if roots[diskIndex] == dl.root {
			break
		}
	}
	// Ensure all the reverse diffs are stored properly
	var parent = emptyRoot
	for i := 0; i <= diskIndex; i++ {
		diff, err := loadReverseDiff(db.diskdb, uint64(i+1))
		if err != nil {
			t.Error("Failed to load reverse diff", "err", err)
		}
		if diff.Parent != parent {
			t.Error("Reverse diff is not continuous")
		}
		parent = diff.Root
	}
	// Ensure immature reverse diffs are not present
	for i := diskIndex + 1; i < len(numbers); i++ {
		blob := rawdb.ReadReverseDiff(db.diskdb, uint64(i+1))
		if len(blob) != 0 {
			t.Error("Unexpected reverse diff", "index", i)
		}
	}
	// Revert the db to historical point with reverse state available
	for i := diskIndex; i > 0; i-- {
		if err := db.Rollback(roots[i-1]); err != nil {
			t.Error("Failed to revert db status", "err", err)
		}
		dl := db.disklayer()
		if dl.Root() != roots[i-1] {
			t.Error("Unexpected disk layer root")
		}
		keys, vals := testKeys[i], testVals[i]
		for j := 0; j < len(keys); j++ {
			layer := db.Snapshot(roots[i])
			blob, err := layer.NodeBlob([]byte(keys[j]), crypto.Keccak256Hash(vals[j]))
			if err != nil {
				t.Error("Failed to retrieve state", "err", err)
			}
			if !bytes.Equal(blob, vals[j]) {
				t.Error("Unexpected state", "key", []byte(keys[j]), "want", vals[j], "got", blob)
			}
		}
	}
	if len(db.layers) != 2 {
		t.Error("Only two layers are expected")
	}
}

func TestDatabaseBatchRollback(t *testing.T) {
	var (
		db, _, roots, testKeys, testVals = fillDB()
		dl                               = db.disklayer()
		diskIndex                        int
	)
	for diskIndex = 0; diskIndex < len(roots); diskIndex++ {
		if roots[diskIndex] == dl.root {
			break
		}
	}
	// Revert the db to historical point with reverse state available
	if err := db.Rollback(common.Hash{}); err != nil {
		t.Error("Failed to revert db status", "err", err)
	}
	ndl := db.disklayer()
	if ndl.Root() != emptyRoot {
		t.Error("Unexpected disk layer root")
	}
	// Ensure all the in-memory diff layers are maintained correctly
	if len(db.layers) != 128 {
		t.Error("Diff layer number mismatch", "want", 128, "got", len(db.layers))
	}
	for i, keys := range testKeys {
		vals := testVals[i]
		for j, key := range keys {
			if len(vals[j]) == 0 {
				continue
			}
			hash := crypto.Keccak256Hash(vals[j])
			blob, _ := ndl.NodeBlob([]byte(key), hash)
			if len(blob) != 0 {
				t.Error("Unexpected state")
			}
		}
	}
}
