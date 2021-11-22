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
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/rlp"
)

func genDiffs(n int) []reverseDiff {
	var (
		parent common.Hash
		ret    []reverseDiff
	)
	for i := 0; i < n; i++ {
		var (
			root   = randomHash()
			states []stateDiff
		)
		for j := 0; j < 10; j++ {
			if rand.Intn(2) == 0 {
				states = append(states, stateDiff{
					Key: randBytes(30),
					Val: randBytes(30),
				})
			} else {
				states = append(states, stateDiff{
					Key: randBytes(30),
					Val: []byte{},
				})
			}
		}
		ret = append(ret, reverseDiff{
			Version: reverseDiffVersion,
			Parent:  parent,
			Root:    root,
			States:  states,
		})
		parent = root
	}
	return ret
}

func TestLoadStoreReverseDiff(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "testing")
	if err != nil {
		panic("Failed to allocate tempdir")
	}
	db, err := rawdb.NewLevelDBDatabaseWithFreezer(dir, 16, 16, path.Join(dir, "test-fr"), "", false)
	if err != nil {
		panic("Failed to create database")
	}
	defer os.RemoveAll(dir)

	var diffs = genDiffs(10)
	for i := 0; i < len(diffs); i++ {
		blob, err := rlp.EncodeToBytes(diffs[i])
		if err != nil {
			t.Fatalf("Failed to encode reverse diff %v", err)
		}
		rawdb.WriteReverseDiff(db, uint64(i+1), blob)
		rawdb.WriteReverseDiffLookup(db, diffs[i].Parent, uint64(i+1))
	}
	for i := 0; i < len(diffs); i++ {
		diff, err := loadReverseDiff(db, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to load reverse diff %v", err)
		}
		if diff.Version != reverseDiffVersion {
			t.Fatalf("Unexpected version want %d got %d", reverseDiffVersion, diff.Version)
		}
		if diff.Root != diffs[i].Root {
			t.Fatalf("Unexpected root want %x got %x", diffs[i].Root, diff.Root)
		}
		if diff.Parent != diffs[i].Parent {
			t.Fatalf("Unexpected parent want %x got %x", diffs[i].Parent, diff.Parent)
		}
		if !reflect.DeepEqual(diff.States, diffs[i].States) {
			t.Fatal("Unexpected states")
		}
		parent, err := loadReverseDiffParent(db, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to load parent %v", err)
		}
		if parent != diffs[i].Parent {
			t.Fatalf("Unexpected parent want %x got %x", diffs[i].Parent, diff.Parent)
		}
	}
}
