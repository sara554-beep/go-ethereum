// Copyright 2023 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/

package pathdb

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/testutil"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestWriteIndexXXX(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	var (
		db, _ = rawdb.NewPebbleDBDatabase("pebble", 1024, 1024, "", false)
	)
	for i := 0; i < 10_000_000; i++ {
		db.Put(testutil.RandBytes(30), testutil.RandBytes(30))
	}
	for i := 0; i < 100000; i++ {
		db.Get(testutil.RandBytes(20))
	}
}

func TestWriteIndex(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	var (
		w          = newWriter()
		db, _      = rawdb.NewPebbleDBDatabase("pebble", 1024, 1024, "", false)
		freezer, _ = openFreezer(t.TempDir(), false)
		hs         = makeHistories(3)
	)
	for i := 0; i < 10_000_000; i++ {
		db.Put(testutil.RandBytes(30), testutil.RandBytes(30))
	}
	for i := 0; i < len(hs); i++ {
		stateID := uint64(i + 1)
		accountData, storageData, accountIndex, storageIndex := hs[i].encode()
		rawdb.WriteStateHistory(freezer, stateID, hs[i].meta.encode(), accountIndex, storageIndex, accountData, storageData)

		for _, account := range hs[i].accountList {
			w.addAccount(account, stateID)

			for _, slot := range hs[i].storageList[account] {
				w.addSlot(account, slot, stateID)
			}
		}
	}
	if err := w.finish(db, true, uint64(len(hs))); err != nil {
		t.Fatalf("Failed to write state index, %v", err)
	}
	var (
		accounts     = make(map[common.Hash][]byte)
		accountsLast = make(map[common.Hash][]byte)
		storages     = make(map[common.Hash]map[common.Hash][]byte)
		storagesLast = make(map[common.Hash]map[common.Hash][]byte)
	)
	for _, h := range hs {
		for acct, acctdata := range h.accounts {
			if _, ok := accounts[acct]; !ok {
				accounts[acct] = common.CopyBytes(acctdata)
			}
			accountsLast[acct] = common.CopyBytes(acctdata)
		}
		for acct, slots := range h.storages {
			if storages[acct] == nil {
				storages[acct] = make(map[common.Hash][]byte)
			}
			if storagesLast[acct] == nil {
				storagesLast[acct] = make(map[common.Hash][]byte)
			}
			for slot, slotdata := range slots {
				if _, ok := storages[acct][slot]; !ok {
					storages[acct][slot] = common.CopyBytes(slotdata)
				}
				storagesLast[acct][slot] = common.CopyBytes(slotdata)
			}
		}
	}

	r := newReader(db, freezer)
	for i, h := range hs {
		stateID := uint64(i + 1)
		for account, exp := range h.accounts {
			data, err := r.read(common.Hash{}, account, stateID-1)
			if err != nil {
				fmt.Println("id", stateID, "accountHash", account.Hex(), "err", err)
			} else {
				if !bytes.Equal(data, exp) {
					fmt.Println("id", stateID, "accountHash", account.Hex(), "data", data)
				}
			}
		}
	}
	NewTraverser(db, freezer).Traverse()
}

func TestWriteIndex2(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	bw, _ := newIndexWriter(db, common.Hash{}, common.HexToHash("0xdeadbeef"))
	for i := 0; i < 4000; i++ {
		bw.append(uint64(i))
	}
	batch := db.NewBatch()
	bw.finish(batch)
	fmt.Println(batch.ValueSize())
	batch.Write()

	fmt.Println("====================")

	bw, _ = newIndexWriter(db, common.Hash{}, common.HexToHash("0xdeadbeef"))
	for i := 4000; i < 8000; i++ {
		bw.append(uint64(i))
	}
	batch = db.NewBatch()
	bw.finish(batch)
	fmt.Println(batch.ValueSize())
	batch.Write()

	fmt.Println("====================")

	bw, _ = newIndexWriter(db, common.Hash{}, common.HexToHash("0xdeadbeef"))
	for i := 8000; i < 12000; i++ {
		bw.append(uint64(i))
	}
	batch = db.NewBatch()
	bw.finish(batch)
	fmt.Println(batch.ValueSize())
	batch.Write()
}
