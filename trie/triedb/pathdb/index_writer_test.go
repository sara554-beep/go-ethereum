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
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

var (
	testAccountIndex = map[common.Hash][]uint64{
		common.Hash{0x1}: {1, 2},
		common.Hash{0x2}: {2, 3},
	}
	testStorageIndex = map[common.Hash]map[common.Hash][]uint64{
		common.Hash{0x1}: {
			common.Hash{0x1}: {1, 2},
			common.Hash{0x2}: {1, 2},
			common.Hash{0x3}: {1, 2},
			common.Hash{0x4}: {1, 2},
			common.Hash{0x1}: {2},
			common.Hash{0x2}: {2},
		},
		common.Hash{0x2}: {
			common.Hash{0x1}: {2},
			common.Hash{0x2}: {2},
			common.Hash{0x3}: {3},
			common.Hash{0x4}: {3},
		},
	}
)

func TestWriteIndex(t *testing.T) {
	var (
		w  = newWriter()
		db = rawdb.NewMemoryDatabase()
	)
	for account, idList := range testAccountIndex {
		for _, id := range idList {
			w.addAccount(account, id)
		}
	}
	for account, slots := range testStorageIndex {
		for slot, idList := range slots {
			for _, id := range idList {
				w.addSlot(account, slot, id)
			}
		}
	}
	if err := w.finish(db, true, 3); err != nil {
		t.Fatalf("Failed to write state index, %v", err)
	}
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
