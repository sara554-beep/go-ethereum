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
	"fmt"
	"testing"
)

/*
import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

func writeTrieNode(db ethdb.KeyValueStore, owner common.Hash, path []byte, hash common.Hash, val []byte) {
	key := EncodeNodeKey(owner, path, hash)
	rawdb.WriteTrieNode(db, key, val)
}

func TestStaleKeyGeneration(t *testing.T) {
	var (
		db    = rawdb.NewMemoryDatabase()
		owner = common.HexToHash("0xa84673b2bae1a148e95ee55e77df208bccbb8f7fd71d99ef2a18f7a417207075")
		blob  = []byte{0x1, 0x2}
	)
	{
		// Main account tries
		writeTrieNode(db, common.Hash{}, []byte{}, common.HexToHash("0xdeadbeef00"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01}, common.HexToHash("0xdeadbeef01"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef02"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01, 0x02, 0x03}, common.HexToHash("0xdeadbeef03"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01, 0x02, 0x04}, common.HexToHash("0xdeadbeef04"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01, 0x02, 0x05}, common.HexToHash("0xdeadbeef05"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x01, 0x03}, common.HexToHash("0xdeadbeef06"), blob)
		writeTrieNode(db, common.Hash{}, []byte{0x02}, common.HexToHash("0xdeadbeef07"), blob)

		// Storage tries
		writeTrieNode(db, owner, []byte{}, common.HexToHash("0xdeadbeef08"), blob)
		writeTrieNode(db, owner, []byte{0x01}, common.HexToHash("0xdeadbeef09"), blob)
		writeTrieNode(db, owner, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef0a"), blob)
		writeTrieNode(db, owner, []byte{0x01, 0x02, 0x03, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, common.HexToHash("0xdeadbeef0b"), blob)
		writeTrieNode(db, owner, []byte{0x01, 0x02, 0x04, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, common.HexToHash("0xdeadbeef0c"), blob)
	}
	var (
		mainDeletions = [][]byte{
			EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02, 0x04}, common.HexToHash("0xdeadbeef04")),
			EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02, 0x05}, common.HexToHash("0xdeadbeef05")),
			EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef02")),
			EncodeNodeKey(common.Hash{}, []byte{0x01}, common.HexToHash("0xdeadbeef01")),
			EncodeNodeKey(common.Hash{}, []byte{}, common.HexToHash("0xdeadbeef00")),
		}
		storageDeletions = [][]byte{
			EncodeNodeKey(owner, []byte{0x01, 0x02, 0x03, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, common.HexToHash("0xdeadbeef0b")),
			EncodeNodeKey(owner, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef0a")),
			EncodeNodeKey(owner, []byte{0x01}, common.HexToHash("0xdeadbeef09")),
			EncodeNodeKey(owner, []byte{}, common.HexToHash("0xdeadbeef08")),
		}
	)
	record := newCommitRecord(db, 100, common.HexToHash("0xdeadbeef"))
	record.onDeletionSet = func(keys [][]byte) {
		if len(keys) != len(mainDeletions) {
			for _, key := range keys {
				t.Logf("Deleted %v", key)
			}
			t.Fatalf("Wrong deletion set %d - %d", len(mainDeletions), len(keys))
		}
		for i := 0; i < len(keys); i++ {
			if !bytes.Equal(keys[i], mainDeletions[i]) {
				t.Fatalf("Wrong deletion entry %v - %v", mainDeletions[i], keys[i])
			}
		}
	}
	// Main trie
	record.add(EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02, 0x04}, common.HexToHash("0xdeadbeef14")))
	record.add(EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02, 0x05}, common.HexToHash("0xdeadbeef15")))
	record.add(EncodeNodeKey(common.Hash{}, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef12")))
	record.add(EncodeNodeKey(common.Hash{}, []byte{0x01}, common.HexToHash("0xdeadbeef11")))
	record.add(EncodeNodeKey(common.Hash{}, []byte{}, common.HexToHash("0xdeadbeef10")))
	record.finalize(newOptimalKeyBloom(1000, 0.01), nil)

	// Storage trie
	record2 := newCommitRecord(db, 100, common.HexToHash("0xdeadbeef"))
	record2.onDeletionSet = func(keys [][]byte) {
		if len(keys) != len(storageDeletions) {
			for _, key := range keys {
				t.Logf("Deleted %v", key)
			}
			t.Fatalf("Wrong deletion set %d - %d", len(storageDeletions), len(keys))
		}
		for i := 0; i < len(keys); i++ {
			if !bytes.Equal(keys[i], storageDeletions[i]) {
				t.Fatalf("Wrong deletion entry %v - %v", storageDeletions[i], keys[i])
			}
		}
	}
	record2.add(EncodeNodeKey(owner, []byte{0x01, 0x02, 0x05, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, common.HexToHash("0xdeadbeef1d"))) // other nodes
	record2.add(EncodeNodeKey(owner, []byte{0x01, 0x02, 0x03, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf}, common.HexToHash("0xdeadbeef1b"))) // rewrite existent
	record2.add(EncodeNodeKey(owner, []byte{0x01, 0x02}, common.HexToHash("0xdeadbeef1a")))                                     // rewrite existent
	record2.add(EncodeNodeKey(owner, []byte{0x01}, common.HexToHash("0xdeadbeef19")))                                           // rewrite existent
	record2.add(EncodeNodeKey(owner, []byte{}, common.HexToHash("0xdeadbeef18")))                                               // rewrite existent
	record2.finalize(newOptimalKeyBloom(1000, 0.01), nil)
}

*/

func TestReverse(t *testing.T) {
	slice := [][]byte{
		{0x01, 0x02},
		{0x03, 0x04},
		{0x05, 0x06},
		{0x07, 0x08},
	}
	for i := len(slice)/2 - 1; i >= 0; i-- {
		opp := len(slice) - 1 - i
		slice[i], slice[opp] = slice[opp], slice[i]
	}
	fmt.Println(slice)
}
