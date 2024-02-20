// Copyright 2023 go-ethereum Authors
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

package verkle

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/testdb"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
)

var (
	accounts = map[common.Address]*types.StateAccount{
		{1}: {
			Nonce:    100,
			Balance:  uint256.NewInt(100),
			CodeHash: common.Hash{0x1}.Bytes(),
		},
		{2}: {
			Nonce:    200,
			Balance:  uint256.NewInt(200),
			CodeHash: common.Hash{0x2}.Bytes(),
		},
	}
	storages = map[common.Address]map[common.Hash][]byte{
		{1}: {
			common.Hash{10}: []byte{10},
			common.Hash{11}: []byte{11},
			common.MaxHash:  []byte{0xff},
		},
		{2}: {
			common.Hash{20}: []byte{20},
			common.Hash{21}: []byte{21},
			common.MaxHash:  []byte{0xff},
		},
	}
)

func TestVerkleTreeReadWrite(t *testing.T) {
	db := testdb.New(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	tr, _ := New(types.EmptyVerkleHash, db, NewPointCache(100))

	for addr, acct := range accounts {
		if err := tr.UpdateAccount(addr, acct); err != nil {
			t.Fatalf("Failed to update account, %v", err)
		}
		for key, val := range storages[addr] {
			if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("Failed to update account, %v", err)
			}
		}
	}

	for addr, acct := range accounts {
		stored, err := tr.GetAccount(addr)
		if err != nil {
			t.Fatalf("Failed to get account, %v", err)
		}
		if !reflect.DeepEqual(stored, acct) {
			t.Fatal("account is not matched")
		}
		for key, val := range storages[addr] {
			stored, err := tr.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("Failed to get storage, %v", err)
			}
			if !bytes.Equal(stored, val) {
				t.Fatal("storage is not matched")
			}
		}
	}
}

func TestTreeKey(t *testing.T) {
	var (
		address      = []byte{0x01}
		addressEval  = evaluateAddressPoint(address)
		smallIndex   = uint256.NewInt(1)
		largeIndex   = uint256.NewInt(10000)
		smallStorage = []byte{0x1}
		largeStorage = bytes.Repeat([]byte{0xff}, 16)
	)
	if !bytes.Equal(VersionKey(address), VersionKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched version key")
	}
	if !bytes.Equal(BalanceKey(address), BalanceKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched balance key")
	}
	if !bytes.Equal(NonceKey(address), NonceKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched nonce key")
	}
	if !bytes.Equal(CodeKeccakKey(address), CodeKeccakKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched code keccak key")
	}
	if !bytes.Equal(CodeSizeKey(address), CodeSizeKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched code size key")
	}
	if !bytes.Equal(CodeChunkKey(address, smallIndex), CodeChunkKeyWithEvaluatedAddress(addressEval, smallIndex)) {
		t.Fatal("Unmatched code chunk key")
	}
	if !bytes.Equal(CodeChunkKey(address, largeIndex), CodeChunkKeyWithEvaluatedAddress(addressEval, largeIndex)) {
		t.Fatal("Unmatched code chunk key")
	}
	if !bytes.Equal(StorageSlotKey(address, smallStorage), StorageSlotKeyWithEvaluatedAddress(addressEval, smallStorage)) {
		t.Fatal("Unmatched storage slot key")
	}
	if !bytes.Equal(StorageSlotKey(address, largeStorage), StorageSlotKeyWithEvaluatedAddress(addressEval, largeStorage)) {
		t.Fatal("Unmatched storage slot key")
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: VirtualApple @ 2.50GHz
// BenchmarkTreeKey
// BenchmarkTreeKey-8   	  398731	      2961 ns/op	      32 B/op	       1 allocs/op
func BenchmarkTreeKey(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BalanceKey([]byte{0x01})
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: VirtualApple @ 2.50GHz
// BenchmarkTreeKeyWithEvaluation
// BenchmarkTreeKeyWithEvaluation-8   	  513855	      2324 ns/op	      32 B/op	       1 allocs/op
func BenchmarkTreeKeyWithEvaluation(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	addr := []byte{0x01}
	eval := evaluateAddressPoint(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BalanceKeyWithEvaluatedAddress(eval)
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: VirtualApple @ 2.50GHz
// BenchmarkStorageKey
// BenchmarkStorageKey-8   	  230516	      4584 ns/op	      96 B/op	       3 allocs/op
func BenchmarkStorageKey(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		StorageSlotKey([]byte{0x01}, bytes.Repeat([]byte{0xff}, 32))
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: VirtualApple @ 2.50GHz
// BenchmarkStorageKeyWithEvaluation
// BenchmarkStorageKeyWithEvaluation-8   	  320125	      3753 ns/op	      96 B/op	       3 allocs/op
func BenchmarkStorageKeyWithEvaluation(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	addr := []byte{0x01}
	eval := evaluateAddressPoint(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StorageSlotKeyWithEvaluatedAddress(eval, bytes.Repeat([]byte{0xff}, 32))
	}
}
