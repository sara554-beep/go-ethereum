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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package verkle

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
)

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
