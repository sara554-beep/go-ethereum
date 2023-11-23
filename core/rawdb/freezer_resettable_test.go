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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestResetFreezer(t *testing.T) {
	items := []struct {
		id   uint64
		blob []byte
	}{
		{0, bytes.Repeat([]byte{0}, 2048)},
		{1, bytes.Repeat([]byte{1}, 2048)},
		{2, bytes.Repeat([]byte{2}, 2048)},
	}
	f, _ := NewResettableFreezer(t.TempDir(), "", false, 2048, freezerTestTableDef)
	defer f.Close()

	f.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		for _, item := range items {
			op.AppendRaw("test", item.id, item.blob)
		}
		return nil
	})
	for _, item := range items {
		blob, _ := f.Ancient("test", item.id)
		if !bytes.Equal(blob, item.blob) {
			t.Fatal("Unexpected blob")
		}
	}

	// Reset freezer
	f.Reset()
	count, _ := f.Ancients()
	if count != 0 {
		t.Fatal("Failed to reset freezer")
	}
	for _, item := range items {
		blob, _ := f.Ancient("test", item.id)
		if len(blob) != 0 {
			t.Fatal("Unexpected blob")
		}
	}

	// Fill the freezer
	f.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		for _, item := range items {
			op.AppendRaw("test", item.id, item.blob)
		}
		return nil
	})
	for _, item := range items {
		blob, _ := f.Ancient("test", item.id)
		if !bytes.Equal(blob, item.blob) {
			t.Fatal("Unexpected blob")
		}
	}
}

func TestFreezerCleanup(t *testing.T) {
	items := []struct {
		id   uint64
		blob []byte
	}{
		{0, bytes.Repeat([]byte{0}, 2048)},
		{1, bytes.Repeat([]byte{1}, 2048)},
		{2, bytes.Repeat([]byte{2}, 2048)},
	}
	datadir := t.TempDir()
	f, _ := NewResettableFreezer(datadir, "", false, 2048, freezerTestTableDef)
	f.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		for _, item := range items {
			op.AppendRaw("test", item.id, item.blob)
		}
		return nil
	})
	f.Close()
	os.Rename(datadir, tmpName(datadir))

	// Open the freezer again, trigger cleanup operation
	f, _ = NewResettableFreezer(datadir, "", false, 2048, freezerTestTableDef)
	f.Close()

	if _, err := os.Lstat(tmpName(datadir)); !os.IsNotExist(err) {
		t.Fatal("Failed to cleanup leftover directory")
	}
}

func TestFooBar(t *testing.T) {
	owner := common.HexToHash("5cc0a47442e6bc69eb1ec9e2ff1fe0c9657c26dfa5836f560fd7141038667982")
	path := common.Hex2Bytes("0c05090307")
	fmt.Println(owner.Hex())
	fmt.Println(path)
	key := storageTrieNodeKey(owner, path)
	fmt.Println(hexutil.Encode(key))

	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf87180a0df5465feffb831b1f31a6184b1efdf75f10f13b2b4900956c22f41a6108c45c9808080808080a0b1902b4fca66415f63634e3ddeae1bfa7b877a1db5ed4c029730e166ba2031ae808080a02ded9e78076e79e96fcd5562c7951f678d22a167429cc75c17d30a08705bb6e780808080")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf8518080808080808080a0b1902b4fca66415f63634e3ddeae1bfa7b877a1db5ed4c029730e166ba2031ae808080a02ded9e78076e79e96fcd5562c7951f678d22a167429cc75c17d30a08705bb6e780808080")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf8419e20a8ccaf952498df75fd7dfb93763a000cc976d1512a01bc3afbbbe2ba92a1a06a62a088d03375c29f8c41b3cd5d4e350f25031c4a712bd7ec2f6555b3365cc5")).Hex())
}
