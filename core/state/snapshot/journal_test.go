// Copyright 2020 The go-ethereum Authors
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
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestMarkerEncodeDecode(t *testing.T) {
	var cases = []struct {
		account  []byte
		storages [][]byte
		encoded  []byte
	}{
		{
			nil,
			nil,
			nil,
		},
		{
			common.HexToHash("deadbeef").Bytes(),
			nil,
			append(append([]byte{genMarkerVersion}, common.HexToHash("deadbeef").Bytes()...)),
		},
		{
			common.HexToHash("deadbeef").Bytes(),
			[][]byte{
				append(common.HexToHash("deadbeef").Bytes(), common.HexToHash("cafebabe").Bytes()...),
			},
			append(append([]byte{genMarkerVersion}, common.HexToHash("deadbeef").Bytes()...), append(common.HexToHash("deadbeef").Bytes(), common.HexToHash("cafebabe").Bytes()...)...),
		},
	}
	// Test encode
	for _, c := range cases {
		marker, err := encodeMarker(c.account, c.storages)
		if err != nil {
			t.Error("Failed to encode generation marker")
		}
		if !bytes.Equal(marker, c.encoded) {
			t.Errorf("Invalid encoded generation marker, want %v, got %v", c.encoded, marker)
		}
	}
	// Test decode
	for _, c := range cases {
		account, storages, err := decodeMarker(c.encoded)
		if err != nil {
			t.Errorf("Failed to decode generation marker %v", err)
		}
		if !bytes.Equal(account, c.account) {
			t.Errorf("Invalid account marker, want %v, got %v", c.account, account)
		}
		if len(c.storages) != len(storages) {
			t.Errorf("Invalid storage markers, want %d, got %d", len(c.storages), len(storages))
		}
		for i := 0; i < len(c.storages); i++ {
			if !bytes.Equal(c.storages[i], storages[i]) {
				t.Errorf("Invalid storage marker, want %v, got %v", c.storages[i], storages[i])
			}
		}
	}
	// Test decode legacy marker
	var legacyCases = []struct {
		account []byte
		storage []byte
		encoded []byte
	}{
		{
			nil, nil, nil,
		},
		{
			common.HexToHash("deadbeef").Bytes(),
			nil,
			common.HexToHash("deadbeef").Bytes(),
		},
		{
			common.HexToHash("deadbeef").Bytes(),
			append(common.HexToHash("deadbeef").Bytes(), common.HexToHash("cafebabe").Bytes()...),
			append(common.HexToHash("deadbeef").Bytes(), common.HexToHash("cafebabe").Bytes()...),
		},
	}
	for _, c := range legacyCases {
		account, storage, err := decodeMarker(c.encoded)
		if err != nil {
			t.Errorf("Failed to decode legacy generation marker %v", err)
		}
		if !bytes.Equal(account, c.account) {
			t.Errorf("Invalid account marker, want %v, got %v", c.account, account)
		}
		if c.storage == nil {
			if len(storage) != 0 {
				t.Error("Invalid storage marker")
			}
		} else {
			if len(storage) != 1 {
				t.Error("Invalid storage marker")
			}
			if !bytes.Equal(storage[0], c.storage) {
				t.Error("Invalid storage marker")
			}
		}
	}
}
