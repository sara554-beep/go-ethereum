// Copyright 2024 The go-ethereum Authors
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

package pathdb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/state"
)

func TestStatesMerge(t *testing.T) {
	a := newStates(nil,
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa0},
			common.Hash{0xb}: {0xb0},
			common.Hash{0xc}: {0xc0},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x10},
				common.Hash{0x2}: {0x20},
			},
			common.Hash{0xb}: {
				common.Hash{0x1}: {0x10},
			},
			common.Hash{0xc}: {
				common.Hash{0x1}: {0x10},
			},
		},
	)
	b := newStates(
		map[common.Hash]struct{}{
			common.Hash{0xa}: {},
			common.Hash{0xc}: {},
		},
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa1},
			common.Hash{0xb}: {0xb1},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x11},
				common.Hash{0x3}: {0x31},
			},
			common.Hash{0xb}: {
				common.Hash{0x1}: {0x11},
			},
		},
	)
	a.merge(b)

	blob, exist := a.account(common.Hash{0xa})
	if !exist || !bytes.Equal(blob, []byte{0xa1}) {
		t.Error("Unexpected value for account a")
	}
	blob, exist = a.account(common.Hash{0xb})
	if !exist || !bytes.Equal(blob, []byte{0xb1}) {
		t.Error("Unexpected value for account b")
	}
	blob, exist = a.account(common.Hash{0xc})
	if !exist || len(blob) != 0 {
		t.Error("Unexpected value for account c")
	}
	blob, exist = a.account(common.Hash{0xd})
	if exist || len(blob) != 0 {
		t.Error("Unexpected value for account d")
	}

	blob, exist = a.storage(common.Hash{0xa}, common.Hash{0x1})
	if !exist || !bytes.Equal(blob, []byte{0x11}) {
		t.Error("Unexpected value for a's storage")
	}
	blob, exist = a.storage(common.Hash{0xa}, common.Hash{0x2})
	if !exist || len(blob) != 0 {
		t.Error("Unexpected value for a's storage")
	}
	blob, exist = a.storage(common.Hash{0xa}, common.Hash{0x3})
	if !exist || !bytes.Equal(blob, []byte{0x31}) {
		t.Error("Unexpected value for a's storage")
	}
	blob, exist = a.storage(common.Hash{0xb}, common.Hash{0x1})
	if !exist || !bytes.Equal(blob, []byte{0x11}) {
		t.Error("Unexpected value for b's storage")
	}
	blob, exist = a.storage(common.Hash{0xc}, common.Hash{0x1})
	if !exist || len(blob) != 0 {
		t.Error("Unexpected value for c's storage")
	}
}

func TestStateRevert(t *testing.T) {
	a := newStates(nil,
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa0},
			common.Hash{0xb}: {0xb0},
			common.Hash{0xc}: {0xc0},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x10},
				common.Hash{0x2}: {0x20},
			},
			common.Hash{0xb}: {
				common.Hash{0x1}: {0x10},
			},
			common.Hash{0xc}: {
				common.Hash{0x1}: {0x10},
			},
		},
	)
	b := newStates(
		map[common.Hash]struct{}{
			common.Hash{0xa}: {},
			common.Hash{0xc}: {},
		},
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa1},
			common.Hash{0xb}: {0xb1},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x11},
				common.Hash{0x3}: {0x31},
			},
			common.Hash{0xb}: {
				common.Hash{0x1}: {0x11},
			},
		},
	)
	a.merge(b)
	a.revert(
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa0},
			common.Hash{0xb}: {0xb0},
			common.Hash{0xc}: {0xc0},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x10},
				common.Hash{0x2}: {0x20},
			},
			common.Hash{0xb}: {
				common.Hash{0x1}: {0x10},
			},
			common.Hash{0xc}: {
				common.Hash{0x1}: {0x10},
			},
		},
	)

	blob, exist := a.account(common.Hash{0xa})
	if !exist || !bytes.Equal(blob, []byte{0xa0}) {
		t.Error("Unexpected value for account a")
	}
	blob, exist = a.account(common.Hash{0xb})
	if !exist || !bytes.Equal(blob, []byte{0xb0}) {
		t.Error("Unexpected value for account b")
	}
	blob, exist = a.account(common.Hash{0xc})
	if !exist || !bytes.Equal(blob, []byte{0xc0}) {
		t.Error("Unexpected value for account c")
	}

	blob, exist = a.storage(common.Hash{0xa}, common.Hash{0x1})
	if !exist || !bytes.Equal(blob, []byte{0x10}) {
		t.Error("Unexpected value for a's storage")
	}
	blob, exist = a.storage(common.Hash{0xa}, common.Hash{0x2})
	if !exist || !bytes.Equal(blob, []byte{0x20}) {
		t.Error("Unexpected value for a's storage")
	}
	blob, exist = a.storage(common.Hash{0xb}, common.Hash{0x1})
	if !exist || !bytes.Equal(blob, []byte{0x10}) {
		t.Error("Unexpected value for b's storage")
	}
	blob, exist = a.storage(common.Hash{0xc}, common.Hash{0x1})
	if !exist || !bytes.Equal(blob, []byte{0x10}) {
		t.Error("Unexpected value for c's storage")
	}
}

func compareStates(a, b *stateSet) bool {
	if !reflect.DeepEqual(a.destructSet, b.destructSet) {
		return false
	}
	if !reflect.DeepEqual(a.accountData, b.accountData) {
		return false
	}
	if !reflect.DeepEqual(a.storageData, b.storageData) {
		return false
	}
	if len(a.journal.destructs) != len(b.journal.destructs) {
		return false
	}
	for i := 0; i < len(a.journal.destructs); i++ {
		if !reflect.DeepEqual(a.journal.destructs[i], b.journal.destructs[i]) {
			return false
		}
	}
	return true
}

func compareStatesWithOrigin(a, b *stateSetWithOrigin) bool {
	if !compareStates(a.stateSet, b.stateSet) {
		return false
	}
	if !reflect.DeepEqual(a.accountOrigin, b.accountOrigin) {
		return false
	}
	if !reflect.DeepEqual(a.storageOrigin, b.storageOrigin) {
		return false
	}
	return true
}

func TestStateEncode(t *testing.T) {
	s := newStates(
		map[common.Hash]struct{}{
			common.Hash{0x1}: {},
		},
		map[common.Hash][]byte{
			common.Hash{0x1}: {0x1},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0x1}: {
				common.Hash{0x1}: {0x1},
			},
		},
	)
	s.journal.add([]destruct{
		{common.Hash{0x1}, true},
		{common.Hash{0x2}, false},
	})
	s.journal.add([]destruct{
		{common.Hash{0x3}, true},
		{common.Hash{0x4}, false},
	})
	buf := bytes.NewBuffer(nil)
	if err := s.encode(buf); err != nil {
		t.Fatalf("Failed to encode states, %v", err)
	}
	var dec stateSet
	if err := dec.decode(rlp.NewStream(buf, 0)); err != nil {
		t.Fatalf("Failed to decode states, %v", err)
	}
	if !compareStates(s, &dec) {
		t.Fatalf("Unexpected content")
	}
}

func TestStateWithOriginEncode(t *testing.T) {
	s := newStateSetWithOrigin(&state.Update{
		DestructSet: map[common.Hash]struct{}{
			common.Hash{0x1}: {},
		},
		AccountData: map[common.Hash][]byte{
			common.Hash{0x1}: {0x1},
		},
		StorageData: map[common.Hash]map[common.Hash][]byte{
			common.Hash{0x1}: {
				common.Hash{0x1}: {0x1},
			},
		},
		AccountOrigin: map[common.Address][]byte{
			common.Address{0x1}: {0x1},
		},
		StorageOrigin: map[common.Address]map[common.Hash][]byte{
			common.Address{0x1}: {
				common.Hash{0x1}: {0x1},
			},
		},
	})
	buf := bytes.NewBuffer(nil)
	if err := s.encode(buf); err != nil {
		t.Fatalf("Failed to encode states, %v", err)
	}
	var dec stateSetWithOrigin
	if err := dec.decode(rlp.NewStream(buf, 0)); err != nil {
		t.Fatalf("Failed to decode states, %v", err)
	}
	if !compareStatesWithOrigin(s, &dec) {
		t.Fatal("Unexpected content")
	}
}
