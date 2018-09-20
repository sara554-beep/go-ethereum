// Copyright 2018 The go-ethereum Authors
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

package light

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

var testCheckpoint = &TrustedCheckpoint{
	Name:         "test",
	SectionIndex: 100,
	SectionHead:  common.HexToHash("0xbeef"),
	CHTRoot:      common.HexToHash("0xdead"),
	BloomRoot:    common.HexToHash("0xdeadbeef"),
}

func TestRWCheckpoint(t *testing.T) {
	mdb := ethdb.NewMemDatabase()
	WriteTrustedCheckpoint(mdb, testCheckpoint.SectionIndex, testCheckpoint)
	if !assertCheckpointEqual(testCheckpoint, ReadTrustedCheckpoint(mdb, testCheckpoint.SectionIndex)) {
		t.Error("the checkpoint retrieved from database is different")
	}
	WriteHeadCheckpoint(mdb, testCheckpoint.SectionIndex)
	if ReadHeadCheckpoint(mdb) != testCheckpoint.SectionIndex {
		t.Error("read head checkpoint failed")
	}
}

func TestHashEqual(t *testing.T) {
	if !testCheckpoint.HashEqual(common.HexToHash("0x5e78c01b58a4fba6f6d7966fbdc328bd6c1ee7bb1ea298ecd9ad5a1544469e08")) {
		t.Error("checkpoint should hash equal to given one")
	}
	emptyCheckpoint := &TrustedCheckpoint{}
	if !emptyCheckpoint.HashEqual(common.Hash{}) {
		t.Error("empty checkpoint should equal to empty hash")
	}
}

func assertCheckpointEqual(want, has *TrustedCheckpoint) bool {
	return want.SectionIndex == has.SectionIndex && want.SectionHead == has.SectionHead && want.CHTRoot == has.CHTRoot &&
		want.BloomRoot == has.BloomRoot
}
