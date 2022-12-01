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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package trie

import (
	"bytes"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	accountIndexLength = 48
	storageIndexLength = 40
)

// stateHistory represents a state change(account, storage slot). The prev
// refers to the content before the change is applied.
type stateHistory struct {
	Hash common.Hash // State element hash(e.g. account hash or slot location)
	Prev []byte      // RLP-encoded value, nil means the state is previously non-existent
}

// accountMetadata describes the metadata belongs to an account history.
type accountMetadata struct {
	Hash       common.Hash
	Offset     uint32
	Length     uint32
	SlotOffset uint32
	SlotNumber uint32
}

type accountIndexes []*accountMetadata

func (is accountIndexes) encode() []byte {
	var (
		tmp [16]byte
		buf = new(bytes.Buffer)
	)
	for _, index := range is {
		buf.Write(index.Hash.Bytes())
		binary.BigEndian.PutUint32(tmp[:4], index.Offset)
		binary.BigEndian.PutUint32(tmp[4:8], index.Length)
		binary.BigEndian.PutUint32(tmp[8:12], index.SlotOffset)
		binary.BigEndian.PutUint32(tmp[12:], index.SlotNumber)
		buf.Write(tmp[:])
	}
	return buf.Bytes()
}

type storageMetadata struct {
	Hash   common.Hash
	Offset uint32
	Length uint32
}
type slotIndexes []*storageMetadata

func (is slotIndexes) encode() []byte {
	var (
		tmp [8]byte
		buf = new(bytes.Buffer)
	)
	for _, index := range is {
		buf.Write(index.Hash.Bytes())
		binary.BigEndian.PutUint32(tmp[:4], index.Offset)
		binary.BigEndian.PutUint32(tmp[4:], index.Length)
		buf.Write(tmp[:])
	}
	return buf.Bytes()
}

func newStorageMetadata(offset uint32, storage []stateHistory) ([]*storageMetadata, []byte, uint32) {
	var (
		data  []byte
		metas []*storageMetadata
	)
	for _, slot := range storage {
		meta := &storageMetadata{
			Hash:   slot.Hash,
			Offset: offset,
			Length: uint32(len(slot.Prev)),
		}
		metas = append(metas, meta)
		data = append(data, slot.Prev...)
		offset += uint32(len(slot.Prev))
	}
	return metas, data, offset
}

func newAccountIndex(element stateHistory, accountOffset uint32, slotIndexes []*storageMetadata, slotOffset uint32) *accountMetadata {
	return &accountMetadata{
		Hash:       element.Hash,
		Offset:     accountOffset,
		Length:     uint32(len(element.Prev)),
		SlotOffset: slotOffset,
		SlotNumber: uint32(len(slotIndexes)),
	}
}

func newStateHistory(nodes map[common.Hash]map[string]*nodeWithPrev) ([]*accountMetadata, []common.Hash, []*storageMetadata, []byte, []byte) {
	var (
		leaves = make(map[common.Hash][]stateHistory)

		accountData    []byte
		slotData       []byte
		accountOffset  uint32
		accountSlotOff uint32
		slotOffset     uint32

		accountMetas []*accountMetadata
		storageMetas []*storageMetadata

		accounts []common.Hash
	)
	for owner, subset := range nodes {
		var elements []stateHistory
		resolvePrevLeaves(subset, func(path []byte, blob []byte) {
			elements = append(elements, stateHistory{
				Hash: common.BytesToHash(path),
				Prev: common.CopyBytes(blob),
			})
		})
		leaves[owner] = elements
	}
	for _, element := range leaves[common.Hash{}] {
		accountHash := element.Hash
		slots := leaves[accountHash]
		sMeta, sdata, soffset := newStorageMetadata(slotOffset, slots)

		storageMetas = append(storageMetas, sMeta...)
		slotData = append(slotData, sdata...)
		slotOffset = soffset

		aMeta := newAccountIndex(element, accountOffset, sMeta, accountSlotOff)

		accountMetas = append(accountMetas, aMeta)
		accountOffset += uint32(len(element.Prev))
		accountSlotOff += uint32(len(sMeta))
		accountData = append(accountData, element.Prev...)

		accounts = append(accounts, accountHash)
	}
	return accountMetas, accounts, storageMetas, accountData, slotData
}

// storeReverseDiff constructs the reverse state diff for the passed bottom-most
// diff layer. After storing the corresponding reverse diff, it will also prune
// the stale reverse diffs from the disk with the given threshold.
// This function will panic if it's called for non-bottom-most diff layer.
func storeStateHistory(disk ethdb.Database, freezer *rawdb.ResettableFreezer, dl *diffLayer) error {
	aMeta, accounts, sMeta, aData, sData := newStateHistory(dl.nodes)
	aIndexEnc := accountIndexes(aMeta).encode()
	sIndexEnc := slotIndexes(sMeta).encode()

	var (
		batch = disk.NewBatch()
		tmp   [8]byte
	)
	binary.BigEndian.PutUint64(tmp[:], dl.id)
	for _, account := range aMeta {
		blob := rawdb.ReadAccountIndex(disk, account.Hash)
		blob = append(blob, tmp[:]...)
		rawdb.WriteAccountIndex(batch, account.Hash, blob)
	}
	for i, slot := range sMeta {
		blob := rawdb.ReadStorageIndex(disk, accounts[i], slot.Hash)
		blob = append(blob, tmp[:]...)
		rawdb.WriteStorageIndex(batch, accounts[i], slot.Hash, blob)
	}
	if err := batch.Write(); err != nil {
		return err
	}
	// The reverse diff object and the lookup are stored in two different
	// places, so there is no atomicity guarantee. It's possible that reverse
	// diff object is written but lookup is not, vice versa. So double-check
	// the presence when using the reverse diff.
	rawdb.WriteStateHistory(freezer, dl.id, aIndexEnc, sIndexEnc, aData, sData)
	return nil
}
