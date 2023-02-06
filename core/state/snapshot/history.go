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

package snapshot

import (
	"encoding/binary"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// State history records the state changes involved in executing a corresponding
// block. State history is the guarantee that historic state can be accessed.
//
// Each state transition will generate a corresponding state history (note that
// not every block has a state change, e.g. in the clique network or post-merge
// where no block reward rules existent). Each state history will have a
// monotonically increasing number act as its unique identifier.
//
// The state history will be written to disk (ancient store) when the corresponding
// diff layer is merged into the disk layer. At the same time, system can prune
// the oldest histories according to config.
//
//                                                        Disk State
//                                                            ^
//                                                            |
//   +------------+     +---------+     +---------+     +---------+
//   | Init State |---->| State 1 |---->|   ...   |---->| State n |
//   +------------+     +---------+     +---------+     +---------+
//
//                     +-----------+      +------+     +-----------+
//                     | History 1 |----> | ...  |---->| History n |
//                     +-----------+      +------+     +-----------+

const (
	storageIndexSize = common.HashLength + 8
	accountIndexSize = common.HashLength + 16
)

// storageIndex describes the metadata belongs to a storage slot.
type storageIndex struct {
	Hash   common.Hash // The hash of storage slot
	Offset uint32      // The offset of item in storage slot data table
	Length uint32      // The length of storage slot data
}

func (index storageIndex) encode() []byte {
	var buffer [storageIndexSize]byte
	copy(buffer[:], index.Hash.Bytes())
	binary.BigEndian.PutUint32(buffer[common.HashLength:], index.Offset)
	binary.BigEndian.PutUint32(buffer[common.HashLength+4:], index.Length)
	return buffer[:]
}

// accountIndex describes the metadata belongs to an account history.
type accountIndex struct {
	Hash       common.Hash // The hash of account state
	Offset     uint32      // The offset of item in account data table
	Length     uint32      // The length of account data
	SlotOffset uint32      // The offset of storage index in storage index table
	SlotCount  uint32      // The counter of storage slots
}

func (index accountIndex) encode() []byte {
	var buffer [accountIndexSize]byte
	copy(buffer[:], index.Hash.Bytes())
	binary.BigEndian.PutUint32(buffer[common.HashLength:], index.Offset)
	binary.BigEndian.PutUint32(buffer[common.HashLength+4:], index.Length)
	binary.BigEndian.PutUint32(buffer[common.HashLength+8:], index.SlotOffset)
	binary.BigEndian.PutUint32(buffer[common.HashLength+12:], index.SlotCount)
	return buffer[:]
}

// stateHistory represents a set of state changes belong to the same block.
// All the state history in disk are linked with each other by a unique id
// (8byte integer), the tail(oldest) state history will be pruned in order
// to control the storage size.
type stateHistory struct {
	accounts    map[common.Hash][]byte
	accountList []common.Hash
	storages    map[common.Hash]map[common.Hash][]byte
	storageList map[common.Hash][]common.Hash
}

// newStateHistory constructs the state history with provided bottom-most
// diff layer. Error can occur if the snapshot is not fully generated, it's
// not acceptable which can lead to gap in freezer. Hack it by waiting
// snapshot generation at startup stage.
func newStateHistory(dl *diffLayer) (*stateHistory, error) {
	var (
		accounts    = make(map[common.Hash][]byte)
		storages    = make(map[common.Hash]map[common.Hash][]byte)
		accountList []common.Hash
		storageList = make(map[common.Hash][]common.Hash)
		parent      = dl.Parent()
	)
	for hash := range dl.destructSet {
		prev, err := parent.AccountRLP(hash)
		if err != nil {
			return nil, err // e.g. snapshot is not fully generated
		}
		accounts[hash] = prev
		accountList = append(accountList, hash)
	}
	for hash := range dl.accountData {
		if _, exist := accounts[hash]; exist {
			continue
		}
		prev, err := parent.AccountRLP(hash)
		if err != nil {
			return nil, err // e.g. snapshot is not fully generated
		}
		accounts[hash] = prev
		accountList = append(accountList, hash)
	}
	sort.Sort(hashes(accountList))

	for accountHash, slots := range dl.storageData {
		if storages[accountHash] == nil {
			storages[accountHash] = make(map[common.Hash][]byte)
		}
		for storageHash := range slots {
			prev, err := parent.Storage(accountHash, storageHash)
			if err != nil {
				return nil, err
			}
			storages[accountHash][storageHash] = prev
			storageList[accountHash] = append(storageList[accountHash], storageHash)
		}
		sort.Sort(hashes(storageList[accountHash]))
	}
	return &stateHistory{
		accounts:    accounts,
		accountList: accountList,
		storages:    storages,
		storageList: storageList,
	}, nil
}

func (history *stateHistory) write(db ethdb.Database, id uint64, freezer *rawdb.ResettableFreezer) error {
	var (
		slots          uint32
		accountIndexes []byte
		storageIndexes []byte
		accountData    []byte
		storageData    []byte
	)
	for _, accountHash := range history.accountList {
		account := history.accounts[accountHash]
		index := accountIndex{
			Hash:   accountHash,
			Offset: uint32(len(accountData)),
			Length: uint32(len(account)),
		}
		storages, exist := history.storages[accountHash]
		if exist {
			for _, storageHash := range history.storageList[accountHash] {
				slot := history.storages[accountHash][storageHash]
				storageIndexes = append(storageIndexes, storageIndex{
					Hash:   storageHash,
					Offset: uint32(len(storageData)),
					Length: uint32(len(slot)),
				}.encode()...)
				storageData = append(storageData, slot...)
			}
			index.SlotOffset = slots
			index.SlotCount = uint32(len(storages))
			slots += uint32(len(storages))
		}
		accountIndexes = append(accountIndexes, index.encode()...)
		accountData = append(accountData, account...)
	}
	rawdb.WriteStateHistory(freezer, id, accountIndexes, storageIndexes, accountData, storageData)

	// TODO design two-levels structure
	var (
		scratch [8]byte
		batch   = db.NewBatch()
	)
	binary.BigEndian.PutUint64(scratch[:], id)

	for _, account := range history.accountList {
		blob := rawdb.ReadAccountIndex(db, account)
		blob = append(blob, scratch[:]...)
		rawdb.WriteAccountIndex(batch, account, blob)

		_, exist := history.storages[account]
		if exist {
			for _, slot := range history.storageList[account] {
				blob := rawdb.ReadStorageIndex(db, account, slot)
				blob = append(blob, scratch[:]...)
				rawdb.WriteStorageIndex(batch, account, slot, blob)
			}
		}
	}
	return batch.Write()
}

// storeStateHistory constructs the state history for the passed bottom-most
// diff layer. After storing the corresponding state history, it will also
// prune the stale histories from the disk with the given threshold.
// This function will panic if it's called for non-bottom-most diff layer.
func storeStateHistory(db ethdb.Database, freezer *rawdb.ResettableFreezer, dl *diffLayer) error {
	if freezer == nil {
		return nil
	}
	history, err := newStateHistory(dl)
	if err != nil {
		return err
	}
	return history.write(db, dl.id, freezer)
}

// truncateFromHead removes the extra trie histories from the head with
// the given parameters. If the passed database is a non-freezer database,
// nothing to do here.
func truncateFromHead(freezer *rawdb.ResettableFreezer, nhead uint64) (int, error) {
	ohead, err := freezer.Ancients()
	if err != nil {
		return 0, err
	}
	if err := freezer.TruncateHead(nhead); err != nil {
		return 0, err
	}
	return int(ohead - nhead), nil
}

// truncateFromTail removes the extra trie histories from the tail with
// the given parameters. If the passed database is a non-freezer database,
// nothing to do here.
func truncateFromTail(freezer *rawdb.ResettableFreezer, ntail uint64) (int, error) {
	otail, err := freezer.Tail()
	if err != nil {
		return 0, err
	}
	if err := freezer.TruncateTail(ntail); err != nil {
		return 0, err
	}
	return int(ntail - otail), nil
}
