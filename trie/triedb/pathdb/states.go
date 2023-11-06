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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// stat stores sizes and count for a parameter.
type stat struct {
	size  uint64
	count int
}

// add size to the stat and increase the counter.
func (s *stat) add(size uint64) {
	s.size += size
	s.count++
}

// destruct represents the record of destruct set modification.
type destruct struct {
	hash  common.Hash
	exist bool
}

// journal contains the list of modifications applied for destruct set.
type journal struct {
	destructs [][]destruct
}

func (j *journal) add(entries []destruct) {
	j.destructs = append(j.destructs, entries)
}

func (j *journal) pop() ([]destruct, error) {
	if len(j.destructs) == 0 {
		return nil, errors.New("destruct journal is not available")
	}
	last := j.destructs[len(j.destructs)-1]
	j.destructs = j.destructs[:len(j.destructs)-1]

	return last, nil
}

func (j *journal) reset() {
	j.destructs = nil
}

// stateSet represents a collection of state modifications belonging to a
// transition, usually refers to a block execution.
type stateSet struct {
	// destructSet is a very special helper marker. If an account is marked as
	// deleted, then it's recorded in this set. However, it's allowed that an
	// account is included here but still available in other sets(e.g. accountData
	// and storageData). The reason is the diff layer includes all the changes
	// in a *block*. It can happen that in the tx_1, account A is self-destructed
	// while in the tx_2 it's recreated. But we still need this marker to indicate
	// the "old" A is deleted, all data in other set belongs to the "new" A.
	destructSet map[common.Hash]struct{}               // Keyed markers for deleted (and potentially) recreated accounts
	accountData map[common.Hash][]byte                 // Keyed accounts for direct retrieval (nil is not expected)
	storageData map[common.Hash]map[common.Hash][]byte // Keyed storage slots for direct retrieval. one per account (nil means deleted)

	accountList []common.Hash                 // List of account for iteration. If it exists, it's sorted, otherwise it's nil
	storageList map[common.Hash][]common.Hash // List of storage slots for iterated retrievals, one per account. Any existing lists are sorted if non-nil

	journal *journal // Track the modifications to destructSet, used for reversal
}

// newStates constructs the state set with the provided data.
func newStates(destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) *stateSet {
	var (
		accountStat stat
		storageStat stat
	)
	// Don't panic for the lazy callers, initialize the nil maps instead.
	if destructs == nil {
		destructs = make(map[common.Hash]struct{})
	}
	if accounts == nil {
		accounts = make(map[common.Hash][]byte)
	}
	if storages == nil {
		storages = make(map[common.Hash]map[common.Hash][]byte)
	}
	// Sanity check that accounts or storage slots are never nil
	for accountHash, blob := range accounts {
		if blob == nil {
			panic(fmt.Sprintf("account %#x nil", accountHash))
		}
		// Determine memory size and track the dirty writes
		accountStat.add(uint64(common.HashLength + len(blob)))
	}
	for accountHash, slots := range storages {
		if slots == nil {
			panic(fmt.Sprintf("storage %#x nil", accountHash))
		}
		// Determine memory size and track the dirty writes
		for _, data := range slots {
			storageStat.add(uint64(common.HashLength + len(data)))
		}
	}
	accountStat.add(uint64(len(destructs) * common.HashLength))

	return &stateSet{
		destructSet: destructs,
		accountData: accounts,
		storageData: storages,
		storageList: make(map[common.Hash][]common.Hash),
		journal:     &journal{},
	}
}

// account returns the account data associated with the specified address hash.
func (s *stateSet) account(hash common.Hash) ([]byte, bool) {
	// If the account is known locally, return it
	if data, ok := s.accountData[hash]; ok {
		return data, true
	}
	// If the account is known locally, but deleted, return it
	if _, ok := s.destructSet[hash]; ok {
		return nil, true
	}
	return nil, false // account is unknown in this set
}

// storage returns the storage slot associated with the specified address hash
// and storage key hash.
func (s *stateSet) storage(accountHash, storageHash common.Hash) ([]byte, bool) {
	// If the account is known locally, try to resolve the slot locally
	if storage, ok := s.storageData[accountHash]; ok {
		if data, ok := storage[storageHash]; ok {
			return data, true
		}
	}
	// If the account is known locally, but deleted, return an empty slot
	if _, ok := s.destructSet[accountHash]; ok {
		return nil, true
	}
	return nil, false // storage is unknown in this set
}

// merge takes the accounts and storages from the given external set and put
// them into the local set. This function combines the data sets, ensuring
// that the merged local set reflects the combined state of both the original
// local set and the provided external set.
func (s *stateSet) merge(set *stateSet) {
	// Overwrite all the account deletion markers and drop the cached data.
	var destructs []destruct
	for accountHash := range set.destructSet {
		delete(s.accountData, accountHash)
		delete(s.storageData, accountHash)

		// Keep track of whether the account has already been marked as
		// destructed. This additional marker is useful for reverting the
		// merge operation.
		_, exist := s.destructSet[accountHash]
		destructs = append(destructs, destruct{
			hash:  accountHash,
			exist: exist,
		})
		s.destructSet[accountHash] = struct{}{}
	}
	s.journal.add(destructs)

	// Overwrite all the updated accounts blindly.
	for accountHash, data := range set.accountData {
		s.accountData[accountHash] = data
	}
	// Overwrite all the updated storage slots (individually)
	for accountHash, storage := range set.storageData {
		// If storage didn't exist (or was deleted) in the set, overwrite blindly
		if _, ok := s.storageData[accountHash]; !ok {
			// To prevent potential concurrent map read/write issues, allocate a
			// new map for the storage instead of claiming it directly from the
			// passed external set. Even after merging, the slots belonging to the
			// external state set remain accessible, so ownership of the map should
			// not be taken, and any mutation on it should be avoided.
			slots := make(map[common.Hash][]byte)
			for storageHash, data := range storage {
				slots[storageHash] = data
			}
			s.storageData[accountHash] = slots
			continue
		}
		// Storage exists in both local and external set, merge the slots
		slots := s.storageData[accountHash]
		for storageHash, data := range storage {
			slots[storageHash] = data
		}
	}
}

// revert takes the original value of accounts and storages as input and reverts
// the modifications applied on the state set.
func (s *stateSet) revert(accountOrigin map[common.Hash][]byte, storageOrigin map[common.Hash]map[common.Hash][]byte) error {
	// Load the destruct journal whose availability is always expected.
	destructs, err := s.journal.pop()
	if err != nil {
		return err
	}
	// Revert the modifications to the destructSet by journal.
	for _, entry := range destructs {
		if entry.exist {
			continue
		}
		delete(s.destructSet, entry.hash)
	}
	// Overwrite the account data with original value blindly.
	for addrHash, blob := range accountOrigin {
		if len(blob) == 0 {
			delete(s.accountData, addrHash)
		} else {
			s.accountData[addrHash] = blob
		}
	}
	// Overwrite the storage data with original value blindly.
	for addrHash, storage := range storageOrigin {
		// It might be possible that the storage set is not existent because
		// the whole storage is marked as deleted.
		slots := s.storageData[addrHash]
		if len(slots) == 0 {
			slots = make(map[common.Hash][]byte)
		}
		for storageHash, blob := range storage {
			if len(blob) == 0 {
				delete(slots, storageHash)
			} else {
				slots[storageHash] = blob
			}
		}
		s.storageData[addrHash] = slots
	}
	return nil
}

// write flushes the accumulated state mutations into provided database batch.
func (s *stateSet) write(batch ethdb.Batch) {
	for addrHash := range s.destructSet {
		rawdb.DeleteAccountSnapshot(batch, addrHash)
	}
	for addrHash, blob := range s.accountData {
		if len(blob) == 0 {
			panic("invalid account data")
		}
		rawdb.WriteAccountSnapshot(batch, addrHash, blob)
	}
	for addrHash, storages := range s.storageData {
		for storageHash, blob := range storages {
			if len(blob) == 0 {
				rawdb.DeleteStorageSnapshot(batch, addrHash, storageHash)
			} else {
				rawdb.WriteStorageSnapshot(batch, addrHash, storageHash, blob)
			}
		}
	}
}

// reset clears all cached state data, including any optional sorted lists that
// may have been generated.
func (s *stateSet) reset() {
	s.destructSet = make(map[common.Hash]struct{})
	s.accountData = make(map[common.Hash][]byte)
	s.storageData = make(map[common.Hash]map[common.Hash][]byte)

	s.accountList = nil
	s.storageList = make(map[common.Hash][]common.Hash)
	s.journal.reset()
}

// stateSetWithOrigin wraps the state set with additional original values of the
// mutated states.
type stateSetWithOrigin struct {
	*stateSet

	// The accountOrigin represents the account data before the state transition,
	// corresponding to both the accountData and destructSet. It's keyed by the
	// account address. The nil value means the account was not present before.
	accountOrigin map[common.Address][]byte

	// The storageOrigin represents the storage data before the state transition,
	// corresponding to storageData. It's keyed by the account address and slot
	// key hash. The nil value means the slot was not present.
	storageOrigin map[common.Address]map[common.Hash][]byte

	// The storageIncomplete set indicates whether the original storage data is
	// incomplete due to a large deletion. Geth is not capable of handling large
	// storage deletions to prevent out-of-memory panics. Consequently, such
	// large deletions are skipped and marked here. This set can be removed once
	// self-destruction is disabled in the following hard fork.
	storageIncomplete map[common.Address]struct{}
}

// newStateSetWithOrigin constructs the state set with the provided data.
func newStateSetWithOrigin(states *triestate.StateUpdate) *stateSetWithOrigin {
	return &stateSetWithOrigin{
		stateSet:          newStates(states.DestructSet, states.AccountData, states.StorageData),
		accountOrigin:     states.AccountOrigin,
		storageOrigin:     states.StorageOrigin,
		storageIncomplete: states.StorageIncomplete,
	}
}
