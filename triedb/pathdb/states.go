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
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/ethstate"
)

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
	journal     *journal                               // Track the modifications to destructSet, used for reversal
}

// newStates constructs the state set with the provided data.
func newStates(destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) *stateSet {
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
	}
	for accountHash, slots := range storages {
		if slots == nil {
			panic(fmt.Sprintf("storage %#x nil", accountHash))
		}
	}
	return &stateSet{
		destructSet: destructs,
		accountData: accounts,
		storageData: storages,
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

// encode serializes the content of state set into the provided writer.
func (s *stateSet) encode(w io.Writer) error {
	// Encode destructs
	destructs := make([]common.Hash, 0, len(s.destructSet))
	for hash := range s.destructSet {
		destructs = append(destructs, hash)
	}
	if err := rlp.Encode(w, destructs); err != nil {
		return err
	}

	// Encode accounts
	type Account struct {
		Hash common.Hash
		Blob []byte
	}
	accounts := make([]Account, 0, len(s.accountData))
	for hash, blob := range s.accountData {
		accounts = append(accounts, Account{Hash: hash, Blob: blob})
	}
	if err := rlp.Encode(w, accounts); err != nil {
		return err
	}

	// Encode storages
	type Storage struct {
		Hash  common.Hash
		Keys  []common.Hash
		Blobs [][]byte
	}
	storages := make([]Storage, 0, len(s.storageData))
	for hash, slots := range s.storageData {
		keys := make([]common.Hash, 0, len(slots))
		vals := make([][]byte, 0, len(slots))
		for key, val := range slots {
			keys = append(keys, key)
			vals = append(vals, val)
		}
		storages = append(storages, Storage{Hash: hash, Keys: keys, Blobs: vals})
	}
	return rlp.Encode(w, storages)
}

func (s *stateSet) decode(r *rlp.Stream) error {
	// Decode destructs
	var (
		destructs   []common.Hash
		destructSet = make(map[common.Hash]struct{})
	)
	if err := r.Decode(&destructs); err != nil {
		return fmt.Errorf("load diff destructs: %v", err)
	}
	for _, hash := range destructs {
		destructSet[hash] = struct{}{}
	}
	s.destructSet = destructSet

	// Decode accounts
	type Account struct {
		Hash common.Hash
		Blob []byte
	}
	var (
		accounts   []Account
		accountSet = make(map[common.Hash][]byte)
	)
	if err := r.Decode(&accounts); err != nil {
		return fmt.Errorf("load diff accounts: %v", err)
	}
	for _, account := range accounts {
		accountSet[account.Hash] = account.Blob
	}
	s.accountData = accountSet

	// Decode storages
	type Storage struct {
		Hash  common.Hash
		Keys  []common.Hash
		Blobs [][]byte
	}
	var (
		storages   []Storage
		storageSet = make(map[common.Hash]map[common.Hash][]byte)
	)
	if err := r.Decode(&storages); err != nil {
		return fmt.Errorf("load diff storage: %v", err)
	}
	for _, storage := range storages {
		storageSet[storage.Hash] = make(map[common.Hash][]byte)
		for i := 0; i < len(storage.Keys); i++ {
			storageSet[storage.Hash][storage.Keys[i]] = storage.Blobs[i]
		}
	}
	s.storageData = storageSet
	s.journal = &journal{}
	return nil
}

// write flushes the accumulated state mutations into provided database batch.
func (s *stateSet) write(db ethdb.KeyValueStore, batch ethdb.Batch) {
	for addrHash := range s.destructSet {
		rawdb.DeleteAccountSnapshot(batch, addrHash)

		it := rawdb.IterateStorageSnapshots(db, addrHash)
		for it.Next() {
			batch.Delete(it.Key())
		}
		it.Release()
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
	s.journal.reset()
}

// stateSetWithOrigin wraps the state set with additional original values of the
// mutated states.
type stateSetWithOrigin struct {
	*stateSet

	// AccountOrigin represents the account data before the state transition,
	// corresponding to both the accountData and destructSet. It's keyed by the
	// account address. The nil value means the account was not present before.
	accountOrigin map[common.Address][]byte

	// StorageOrigin represents the storage data before the state transition,
	// corresponding to storageData. It's keyed by the account address and slot
	// key hash. The nil value means the slot was not present.
	storageOrigin map[common.Address]map[common.Hash][]byte
}

// newStateSetWithOrigin constructs the state set with the provided data.
func newStateSetWithOrigin(states *ethstate.Update) *stateSetWithOrigin {
	return &stateSetWithOrigin{
		stateSet:      newStates(states.DestructSet, states.AccountData, states.StorageData),
		accountOrigin: states.AccountOrigin,
		storageOrigin: states.StorageOrigin,
	}
}

// encode serializes the content of state set into the provided writer.
func (s *stateSetWithOrigin) encode(w io.Writer) error {
	// Encode state set
	if err := s.stateSet.encode(w); err != nil {
		return err
	}

	// Encode accounts
	type Account struct {
		Address common.Address
		Blob    []byte
	}
	accounts := make([]Account, 0, len(s.accountOrigin))
	for address, blob := range s.accountOrigin {
		accounts = append(accounts, Account{Address: address, Blob: blob})
	}
	if err := rlp.Encode(w, accounts); err != nil {
		return err
	}

	// Encode storages
	type Storage struct {
		Address common.Address
		Keys    []common.Hash
		Blobs   [][]byte
	}
	storages := make([]Storage, 0, len(s.storageOrigin))
	for address, slots := range s.storageOrigin {
		keys := make([]common.Hash, 0, len(slots))
		vals := make([][]byte, 0, len(slots))
		for key, val := range slots {
			keys = append(keys, key)
			vals = append(vals, val)
		}
		storages = append(storages, Storage{Address: address, Keys: keys, Blobs: vals})
	}
	return rlp.Encode(w, storages)
}

func (s *stateSetWithOrigin) decode(r *rlp.Stream) error {
	if err := s.stateSet.decode(r); err != nil {
		return err
	}
	// Decode account origin
	type Account struct {
		Address common.Address
		Blob    []byte
	}
	var (
		accounts   []Account
		accountSet = make(map[common.Address][]byte)
	)
	if err := r.Decode(&accounts); err != nil {
		return fmt.Errorf("load diff account origin set: %v", err)
	}
	for _, account := range accounts {
		accountSet[account.Address] = account.Blob
	}
	s.accountOrigin = accountSet

	// Decode storage origin
	type Storage struct {
		Address common.Address
		Keys    []common.Hash
		Blobs   [][]byte
	}
	var (
		storages   []Storage
		storageSet = make(map[common.Address]map[common.Hash][]byte)
	)
	if err := r.Decode(&storages); err != nil {
		return fmt.Errorf("load diff storage origin: %v", err)
	}
	for _, storage := range storages {
		storageSet[storage.Address] = make(map[common.Hash][]byte)
		for i := 0; i < len(storage.Keys); i++ {
			storageSet[storage.Address][storage.Keys[i]] = storage.Blobs[i]
		}
	}
	s.storageOrigin = storageSet

	return nil
}
