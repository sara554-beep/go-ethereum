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
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/exp/maps"
)

// counter helps in tracking items and their corresponding sizes.
type counter struct {
	n    int
	size int
}

// add size to the counter and increase the item counter.
func (c *counter) add(size int) {
	c.n++
	c.size += size
}

// report uploads the cached statistics to meters.
func (c *counter) report(count metrics.Meter, size metrics.Meter) {
	count.Mark(int64(c.n))
	size.Mark(int64(c.size))
}

// destruct represents the record of destruct set modification.
type destruct struct {
	Hash  common.Hash
	Exist bool
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

func (j *journal) encode(w io.Writer) error {
	return rlp.Encode(w, j.destructs)
}

func (j *journal) decode(r *rlp.Stream) error {
	var dec [][]destruct
	if err := r.Decode(&dec); err != nil {
		return err
	}
	j.destructs = dec
	return nil
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
	size        int                                    // Memory size of the state data (destructSet, accountData and storageData)

	journal     *journal                      // Track the modifications to destructSet, used for reversal
	accountList []common.Hash                 // List of account for iteration. If it exists, it's sorted, otherwise it's nil
	storageList map[common.Hash][]common.Hash // List of storage slots for iterated retrievals, one per account. Any existing lists are sorted if non-nil
	lock        sync.RWMutex                  // Lock for guarding the two lists above
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
	s := &stateSet{
		destructSet: destructs,
		accountData: accounts,
		storageData: storages,
		journal:     &journal{},
		storageList: make(map[common.Hash][]common.Hash),
	}
	s.size = s.check()
	return s
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

// check sanitizes accounts and storage slots to ensure the data validity.
// Additionally, it computes the total memory size occupied by the maps.
func (s *stateSet) check() int {
	size := len(s.destructSet) * common.HashLength
	for accountHash, blob := range s.accountData {
		if blob == nil {
			panic(fmt.Sprintf("account %#x nil", accountHash)) // nil account blob is not permitted
		}
		size += common.HashLength + len(blob)
	}
	for accountHash, slots := range s.storageData {
		if slots == nil {
			panic(fmt.Sprintf("storage %#x nil", accountHash)) // nil slots is not permitted
		}
		for _, val := range slots {
			size += 2*common.HashLength + len(val)
		}
	}
	return size
}

// AccountList returns a sorted list of all accounts in this state set, including
// the deleted ones.
//
// Note, the returned slice is not a copy, so do not modify it.
func (s *stateSet) AccountList() []common.Hash {
	// If an old list already exists, return it
	s.lock.RLock()
	list := s.accountList
	s.lock.RUnlock()

	if list != nil {
		return list
	}
	// No old sorted account list exists, generate a new one
	s.lock.Lock()
	defer s.lock.Unlock()

	s.accountList = make([]common.Hash, 0, len(s.destructSet)+len(s.accountData))
	for hash := range s.accountData {
		s.accountList = append(s.accountList, hash)
	}
	for hash := range s.destructSet {
		if _, ok := s.accountData[hash]; !ok {
			s.accountList = append(s.accountList, hash)
		}
	}
	slices.SortFunc(s.accountList, common.Hash.Cmp)
	return s.accountList
}

// StorageList returns a sorted list of all storage slot hashes in this state set
// for the given account. If the whole storage is destructed in this layer, then
// an additional flag *destructed = true* will be returned, otherwise the flag is
// false. Besides, the returned list will include the hash of deleted storage slot.
// Note a special case is an account is deleted in a prior tx but is recreated in
// the following tx with some storage slots set. In this case the returned list is
// not empty but the flag is true.
//
// Note, the returned slice is not a copy, so do not modify it.
func (s *stateSet) StorageList(accountHash common.Hash) ([]common.Hash, bool) {
	s.lock.RLock()
	_, destructed := s.destructSet[accountHash]
	if _, ok := s.storageData[accountHash]; !ok {
		// Account not tracked by this layer
		s.lock.RUnlock()
		return nil, destructed
	}
	// If an old list already exists, return it
	if list, exist := s.storageList[accountHash]; exist {
		s.lock.RUnlock()
		return list, destructed // the cached list can't be nil
	}
	s.lock.RUnlock()

	// No old sorted account list exists, generate a new one
	s.lock.Lock()
	defer s.lock.Unlock()

	storageList := maps.Keys(s.storageData[accountHash])
	slices.SortFunc(storageList, common.Hash.Cmp)
	s.storageList[accountHash] = storageList
	return storageList, destructed
}

// clearCache invalidates the cached account list and storage lists.
func (s *stateSet) clearCache() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.accountList = nil
	maps.Clear(s.storageList)
}

// merge takes the accounts and storages from the given external set and
// integrates them into the local set. This function combines the data sets,
// ensuring that the merged local set accurately reflects the combined state
// of both the original local set and the provided external set.
func (s *stateSet) merge(set *stateSet) int64 {
	var (
		updates           int
		deletes           int
		accountOverwrites counter
		storageOverwrites counter
		destructs         []destruct
	)
	// Apply account deletion markers and discard any previously cached data if exists
	for accountHash := range set.destructSet {
		if origin, ok := s.accountData[accountHash]; ok {
			deletes += common.HashLength + len(origin)
			accountOverwrites.add(common.HashLength + len(origin))
			delete(s.accountData, accountHash)
		}
		if _, ok := s.storageData[accountHash]; ok {
			// Looping through the nested map may cause slight performance degradation.
			// However, since account destruction is no longer possible after the cancun
			// fork, this overhead is considered acceptable.
			for _, val := range s.storageData[accountHash] {
				deletes += 2*common.HashLength + len(val)
				storageOverwrites.add(2*common.HashLength + len(val))
			}
			delete(s.storageData, accountHash)
		}
		// Keep track of whether the account has already been marked as destructed.
		// This additional marker is useful for undoing the merge operation.
		_, exist := s.destructSet[accountHash]
		destructs = append(destructs, destruct{
			Hash:  accountHash,
			Exist: exist,
		})
		if exist {
			continue
		}
		updates += common.HashLength
		s.destructSet[accountHash] = struct{}{}
	}
	s.journal.add(destructs)

	// Apply the updated account data
	for accountHash, data := range set.accountData {
		if origin, ok := s.accountData[accountHash]; ok {
			updates += len(data) - len(origin)
			accountOverwrites.add(common.HashLength + len(origin))
		} else {
			updates += common.HashLength + len(data)
		}
		s.accountData[accountHash] = data
	}
	// Apply all the updated storage slots (individually)
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
				updates += 2*common.HashLength + len(data)
			}
			s.storageData[accountHash] = slots
			continue
		}
		// Storage exists in both local and external set, merge the slots
		slots := s.storageData[accountHash]
		for storageHash, data := range storage {
			if origin, ok := slots[storageHash]; ok {
				updates += len(data) - len(origin)
				storageOverwrites.add(2*common.HashLength + len(origin))
			} else {
				updates += 2*common.HashLength + len(data)
			}
			slots[storageHash] = data
		}
	}
	accountOverwrites.report(gcAccountMeter, gcAccountBytesMeter)
	storageOverwrites.report(gcStorageMeter, gcStorageBytesMeter)
	delta := updates - deletes
	s.updateSize(delta)
	s.clearCache()
	return int64(delta)
}

// revert takes the original value of accounts and storages as input and reverts
// the latest state transition applied on the state set.
func (s *stateSet) revert(accountOrigin map[common.Hash][]byte, storageOrigin map[common.Hash]map[common.Hash][]byte) (int64, error) {
	// Load the destruct journal whose availability is always expected
	destructs, err := s.journal.pop()
	if err != nil {
		return 0, err
	}
	// Revert the modifications to the destruct set by journal
	var delta int
	for _, entry := range destructs {
		if entry.Exist {
			continue
		}
		delete(s.destructSet, entry.Hash)
		delta -= common.HashLength
	}
	// Overwrite the account data with original value blindly
	for addrHash, blob := range accountOrigin {
		if len(blob) == 0 {
			if data, ok := s.accountData[addrHash]; ok {
				delta -= common.HashLength + len(data)
			} else {
				return 0, fmt.Errorf("null to null account transition, %x", addrHash)
			}
			delete(s.accountData, addrHash)
		} else {
			if data, ok := s.accountData[addrHash]; ok {
				delta += len(blob) - len(data)
			} else {
				delta += len(blob) + common.HashLength
			}
			s.accountData[addrHash] = blob
		}
	}
	// Overwrite the storage data with original value blindly
	for addrHash, storage := range storageOrigin {
		// It might be possible that the storage set is not existent because
		// the whole storage is deleted.
		slots := s.storageData[addrHash]
		if len(slots) == 0 {
			slots = make(map[common.Hash][]byte)
		}
		for storageHash, blob := range storage {
			if len(blob) == 0 {
				if data, ok := slots[storageHash]; ok {
					delta -= 2*common.HashLength + len(data)
				} else {
					return 0, fmt.Errorf("null to null storage transition, %x %x", addrHash, storageHash)
				}
				delete(slots, storageHash)
			} else {
				if data, ok := slots[storageHash]; ok {
					delta += len(blob) - len(data)
				} else {
					delta += 2*common.HashLength + len(blob)
				}
				slots[storageHash] = blob
			}
		}
		if len(slots) == 0 {
			delete(s.storageData, addrHash)
		} else {
			s.storageData[addrHash] = slots
		}
	}
	s.clearCache()
	s.updateSize(delta)
	return int64(delta), nil
}

func (s *stateSet) updateSize(delta int) {
	if s.size+delta < 0 {
		log.Error("State set size underflow", "size", common.StorageSize(s.size), "diff", common.StorageSize(delta))
		s.size = 0
		return
	}
	s.size += delta
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
	for accountHash, slots := range s.storageData {
		keys := make([]common.Hash, 0, len(slots))
		vals := make([][]byte, 0, len(slots))
		for key, val := range slots {
			keys = append(keys, key)
			vals = append(vals, val)
		}
		storages = append(storages, Storage{Hash: accountHash, Keys: keys, Blobs: vals})
	}
	if err := rlp.Encode(w, storages); err != nil {
		return err
	}
	// Encode journal
	return s.journal.encode(w)
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
		AccountHash common.Hash
		Keys        []common.Hash
		Vals        [][]byte
	}
	var (
		storages   []Storage
		storageSet = make(map[common.Hash]map[common.Hash][]byte)
	)
	if err := r.Decode(&storages); err != nil {
		return fmt.Errorf("load diff storage: %v", err)
	}
	for _, entry := range storages {
		storageSet[entry.AccountHash] = make(map[common.Hash][]byte)
		for i := 0; i < len(entry.Keys); i++ {
			storageSet[entry.AccountHash][entry.Keys[i]] = entry.Vals[i]
		}
	}
	s.storageData = storageSet
	s.storageList = make(map[common.Hash][]common.Hash)

	// Decode journal
	s.journal = &journal{}
	return s.journal.decode(r)
}

// write flushes state mutations into the provided database batch as a whole.
func (s *stateSet) write(db ethdb.KeyValueStore, batch ethdb.Batch, genMarker []byte) (int, int) {
	return writeStates(db, batch, genMarker, s.destructSet, s.accountData, s.storageData)
}

// reset clears all cached state data, including any optional sorted lists that
// may have been generated.
func (s *stateSet) reset() {
	s.destructSet = make(map[common.Hash]struct{})
	s.accountData = make(map[common.Hash][]byte)
	s.storageData = make(map[common.Hash]map[common.Hash][]byte)
	s.journal.reset()
}

// StateSetWithOrigin wraps the state set with additional original values of the
// mutated states.
type StateSetWithOrigin struct {
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

// NewStateSetWithOrigin constructs the state set with the provided data.
func NewStateSetWithOrigin(destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, accountOrigin map[common.Address][]byte, storageOrigin map[common.Address]map[common.Hash][]byte) *StateSetWithOrigin {
	// Don't panic for the lazy callers, initialize the nil maps instead.
	if accountOrigin == nil {
		accountOrigin = make(map[common.Address][]byte)
	}
	if storageOrigin == nil {
		storageOrigin = make(map[common.Address]map[common.Hash][]byte)
	}
	return &StateSetWithOrigin{
		stateSet:      newStates(destructs, accounts, storages),
		accountOrigin: accountOrigin,
		storageOrigin: storageOrigin,
	}
}

// encode serializes the content of state set into the provided writer.
func (s *StateSetWithOrigin) encode(w io.Writer) error {
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

func (s *StateSetWithOrigin) decode(r *rlp.Stream) error {
	if s.stateSet == nil {
		s.stateSet = &stateSet{}
	}
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
