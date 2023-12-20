// Copyright 2014 The go-ethereum Authors
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

package state

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type Storage map[common.Hash]common.Hash

func (s Storage) Copy() Storage {
	cpy := make(Storage, len(s))
	for key, value := range s {
		cpy[key] = value
	}
	return cpy
}

// hasherType indicates the type of hasher for hashing storage.
type hasherType int

const (
	typeUnknown hasherType = iota
	typeMerkle
	typeVerkle
)

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// - First you need to obtain a state object.
// - Account values as well as storages can be accessed and modified through the object.
// - Finally, call commit to return the changes of storage trie and update account data.
type stateObject struct {
	db       *StateDB
	address  common.Address      // address of ethereum account
	addrHash common.Hash         // hash of ethereum address of the account
	origin   *types.StateAccount // Account original data without any change applied, nil means it was not existent
	data     types.StateAccount  // Account data with all mutations applied in the scope of block

	trie           Trie                     // storage trie, which becomes non-nil on first access
	code           []byte                   // contract bytecode, which gets set when code is loaded
	originStorage  Storage                  // Storage entries that have been resolved in the current block
	dirtyStorage   Storage                  // Storage entries that have been modified in the current transaction
	pendingStorage Storage                  // Storage entries that have been modified in the current block
	pendingStates  map[common.Hash]struct{} // Flags if the pending states have been written

	// Cache flags.
	dirtyCode bool // true if the code was updated

	// Flag whether the account was marked as self-destructed. The self-destructed
	// account is still accessible in the scope of same transaction.
	selfDestructed bool

	// Flag whether the object was created in the current transaction
	created bool

	hasher hasherType // the type of hasher for hashing stoarge
}

// empty returns whether the account is considered empty.
func (s *stateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

// newObject creates a state object.
func newObject(db *StateDB, address common.Address, acct *types.StateAccount) *stateObject {
	var (
		origin  = acct
		created = acct == nil // true if the account was not existent
		hasher  = typeUnknown
	)
	if acct == nil {
		acct = types.NewEmptyStateAccount()
	}
	if db.db.TrieDB() != nil {
		if db.db.TrieDB().IsVerkle() {
			hasher = typeVerkle
		} else {
			hasher = typeMerkle
		}
	}
	return &stateObject{
		db:             db,
		address:        address,
		addrHash:       crypto.Keccak256Hash(address[:]),
		origin:         origin,
		data:           *acct,
		originStorage:  make(Storage),
		dirtyStorage:   make(Storage),
		pendingStorage: make(Storage),
		pendingStates:  make(map[common.Hash]struct{}),
		created:        created,
		hasher:         hasher,
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

func (s *stateObject) markSelfdestructed() {
	s.selfDestructed = true
}

func (s *stateObject) touch() {
	s.db.journal.append(touchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

// GetState retrieves a value from the account storage trie.
func (s *stateObject) GetState(key common.Hash) common.Hash {
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage[key]
	if dirty {
		return value
	}
	// Otherwise return the entry's original value
	return s.GetCommittedState(key)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stateObject) GetCommittedState(key common.Hash) common.Hash {
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value
	}
	if value, cached := s.originStorage[key]; cached {
		return value
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	if _, destructed := s.db.stateObjectsDestruct[s.address]; destructed {
		return common.Hash{}
	}
	// If no live storage slot available, attempt to read from database.
	value, err := s.db.reader.Storage(s.address, s.data.Root, key)
	if err != nil {
		s.db.setError(err)
		return common.Hash{}
	}
	s.originStorage[key] = value // cache the slot regardless it's empty or not
	return value
}

// SetState updates a value in account storage.
func (s *stateObject) SetState(key, value common.Hash) {
	// If the new value is the same as old, don't set
	prev := s.GetState(key)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	s.db.journal.append(storageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
}

func (s *stateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stateObject) finalise(prefetch bool) {
	slotsToPrefetch := make([][]byte, 0, len(s.dirtyStorage))
	for key, value := range s.dirtyStorage {
		s.pendingStorage[key] = value
		s.pendingStates[key] = struct{}{}

		if value != s.originStorage[key] {
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	if s.db.prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && s.data.Root != types.EmptyRootHash {
		s.db.prefetcher.prefetch(s.addrHash, s.data.Root, s.address, slotsToPrefetch)
	}
	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(Storage)
	}
}

// updateTrie is responsible for persisting cached storage changes into the
// object's storage trie. In case the storage trie is not yet loaded, this
// function will load the trie automatically. If any issues arise during the
// loading or updating of the trie, an error will be returned. Furthermore,
// this function will return the mutated storage trie, or nil if there is no
// storage change at all.
func (s *stateObject) updateTrie() error {
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise(false)

	// Short circuit if nothing changed, don't bother with hashing anything
	if len(s.pendingStates) == 0 {
		return nil
	}
	// Track the amount of time wasted on updating the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageUpdates += time.Since(start) }(time.Now())
	}
	// Insert all the pending storage updates into the trie
	usedStorage := make([][]byte, 0, len(s.pendingStates))
	for key := range s.pendingStates {
		value := s.pendingStorage[key]
		if (value == common.Hash{}) {
			if err := s.trie.DeleteStorage(s.address, key[:]); err != nil {
				return err
			}
			s.db.StorageDeleted += 1
		} else {
			// Encoding []byte cannot fail, ok to ignore the error.
			trimmed := common.TrimLeftZeroes(value[:])
			if err := s.trie.UpdateStorage(s.address, key[:], trimmed); err != nil {
				return err
			}
			s.db.StorageUpdated += 1
		}
		// Cache the items for preloading
		usedStorage = append(usedStorage, common.CopyBytes(key[:])) // Copy needed for closure
	}
	if s.db.prefetcher != nil {
		s.db.prefetcher.used(s.addrHash, s.data.Root, usedStorage)
	}
	s.pendingStates = make(map[common.Hash]struct{})
	return nil
}

// openStorageTrie loads the storage trie if it hasn't been loaded yet. Notably, the
// trie used for hashing storage differs between the merkle and verkle worlds.
// Specifically, a separate storage trie is used for hashing storage when a merkle
// trie is used; otherwise, the global verkle trie is used for hashing storage.
func (s *stateObject) openStorageTrie() error {
	// The trie is already loaded, return it.
	if s.trie != nil {
		return nil
	}
	// Short circuit if the type of storage hasher is unknown.
	if s.hasher == typeUnknown {
		return errors.New("unknown state hasher")
	}
	// If the type of the used hasher is verkle tree, reuse the global one,
	// which is assumed to be loaded already.
	if s.hasher == typeVerkle {
		s.trie = s.db.trie
		return nil
	}
	// If the type of the used hasher is merkle tree, try to resolve it either
	// from prefetcher if it's active, or from state database.
	if s.db.prefetcher != nil {
		s.trie = s.db.prefetcher.trie(s.addrHash, s.data.Root)
	}
	if s.trie == nil {
		var err error
		s.trie, err = s.db.db.OpenStorageTrie(s.db.originalRoot, s.address, s.data.Root)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateRoot flushes all cached storage mutations to trie, recalculating the
// new storage trie root.
func (s *stateObject) updateRoot() {
	// Ensure the associated storage trie is available, bail out if failed to
	// resolve it, e.g. the trie loading is not supported by state database.
	if err := s.openStorageTrie(); err != nil {
		s.db.setError(err)
		return
	}
	// Short circuit if failed to flush the pending states.
	if err := s.updateTrie(); err != nil {
		s.db.setError(err)
		return
	}
	// Short circuit if nothing is changed.
	if len(s.pendingStorage) == 0 {
		return
	}
	// Track the amount of time wasted on hashing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}
	// Update the storage root only if merkle trie is used as hasher.
	if s.hasher == typeMerkle {
		s.data.Root = s.trie.Hash()
	}
}

// commitAccount stores the account's original and current value to result.
func (s *stateObject) commitAccount(update *Update) {
	// Store the account value after mutation
	update.Accounts[s.addrHash] = types.SlimAccountRLP(s.data)

	// Store the account's original value only if the value has not been recorded
	// yet. Otherwise, the account must be destructed in the same block.
	if _, ok := update.AccountsOrigin[s.address]; ok {
		return
	}
	if s.origin == nil {
		update.AccountsOrigin[s.address] = nil
	} else {
		update.AccountsOrigin[s.address] = types.SlimAccountRLP(*s.origin)
	}
}

// commitStorage stores the account's original and current value of mutated
// storage to result.
func (s *stateObject) commitStorage(update *Update) {
	var (
		hasher        = crypto.NewKeccakState()
		storage       map[common.Hash][]byte
		storageOrigin map[common.Hash][]byte
	)
	for key, val := range s.pendingStorage {
		if val == s.originStorage[key] {
			continue
		}
		khash := crypto.HashData(hasher, key[:])

		// Store the current value to result set
		if storage == nil {
			if update.Storages[s.addrHash] == nil {
				update.Storages[s.addrHash] = make(map[common.Hash][]byte)
			}
			storage = update.Storages[s.addrHash]
		}
		encoded, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(val[:]))
		storage[khash] = encoded

		// Store the original value in the result set only if the value has
		// not been recorded yet. Otherwise, the account must be destructed
		// in the same block, and storage slots belonging to the old account
		// were already deleted and tracked in the set.
		if storageOrigin == nil {
			if update.StoragesOrigin[s.address] == nil {
				update.StoragesOrigin[s.address] = make(map[common.Hash][]byte)
			}
			storageOrigin = update.StoragesOrigin[s.address]
		}
		if _, found := storageOrigin[khash]; !found {
			old := s.originStorage[key]
			if old == (common.Hash{}) {
				storageOrigin[khash] = nil // nil if it was not present previously
			} else {
				encoded, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(old[:]))
				storageOrigin[khash] = encoded
			}
		}
		// Aggregate the mutated storage slots into clean set as committed.
		s.originStorage[key] = val
	}
	s.pendingStorage = make(Storage)
}

// commit obtains a set of dirty storage trie nodes and updates the account data.
// The returned set can be nil if nothing to commit. This function assumes all
// storage mutations have already been flushed into trie by updateRoot.
func (s *stateObject) commit(update *Update) error {
	// Aggregate the account changes into result set.
	s.commitAccount(update)

	// Short circuit if storage is not changed at all.
	if len(s.pendingStorage) == 0 {
		s.origin = s.data.Copy()
		return nil
	}
	// Track the amount of time wasted on committing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}
	// Aggregate the storage changes into result set
	s.commitStorage(update)

	// If the used hasher is merkle tree, commit the storage changes as the
	// storage trie is separated from the main one.
	if s.hasher == typeMerkle {
		// The trie is currently in an open state and could potentially contain
		// cached mutations. Call commit to acquire a set of nodes that have been
		// modified, the set can be nil if nothing to commit.
		root, nodes, err := s.trie.Commit(false)
		if err != nil {
			return err
		}
		if s.data.Root != root {
			return fmt.Errorf("unexpected storage root, %x != %x", root, s.data.Root)
		}
		// Aggregate the dirty trie node into the global set
		if err := update.AddNodes(nodes); err != nil {
			return err
		}
	}
	s.origin = s.data.Copy() // Update original account data after commit
	return nil
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *stateObject) AddBalance(amount *uint256.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}
		return
	}
	s.SetBalance(new(uint256.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *stateObject) SubBalance(amount *uint256.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(uint256.Int).Sub(s.Balance(), amount))
}

func (s *stateObject) SetBalance(amount *uint256.Int) {
	s.db.journal.append(balanceChange{
		account: &s.address,
		prev:    new(uint256.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *stateObject) setBalance(amount *uint256.Int) {
	s.data.Balance = amount
}

func (s *stateObject) deepCopy(db *StateDB) *stateObject {
	obj := &stateObject{
		db:       db,
		address:  s.address,
		addrHash: s.addrHash,
		origin:   s.origin,
		data:     s.data,
	}
	if s.trie != nil {
		obj.trie = mustCopyTrie(s.trie)
	}
	obj.code = s.code
	obj.originStorage = s.originStorage.Copy()
	obj.dirtyStorage = s.dirtyStorage.Copy()
	obj.pendingStorage = s.pendingStorage.Copy()
	obj.pendingStates = make(map[common.Hash]struct{})
	for key := range s.pendingStates {
		obj.pendingStates[key] = struct{}{}
	}
	obj.selfDestructed = s.selfDestructed
	obj.dirtyCode = s.dirtyCode
	obj.created = s.created
	obj.hasher = s.hasher
	return obj
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *stateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stateObject) Code() []byte {
	if len(s.code) != 0 {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := s.db.db.ReadCode(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *stateObject) CodeSize() int {
	if len(s.code) != 0 {
		return len(s.code)
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return 0
	}
	size, err := s.db.db.ReadCodeSize(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code size %x: %v", s.CodeHash(), err))
	}
	return size
}

func (s *stateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code()
	s.db.journal.append(codeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *stateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *stateObject) SetNonce(nonce uint64) {
	s.db.journal.append(nonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *stateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *stateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stateObject) Balance() *uint256.Int {
	return s.data.Balance
}

func (s *stateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *stateObject) Root() common.Hash {
	return s.data.Root
}
