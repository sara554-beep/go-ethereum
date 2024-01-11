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
	"fmt"
	"io"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
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

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// - First you need to obtain a state object.
// - Account values as well as storages can be accessed and modified through the object.
// - Finally, call commit to return the changes of storage trie and update account data.
type stateObject struct {
	db       *StateDB
	hasher   StorageHasher       // storage hasher, become nil when first access
	address  common.Address      // address of ethereum account
	addrHash common.Hash         // hash of ethereum address of the account
	origin   *types.StateAccount // Account original data without any change applied, nil means it was not existent
	data     types.StateAccount  // Account data with all mutations applied in the scope of block

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
	)
	if acct == nil {
		acct = types.NewEmptyStateAccount()
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

// updateHasher is responsible for flushing cached storage changes into the
// storage hasher. This function assumes the storage hasher is available.
func (s *stateObject) updateHasher() error {
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
			if err := s.hasher.DeleteStorage(s.address, key[:]); err != nil {
				return err
			}
		} else {
			// Encoding []byte cannot fail, ok to ignore the error.
			trimmed := common.TrimLeftZeroes(value[:])
			if err := s.hasher.UpdateStorage(s.address, key[:], trimmed); err != nil {
				return err
			}
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

// updateRoot flushes all cached storage mutations to storage hasher and recalculate
// the new storage root. The unexpected error will be recorded if any occurs.
func (s *stateObject) updateRoot() {
	if s.hasher == nil {
		hasher, err := s.db.hasher.StorageHasher(s.address, s.data.Root)
		if err != nil {
			s.db.setError(err)
			return
		}
		s.hasher = hasher
	}
	// Short circuit if failed to flush the pending states.
	if err := s.updateHasher(); err != nil {
		s.db.setError(err)
		return
	}
	// Short circuit if nothing is changed.
	if len(s.pendingStorage) == 0 {
		return
	}
	// Track the amount of time wasted on hashing the storage changes
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}
	s.data.Root = s.hasher.Hash()
}

// commitStorage stores the account's original and current value of mutated
// storage to result.
func (s *stateObject) commitStorage(update *AccountUpdate) {
	hasher := crypto.NewKeccakState()
	for key, slot := range s.pendingStorage {
		if slot == s.originStorage[key] {
			continue
		}
		if update.Storages == nil {
			update.Storages = make(map[common.Hash][]byte)
		}
		if update.StoragesOrigin == nil {
			update.StoragesOrigin = make(map[common.Hash][]byte)
		}
		khash := crypto.HashData(hasher, key[:])
		encoded, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(slot[:]))
		update.Storages[khash] = encoded

		old := s.originStorage[key]
		if old == (common.Hash{}) {
			update.StoragesOrigin[khash] = nil // nil if it was not present previously
		} else {
			encoded, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(old[:]))
			update.StoragesOrigin[khash] = encoded
		}
		s.originStorage[key] = slot
	}
	s.pendingStorage = make(Storage)
}

// commit obtains a set of dirty storage trie nodes and updates the account data.
// The returned set can be nil if nothing to commit. This function assumes all
// storage mutations have already been flushed into trie by updateRoot.
func (s *stateObject) commit() (*AccountUpdate, *trienode.NodeSet, error) {
	update := &AccountUpdate{
		Address: s.address,
		Data:    types.SlimAccountRLP(s.data),
	}
	if s.origin != nil {
		update.Origin = types.SlimAccountRLP(*s.origin)
	} // s.origin == nil means the account was not present
	if s.dirtyCode {
		update.Code = &ContractCode{
			Address: s.address,
			Hash:    common.BytesToHash(s.CodeHash()),
			Blob:    s.code,
		}
		s.dirtyCode = false
	}
	// Commit storage changes if the storage is mutated.
	var nodes *trienode.NodeSet
	if len(s.pendingStorage) != 0 {
		s.commitStorage(update)

		// Track the amount of time wasted on committing the storage hasher
		if metrics.EnabledExpensive {
			defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
		}
		// The hasher could potentially contain cached mutations. Call commit to
		// acquire a set of nodes that have been modified, the set can be nil if
		// nothing to commit.
		var (
			err  error
			root common.Hash
		)
		root, nodes, err = s.hasher.Commit()
		if err != nil {
			return nil, nil, err
		}
		if s.data.Root != root {
			return nil, nil, fmt.Errorf("unexpected storage root, %x != %x", root, s.data.Root)
		}
	}
	s.origin = s.data.Copy() // Update original account data after commit
	return update, nodes, nil
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
	if s.hasher != nil {
		obj.hasher = s.hasher.Copy(db.hasher)
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
