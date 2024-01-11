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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// CodeReader wraps the ReadCode and ReadCodeSize methods of a backing contract
// code store, providing an interface for retrieving contract code and its size.
type CodeReader interface {
	// ReadCode retrieves a particular contract's code.
	ReadCode(addr common.Address, codeHash common.Hash) ([]byte, error)

	// ReadCodeSize retrieves a particular contracts code's size.
	ReadCodeSize(addr common.Address, codeHash common.Hash) (int, error)
}

// CodeWriter wraps the WriteCodes method of a backing contract code store,
// providing an interface for writing contract codes back to database.
type CodeWriter interface {
	// WriteCodes persists the provided contract codes.
	WriteCodes(addresses []common.Address, codeHashes []common.Hash, codes [][]byte) error
}

// CodeStore defines the essential methods for reading and writing contract codes,
// providing a comprehensive interface for code management.
type CodeStore interface {
	CodeReader
	CodeWriter
}

// AccountReader wraps the Account method of a backing state database
// providing an interface for accessing account data.
type AccountReader interface {
	// Account retrieves the account associated with a particular address.
	//
	// (a) A nil account is returned if it's not existent
	// (b) An error is returned if any unexpected error occurs
	// (c) The returned account is safe to modify
	Account(addr common.Address) (*types.StateAccount, error)
}

// StorageReader wraps the Storage method of a backing state database
// providing an interface for accessing storage slot.
type StorageReader interface {
	// Storage retrieves the storage slot associated with a particular account
	// address and slot key.
	//
	// (a) An empty slot is returned if it's not existent
	// (b) An error is returned if any unexpected error occurs
	// (c) The returned storage slot is safe to modify
	Storage(addr common.Address, storageRoot common.Hash, slot common.Hash) (common.Hash, error)
}

// Reader defines the interface for accessing accounts or storage slots
// associated with a specific state.
type Reader interface {
	AccountReader
	StorageReader

	// Copy returns a deep-copied state reader.
	Copy() Reader
}

// AccountHasher wraps the multiple mutation methods of a backing state database
// providing an interface for writing account changes to database.
type AccountHasher interface {
	// UpdateAccount abstracts an account write to the trie. It encodes the
	// provided account object with associated algorithm and then updates it
	// in the hasher with provided address.
	UpdateAccount(address common.Address, account *types.StateAccount) error

	// DeleteAccount abstracts an account deletion from the hasher.
	DeleteAccount(address common.Address) error

	// UpdateContractCode abstracts code write to the hasher.
	UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error

	// Hash returns the root hash of the state. It does not write to the database
	// and can be used even if the hasher doesn't have one.
	Hash() common.Hash

	// Commit collects all dirty nodes produced by hasher for state rehashing.
	// The nodeset can be nil if the state is not changed at all.
	Commit() (common.Hash, *trienode.NodeSet, error)
}

// StorageHasher wraps the multiple mutation methods of a backing state database
// providing an interface for writing storage changes to database.
type StorageHasher interface {
	// UpdateStorage associates key with value in the hasher.
	UpdateStorage(address common.Address, key, value []byte) error

	// DeleteStorage removes any existing value for key from the hasher.
	DeleteStorage(address common.Address, key []byte) error

	// Hash returns the root hash of the storage. It does not write to the
	// database and can be used even if the hasher doesn't have one.
	Hash() common.Hash

	// Commit collects all dirty nodes produced by hasher for storage rehashing.
	// The nodeset can be nil if the storage is not changed at all.
	Commit() (common.Hash, *trienode.NodeSet, error)

	// Copy returns a deep-copied storage hasher.
	Copy(Hasher) StorageHasher
}

// Hasher defines the interface for hashing account and storage changes.
type Hasher interface {
	AccountHasher

	// Copy returns a deep-copied hasher.
	Copy() Hasher

	// StorageHasher returns a storage writer for the specific account.
	StorageHasher(address common.Address, root common.Hash) (StorageHasher, error)
}

// StorageDeleter defines the interface for deleting storage of specific account.
type StorageDeleter interface {
	// Delete wipes the storage belongs to the specific account and returns all
	// deleted slots along with associated hashing proofs.
	Delete(address common.Address, root common.Hash) (bool, map[common.Hash][]byte, *trienode.NodeSet, error)
}

// Database defines the essential methods for reading and writing ethereum states,
// providing a comprehensive interface for ethereum state management.
type Database interface {
	CodeStore

	// Reader returns a state reader interface with the specified state root.
	Reader(stateRoot common.Hash) (Reader, error)

	// Hasher returns a state hasher interface with the specified state root.
	Hasher(stateRoot common.Hash) (Hasher, error)

	// StorageDeleter returns a storage deleter interface with the specified state root.
	StorageDeleter(stateRoot common.Hash) (StorageDeleter, error)

	// TrieDB returns the underlying trie database for managing trie nodes.
	TrieDB() *trie.Database

	// Commit accepts the state changes made by execution and applies it to database.
	// An error will be returned in these following scenarios:
	//
	// - The state transition in underlying state layers fail
	// - The write functionality is not supported(e.g. for read-only state database)
	Commit(originRoot common.Hash, root common.Hash, block uint64, update *Update, nodes *trienode.MergedNodeSet) error
}

// Trie is a state trie interface, can be implemented by Ethereum Merkle Patricia
// Trie or Ethereum Verkle Trie.
type Trie interface {
	// GetKey returns the sha3 preimage of a hashed key that was previously used
	// to store a value.
	//
	// TODO(fjl): remove this when StateTrie is removed
	GetKey([]byte) []byte

	// GetAccount abstracts an account read from the trie. It retrieves the
	// account blob from the trie with provided account address and decodes it
	// with associated decoding algorithm. If the specified account is not in
	// the trie, nil will be returned. If the trie is corrupted(e.g. some nodes
	// are missing or the account blob is incorrect for decoding), an error will
	// be returned.
	GetAccount(address common.Address) (*types.StateAccount, error)

	// GetStorage returns the value for key stored in the trie. The value bytes
	// must not be modified by the caller. If a node was not found in the database,
	// a trie.MissingNodeError is returned.
	GetStorage(addr common.Address, key []byte) ([]byte, error)

	// UpdateAccount abstracts an account write to the trie. It encodes the
	// provided account object with associated algorithm and then updates it
	// in the trie with provided address.
	UpdateAccount(address common.Address, account *types.StateAccount) error

	// UpdateStorage associates key with value in the trie. If value has length zero,
	// any existing value is deleted from the trie. The value bytes must not be modified
	// by the caller while they are stored in the trie. If a node was not found in the
	// database, a trie.MissingNodeError is returned.
	UpdateStorage(address common.Address, key, value []byte) error

	// DeleteAccount abstracts an account deletion from the trie.
	DeleteAccount(address common.Address) error

	// DeleteStorage removes any existing value for key from the trie. If a node
	// was not found in the database, a trie.MissingNodeError is returned.
	DeleteStorage(address common.Address, key []byte) error

	// UpdateContractCode abstracts code write to the trie. It is expected
	// to be moved to the stateWriter interface when the latter is ready.
	UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error

	// Hash returns the root hash of the trie. It does not write to the database and
	// can be used even if the trie doesn't have one.
	Hash() common.Hash

	// Commit collects all dirty nodes in the trie and replace them with the
	// corresponding node hash. All collected nodes(including dirty leaves if
	// collectLeaf is true) will be encapsulated into a nodeset for return.
	// The returned nodeset can be nil if the trie is clean(nothing to commit).
	// Once the trie is committed, it's not usable anymore. A new trie must
	// be created with new root and updated trie database for following usage
	Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error)

	// NodeIterator returns an iterator that returns nodes of the trie. Iteration
	// starts at the key after the given start key. And error will be returned
	// if fails to create node iterator.
	NodeIterator(startKey []byte) (trie.NodeIterator, error)

	// Prove constructs a state proof for key. The result contains all encoded nodes
	// on the path to the value at key. The value itself is also included in the last
	// node and can be retrieved by verifying the proof.
	//
	// If the trie does not contain a value for key, the returned proof contains all
	// nodes of the longest existing prefix of the key (at least the root), ending
	// with the node that proves the absence of the key.
	Prove(key []byte, proofDb ethdb.KeyValueWriter) error
}
