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
	"github.com/ethereum/go-ethereum/core/state/snapshot"
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

// Reader defines the interface for accessing accounts or storage slots
// associated with a specific state.
type Reader interface {
	// Account retrieves the account associated with a particular address.
	// Null account is returned if the requested one is not existent. Error
	// is only returned if any unexpected error occurs.
	Account(addr common.Address) (*types.StateAccount, error)

	// Storage retrieves the storage slot associated with a particular account
	// address and slot key. Null slot is returned if the requested one is
	// not existent. Error is only returned if any unexpected error occurs.
	Storage(addr common.Address, storageRoot common.Hash, slot common.Hash) (common.Hash, error)
}

// Hasher defines the interface for hashing state with associated state
// changes caused by state execution.
type Hasher interface {
	// UpdateAccount abstracts an account write to the trie. It encodes the
	// provided account object with associated algorithm and then updates it
	// in the trie with provided address.
	UpdateAccount(address common.Address, account *types.StateAccount) error

	// UpdateStorage associates key with value in the trie. If value has length zero,
	// any existing value is deleted from the trie. The value bytes must not be modified
	// by the caller while they are stored in the trie. If a node was not found in the
	// database, a trie.MissingNodeError is returned.
	UpdateStorage(address common.Address, root common.Hash, key, value []byte) error

	// DeleteAccount abstracts an account deletion from the trie.
	DeleteAccount(address common.Address) error

	// DeleteStorage removes any existing value for key from the trie. If a node
	// was not found in the database, a trie.MissingNodeError is returned.
	DeleteStorage(address common.Address, root common.Hash, key []byte) error

	// UpdateContractCode abstracts code write to the trie. It is expected
	// to be moved to the stateWriter interface when the latter is ready.
	UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error

	// Hash returns the root hash of the trie. It does not write to the database and
	// can be used even if the trie doesn't have one.
	Hash(address *common.Address, origin common.Hash) (common.Hash, error)

	// Commit collects all dirty nodes in the trie and replace them with the
	// corresponding node hash. All collected nodes(including dirty leaves if
	// collectLeaf is true) will be encapsulated into a nodeset for return.
	// The returned nodeset can be nil if the trie is clean(nothing to commit).
	// Once the trie is committed, it's not usable anymore. A new trie must
	// be created with new root and updated trie database for following usage
	Commit(address *common.Address, root common.Hash, collectLeaf bool) (*trienode.NodeSet, error)

	// Copy returns a deep-copied state hasher.
	Copy() Hasher
}

// Database defines the essential methods for reading and writing ethereum states,
// providing a comprehensive interface for ethereum state management.
type Database interface {
	CodeStore

	// Reader returns a state reader interface with the specified state root.
	Reader(stateRoot common.Hash) (Reader, error)

	// Hasher returns a state hasher interface with the specified state root.
	Hasher(stateRoot common.Hash) (Hasher, error)

	// TrieDB returns the underlying trie database for managing trie nodes.
	TrieDB() *trie.Database

	// Snapshot returns the associated state snapshot; it may be nil if not configured.
	Snapshot() *snapshot.Tree

	// Commit accepts the state changes made by execution and applies it to database.
	Commit(originRoot common.Hash, root common.Hash, block uint64, trans *Transition) error
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

	// Hash returns the root hash of the trie. It does not write to the database and
	// can be used even if the trie doesn't have one.
	Hash() common.Hash

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
