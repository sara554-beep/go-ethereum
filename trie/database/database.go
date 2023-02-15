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

package database

import "github.com/ethereum/go-ethereum/common"

// Reader wraps the Node method of a backing trie store.
type Reader interface {
	// Node retrieves the RLP-encoded trie node blob with the provided
	// trie identifier, node path and the corresponding node hash.
	// No error will be returned if the node is not found.
	Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}

// Database defines the methods needed to access/update trie nodes in
// different state scheme.
type Database interface {
	// GetReader returns a reader for accessing all trie nodes with provided
	// state root. Nil is returned in case the state is not available.
	GetReader(root common.Hash) Reader

	// Update performs a state transition from specified parent to root by committing
	// dirty nodes provided in the nodeset.
	//Update(root common.Hash, parent common.Hash, nodes map[common.Hash]*NodeSet) error

	// Commit writes all relevant trie nodes belonging to the specified state to disk.
	// Report specifies whether logs will be displayed in info level.
	Commit(root common.Hash, report bool) error

	// Initialized returns an indicator if the state data is already initialized
	// according to the state scheme.
	Initialized(genesisRoot common.Hash) bool

	// Size returns the current storage size of the memory cache in front of the
	// persistent database layer.
	Size() common.StorageSize

	// Scheme returns the node scheme used in the database.
	Scheme() string

	// Close closes the trie database backend and releases all held resources.
	Close() error
}
