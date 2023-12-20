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
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// Code represents the mutated contract code along with its metadata.
type Code struct {
	Hash    common.Hash    // Hash of the contract code
	Blob    []byte         // Blob of the contract code
	Address common.Address // Corresponding account address
}

// Update represents the difference between two states made by state execution.
// It contains information about the mutated states, including their original
// values. Additionally, it provides an associated set of trie nodes reflecting
// the trie changes.
type Update struct {
	Destructs         map[common.Hash]struct{}                  // The list of destructed accounts
	Accounts          map[common.Hash][]byte                    // The mutated accounts in 'slim RLP' encoding
	AccountsOrigin    map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	Storages          map[common.Hash]map[common.Hash][]byte    // The mutated slots in prefix-zero trimmed rlp format
	StoragesOrigin    map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp format
	StorageIncomplete map[common.Address]struct{}               // The marker for incomplete storage sets
	Codes             []Code                                    // The list of dirty codes
	Nodes             *trienode.MergedNodeSet                   // Aggregated dirty node set

	accountTrieNodesUpdated int // the number of trie nodes updated in account trie
	accountTrieNodesDeleted int // the number of trie nodes deleted in account trie
	storageTrieNodesUpdated int // the number of trie nodes updated in storage trie
	storageTrieNodesDeleted int // the number of trie nodes deleted in account trie
	contractCodeCount       int // the number of contract codes updated
	contractCodeSize        int // the total size of updated contract code
}

// NewUpdate constructs a state update object.
func NewUpdate() *Update {
	return &Update{
		Destructs:         make(map[common.Hash]struct{}),
		Accounts:          make(map[common.Hash][]byte),
		Storages:          make(map[common.Hash]map[common.Hash][]byte),
		AccountsOrigin:    make(map[common.Address][]byte),
		StoragesOrigin:    make(map[common.Address]map[common.Hash][]byte),
		StorageIncomplete: make(map[common.Address]struct{}),
		Nodes:             trienode.NewMergedNodeSet(),
	}
}

// AddCode tracks the provided dirty contract code locally.
func (update *Update) AddCode(address common.Address, hash common.Hash, blob []byte) {
	update.Codes = append(update.Codes, Code{
		Address: address,
		Hash:    hash,
		Blob:    blob,
	})
	update.contractCodeCount++
	update.contractCodeSize += len(blob)
}

// AddNodes tracks the provided trie node set locally.
func (update *Update) AddNodes(nodes *trienode.NodeSet) error {
	if nodes == nil {
		return nil
	}
	if err := update.Nodes.Merge(nodes); err != nil {
		return err
	}
	if nodes.Owner == (common.Hash{}) {
		update.accountTrieNodesUpdated, update.accountTrieNodesDeleted = nodes.Size()
		return nil
	}
	updates, deleted := nodes.Size()
	update.storageTrieNodesUpdated += updates
	update.storageTrieNodesDeleted += deleted
	return nil
}
