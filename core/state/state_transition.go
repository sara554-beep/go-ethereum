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

// Code represents the mutated contract code along with its meta data.
type Code struct {
	Address common.Address // Corresponding account address
	Hash    common.Hash    // Hash of the contract code
	Blob    []byte
}

// Transition represents the transition between two states made during state execution.
// It contains information about the mutated states, including their original values.
// Additionally, it provides an associated set of trie nodes reflecting the trie changes.
type Transition struct {
	Destructs         map[common.Hash]struct{}                  // The list of destructed accounts
	Accounts          map[common.Hash][]byte                    // The mutated accounts in 'slim RLP' encoding
	AccountsOrigin    map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	Storages          map[common.Hash]map[common.Hash][]byte    // The mutated slots in prefix-zero trimmed rlp format
	StoragesOrigin    map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp format
	StorageIncomplete map[common.Address]struct{}               // The marker for incomplete storage sets
	Codes             []Code                                    // The list of dirty codes
	Nodes             *trienode.MergedNodeSet                   // Aggregated dirty node set
}

// NewTransition constructs a transition object.
func NewTransition() *Transition {
	return &Transition{
		Destructs:         make(map[common.Hash]struct{}),
		Accounts:          make(map[common.Hash][]byte),
		Storages:          make(map[common.Hash]map[common.Hash][]byte),
		AccountsOrigin:    make(map[common.Address][]byte),
		StoragesOrigin:    make(map[common.Address]map[common.Hash][]byte),
		StorageIncomplete: make(map[common.Address]struct{}),
		Nodes:             trienode.NewMergedNodeSet(),
	}
}

// AddCode tracks the provided dirty contract code.
func (t *Transition) AddCode(address common.Address, hash common.Hash, blob []byte) {
	t.Codes = append(t.Codes, Code{
		Address: address,
		Hash:    hash,
		Blob:    blob,
	})
}
