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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
)

type StateDBRead struct {
	*stateCore

	snaps *snapshot.Tree
	snap  snapshot.Snapshot
}

func NewStateDBReadRead(root common.Hash, snaps *snapshot.Tree) (*StateDBRead, error) {
	var snap snapshot.Snapshot
	if snaps != nil {
		snap = snaps.Snapshot(root)
	}
	core, err := newStateCore(root, nil)
	if err != nil {
		return nil, err
	}
	sdb := &StateDBRead{
		stateCore: core,
		snaps:     snaps,
		snap:      snap,
	}
	return sdb, nil
}

// setError remembers the first non-nil error it is called with.
func (db *StateDBRead) setError(err error) {
	if db.dbErr == nil {
		db.dbErr = err
	}
}

func (db *StateDBRead) Error() error {
	return db.dbErr
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (db *StateDBRead) Copy() *StateDBRead {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDBRead{
		stateCore: db.stateCore.Copy(),
		snaps:     db.snaps,
		snap:      db.snap,
	}
	return state
}
