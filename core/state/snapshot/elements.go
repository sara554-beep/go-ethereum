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

package snapshot

import "github.com/ethereum/go-ethereum/common"

// StateWithPrev represents a state element with associated original value.
type StateWithPrev struct {
	Value []byte // Nil means the state is deleted
	Prev  []byte // Nil means the state was not existent previously
}

// size returns the approximate memory size occupied by state element.
func (state StateWithPrev) size() int {
	return len(state.Value) + len(state.Prev)
}

// AccountData represents an account change set belongs to a specific block.
type AccountData map[common.Hash]StateWithPrev

// NewAccountData initializes a fresh new account set.
func NewAccountData() AccountData {
	return make(map[common.Hash]StateWithPrev)
}

// Copy returns an independent deep-copied account data.
func (data AccountData) Copy() AccountData {
	cpy := make(map[common.Hash]StateWithPrev)
	for k, v := range data {
		cpy[k] = StateWithPrev{
			Value: common.CopyBytes(v.Value),
			Prev:  common.CopyBytes(v.Prev),
		}
	}
	return cpy
}

// StorageData represents a storage state change set belongs to a specific block.
type StorageData map[common.Hash]map[common.Hash]StateWithPrev

// NewStorageData initializes a fresh new storage slots set.
func NewStorageData() StorageData {
	return make(map[common.Hash]map[common.Hash]StateWithPrev)
}

// Copy returns an independent deep-copied account data.
func (data StorageData) Copy() StorageData {
	cpy := make(map[common.Hash]map[common.Hash]StateWithPrev)
	for account, slots := range data {
		cpy[account] = make(map[common.Hash]StateWithPrev)
		for k, v := range slots {
			cpy[account][k] = StateWithPrev{
				Value: common.CopyBytes(v.Value),
				Prev:  common.CopyBytes(v.Prev),
			}
		}
	}
	return cpy
}
