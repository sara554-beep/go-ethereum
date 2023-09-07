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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package state

import (
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
)

// OverrideAccount indicates the overriding fields of account during the execution
// of a message call.
//
// Note, state and stateDiff can't be specified at the same time. If state is
// set, message execution will only use the data in the given state. Otherwise
// if statDiff is set, all diff will be applied first and then execute the call
// message.
type OverrideAccount struct {
	Nonce     *hexutil.Uint64              `json:"nonce"`
	Code      *hexutil.Bytes               `json:"code"`
	Balance   **hexutil.Big                `json:"balance"`
	State     *map[common.Hash]common.Hash `json:"state"`
	StateDiff *map[common.Hash]common.Hash `json:"stateDiff"`
}

func (o OverrideAccount) validate() error {
	if o.State != nil && o.StateDiff != nil {
		return errors.New("state diff and state overrides are incompatible")
	}
	return nil
}

// Overrides is the collection of overridden accounts.
type Overrides map[common.Address]OverrideAccount

// merkleReaderWithOverrides implements the StateReader interface, warps merkle
// reader with additional state overrides. It's designed use in testing and API
// serving.
type merkleReaderWithOverrides struct {
	reader    StateReader
	overrides Overrides
}

// newMerkleReaderWithOverrides constructs a reader of the specific state with
// provided state overrides.
func newMerkleReaderWithOverrides(db *merkleDBWithOverrides, root common.Hash) (*merkleReaderWithOverrides, error) {
	r, err := db.merkleDB.StateReader(root)
	if err != nil {
		return nil, err
	}
	return &merkleReaderWithOverrides{reader: r, overrides: db.overrides}, nil
}

// Account implements StateReader, retrieving the account specified by the address
// from the associated state.
func (r *merkleReaderWithOverrides) Account(addr common.Address) (*types.StateAccount, error) {
	acct, err := r.reader.Account(addr)
	if err != nil {
		return nil, err
	}
	if override, ok := r.overrides[addr]; ok {
		if override.Nonce != nil {
			acct.Nonce = uint64(*override.Nonce)
		}
		if override.Balance != nil {
			acct.Balance = (*big.Int)(*override.Balance)
		}
		if override.Code != nil {
			acct.CodeHash = crypto.Keccak256(*override.Code)
		}
	}
	return acct, nil
}

// Storage implements StateReader, retrieving the storage slot specified by the
// address and slot key from the associated state.
func (r *merkleReaderWithOverrides) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	if override, ok := r.overrides[addr]; ok {
		if override.StateDiff != nil {
			val, ok := (*override.StateDiff)[slot]
			if ok {
				return val, nil
			}
			// fallback to non-override storage
		}
		if override.State != nil {
			val, ok := (*override.State)[slot]
			if ok {
				return val, nil
			}
			return common.Hash{}, nil
		}
	}
	return r.reader.Storage(addr, slot)
}

// merkleDBWithOverrides implements Database interface, wraps the merkleDB with
// additional state overrides. It's designed use in testing and API serving.
type merkleDBWithOverrides struct {
	*merkleDB

	overrides Overrides
}

// NewDatabaseWithOverrides creates a new state database by applying provided state
// overrides.
func NewDatabaseWithOverrides(codeDB CodeStore, trieDB *trie.Database, snaps *snapshot.Tree, overrides Overrides) (Database, error) {
	for _, o := range overrides {
		if err := o.validate(); err != nil {
			return nil, err
		}
	}
	return &merkleDBWithOverrides{
		merkleDB:  newMerkleDB(codeDB, trieDB, snaps),
		overrides: overrides,
	}, nil
}

// StateReader returns a state reader interface with the specified state root.
func (db *merkleDBWithOverrides) StateReader(stateRoot common.Hash) (StateReader, error) {
	return newMerkleReaderWithOverrides(db, stateRoot)
}
