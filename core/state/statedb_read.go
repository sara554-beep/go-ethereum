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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type snapOnly struct {
	root   common.Hash
	db     Database
	snap   snapshot.Snapshot
	hasher crypto.KeccakState
}

func newSnapOnly(root common.Hash, db Database, snap snapshot.Snapshot) *snapOnly {
	return &snapOnly{
		root:   root,
		db:     db,
		snap:   snap,
		hasher: crypto.NewKeccakState(),
	}
}

// Account retrieves the account associated with a particular
// account address. Nil will be returned in case the account
// is not existent.
func (t *snapOnly) Account(address common.Address) (*types.StateAccount, error) {
	acc, err := t.snap.Account(crypto.HashData(t.hasher, address.Bytes()))
	if err != nil {
		return nil, err
	}
	if acc == nil {
		return nil, nil
	}
	data := &types.StateAccount{
		Nonce:    acc.Nonce,
		Balance:  acc.Balance,
		CodeHash: acc.CodeHash,
		Root:     common.BytesToHash(acc.Root),
	}
	if len(data.CodeHash) == 0 {
		data.CodeHash = emptyCodeHash
	}
	if data.Root == (common.Hash{}) {
		data.Root = emptyRoot
	}
	return data, nil
}

// Storage retrieves the storage data associated with a particular
// slot hash along with an account address. Nil will be returned
// in case the corresponding account or the storage slot itself
// is not existent.
func (t *snapOnly) Storage(address common.Address, storageHash common.Hash, root common.Hash) ([]byte, error) {
	return t.snap.Storage(crypto.HashData(t.hasher, address.Bytes()), crypto.Keccak256Hash(storageHash.Bytes()))
}

// ContractCode retrieves a particular contract's code.
func (t *snapOnly) ContractCode(address common.Hash, hash common.Hash) ([]byte, error) {
	return t.db.ContractCode(address, hash)
}

// ContractCodeSize retrieves a particular contracts code's size.
func (t *snapOnly) ContractCodeSize(address common.Hash, hash common.Hash) (int, error) {
	return t.db.ContractCodeSize(address, hash)
}

type StateReader struct {
	*stateCore
}

func NewStateReader(root common.Hash, db Database, snap snapshot.Snapshot) (*StateReader, error) {
	core, err := newStateCore(root, newSnapOnly(root, db, snap))
	if err != nil {
		return nil, err
	}
	return &StateReader{
		stateCore: core,
	}, nil
}
