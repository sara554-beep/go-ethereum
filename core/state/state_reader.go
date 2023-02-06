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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// stateReader defines the necessary functions for retrieving state.
type stateReader interface {
	// Account retrieves the account associated with a particular
	// account address. Nil will be returned in case the account
	// is not existent.
	Account(address common.Address) (*types.StateAccount, error)

	// Storage retrieves the storage data associated with a particular
	// slot hash along with an account address. Nil will be returned
	// in case the corresponding account or the storage slot itself
	// is not existent.
	Storage(address common.Address, storageHash common.Hash, root common.Hash) ([]byte, error)

	// ContractCode retrieves a particular contract's code.
	ContractCode(address common.Hash, hash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(address common.Hash, hash common.Hash) (int, error)
}

type tries struct {
	root         common.Hash
	db           Database
	accountTrie  Trie
	storageTries map[common.Hash]Trie
}

func newTries(root common.Hash, db Database) (*tries, error) {
	tr, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	return &tries{
		root:         root,
		db:           db,
		accountTrie:  tr,
		storageTries: make(map[common.Hash]Trie),
	}, nil
}

func (ts *tries) storageTrie(addrHash common.Hash, root common.Hash) (Trie, error) {
	if tr, ok := ts.storageTries[addrHash]; ok {
		return tr, nil
	}
	tr, err := ts.db.OpenStorageTrie(ts.root, addrHash, root)
	if err != nil {
		return nil, err
	}
	ts.storageTries[addrHash] = tr
	return tr, nil
}

func (ts *tries) Copy() *tries {
	ss := make(map[common.Hash]Trie)
	for hash, t := range ts.storageTries {
		ss[hash] = ts.db.CopyTrie(t)
	}
	return &tries{
		root:         ts.root,
		db:           ts.db,
		accountTrie:  ts.db.CopyTrie(ts.accountTrie),
		storageTries: ss,
	}
}

type trieAndSnap struct {
	root   common.Hash
	db     Database
	ts     *tries
	snap   snapshot.Snapshot
	hasher crypto.KeccakState
}

func newTrieAndSnap(root common.Hash, db Database, ts *tries, snap snapshot.Snapshot) *trieAndSnap {
	return &trieAndSnap{
		root:   root,
		db:     db,
		ts:     ts,
		snap:   snap,
		hasher: crypto.NewKeccakState(),
	}
}

func (reader *trieAndSnap) Account(address common.Address) (*types.StateAccount, error) {
	var data *types.StateAccount
	if reader.snap != nil {
		acc, err := reader.snap.Account(crypto.HashData(reader.hasher, address.Bytes()))
		if err == nil {
			if acc == nil {
				return nil, nil
			}
			data = &types.StateAccount{
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
		}
	}
	// If snapshot unavailable or reading from it failed, load from the database
	if data == nil {
		var err error
		data, err = reader.ts.accountTrie.TryGetAccount(address)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (reader *trieAndSnap) Storage(address common.Address, storageHash, root common.Hash) ([]byte, error) {
	var (
		enc []byte
		err error
	)
	if reader.snap != nil {
		enc, err = reader.snap.Storage(crypto.HashData(reader.hasher, address.Bytes()), crypto.Keccak256Hash(storageHash.Bytes()))
	}
	if reader.snap == nil || err != nil {
		tr, err := reader.ts.storageTrie(crypto.HashData(reader.hasher, address.Bytes()), root)
		if err != nil {
			return nil, err
		}
		enc, err = tr.TryGet(storageHash.Bytes())
	}
	return enc, err
}

func (reader *trieAndSnap) ContractCode(addrHash common.Hash, hash common.Hash) ([]byte, error) {
	return reader.db.ContractCode(addrHash, hash)
}

func (reader *trieAndSnap) ContractCodeSize(address common.Hash, hash common.Hash) (int, error) {
	return reader.db.ContractCodeSize(address, hash)
}

func (reader *trieAndSnap) update(ts *tries) {
	reader.ts = ts
}
