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
	"github.com/ethereum/go-ethereum/metrics"
)

// ContractCode represents the contract code with associated metadata.
type ContractCode struct {
	Address common.Address // Address is the unique identifier of the contract.
	Hash    common.Hash    // Hash is the cryptographic hash of the contract code.
	Blob    []byte         // Blob is the binary representation of the contract code.
}

// AccountDelete represents a data structure for deleting an Ethereum account.
type AccountDelete struct {
	Address        common.Address         // Address is the account identifier
	Origin         []byte                 // Origin is the original value of account data in slim-RLP encoding.
	StoragesOrigin map[common.Hash][]byte // StoragesOrigin stores the original values of mutated slots in prefix-zero trimmed RLP format.
}

// AccountUpdate represents a data structure for updating an Ethereum account.
type AccountUpdate struct {
	Address        common.Address         // Address is the account identifier
	Data           []byte                 // Data is the slim-RLP encoded account data.
	Origin         []byte                 // Origin is the original value of account data in slim-RLP encoding.
	Code           *ContractCode          // Code represents mutated contract code; nil means it's not modified.
	Storages       map[common.Hash][]byte // Storages stores mutated slots in prefix-zero-trimmed RLP format.
	StoragesOrigin map[common.Hash][]byte // StoragesOrigin stores the original values of mutated slots in prefix-zero trimmed RLP format.
}

// Update represents the difference between two states resulting from state execution.
// It contains information about mutated contract codes, accounts, and storage slots,
// along with their original values.
type Update struct {
	Destructs      map[common.Hash]struct{}                  // Destructs contains the list of destructed accounts.
	Accounts       map[common.Hash][]byte                    // Accounts stores mutated accounts in 'slim RLP' encoding.
	AccountsOrigin map[common.Address][]byte                 // AccountsOrigin stores the original values of mutated accounts in 'slim RLP' encoding.
	Storages       map[common.Hash]map[common.Hash][]byte    // Storages stores mutated slots in prefix-zero trimmed RLP format.
	StoragesOrigin map[common.Address]map[common.Hash][]byte // StoragesOrigin stores the original values of mutated slots in prefix-zero trimmed RLP format.
	Codes          []*ContractCode                           // Codes contains the list of dirty codes.

	// internal counters
	accountUpdate    int
	accountDelete    int
	storageUpdate    int
	storageDelete    int
	contractCodeSize int
}

// NewUpdate constructs a state update object, representing the differences
// between two states by performing state execution. It takes a map of deleted
// accounts and a map of updated accounts to generate a comprehensive Update.
func NewUpdate(deletes map[common.Hash]*AccountDelete, updates map[common.Hash]*AccountUpdate) *Update {
	var (
		destructs      = make(map[common.Hash]struct{})
		accounts       = make(map[common.Hash][]byte)
		accountsOrigin = make(map[common.Address][]byte)
		storages       = make(map[common.Hash]map[common.Hash][]byte)
		storagesOrigin = make(map[common.Address]map[common.Hash][]byte)
		codes          []*ContractCode

		storageUpdate int
		storageDelete int
		codeSize      int
	)
	// Process deleted accounts as the first step. Might be possible
	// some accounts are destructed and resurrected in the same block,
	// therefore, deletions need to be handled first and the combine
	// the mutations by resurrected accounts later.
	for addrHash, delete := range deletes {
		address := delete.Address
		destructs[addrHash] = struct{}{}
		accountsOrigin[address] = delete.Origin
		if len(delete.StoragesOrigin) > 0 {
			storagesOrigin[address] = delete.StoragesOrigin
			storageDelete += len(delete.StoragesOrigin)
		}
	}
	// Process updated accounts then.
	for addrHash, update := range updates {
		// Aggregate dirty contract codes.
		address := update.Address
		if update.Code != nil {
			codes = append(codes, update.Code)
			codeSize += len(update.Code.Blob)
		}
		accounts[addrHash] = update.Data

		// Original account data is only aggregated if it's not tracked yet,
		// otherwise it means the account with same address is destructed in
		// the same block and the origin data associated here is incorrect.
		if _, found := accountsOrigin[address]; !found {
			accountsOrigin[address] = update.Origin
		}
		if len(update.Storages) > 0 {
			storages[addrHash] = update.Storages
			storageUpdate += len(update.Storages)
		}
		// Original storage data is only aggregated if it's not tracked yet,
		// otherwise it means the account with same address is destructed in
		// the same block and the leftover storage slots belonging to the
		// deleted one are already recorded as the original value.
		if len(update.StoragesOrigin) > 0 {
			origin := storagesOrigin[address]
			if origin == nil {
				origin = make(map[common.Hash][]byte)
				storagesOrigin[address] = origin
			}
			for key, slot := range update.StoragesOrigin {
				if _, found := origin[key]; !found {
					origin[key] = slot
				}
			}
		}
	}
	return &Update{
		Destructs:        destructs,
		Accounts:         accounts,
		AccountsOrigin:   accountsOrigin,
		Storages:         storages,
		StoragesOrigin:   storagesOrigin,
		Codes:            codes,
		accountUpdate:    len(updates),
		accountDelete:    len(deletes),
		storageUpdate:    storageUpdate,
		storageDelete:    storageDelete,
		contractCodeSize: codeSize,
	}
}

// setMetrics uploads the metrics data.
func (update *Update) setMetrics() {
	if !metrics.EnabledExpensive {
		return
	}
	accountUpdatedMeter.Mark(int64(update.accountUpdate))
	storageUpdatedMeter.Mark(int64(update.storageUpdate))
	accountDeletedMeter.Mark(int64(update.accountDelete))
	storageDeletedMeter.Mark(int64(update.storageDelete))
	contractCodeCountMeter.Mark(int64(len(update.Codes)))
	contractCodeSizeMeter.Mark(int64(update.contractCodeSize))
}
