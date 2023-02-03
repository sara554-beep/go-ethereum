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
	"github.com/ethereum/go-ethereum/core/types"
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
	Storage(address common.Hash, storageHash common.Hash) ([]byte, error)

	// ContractCode retrieves a particular contract's code.
	ContractCode(address common.Address, hash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(address common.Address, hash common.Hash) (int, error)
}
