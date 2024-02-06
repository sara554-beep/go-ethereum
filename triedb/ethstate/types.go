// Copyright 2024 The go-ethereum Authors
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

package ethstate

import "github.com/ethereum/go-ethereum/common"

// Update encapsulates information about state mutations, including both the changes
// made and their original values, within the context of a state transition.
type Update struct {
	DestructSet map[common.Hash]struct{}               // Keyed markers for deleted (and potentially) recreated accounts
	AccountData map[common.Hash][]byte                 // Keyed accounts for direct retrieval (nil is not expected)
	StorageData map[common.Hash]map[common.Hash][]byte // Keyed storage slots for direct retrieval. one per account (nil means deleted)

	// AccountOrigin represents the account data before the state transition,
	// keyed by the account address. The nil value means the account was not
	// present before.
	AccountOrigin map[common.Address][]byte

	// StorageOrigin represents the storage data before the state transition,
	// keyed by the account address and slot key hash. The nil value means the
	// slot was not present.
	StorageOrigin map[common.Address]map[common.Hash][]byte
}
