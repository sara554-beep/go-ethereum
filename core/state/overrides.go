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
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

//go:generate go run github.com/fjl/gencodec -type OverrideAccount -field-override overrideMarshaling -out gen_override_json.go

// OverrideAccount indicates the overriding fields of account during the
// execution of a message call.
//
// Note, state and stateDiff can't be specified at the same time.
// - If state is set, state lookup will be performed in this state set first;
// - If statDiff is set, state lookup will be performed in the diff set first;
type OverrideAccount struct {
	Nonce     *uint64                      `json:"nonce"`
	Code      *[]byte                      `json:"code"`
	Balance   **big.Int                    `json:"balance"`
	State     *map[common.Hash]common.Hash `json:"state"`
	StateDiff *map[common.Hash]common.Hash `json:"stateDiff"`
}

// nolint
type overrideMarshaling struct {
	Nonce   *hexutil.Uint64
	Code    *hexutil.Bytes
	Balance **hexutil.Big
}

type readerWithOverrides struct {
	reader    Reader
	overrides map[common.Address]OverrideAccount
}

func sanitize(overrides map[common.Address]OverrideAccount) error {
	for addr, account := range overrides {
		if account.State != nil && account.StateDiff != nil {
			return fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr.Hex())
		}
	}
	return nil
}

// newReaderWithOverrides constructs a state header with provided state overrides.
// It's mostly used in RPC serving for development or testing purposes.
func newReaderWithOverrides(r Reader, overrides map[common.Address]OverrideAccount) (*readerWithOverrides, error) {
	if err := sanitize(overrides); err != nil {
		return nil, err
	}
	return &readerWithOverrides{
		reader:    r,
		overrides: overrides,
	}, nil
}

// Account implements Reader, retrieving the account specified by the address
// from the associated state. Nil account is returned if the requested one is
// not existent in the state. Error is only returned if any unexpected error occurs.
func (r *readerWithOverrides) Account(addr common.Address) (*types.StateAccount, error) {
	account, err := r.reader.Account(addr)
	if err != nil {
		return nil, err
	}
	override, found := r.overrides[addr]
	if found {
		if override.Nonce != nil {
			account.Nonce = *override.Nonce
		}
		if override.Balance != nil {
			account.Balance = *override.Balance
		}
	}
	return account, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key from the associated state. Null slot is returned if
// the requested one is not existent in the state. Error is only returned if
// any unexpected error occurs.
func (r *readerWithOverrides) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	override, found := r.overrides[addr]
	if found {
		if override.State != nil {
			return (*override.State)[key], nil
		}
		if override.StateDiff != nil {
			slot, found := (*override.StateDiff)[key]
			if found {
				return slot, nil
			}
		}
	}
	return r.reader.Storage(addr, root, key)
}

// Copy returns a deep-copied reader.
func (r *readerWithOverrides) Copy() Reader {
	overrides := make(map[common.Address]OverrideAccount)
	for addr, override := range r.overrides {
		overrides[addr] = override
	}
	return &readerWithOverrides{
		reader:    r.reader.Copy(),
		overrides: overrides,
	}
}
