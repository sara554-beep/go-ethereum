// Copyright 2020 The go-ethereum Authors
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

package contract

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
)

// RuntimeCodehash is the code hash of the lotterybook contract at runtime
// and can act as a unique identifier for the contract.
var RuntimeCodehash common.Hash

func init() {
	// Scripts to evaluate the runtime codehash.
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)
	backend := backends.NewSimulatedBackend(core.GenesisAlloc{addr: {Balance: big.NewInt(2e18)}}, 10000000)
	defer backend.Close()

	contractAddr, _, _, _ := DeployLotteryBook(bind.NewKeyedTransactor(key), backend)
	backend.Commit()
	code, _ := backend.CodeAt(context.Background(), contractAddr, nil)
	RuntimeCodehash = crypto.Keccak256Hash(code)
}
