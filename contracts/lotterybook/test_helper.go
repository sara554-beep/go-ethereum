// Copyright 2019 The go-ethereum Authors
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

package lotterybook

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
)

type testEnv struct {
	draweeDb   ethdb.Database
	drawerDb   ethdb.Database
	draweeKey  *ecdsa.PrivateKey
	draweeAddr common.Address
	drawerKey  *ecdsa.PrivateKey
	drawerAddr common.Address
	backend    *backends.SimulatedBackend
}

func newTestEnv(t *testing.T) *testEnv {
	db1, db2 := rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase()
	key, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)
	addr2 := crypto.PubkeyToAddress(key2.PublicKey)
	sim := backends.NewSimulatedBackend(core.GenesisAlloc{addr: {Balance: big.NewInt(2e18)}, addr2: {Balance: big.NewInt(2e18)}}, 10000000)
	return &testEnv{
		draweeDb:   db1,
		draweeKey:  key,
		draweeAddr: addr,
		drawerDb:   db2,
		drawerKey:  key2,
		drawerAddr: addr2,
		backend:    sim,
	}
}

func (env *testEnv) close() { env.backend.Close() }

func (env *testEnv) commitEmptyBlocks(number int) {
	for i := 0; i < number; i++ {
		env.backend.Commit()
	}
}

func (env *testEnv) commitEmptyUntil(end uint64) {
	for {
		if env.backend.Blockchain().CurrentHeader().Number.Uint64() == end {
			return
		}
		env.backend.Commit()
	}
}

func (env *testEnv) checkEvent(sink chan []LotteryEvent, expect []LotteryEvent) bool {
	select {
	case ev := <-sink:
		if len(ev) != len(expect) {
			return false
		}
		for index := range ev {
			if ev[index].Id != expect[index].Id {
				return false
			}
			if ev[index].Status != expect[index].Status {
				fmt.Println("EXPECT", expect[index].Status, ev[index].Status)
				return false
			}
		}
	case <-time.NewTimer(time.Second).C:
		return false
	}
	select {
	case <-sink:
		return false // Unexpect incoming events
	case <-time.NewTimer(time.Microsecond * 100).C:
		return true
	}
}
