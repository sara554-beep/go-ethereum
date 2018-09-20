// Copyright 2018 The go-ethereum Authors
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

package registrar

import (
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/registrar/contract"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/light"
)

var (
	key, _    = crypto.GenerateKey()
	addr      = crypto.PubkeyToAddress(key.PublicKey)
	emptyHash = [32]byte{}

	checkpoint0 = light.TrustedCheckpoint{
		SectionIdx:  0,
		SectionHead: common.HexToHash("0x7fa3c32f996c2bfb41a1a65b3d8ea3e0a33a1674cde43678ad6f4235e764d17d"),
		CHTRoot:     common.HexToHash("0x98fc5d3de23a0fecebad236f6655533c157d26a1aedcd0852a514dc1169e6350"),
		BloomRoot:   common.HexToHash("0x99b5adb52b337fe25e74c1c6d3835b896bd638611b3aebddb2317cce27a3f9fa"),
	}
	checkpoint1 = light.TrustedCheckpoint{
		SectionIdx:  1,
		SectionHead: common.HexToHash("0x2d4dee68102125e59b0cc61b176bd89f0d12b3b91cfaf52ef8c2c82fb920c2d2"),
		CHTRoot:     common.HexToHash("0x7d428008ece3b4c4ef5439f071930aad0bb75108d381308df73beadcd01ded95"),
		BloomRoot:   common.HexToHash("0x652571f7736de17e7bbb427ac881474da684c6988a88bf51b10cca9a2ee148f4"),
	}
)

var (
	// The block frequency for creating checkpoint(only used in test)
	sectionSize = big.NewInt(2048)

	// The number of confirmations needed to generate a checkpoint(only used in test).
	processConfirms = big.NewInt(16)

	// The maximum time range in which a registered checkpoint is allowed to be modified(only used in test).
	freezeThreshold = big.NewInt(512)
)

// validateOperation executes the operation, watches and delivers all events fired by the backend and ensures the
// correctness by assert function.
func validateOperation(t *testing.T, c *contract.Contract, backend *backends.SimulatedBackend, operation func(),
	assert func(<-chan *contract.ContractNewCheckpointEvent) error, opName string) {
	// Watch all events and deliver them to assert function
	var (
		sink   = make(chan *contract.ContractNewCheckpointEvent)
		sub, _ = c.WatchNewCheckpointEvent(nil, sink, nil)
	)
	defer func() {
		// Close all subscribers
		sub.Unsubscribe()
	}()
	operation()

	// flush pending block
	backend.Commit()
	if err := assert(sink); err != nil {
		t.Errorf("operation {%s} failed, err %s", opName, err)
	}
}

// validateEvents checks that the correct number of contract events
// fired by contract backend.
func validateEvents(target int, sink interface{}) (bool, []reflect.Value) {
	chanval := reflect.ValueOf(sink)
	chantyp := chanval.Type()
	if chantyp.Kind() != reflect.Chan || chantyp.ChanDir()&reflect.RecvDir == 0 {
		return false, nil
	}
	count := 0
	var recv []reflect.Value
	timeout := time.After(1 * time.Second)
	cases := []reflect.SelectCase{{Chan: chanval, Dir: reflect.SelectRecv}, {Chan: reflect.ValueOf(timeout), Dir: reflect.SelectRecv}}
	for {
		chose, v, _ := reflect.Select(cases)
		if chose == 1 {
			// Not enough event received
			return false, nil
		}
		count += 1
		recv = append(recv, v)
		if count == target {
			break
		}
	}
	done := time.After(50 * time.Millisecond)
	cases = cases[:1]
	cases = append(cases, reflect.SelectCase{Chan: reflect.ValueOf(done), Dir: reflect.SelectRecv})
	chose, _, _ := reflect.Select(cases)
	// If chose equal 0, it means receiving redundant events.
	return chose == 1, recv
}

func signCheckpoint(privateKey *ecdsa.PrivateKey, index uint64, hash common.Hash) []byte {
	buf := make([]byte, 8+common.HashLength)
	binary.BigEndian.PutUint64(buf, index)
	copy(buf[8:], hash.Bytes())
	sig, _ := crypto.Sign(hash.Bytes(), privateKey)
	return sig
}

func recoverSigner(hash [32]byte, sig []byte, expectSigner common.Address) bool {
	pubkey, err := crypto.Ecrecover(hash[:], sig)
	if err != nil {
		return false
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer == expectSigner
}

// Tests checkpoint managements.
func TestCheckpointRegister(t *testing.T) {
	// Deploy registrar contract
	transactOpts := bind.NewKeyedTransactor(key)
	contractBackend := backends.NewSimulatedBackend(nil, core.GenesisAlloc{addr: {Balance: big.NewInt(1000000000)}}, 10000000)
	_, _, c, err := contract.DeployContract(transactOpts, contractBackend, []common.Address{addr}, sectionSize, processConfirms, freezeThreshold)
	if err != nil {
		t.Error("deploy registrar contract failed", err)
	}
	contractBackend.Commit()

	// Register unstable checkpoint
	validateOperation(t, c, contractBackend, func() {
		c.SetCheckpoint(transactOpts, big.NewInt(0), checkpoint0.Hash(), signCheckpoint(key, 0, checkpoint0.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(0))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != emptyHash {
			return errors.New("unstable checkpoint should be banned")
		}
		return nil
	}, "register unstable checkpoint")

	contractBackend.ShiftBlocks(int(sectionSize.Uint64() + processConfirms.Uint64()))

	// Register by unauthorized user
	validateOperation(t, c, contractBackend, func() {
		user2, _ := crypto.GenerateKey()
		unauthorized := bind.NewKeyedTransactor(user2)
		c.SetCheckpoint(unauthorized, big.NewInt(0), checkpoint0.Hash(), signCheckpoint(user2, 0, checkpoint0.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(0))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != emptyHash {
			return errors.New("checkpoint from unauthorized user should be banned")
		}
		return nil
	}, "register by unauthorized user")

	// Register a stable checkpoint
	validateOperation(t, c, contractBackend, func() {
		c.SetCheckpoint(transactOpts, big.NewInt(0), checkpoint0.Hash(), signCheckpoint(key, 0, checkpoint0.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(0))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != checkpoint0.Hash() {
			return errors.New("register stable checkpoint failed")
		}
		if valid, recv := validateEvents(1, events); !valid {
			return errors.New("receive incorrect number of events")
		} else {
			event := recv[0].Interface().(*contract.ContractNewCheckpointEvent)
			if !recoverSigner(event.CheckpointHash, event.Signature, addr) {
				return errors.New("recover signer failed")
			}
		}
		return nil
	}, "register stable checkpoint")

	// Modify the latest checkpoint
	newCheckpoint := light.TrustedCheckpoint{
		SectionIdx:  0,
		SectionHead: common.HexToHash("0x7fa3c32f996c2bfb41a1a65b3d8ea3e0a33a1674cde43678ad6f4235e764d17d"),
		CHTRoot:     common.HexToHash("0x98fc5d3de23a0fecebad236f6655533c157d26a1aedcd0852a514dc1169e6350"),
		BloomRoot:   common.HexToHash("0xdeadbeef"),
	}
	validateOperation(t, c, contractBackend, func() {
		c.SetCheckpoint(transactOpts, big.NewInt(0), newCheckpoint.Hash(), signCheckpoint(key, 0, newCheckpoint.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(0))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != newCheckpoint.Hash() {
			return errors.New("register stable checkpoint failed")
		}
		if valid, recv := validateEvents(1, events); !valid {
			return errors.New("receive incorrect number of events")
		} else {
			event := recv[0].Interface().(*contract.ContractNewCheckpointEvent)
			if !recoverSigner(event.CheckpointHash, event.Signature, addr) {
				return errors.New("recover signer failed")
			}
		}
		return nil
	}, "modify latest checkpoint")

	// Register checkpoint 1
	contractBackend.ShiftBlocks(int(sectionSize.Uint64()))

	validateOperation(t, c, contractBackend, func() {
		c.SetCheckpoint(transactOpts, big.NewInt(1), checkpoint1.Hash(), signCheckpoint(key, 1, checkpoint1.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(1))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != checkpoint1.Hash() {
			return errors.New("register stable checkpoint failed")
		}
		if valid, recv := validateEvents(1, events); !valid {
			return errors.New("receive incorrect number of events")
		} else {
			event := recv[0].Interface().(*contract.ContractNewCheckpointEvent)
			if !recoverSigner(event.CheckpointHash, event.Signature, addr) {
				return errors.New("recover signer failed")
			}
		}
		return nil
	}, "register stable checkpoint 1")

	// Modify the registered checkpoint after a very long time
	contractBackend.ShiftBlocks(int(freezeThreshold.Uint64()))

	newCheckpoint1 := light.TrustedCheckpoint{
		SectionIdx:  1,
		SectionHead: common.HexToHash("0x2d4dee68102125e59b0cc61b176bd89f0d12b3b91cfaf52ef8c2c82fb920c2d2"),
		CHTRoot:     common.HexToHash("0x7d428008ece3b4c4ef5439f071930aad0bb75108d381308df73beadcd01ded95"),
		BloomRoot:   common.HexToHash("0xdeadbeef"),
	}
	validateOperation(t, c, contractBackend, func() {
		c.SetCheckpoint(transactOpts, big.NewInt(1), newCheckpoint1.Hash(), signCheckpoint(key, 1, newCheckpoint1.Hash()))
	}, func(events <-chan *contract.ContractNewCheckpointEvent) error {
		hash, err := c.GetCheckpoint(nil, big.NewInt(1))
		if err != nil {
			return errors.New("get checkpoint failed")
		}
		if hash != checkpoint1.Hash() {
			return errors.New("checkpoint modified out of allowed time range")
		}
		return nil
	}, "modify checkpoint out of allowed time range")
}
