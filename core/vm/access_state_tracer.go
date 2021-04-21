// Copyright 2021 The go-ethereum Authors
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

package vm

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// accessList is an accumulator for the set of accounts and storage slots an EVM
// contract execution touches.
type accessState struct {
	accounts map[common.Address]struct{}
	slots    map[common.Address]map[common.Hash]struct{}
}

func newAccessState() *accessState {
	return &accessState{
		accounts: make(map[common.Address]struct{}),
		slots:    make(map[common.Address]map[common.Hash]struct{}),
	}
}

// addAddress adds an address to the accesslist.
func (al *accessState) addAddress(address common.Address) {
	al.accounts[address] = struct{}{}
}

// addSlot adds a storage slot to the accesslist.
func (al *accessState) addSlot(address common.Address, slot common.Hash) {
	if _, ok := al.slots[address]; !ok {
		al.slots[address] = make(map[common.Hash]struct{})
	}
	al.slots[address][slot] = struct{}{}
}

// accesslist converts the accesslist to a types.AccessList.
func (al *accessState) Accounts() []string {
	var ret []string
	for acct := range al.accounts {
		ret = append(ret, acct.String())
	}
	return ret
}

// accesslist converts the accesslist to a types.AccessList.
func (al *accessState) Slots() []string {
	var ret []string
	for acct, slots := range al.slots {
		for slot := range slots {
			ret = append(ret, fmt.Sprintf("%s-%s", acct.String(), slot.String()))
		}
	}
	return ret
}

// AccessStateTracer is a tracer that accumulates touched accounts and storage
// slots into an internal set which differentiates read and write.
type AccessStateTracer struct {
	count     int           // The number of transactions have been traced.
	readList  *accessState  // Set of accounts and storage slots read in the transaction scope
	writeList *accessState  // Set of accounts and storage slots written in the transaction scope
	context   *BlockContext // The destination to report the tracing result
}

func NewAccessStateTracer(context *BlockContext) *AccessStateTracer {
	return &AccessStateTracer{context: context}
}

func (a *AccessStateTracer) CaptureStart(env *EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	a.readList = newAccessState()
	a.writeList = newAccessState()
}

// CaptureState captures all opcodes that touch storage or addresses and adds them to the accesslist.
func (a *AccessStateTracer) CaptureState(env *EVM, pc uint64, op OpCode, gas, cost uint64, scope *ScopeContext, rData []byte, depth int, err error) {
	stack := scope.Stack
	if op == SLOAD && stack.len() >= 1 {
		slot := common.Hash(stack.data[stack.len()-1].Bytes32())
		a.readList.addSlot(scope.Contract.Address(), slot)
	}
	if op == SSTORE && stack.len() >= 1 {
		slot := common.Hash(stack.data[stack.len()-1].Bytes32())
		a.writeList.addSlot(scope.Contract.Address(), slot)
	}
	if (op == EXTCODECOPY || op == EXTCODEHASH || op == EXTCODESIZE || op == BALANCE) && stack.len() >= 1 {
		addr := common.Address(stack.data[stack.len()-1].Bytes20())
		a.readList.addAddress(addr)
	}
	if op == SELFDESTRUCT && stack.len() >= 1 {
		addr := common.Address(stack.data[stack.len()-1].Bytes20())
		a.writeList.addAddress(scope.Contract.Address())
		a.writeList.addAddress(addr)
	}
	if (op == CREATE || op == CREATE2) && stack.len() >= 1 {
		// TODO the created account should be tracked
		a.writeList.addAddress(scope.Contract.Address()) // Nonce is changed
	}
	// TODO the plain transfer should be tracked
	//if (op == DELEGATECALL || op == CALL || op == STATICCALL || op == CALLCODE) && stack.len() >= 5 {
	//	addr := common.Address(stack.data[stack.len()-2].Bytes20())
	//	a.readList.addAddress(addr) // Not sure it's needed...
	//}
}

func (*AccessStateTracer) CaptureFault(env *EVM, pc uint64, op OpCode, gas, cost uint64, scope *ScopeContext, depth int, err error) {
}

func (a *AccessStateTracer) CaptureEnd(output []byte, gasUsed uint64, t time.Duration, err error) {
	a.context.AddAccessedAccount(a.readList.Accounts(), a.writeList.Accounts(), a.count)
	a.context.AddAccessedSlot(a.readList.Slots(), a.writeList.Slots(), a.count)
	a.readList = nil
	a.writeList = nil
	a.count++
}
