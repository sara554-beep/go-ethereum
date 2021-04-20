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
	"github.com/ethereum/go-ethereum/core/types"
)

// accessList is an accumulator for the set of accounts and storage slots an EVM
// contract execution touches.
type accessList map[common.Address]accessListSlots

// accessListSlots is an accumulator for the set of storage slots within a single
// contract that an EVM contract execution touches.
type accessListSlots map[common.Hash]struct{}

// newAccessList creates a new accessList.
func newAccessList() accessList {
	return make(map[common.Address]accessListSlots)
}

// addAddress adds an address to the accesslist.
func (al accessList) addAddress(address common.Address) {
	// Set address if not previously present
	if _, present := al[address]; !present {
		al[address] = make(map[common.Hash]struct{})
	}
}

// addSlot adds a storage slot to the accesslist.
func (al accessList) addSlot(address common.Address, slot common.Hash, addAddress bool) {
	// Set address if not previously present
	al.addAddress(address)

	// Set the slot on the surely existent storage set
	al[address][slot] = struct{}{}
}

// equal checks if the content of the current access list is the same as the
// content of the other one.
func (al accessList) equal(other accessList) bool {
	// Cross reference the accounts first
	if len(al) != len(other) {
		return false
	}
	for addr := range al {
		if _, ok := other[addr]; !ok {
			return false
		}
	}
	for addr := range other {
		if _, ok := al[addr]; !ok {
			return false
		}
	}
	// Accounts match, cross reference the storage slots too
	for addr, slots := range al {
		otherslots := other[addr]

		if len(slots) != len(otherslots) {
			return false
		}
		for hash := range slots {
			if _, ok := otherslots[hash]; !ok {
				return false
			}
		}
		for hash := range otherslots {
			if _, ok := slots[hash]; !ok {
				return false
			}
		}
	}
	return true
}

// accesslist converts the accesslist to a types.AccessList.
func (al accessList) accessList() types.AccessList {
	acl := make(types.AccessList, 0, len(al))
	for addr, slots := range al {
		tuple := types.AccessTuple{Address: addr}
		for slot := range slots {
			tuple.StorageKeys = append(tuple.StorageKeys, slot)
		}
		acl = append(acl, tuple)
	}
	return acl
}

// accesslist converts the accesslist to a types.AccessList.
func (al accessList) accounts() []common.Address {
	var ret []common.Address
	for addr := range al {
		ret = append(ret, addr)
	}
	return ret
}

// AccessListTracer is a tracer that accumulates touched accounts and storage
// slots into an internal set.
type AccessListTracer struct {
	excl map[common.Address]struct{} // Set of account to exclude from the list
	list accessList                  // Set of accounts and storage slots touched
}

// NewAccessListTracer creates a new tracer that can generate AccessLists.
// An optional AccessList can be specified to occupy slots and addresses in
// the resulting accesslist.
func NewAccessListTracer(acl types.AccessList, from, to common.Address, precompiles []common.Address) *AccessListTracer {
	excl := map[common.Address]struct{}{
		from: {}, to: {},
	}
	for _, addr := range precompiles {
		excl[addr] = struct{}{}
	}
	list := newAccessList()
	for _, al := range acl {
		if _, ok := excl[al.Address]; !ok {
			list.addAddress(al.Address)
		}
		for _, slot := range al.StorageKeys {
			list.addSlot(al.Address, slot, true)
		}
	}
	return &AccessListTracer{
		excl: excl,
		list: list,
	}
}

func (a *AccessListTracer) CaptureStart(env *EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
}

// CaptureState captures all opcodes that touch storage or addresses and adds them to the accesslist.
func (a *AccessListTracer) CaptureState(env *EVM, pc uint64, op OpCode, gas, cost uint64, scope *ScopeContext, rData []byte, depth int, err error) {
	stack := scope.Stack
	if (op == SLOAD || op == SSTORE) && stack.len() >= 1 {
		slot := common.Hash(stack.data[stack.len()-1].Bytes32())
		a.list.addSlot(scope.Contract.Address(), slot, true)
	}
	if (op == EXTCODECOPY || op == EXTCODEHASH || op == EXTCODESIZE || op == BALANCE || op == SELFDESTRUCT) && stack.len() >= 1 {
		addr := common.Address(stack.data[stack.len()-1].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
		}
	}
	if (op == DELEGATECALL || op == CALL || op == STATICCALL || op == CALLCODE) && stack.len() >= 5 {
		addr := common.Address(stack.data[stack.len()-2].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
		}
	}
}

func (*AccessListTracer) CaptureFault(env *EVM, pc uint64, op OpCode, gas, cost uint64, scope *ScopeContext, depth int, err error) {
}

func (*AccessListTracer) CaptureEnd(output []byte, gasUsed uint64, t time.Duration, err error) {}

// AccessList returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessList() types.AccessList {
	return a.list.accessList()
}

// Equal returns if the content of two access list traces are equal.
func (a *AccessListTracer) Equal(other *AccessListTracer) bool {
	return a.list.equal(other.list)
}

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
	a.context.AddAccessedAccount(a.readList.Accounts(), a.writeList.accounts(), a.count)
	a.context.AddAccessedSlot(a.readList.Slots(), a.writeList.Slots(), a.count)
	a.readList = nil
	a.writeList = nil
	a.count++
}
