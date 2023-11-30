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
	"github.com/ethereum/go-ethereum/core/types"
)

type actionType int

const (
	updateOp actionType = iota
	deleteOp
)

type state struct {
	actions []actionType
	next    int
}

type tracker struct {
	objects   map[common.Address]*state
	destructs map[common.Address]*types.StateAccount
}

func newTracker() *tracker {
	return &tracker{
		objects:   make(map[common.Address]*state),
		destructs: make(map[common.Address]*types.StateAccount),
	}
}

func (t *tracker) mark(addr common.Address, action actionType) {
	if _, ok := t.objects[addr]; !ok {
		t.objects[addr] = &state{next: 0}
	}
	t.objects[addr].actions = append(t.objects[addr].actions, action)
}

func (t *tracker) markDirty(addr common.Address) {
	t.mark(addr, updateOp)
}

func (t *tracker) markDestruct(addr common.Address, origin *types.StateAccount) {
	if _, ok := t.destructs[addr]; !ok {
		t.destructs[addr] = origin
	}
	t.mark(addr, deleteOp)
}

func (t *tracker) isDestructed(addr common.Address) bool {
	_, ok := t.destructs[addr]
	return ok
}

func (t *tracker) execute(fn func(addr common.Address, destruct bool) error) error {
	for addr, obj := range t.objects {
		if obj.next == len(obj.actions) {
			continue
		}
		if err := fn(addr, obj.actions[len(obj.actions)-1] == deleteOp); err != nil {
			return err
		}
		obj.next = len(obj.actions)
	}
	return nil
}

func (t *tracker) allDone() bool {
	for _, obj := range t.objects {
		if obj.next != len(obj.actions) {
			return false
		}
	}
	return true
}

func (t *tracker) dirties() []common.Address {
	var addresses []common.Address
	for addr, obj := range t.objects {
		if obj.actions[len(obj.actions)-1] == deleteOp {
			continue
		}
		addresses = append(addresses, addr)
	}
	return addresses
}
