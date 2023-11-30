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

import "github.com/ethereum/go-ethereum/common"

type actionType int

const (
	updateOp actionType = iota
	deleteOp
)

type state struct {
	actions []actionType // list of pending actions
	next    int          // the position of next action to execute
}

type tracker struct {
	objects map[common.Address]*state
}

func (t *tracker) markDirty(addr common.Address) {
	if _, ok := t.objects[addr]; !ok {
		t.objects[addr] = &state{next: 0}
	}
	t.objects[addr].actions = append(t.objects[addr].actions, updateOp)
}

func (t *tracker) markDestruct(addr common.Address) {
	if _, ok := t.objects[addr]; !ok {
		t.objects[addr] = &state{next: 0}
	}
	t.objects[addr].actions = append(t.objects[addr].actions, deleteOp)
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

func (t *tracker) dirties() ([]common.Address, []bool) {
	var (
		addresses []common.Address
		destructs []bool
	)
	for addr, obj := range t.objects {
		addresses = append(addresses, addr)
		destructs = append(destructs, obj.actions[len(obj.actions)-1] == deleteOp)
	}
	return addresses, destructs
}
