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

package clientpool

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/les/utils"
	"github.com/ethereum/go-ethereum/p2p/nodestate"
)

// balanceTrackerSetup contains node state flags and fields used by balance manager.
type balanceTrackerSetup struct {
	// controlled by priorityPool
	PriorityFlag, UpdateFlag nodestate.Flags
	balanceField             nodestate.Field

	// External field defintions
	statusField   nodestate.Field
	capacityField nodestate.Field
}

// newBalanceTrackerSetup creates a new balanceTrackerSetup and initializes the fields
// and flags controlled by balanceManager
func newBalanceTrackerSetup(setup *nodestate.Setup) balanceTrackerSetup {
	return balanceTrackerSetup{
		// PriorityFlag is set if the node has a positive balance
		PriorityFlag: setup.NewFlag("priority"),

		// UpdateFlag set and then immediately reset if the balance has been updated and
		// therefore priority is suddenly changed
		UpdateFlag: setup.NewFlag("balanceUpdate"), // todo ??

		// balanceField contains the NodeBalance struct which implements nodePriority,
		// allowing on-demand priority calculation and future priority estimation
		balanceField: setup.NewField("balance", reflect.TypeOf(&NodeBalance{})),
	}
}

// Connect sets the fields used by balanceManager as an input
func (bts *balanceTrackerSetup) Connect(statusField nodestate.Field, capacityField nodestate.Field) {
	bts.statusField = statusField
	bts.capacityField = capacityField
}

// balanceManager tracks positive and negative balances for connected nodes.
//
// Both balances are exponentially expired values. Costs are deducted from the
// positive balance if present, otherwise added to the negative balance.
//
// If the capacity is non-zero then a time cost is applied continuously while
// individual request costs are applied immediately.
//
// The two balances are translated into a single priority value that also depends
// on the actual capacity.
type balanceManager struct {
	balanceTrackerSetup
	clock                      mclock.Clock
	lock                       sync.Mutex
	ns                         *nodestate.NodeStateMachine
	ndb                        *nodeDB
	posExp, negExp             utils.ValueExpirer
	posExpTC, negExpTC         uint64
	posThreshold, negThreshold uint64

	activeBalance   utils.ExpiredValue
	inactiveBalance utils.ExpiredValue
	totalTimer      *utils.ThresholdTimer
	quit            chan struct{}
}

// NewBalanceTracker creates a new balanceManager
func NewBalanceTracker(ns *nodestate.NodeStateMachine, setup balanceTrackerSetup, db ethdb.KeyValueStore, clock mclock.Clock, posExp, negExp utils.ValueExpirer) *balanceManager {
	ndb := newNodeDB(db, clock)
	bt := &balanceManager{
		ns:                  ns,
		balanceTrackerSetup: setup,
		ndb:                 ndb,
		clock:               clock,
		posExp:              posExp,
		negExp:              negExp,
		totalTimer:          utils.NewThresholdTimer(clock, time.Second*10),
		quit:                make(chan struct{}),
	}
	bt.ndb.forEachBalance(false, func(id enode.ID, balance utils.ExpiredValue) bool {
		bt.inactiveBalance.AddExp(balance)
		return true
	})

	ns.SubscribeField(bt.capacityField, func(node *enode.Node, state nodestate.Flags, oldvalue, newvalue interface{}) {
		item := ns.GetField(node, bt.balanceField)
		if item == nil {
			return
		}
		balance := item.(*NodeBalance)
		balance.setCapacity(newValue.(uint64))
	})

	ns.SubscribeField(bt.statusField, func(node *enode.Node, state nodestate.Flags, oldvalue, newvalue interface{}) {
		if newvalue == nil {
			return
		}
		client := newvalue.(clientStatus)
		switch client.status {
		case connected:
			ns.SetField(node, bt.balanceField, bt.newNodeBalance(node, client.ipaddr))
		case active:
			item := ns.GetField(node, bt.balanceField)
			if item == nil {
				return
			}
			item.(*NodeBalance).activate()
		case inactive:
			item := ns.GetField(node, bt.balanceField)
			if item == nil {
				return
			}
			item.(*NodeBalance).deactivate()
		case disconnected:
			ns.SetField(node, bt.balanceField, nil)
			ns.SetState(node, nodestate.Flags{}, bt.PriorityFlag, 0)
		}
	})
	// The positive and negative balances of clients are stored in database
	// and both of these decay exponentially over time. Delete them if the
	// value is small enough.
	bt.ndb.evictCallBack = bt.canDropBalance

	go func() {
		for {
			select {
			case <-clock.After(persistExpirationRefresh):
				now := clock.Now()
				bt.ndb.setExpiration(posExp.LogOffset(now), negExp.LogOffset(now))
			case <-bt.quit:
				return
			}
		}
	}()
	return bt
}

// Stop saves expiration offset and unsaved node balances and shuts balanceManager down
func (m *balanceManager) Stop() {
	now := m.clock.Now()
	m.ndb.setExpiration(m.posExp.LogOffset(now), m.negExp.LogOffset(now))
	close(m.quit)
	m.ns.ForEach(nodestate.Flags{}, nodestate.Flags{}, func(node *enode.Node, state nodestate.Flags) {
		if n, ok := m.ns.GetField(node, m.balanceField).(*NodeBalance); ok {
			n.lock.Lock()
			n.storeBalance(true, true)
			n.lock.Unlock()
			m.ns.SetField(node, m.balanceField, nil)
		}
	})
	m.ndb.close()
}

// GetPosBalanceIDs lists node IDs with an associated positive balance
func (m *balanceManager) GetPosBalanceIDs(start, stop enode.ID, maxCount int) (result []enode.ID) {
	return m.ndb.getPosBalanceIDs(start, stop, maxCount)
}

// SetExpirationTCs sets positive and negative token expiration time constants.
// Specified in seconds, 0 means infinite (no expiration).
func (m *balanceManager) SetExpirationTCs(pos, neg uint64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.posExpTC, m.negExpTC = pos, neg
	now := m.clock.Now()
	if pos > 0 {
		m.posExp.SetRate(now, 1/float64(pos*uint64(time.Second)))
	} else {
		m.posExp.SetRate(now, 0)
	}
	if neg > 0 {
		m.negExp.SetRate(now, 1/float64(neg*uint64(time.Second)))
	} else {
		m.negExp.SetRate(now, 0)
	}
}

// GetExpirationTCs returns the current positive and negative token expiration
// time constants
func (m *balanceManager) GetExpirationTCs() (pos, neg uint64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.posExpTC, m.negExpTC
}

// newNodeBalance loads balances from the database and creates a NodeBalance instance
// for the given node. It also sets the PriorityFlag and adds balanceCallbackZero if
// the node has a positive balance.
func (m *balanceManager) newNodeBalance(node *enode.Node, ipaddr string) *NodeBalance {
	pb := m.ndb.getOrNewBalance(node.ID().Bytes(), false)
	nb := m.ndb.getOrNewBalance([]byte(ipaddr), true) // todo
	n := &NodeBalance{
		bt:        m,
		node:      node,
		ipaddr:    ipaddr,
		balance:   balance{pos: pb, neg: nb},
		initTime:  m.clock.Now(),
		diffTimer: utils.NewDiffTimer(m.clock),
	}
	for i := range n.callbackIndex {
		n.callbackIndex[i] = -1
	}
	if n.checkPriorityStatus() {
		n.bt.ns.SetState(n.node, n.bt.PriorityFlag, nodestate.Flags{}, 0)
	}
	return n
}

// storeBalance stores either a positive or a negative balance in the database
func (m *balanceManager) storeBalance(id []byte, neg bool, value utils.ExpiredValue) {
	if m.canDropBalance(m.clock.Now(), neg, value) {
		m.ndb.delBalance(id, neg) // balance is small enough, drop it directly.
	} else {
		m.ndb.setBalance(id, neg, value)
	}
}

// canDropBalance tells whether a positive or negative balance is below the threshold
// and therefore can be dropped from the database
func (m *balanceManager) canDropBalance(now mclock.AbsTime, neg bool, b utils.ExpiredValue) bool {
	if neg {
		return b.Value(m.negExp.LogOffset(now)) <= negThreshold
	} else {
		return b.Value(m.posExp.LogOffset(now)) <= posThreshold
	}
}

// totalTokenAmount returns the current total amount of service tokens in existence
func (m *balanceManager) totalTokenAmount() uint64 {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.totalTimer.Update(func(_ mclock.AbsTime) {
		m.activeBalance = utils.ExpiredValue{}
		m.ns.ForEach(nodestate.Flags{}, nodestate.Flags{}, func(node *enode.Node, state nodestate.Flags) {
			item := m.ns.GetField(node, m.balanceField)
			if item == nil {
				return
			}
			pos, _ := item.(*NodeBalance).GetRawBalance()
			m.activeBalance.AddExp(pos)
		})
	})
	total := m.activeBalance
	total.AddExp(m.inactiveBalance)
	return total.Value(m.posExp.LogOffset(now))
}

// switchBalance switchs the status of service token.
func (m *balanceManager) switchBalance(amount utils.ExpiredValue, active bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !active {
		m.activeBalance.SubExp(amount)
		m.inactiveBalance.AddExp(amount)
		return
	}
	m.activeBalance.AddExp(amount)
	m.inactiveBalance.SubExp(amount)
}

// adjustBalance adjusts the amount of service token.
func (m *balanceManager) adjustBalance(old, new utils.ExpiredValue, active bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !active {
		m.inactiveBalance.SubExp(old)
		m.inactiveBalance.AddExp(new)
		return
	}
	m.activeBalance.SubExp(old)
	m.activeBalance.AddExp(new)
}
