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

package clientpool

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/les/utils"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/nodestate"
)

const (
	defaultPosExpTC = 36000 // default time constant (in seconds) for exponentially reducing positive balance
	defaultNegExpTC = 3600  // default time constant (in seconds) for exponentially reducing negative balance

	// activeBias is applied to already connected clients So that
	// already connected client won't be kicked out very soon and we
	// can ensure all connected clients can have enough time to request
	// or sync some data.
	//
	// todo(rjl493456442) make it configurable. It can be the option of
	// free trial time!
	activeBias      = time.Minute * 3
	inactiveTimeout = time.Second * 10
)

var (
	clientPoolSetup = &nodestate.Setup{}

	// todo removed the clientFlag. If the flag is none, then entry will be removed from nsm
	clientField = clientPoolSetup.NewField("clientInfo", reflect.TypeOf(&clientInfo{}))
	statusField = clientpoolSetup.NewField("status", reflect.TypeOf(clientStatus{}))

	trackerSetup = newBalanceTrackerSetup(clientPoolSetup)
	poolSetup    = newPriorityPoolSetup(clientPoolSetup)
)

var (
	clientpoolSetup = &nodestate.Setup{}
)

func init() {
	trackerSetup.Connect(statusField, poolSetup.CapacityField)
	poolSetup.Connect(trackerSetup.balanceField, statusField, trackerSetup.UpdateFlag) // NodeBalance implements nodePriority
}

// ClientPool implements a client database that assigns a priority to each client
// based on a positive and negative balance. Positive balance is externally assigned
// to prioritized clients and is decreased with connection time and processed
// requests (unless the price factors are zero). If the positive balance is zero
// then negative balance is accumulated.
//
// Balance tracking and priority calculation for connected clients is done by
// balanceTracker. activeQueue ensures that clients with the lowest positive or
// highest negative balance get evicted when the total capacity allowance is full
// and new clients with a better balance want to connect.
//
// Already connected nodes receive a small bias in their favor in order to avoid
// accepting and instantly kicking out clients. In theory, we try to ensure that
// each client can have several minutes of connection time.
//
// Balances of disconnected clients are stored in nodeDB including positive balance
// and negative banalce. Boeth positive balance and negative balance will decrease
// exponentially. If the balance is low enough, then the record will be dropped.
type ClientPool struct {
	balanceTrackerSetup
	priorityPoolSetup
	lock       sync.Mutex
	clock      mclock.Clock
	closed     bool
	removePeer func(enode.ID)
	ns         *nodestate.NodeStateMachine
	pp         *priorityPool
	bt         *balanceManager

	defaultPosFactors, defaultNegFactors PriceFactors
	posExpTC, negExpTC                   uint64
	minCap                               uint64 // The minimal capacity value allowed for any client
	capLimit                             uint64
	freeClientCap                        uint64 // The capacity value of each free client
}

// clientInfo defines all information required by clientpool.
type clientInfo struct {
	node        *enode.Node
	address     string
	peer        clientPeer
	connected   bool
	connectedAt mclock.AbsTime
	balance     *NodeBalance
}

// New creates a new client pool
func New(lespayDb ethdb.Database, minCap, freeClientCap uint64, activeBias time.Duration, clock mclock.Clock, removePeer func(enode.ID)) *ClientPool {
	if minCap > freeClientCap {
		panic(nil)
	}
	ns := nodestate.NewNodeStateMachine(nil, nil, clock, clientPoolSetup)
	pool := &ClientPool{
		ns:                  ns,
		balanceTrackerSetup: trackerSetup,
		priorityPoolSetup:   poolSetup,
		clock:               clock,
		minCap:              minCap,
		freeClientCap:       freeClientCap,
		removePeer:          removePeer,
	}
	pool.bt = NewBalanceTracker(ns, trackerSetup, lespayDb, clock, &utils.Expirer{}, &utils.Expirer{})
	pool.pp = newPriorityPool(ns, poolSetup, clock, minCap, activeBias, 4)

	// set default expiration constants used by tests
	// Note: server overwrites this if token sale is active
	pool.bt.SetExpirationTCs(0, defaultNegExpTC)

	ns.SubscribeField(statusField, func(n *enode.Node, state nodestate.Flags, oldValue, newValue interface{}) {
		if newValue == nil {
			return
		}
		client := newValue.(*clientStatus)
		switch client.status {
		case connected:
		case active:
			cap, _ := ns.GetField(node, pool.CapacityField).(uint64)
			if cap > freeClientCap {
				pool.pp.RequestCapacity(node, freeClientCap, 0, true)
			}
		case inactive:
		case disconnected:
		}
	})

	ns.SubscribeField(pool.CapacityField, func(node *enode.Node, state nodestate.Flags, oldValue, newValue interface{}) {
		// Capture the state transtion the capacity of client is changed
		c, _ := ns.GetField(node, clientField).(*clientInfo)
		if c != nil {
			cap, _ := newValue.(uint64)
			c.peer.UpdateCapacity(cap)
		}
	})

	ns.Start()
	return pool
}

// stop shuts the client pool down
func (f *ClientPool) Stop() {
	f.lock.Lock()
	f.closed = true
	f.lock.Unlock()
	f.ns.ForEach(f.ActiveFlag.Or(f.InactiveFlag), nodestate.Flags{}, func(node *enode.Node, state nodestate.Flags) {
		// enforces saving all balances in balanceManager
		f.disconnectNode(node)
	})
	f.bt.Stop()
	f.ns.Stop()
}

// connect should be called after a successful handshake. If the connection was
// rejected, there is no need to call disconnect.
func (f *ClientPool) Connect(peer clientPeer, reqCapacity uint64) (uint64, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// Short circuit if ClientPool is already closed.
	if f.closed {
		return 0, fmt.Errorf("Client pool is already closed")
	}
	// Dedup connected peers.
	node, ipaddr := peer.Node(), peer.IPAddr()
	if f.ns.GetField(node, clientField) != nil {
		log.Debug("Client already connected", "address", ipaddr)
		return 0, fmt.Errorf("Client already connected address=%s", ipaddr)
	}
	now := f.clock.Now()
	c := &clientInfo{
		node:        node,
		address:     ipaddr,
		peer:        peer,
		connected:   true,
		connectedAt: now,
	}
	f.ns.SetField(node, clientField, c)
	f.ns.SetField(node, statusField, clientStatus{status: connected, ipaddr: ipaddr})
	if c.balance, _ = f.ns.GetField(node, f.balanceField).(*NodeBalance); c.balance == nil {
		f.Disconnect(peer)
		return 0, nil
	}
	c.balance.SetPriceFactors(f.defaultPosFactors, f.defaultNegFactors)
	if reqCapacity < f.minCap {
		reqCapacity = f.minCap
	}
	if reqCapacity > f.freeClientCap && c.balance.Priority(now, reqCapacity) <= 0 {
		f.Disconnect(peer)
		return 0, nil
	}
	f.ns.SetField(node, statusField, clientStatus{status: inactive, ipaddr: ipaddr})
	if _, allowed := f.pp.RequestCapacity(node, reqCapacity, activeBias, true); allowed {
		return reqCapacity, nil
	}
	if !peer.AllowInactive() {
		f.Disconnect(peer)
	}
	return 0, nil
}

// disconnect should be called when a connection is terminated. If the disconnection
// was initiated by the pool itself using disconnectFn then calling disconnect is
// not necessary but permitted.
func (f *ClientPool) Disconnect(p clientPeer) {
	f.disconnectNode(p.Node())
}

// disconnectNode removes node fields and flags related to connected status
func (f *ClientPool) disconnectNode(node *enode.Node) {
	f.ns.SetField(node, statusField, nil)
	f.ns.SetField(node, clientField, nil)
}

// SetDefaultFactors sets the default price factors applied to subsequently connected clients
func (f *ClientPool) SetDefaultFactors(posFactors, negFactors PriceFactors) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.defaultPosFactors = posFactors
	f.defaultNegFactors = negFactors
}

// CapacityInfo returns the total capacity allowance, the total capacity of connected
// clients and the total capacity of connected and prioritized clients
func (f *ClientPool) CapacityInfo() (uint64, uint64, uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// total priority active cap will be supported when the token issuer module is added
	return f.capLimit, f.pp.ActiveCapacity(), 0
}

// SetLimits sets the maximum number and total capacity of connected clients,
// dropping some of them if necessary.
func (f *ClientPool) SetLimits(totalConn int, totalCap uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.capLimit = totalCap
	f.pp.SetLimits(uint64(totalConn), totalCap)
}

// setCapacity sets the assigned capacity of a connected client
func (f *ClientPool) SetCapacity(node *enode.Node, freeID string, capacity uint64, bias time.Duration, setCap bool) (uint64, error) {
	c, _ := f.ns.GetField(node, clientField).(*clientInfo)
	if c == nil {
		if setCap {
			return 0, fmt.Errorf("client %064x is not connected", node.ID())
		}
		c = &clientInfo{node: node}
		f.ns.SetField(node, clientField, c)
		if c.balance, _ = f.ns.GetField(node, f.balanceField).(*NodeBalance); c.balance == nil {
			log.Error("balanceField is missing", "node", node.ID())
			return 0, fmt.Errorf("balanceField of %064x is missing", node.ID())
		}
		defer func() {
			f.ns.SetField(node, clientField, nil)
		}()
	}
	minPriority, allowed := f.pp.RequestCapacity(node, capacity, bias, setCap)
	if allowed {
		return 0, nil
	}
	missing := c.balance.PosBalanceMissing(minPriority, capacity, bias)
	if missing < 1 {
		// ensure that we never return 0 missing and insufficient priority error
		missing = 1
	}
	return missing, errors.New(">>>")
}

// SetCapacityLocked is the equivalent of setCapacity used when f.lock is already locked
func (f *ClientPool) SetCapacityLocked(node *enode.Node, freeID string, capacity uint64, minConnTime time.Duration, setCap bool) (uint64, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.SetCapacity(node, freeID, capacity, minConnTime, setCap)
}

// forClients calls the supplied callback for either the listed node IDs or all connected
// nodes. It passes a valid clientInfo to the callback and ensures that the necessary
// fields and flags are set in order for balanceManager and priorityPool to work even if
// the node is not connected.
func (f *ClientPool) forClients(ids []enode.ID, cb func(client *clientInfo)) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if len(ids) == 0 {
		f.ns.ForEach(f.ActiveFlag.Or(f.InactiveFlag), nodestate.Flags{}, func(node *enode.Node, state nodestate.Flags) {
			c, _ := f.ns.GetField(node, clientField).(*clientInfo)
			if c != nil {
				cb(c)
			}
		})
	} else {
		for _, id := range ids {
			node := f.ns.GetNode(id)
			if node == nil {
				node = enode.SignNull(&enr.Record{}, id)
			}
			c, _ := f.ns.GetField(node, clientField).(*clientInfo)
			if c != nil {
				cb(c)
			} else {
				c = &clientInfo{node: node}
				f.ns.SetField(node, clientField, c)
				f.ns.SetField(node, freeIdField, "")
				if c.balance, _ = f.ns.GetField(node, f.balanceField).(*NodeBalance); c.balance != nil {
					cb(c)
				} else {
					log.Error("balanceField is missing")
				}
				f.ns.SetField(node, freeIdField, nil)
				f.ns.SetField(node, clientField, nil)
			}
		}
	}
}
