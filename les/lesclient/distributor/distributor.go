// Copyright 2017 The go-ethereum Authors
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

package distributor

import (
	"container/list"
	"github.com/ethereum/go-ethereum/les/metrics"
	"github.com/ethereum/go-ethereum/les/utilities"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// RequestDistributor implements a mechanism that distributes requests to
// suitable peers, obeying flow control rules and prioritizing them in creation
// order (even when a resend is necessary).
type RequestDistributor struct {
	clock        mclock.Clock
	reqQueue     *list.List
	lastReqOrder uint64
	peers2       map[enode.ID]DistPeer
	peerLock     sync.RWMutex
	loopChn      chan struct{}
	loopNextSent bool
	lock         sync.Mutex

	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewRequestDistributor creates a new Request distributor
func NewRequestDistributor(clock mclock.Clock) *RequestDistributor {
	d := &RequestDistributor{
		clock:    clock,
		reqQueue: list.New(),
		loopChn:  make(chan struct{}, 2),
		closeCh:  make(chan struct{}),
		peers2:   make(map[enode.ID]DistPeer),
	}
	d.wg.Add(1)
	go d.loop()
	return d
}

// registerPeer implements peerSetNotify
func (d *RequestDistributor) RegisterPeer(p DistPeer) {
	d.peerLock.Lock()
	d.peers2[p.ID()] = p
	d.peerLock.Unlock()
}

// unregisterPeer implements peerSetNotify
func (d *RequestDistributor) UnregisterPeer(p DistPeer) {
	d.peerLock.Lock()
	delete(d.peers2, p.ID())
	d.peerLock.Unlock()
}

var (
	// distMaxWait is the maximum waiting time after which further necessary waiting
	// times are recalculated based on new feedback from the servers
	distMaxWait = time.Millisecond * 50

	// waitForPeers is the time window in which a Request does not fail even if it
	// has no suitable peers to send to at the moment
	waitForPeers = time.Second * 3
)

// main event loop
func (d *RequestDistributor) loop() {
	defer d.wg.Done()
	for {
		select {
		case <-d.closeCh:
			d.lock.Lock()
			elem := d.reqQueue.Front()
			for elem != nil {
				req := elem.Value.(*DistReq)
				close(req.sentChn)
				req.sentChn = nil
				elem = elem.Next()
			}
			d.lock.Unlock()
			return
		case <-d.loopChn:
			d.lock.Lock()
			d.loopNextSent = false
		loop:
			for {
				peer, req, wait := d.nextRequest()
				if req != nil && wait == 0 {
					chn := req.sentChn // save sentChn because Remove sets it to nil
					d.Remove(req)
					send := req.Request(peer)
					if send != nil {
						peer.QueueSend(send)
						metrics.RequestSendDelay.Update(time.Duration(d.clock.Now() - req.enterQueue))
					}
					chn <- peer
					close(chn)
				} else {
					if wait == 0 {
						// no Request to send and nothing to wait for; the next
						// queued Request will wake up the loop
						break loop
					}
					d.loopNextSent = true // a "next" signal has been sent, do not send another one until this one has been received
					if wait > distMaxWait {
						// waiting times may be reduced by incoming Request replies, if it is too long, recalculate it periodically
						wait = distMaxWait
					}
					go func() {
						d.clock.Sleep(wait)
						d.loopChn <- struct{}{}
					}()
					break loop
				}
			}
			d.lock.Unlock()
		}
	}
}

// selectPeerItem represents a peer to be selected for a Request by weightedRandomSelect
type selectPeerItem struct {
	peer   DistPeer
	req    *DistReq
	weight int64
}

// Weight implements wrsItem interface
func (sp selectPeerItem) Weight() int64 {
	return sp.weight
}

// nextRequest returns the next possible Request from any peer, along with the
// associated peer and necessary waiting time
func (d *RequestDistributor) nextRequest() (DistPeer, *DistReq, time.Duration) {
	checkedPeers := make(map[DistPeer]struct{})
	elem := d.reqQueue.Front()
	var (
		bestWait time.Duration
		sel      *utilities.WeightedRandomSelect
	)

	d.peerLock.RLock()
	defer d.peerLock.RUnlock()

	peerCount := len(d.peers2)
	for (len(checkedPeers) < peerCount || elem == d.reqQueue.Front()) && elem != nil {
		req := elem.Value.(*DistReq)
		canSend := false
		now := d.clock.Now()
		if req.waitForPeers > now {
			canSend = true
			wait := time.Duration(req.waitForPeers - now)
			if bestWait == 0 || wait < bestWait {
				bestWait = wait
			}
		}
		for _, peer := range d.peers2 {
			if _, ok := checkedPeers[peer]; !ok && peer.CanQueue() && req.CanSend(peer) {
				canSend = true
				cost := req.GetCost(peer)
				wait, bufRemain := peer.WaitBefore(cost)
				if wait == 0 {
					if sel == nil {
						sel = utilities.NewWeightedRandomSelect()
					}
					sel.Update(selectPeerItem{peer: peer, req: req, weight: int64(bufRemain*1000000) + 1})
				} else {
					if bestWait == 0 || wait < bestWait {
						bestWait = wait
					}
				}
				checkedPeers[peer] = struct{}{}
			}
		}
		next := elem.Next()
		if !canSend && elem == d.reqQueue.Front() {
			close(req.sentChn)
			d.Remove(req)
		}
		elem = next
	}

	if sel != nil {
		c := sel.Choose().(selectPeerItem)
		return c.peer, c.req, 0
	}
	return nil, nil, bestWait
}

// Queue adds a Request to the distribution Queue, returns a channel where the
// receiving peer is sent once the Request has been sent (Request callback returned).
// If the Request is cancelled or timed out without suitable peers, the channel is
// closed without sending any peer references to it.
func (d *RequestDistributor) Queue(r *DistReq) chan DistPeer {
	d.lock.Lock()
	defer d.lock.Unlock()

	if r.reqOrder == 0 {
		d.lastReqOrder++
		r.reqOrder = d.lastReqOrder
		r.waitForPeers = d.clock.Now() + mclock.AbsTime(waitForPeers)
	}
	// Assign the timestamp when the Request is queued no matter it's
	// a new one or re-queued one.
	r.enterQueue = d.clock.Now()

	back := d.reqQueue.Back()
	if back == nil || r.reqOrder > back.Value.(*DistReq).reqOrder {
		r.element = d.reqQueue.PushBack(r)
	} else {
		before := d.reqQueue.Front()
		for before.Value.(*DistReq).reqOrder < r.reqOrder {
			before = before.Next()
		}
		r.element = d.reqQueue.InsertBefore(r, before)
	}

	if !d.loopNextSent {
		d.loopNextSent = true
		d.loopChn <- struct{}{}
	}

	r.sentChn = make(chan DistPeer, 1)
	return r.sentChn
}

// Cancel removes a Request from the Queue if it has not been sent yet (returns
// false if it has been sent already). It is guaranteed that the callback functions
// will not be called after Cancel returns.
func (d *RequestDistributor) Cancel(r *DistReq) bool {
	d.lock.Lock()
	defer d.lock.Unlock()

	if r.sentChn == nil {
		return false
	}

	close(r.sentChn)
	d.Remove(r)
	return true
}

// Remove removes a Request from the Queue
func (d *RequestDistributor) Remove(r *DistReq) {
	r.sentChn = nil
	if r.element != nil {
		d.reqQueue.Remove(r.element)
		r.element = nil
	}
}

func (d *RequestDistributor) Close() {
	close(d.closeCh)
	d.wg.Wait()
}
