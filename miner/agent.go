// Copyright 2015 The go-ethereum Authors
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

package miner

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/log"
)

type CpuAgent struct {
	mu sync.Mutex

	workCh        chan *Work
	works         map[common.Hash]*Work
	stop          chan struct{}
	quitCurrentOp chan struct{}
	returnCh      chan<- *Result

	chain  consensus.ChainReader
	engine consensus.Engine

	started int32 // started indicates whether the agent is currently started
}

func NewCpuAgent(chain consensus.ChainReader, engine consensus.Engine) *CpuAgent {
	agent := &CpuAgent{
		chain:  chain,
		engine: engine,
		stop:   make(chan struct{}, 1),
		workCh: make(chan *Work, 1),
		works:  make(map[common.Hash]*Work),
	}
	return agent
}

func (self *CpuAgent) Work() chan<- *Work            { return self.workCh }
func (self *CpuAgent) SetReturnCh(ch chan<- *Result) { self.returnCh = ch }

func (self *CpuAgent) Start() {
	if !atomic.CompareAndSwapInt32(&self.started, 0, 1) {
		return // agent already started
	}
	go self.update()
	if _, ok := self.engine.(consensus.PoW); ok {
		go self.submitStaleSolution()
	}
}

func (self *CpuAgent) Stop() {
	if !atomic.CompareAndSwapInt32(&self.started, 1, 0) {
		return // agent already stopped
	}

	// Close the pending routines.
close:
	for {
		select {
		case self.stop <- struct{}{}:
		default:
			break close
		}
	}

done:
	// Empty work channel
	for {
		select {
		case <-self.workCh:
		default:
			break done
		}
	}
}

func (self *CpuAgent) update() {
	ticker := time.NewTicker(5 * time.Second)
out:
	for {
		select {
		case work := <-self.workCh:
			self.mu.Lock()
			if self.quitCurrentOp != nil {
				close(self.quitCurrentOp)
			}
			self.quitCurrentOp = make(chan struct{})
			go self.mine(work, self.quitCurrentOp)
			// Track the mining work
			self.works[work.Block.HashNoNonce()] = work
			self.mu.Unlock()

		case <-ticker.C:
			// Clear out stale mining works
			self.mu.Lock()
			for hash, work := range self.works {
				if time.Since(work.createdAt) > 10*(15*time.Second) {
					delete(self.works, hash)
				}
			}
			self.mu.Unlock()

		case <-self.stop:
			self.mu.Lock()
			if self.quitCurrentOp != nil {
				close(self.quitCurrentOp)
				self.quitCurrentOp = nil
			}
			self.mu.Unlock()
			break out
		}
	}
}

// submitStaleSolution polls on the stale channel provided by PoW consensus engine,
// submits all valid but stale solution to miner.
func (self *CpuAgent) submitStaleSolution() {
	pow := self.engine.(consensus.PoW)
out:
	for {
		select {
		case solution := <-pow.StaleSolution():
			self.mu.Lock()
			work := self.works[solution.HashNoNonce()]
			self.mu.Unlock()
			if work == nil {
				continue
			}
			self.returnCh <- &Result{work, solution}
		case <-self.stop:
			break out
		}
	}
}

func (self *CpuAgent) mine(work *Work, stop <-chan struct{}) {
	if result, err := self.engine.Seal(self.chain, work.Block, stop); result != nil {
		log.Info("Successfully sealed new block", "number", result.Number(), "hash", result.Hash())
		self.returnCh <- &Result{work, result}
	} else {
		if err != nil && err != consensus.ErrEngineNotStart {
			log.Warn("Block sealing failed", "err", err)
		}
		self.returnCh <- nil
	}
}
