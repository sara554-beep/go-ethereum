// Copyright 2016 The go-ethereum Authors
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

package les

import (
	"crypto/ecdsa"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// LesServer
type LesServer struct {
	lesCommons

	handler    *serverHandler
	lesTopics  []discv5.Topic
	privateKey *ecdsa.PrivateKey

	// Flow control and capacity management
	fcManager          *flowcontrol.ClientManager
	costTracker        *costTracker
	defParams          flowcontrol.ServerParams
	servingQueue       *servingQueue
	freeClientCap      uint64
	freeClientPool     *freeClientPool
	priorityClientPool *priorityClientPool

	threadsIdle int // Request serving threads count when system is idle.
	threadsBusy int // Request serving threads count when system is busy(block insertion).

	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewLesServer(eth *eth.Ethereum, config *eth.Config) (*LesServer, error) {
	// Collect les protocol version information supported by local node.
	lesTopics := make([]discv5.Topic, len(AdvertiseProtocolVersions))
	for i, pv := range AdvertiseProtocolVersions {
		lesTopics[i] = lesTopic(eth.BlockChain().Genesis().Hash(), pv)
	}
	// Calculate the number of threads used to service the light client
	// requests based on the user-specified value.
	threads := config.LightServ * 4 / 100
	if threads < 4 {
		threads = 4
	}
	srv := &LesServer{
		lesCommons: lesCommons{
			genesis:          eth.BlockChain().Genesis().Hash(),
			config:           config,
			chainConfig:      eth.BlockChain().Config(),
			iConfig:          light.DefaultServerIndexerConfig,
			chainDb:          eth.ChainDb(),
			peers:            newPeerSet(),
			chainReader:      eth.BlockChain(),
			chtIndexer:       light.NewChtIndexer(eth.ChainDb(), nil, params.CHTFrequency, params.HelperTrieProcessConfirmations),
			bloomTrieIndexer: light.NewBloomTrieIndexer(eth.ChainDb(), nil, params.BloomBitsBlocks, params.BloomTrieFrequency),
		},
		lesTopics:    lesTopics,
		fcManager:    flowcontrol.NewClientManager(nil, &mclock.System{}),
		costTracker:  newCostTracker(eth.ChainDb(), config),
		servingQueue: newServingQueue(int64(time.Millisecond * 10)),
		threadsBusy:  config.LightServ/100 + 1,
		threadsIdle:  threads,
		closeCh:      make(chan struct{}),
	}
	srv.handler = newServerHandler(srv, eth.BlockChain(), eth.ChainDb(), eth.TxPool(), eth.IsSynced)

	// Get the information of local cht and bloom trie and display.
	chtSectionCount, _, _ := srv.chtIndexer.Sections()
	if chtSectionCount != 0 {
		chtLastSection := chtSectionCount - 1
		chtSectionHead := srv.chtIndexer.SectionHead(chtLastSection)
		chtRoot := light.GetChtRoot(eth.ChainDb(), chtLastSection, chtSectionHead)
		log.Info("Loaded CHT", "section", chtLastSection, "head", chtSectionHead, "root", chtRoot)
	}
	bloomTrieSectionCount, _, _ := srv.bloomTrieIndexer.Sections()
	if bloomTrieSectionCount != 0 {
		bloomTrieLastSection := bloomTrieSectionCount - 1
		bloomTrieSectionHead := srv.bloomTrieIndexer.SectionHead(bloomTrieLastSection)
		bloomTrieRoot := light.GetBloomTrieRoot(eth.ChainDb(), bloomTrieLastSection, bloomTrieSectionHead)
		log.Info("Loaded bloom trie", "section", bloomTrieLastSection, "head", bloomTrieSectionHead, "root", bloomTrieRoot)
	}
	srv.chtIndexer.Start(eth.BlockChain())
	return srv, nil
}

func (s *LesServer) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: "les",
			Version:   "1.0",
			Service:   NewPrivateLightServerAPI(s),
			Public:    false,
		},
	}
}

func (s *LesServer) Protocols() []p2p.Protocol {
	return s.makeProtocols(ServerProtocolVersions, s.handler.runPeer, func(id enode.ID) interface{} {
		if p := s.peers.Peer(fmt.Sprintf("%x", id.Bytes())); p != nil {
			return p.Info()
		}
		return nil
	})
}

// Start starts the LES server.
func (s *LesServer) Start(srvr *p2p.Server) {
	s.privateKey = srvr.PrivateKey

	totalRecharge := s.costTracker.totalRecharge()
	if s.config.LightPeers > 0 {
		s.freeClientCap = minCapacity
		s.defParams = flowcontrol.ServerParams{
			BufLimit:    s.freeClientCap * bufLimitRatio,
			MinRecharge: s.freeClientCap,
		}
	}
	freePeers := int(totalRecharge / s.freeClientCap)
	if freePeers < s.config.LightPeers {
		log.Warn("Light peer count limited", "specified", s.config.LightPeers, "allowed", freePeers)
	}
	s.freeClientPool = newFreeClientPool(s.chainDb, s.freeClientCap, 10000, mclock.System{}, func(id string) { go s.peers.Unregister(id) })
	s.priorityClientPool = newPriorityClientPool(s.freeClientCap, s.peers, s.freeClientPool)

	s.wg.Add(1)
	s.startEventLoop()

	s.handler.start()
	if srvr.DiscV5 != nil {
		for _, topic := range s.lesTopics {
			topic := topic
			go func() {
				logger := log.New("topic", topic)
				logger.Info("Starting topic registration")
				defer logger.Info("Terminated topic registration")

				srvr.DiscV5.RegisterTopic(topic, s.closeCh)
			}()
		}
	}
}

// Stop stops the LES service.
func (s *LesServer) Stop() {
	close(s.closeCh)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	s.peers.Close()

	s.freeClientPool.stop()
	s.costTracker.stop()
	s.handler.stop()
	s.servingQueue.stop()

	// Note, bloom trie indexer is closed by parent bloombits indexer.
	s.chtIndexer.Close()
	s.wg.Wait()
	log.Info("Les server stopped")
}

func (s *LesServer) SetBloomBitsIndexer(bloomIndexer *core.ChainIndexer) {
	bloomIndexer.AddChildIndexer(s.bloomTrieIndexer)
}

// startEventLoop starts an event handler loop that updates the recharge curve of
// the client manager and adjusts the client pool's size according to the total
// capacity updates coming from the client manager
func (s *LesServer) startEventLoop() {
	defer s.wg.Done()

	processCh := make(chan bool, 100)
	sub := s.handler.blockchain.SubscribeBlockProcessingEvent(processCh)
	defer sub.Unsubscribe()

	totalRechargeCh := make(chan uint64, 100)
	totalRecharge := s.costTracker.subscribeTotalRecharge(totalRechargeCh)

	totalCapacityCh := make(chan uint64, 100)
	totalCapacity := s.fcManager.SubscribeTotalCapacity(totalCapacityCh)
	s.priorityClientPool.setLimits(s.config.LightPeers, totalCapacity)

	var busy bool
	updateRecharge := func() {
		if busy {
			s.servingQueue.setThreads(s.threadsBusy)
			s.fcManager.SetRechargeCurve(flowcontrol.PieceWiseLinear{{0, 0}, {totalRecharge, totalRecharge}})
		} else {
			s.servingQueue.setThreads(s.threadsIdle)
			s.fcManager.SetRechargeCurve(flowcontrol.PieceWiseLinear{{0, 0}, {totalRecharge / 10, totalRecharge}, {totalRecharge, totalRecharge}})
		}
	}
	updateRecharge()
	for {
		select {
		case busy = <-processCh:
			updateRecharge()
		case totalRecharge = <-totalRechargeCh:
			updateRecharge()
		case totalCapacity = <-totalCapacityCh:
			s.priorityClientPool.setLimits(s.config.LightPeers, totalCapacity)
		case <-s.closeCh:
			return
		}
	}
}
