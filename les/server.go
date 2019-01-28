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

// Package les implements the Light Ethereum Subprotocol.
package les

import (
	"crypto/ecdsa"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
const SubscribeChainHeadEvent = 10

type LesServer struct {
	lesCommons

	fcManager    *flowcontrol.ClientManager // nil if our node is client only
	fcCostStats  *requestCostStats
	defParams    *flowcontrol.ServerParams
	lesTopics    []discv5.Topic
	privateKey   *ecdsa.PrivateKey
	quitSync     chan struct{}
	onlyAnnounce bool

	// Checkpoint contract relative fields
	backend *eth.EthAPIBackend
	genesis common.Hash // Genesis block hash for contract address detection
}

func NewLesServer(e *eth.Ethereum, config *eth.Config) (*LesServer, error) {
	quitSync := make(chan struct{})
	lesTopics := make([]discv5.Topic, len(AdvertiseProtocolVersions))
	for i, pv := range AdvertiseProtocolVersions {
		lesTopics[i] = lesTopic(e.BlockChain().Genesis().Hash(), pv)
	}

	srv := &LesServer{
		lesCommons: lesCommons{
			config:           config,
			iConfig:          light.DefaultServerIndexerConfig,
			chainDb:          e.ChainDb(),
			chtIndexer:       light.NewChtIndexer(e.ChainDb(), nil, params.CHTFrequencyServer, params.HelperTrieProcessConfirmations),
			bloomTrieIndexer: light.NewBloomTrieIndexer(e.ChainDb(), nil, params.BloomBitsBlocks, params.BloomTrieFrequency),
		},
		genesis:      e.BlockChain().Genesis().Hash(),
		quitSync:     quitSync,
		lesTopics:    lesTopics,
		onlyAnnounce: config.OnlyAnnounce,
		backend:      e.APIBackend,
	}

	logger := log.New()

	chtV1SectionCount, _, _ := srv.chtIndexer.Sections() // indexer still uses LES/1 4k section size for backwards server compatibility
	chtV2SectionCount := chtV1SectionCount / (params.CHTFrequencyClient / params.CHTFrequencyServer)
	if chtV2SectionCount != 0 {
		// convert to LES/2 section
		chtLastSection := chtV2SectionCount - 1
		// convert last LES/2 section index back to LES/1 index for chtIndexer.SectionHead
		chtLastSectionV1 := (chtLastSection+1)*(params.CHTFrequencyClient/params.CHTFrequencyServer) - 1
		chtSectionHead := srv.chtIndexer.SectionHead(chtLastSectionV1)
		chtRoot := light.GetChtRoot(e.ChainDb(), chtLastSectionV1, chtSectionHead)
		logger.Info("Loaded CHT", "section", chtLastSection, "head", chtSectionHead, "root", chtRoot)
	}
	bloomTrieSectionCount, _, _ := srv.bloomTrieIndexer.Sections()
	if bloomTrieSectionCount != 0 {
		bloomTrieLastSection := bloomTrieSectionCount - 1
		bloomTrieSectionHead := srv.bloomTrieIndexer.SectionHead(bloomTrieLastSection)
		bloomTrieRoot := light.GetBloomTrieRoot(e.ChainDb(), bloomTrieLastSection, bloomTrieSectionHead)
		logger.Info("Loaded bloom trie", "section", bloomTrieLastSection, "head", bloomTrieSectionHead, "root", bloomTrieRoot)
	}

	srv.chtIndexer.Start(e.BlockChain())

	rconfig := newEmptyConfig()
	rconfig.ContractConfig = config.Genesis.Config.CheckpointContract

	registrar := newCheckpointRegistrar(e.ChainDb(), e.APIBackend, rconfig, light.DefaultServerIndexerConfig, srv.chtIndexer, srv.bloomTrieIndexer, srv.genesis, false, quitSync)
	pm, err := NewProtocolManager(e.BlockChain().Config(), light.DefaultServerIndexerConfig, config.ULC, false, config.NetworkId, e.EventMux(), newPeerSet(), e.BlockChain(), e.TxPool(), e.ChainDb(), nil, nil, registrar, quitSync, new(sync.WaitGroup))
	if err != nil {
		return nil, err
	}
	srv.protocolManager = pm

	pm.server = srv

	srv.defParams = &flowcontrol.ServerParams{
		BufLimit:    300000000,
		MinRecharge: 50000,
	}
	srv.fcManager = flowcontrol.NewClientManager(uint64(config.LightServ), 10, 1000000000)
	srv.fcCostStats = newCostStats(e.ChainDb())
	return srv, nil
}

func (s *LesServer) Protocols() []p2p.Protocol {
	return s.makeProtocols(ServerProtocolVersions)
}

// Start starts the LES server
func (s *LesServer) Start(srvr *p2p.Server) {
	s.protocolManager.Start(s.config.LightPeers)
	if srvr.DiscV5 != nil {
		for _, topic := range s.lesTopics {
			topic := topic
			go func() {
				logger := log.New("topic", topic)
				logger.Info("Starting topic registration")
				defer logger.Info("Terminated topic registration")

				srvr.DiscV5.RegisterTopic(topic, s.quitSync)
			}()
		}
	}
	s.privateKey = srvr.PrivateKey
	s.protocolManager.blockLoop()
}

func (s *LesServer) SetBloomBitsIndexer(bloomIndexer *core.ChainIndexer) {
	bloomIndexer.AddChildIndexer(s.bloomTrieIndexer)
}

// SetClient sets the rpc client and starts running checkpoint contract if it is not yet watched.
func (s *LesServer) SetContractBackend(backend bind.ContractBackend) {
	if s.protocolManager.reg != nil {
		s.protocolManager.reg.start(backend)
	}
}

// Stop stops the LES service
func (s *LesServer) Stop() {
	s.chtIndexer.Close()
	// bloom trie indexer is closed by parent bloombits indexer
	s.fcCostStats.store()
	s.fcManager.Stop()
	go func() {
		<-s.protocolManager.noMorePeers
	}()
	s.protocolManager.Stop()
}

// APIs implements LesServer, returns all API service provided by les server.
func (s *LesServer) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: "les",
			Version:   "1.0",
			Service:   NewPrivateLesServerAPI(s),
			Public:    false,
		},
	}
}

// latestLocalCheckpoint finds the common stored section index and returns a set of
// post-processed trie roots (CHT and BloomTrie) associated with
// the appropriate section index and head hash as a local checkpoint package.
//
// Note for cht, the section size in LES1 is 4K, so indexer still uses LES/1
// 4k section size for backwards server compatibility. For bloomTrie, the size
// of the section used for indexer is 32K.
func (s *LesServer) latestLocalCheckpoint() light.TrustedCheckpoint {
	chtCount, _, _ := s.chtIndexer.Sections()
	bloomTrieCount, _, _ := s.bloomTrieIndexer.Sections()
	count := chtCount / (s.iConfig.PairChtSize / s.iConfig.ChtSize)
	// Cap the section index if the two sections are not consistent.
	if count > bloomTrieCount {
		count = bloomTrieCount
	}
	if count == 0 {
		// No checkpoint information can be provided.
		return *light.EmptyCheckpoint
	}
	return s.getLocalCheckpoint(count - 1)
}

// getLocalCheckpoint returns a set of post-processed trie roots (CHT and BloomTrie)
// associated with the appropriate head hash by specific section index.
//
// The returned checkpoint is only the checkpoint generated by the local indexers,
// not the stable checkpoint registered in the registrar contract.
func (s *LesServer) getLocalCheckpoint(index uint64) light.TrustedCheckpoint {
	// convert last LES/2 section index back to LES/1 index for chtIndexer.SectionHead
	latest := (index+1)*(s.iConfig.PairChtSize/s.iConfig.ChtSize) - 1
	sectionHead := s.chtIndexer.SectionHead(latest)
	return light.TrustedCheckpoint{
		SectionIndex: index,
		SectionHead:  sectionHead,
		CHTRoot:      light.GetChtRoot(s.chainDb, latest, sectionHead),
		BloomRoot:    light.GetBloomTrieRoot(s.chainDb, index, sectionHead),
	}
}

func (pm *ProtocolManager) blockLoop() {
	pm.wg.Add(1)
	headCh := make(chan core.ChainHeadEvent, 10)
	headSub := pm.blockchain.SubscribeChainHeadEvent(headCh)
	go func() {
		var lastHead *types.Header
		lastBroadcastTd := common.Big0
		for {
			select {
			case ev := <-headCh:
				peers := pm.peers.AllPeers()
				if len(peers) > 0 {
					header := ev.Block.Header()
					hash := header.Hash()
					number := header.Number.Uint64()
					td := rawdb.ReadTd(pm.chainDb, hash, number)
					if td != nil && td.Cmp(lastBroadcastTd) > 0 {
						var reorg uint64
						if lastHead != nil {
							reorg = lastHead.Number.Uint64() - rawdb.FindCommonAncestor(pm.chainDb, header, lastHead).Number.Uint64()
						}
						lastHead = header
						lastBroadcastTd = td

						log.Debug("Announcing block to peers", "number", number, "hash", hash, "td", td, "reorg", reorg)

						announce := announceData{Hash: hash, Number: number, Td: td, ReorgDepth: reorg}
						var (
							signed         bool
							signedAnnounce announceData
						)

						for _, p := range peers {
							switch p.announceType {

							case announceTypeSimple:
								select {
								case p.announceChn <- announce:
								default:
									pm.removePeer(p.id)
								}

							case announceTypeSigned:
								if !signed {
									signedAnnounce = announce
									signedAnnounce.sign(pm.server.privateKey)
									signed = true
								}

								select {
								case p.announceChn <- signedAnnounce:
								default:
									pm.removePeer(p.id)
								}
							}
						}
					}
				}
			case <-pm.quitSync:
				headSub.Unsubscribe()
				pm.wg.Done()
				return
			}
		}
	}()
}
