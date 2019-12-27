package lesserver

import (
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/les/lesserver/clientpool"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/les/checkpointoracle"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/les/metrics"
	"github.com/ethereum/go-ethereum/les/protocol"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

type chainReader interface {
	CurrentHeader() *types.Header
}

type LesServer struct {
	genesis                      common.Hash
	config                       *eth.Config
	chainConfig                  *params.ChainConfig
	iConfig                      *light.IndexerConfig
	chainDb                      ethdb.Database
	peers                        *peerSet
	chainReader                  chainReader
	chtIndexer, bloomTrieIndexer *core.ChainIndexer
	oracle                       *checkpointoracle.CheckpointOracle

	closeCh chan struct{}
	wg      sync.WaitGroup

	archiveMode bool // Flag whether the ethereum node runs in archive mode.
	handler     *serverHandler
	lesTopics   []discv5.Topic
	privateKey  *ecdsa.PrivateKey

	// Flow control and capacity management
	fcManager    *flowcontrol.ClientManager
	costTracker  *costTracker
	defParams    flowcontrol.ServerParams
	servingQueue *servingQueue
	clientPool   *clientpool.ClientPool

	minCapacity, maxCapacity, freeCapacity uint64
	threadsIdle                            int // Request serving threads count when system is idle.
	threadsBusy                            int // Request serving threads count when system is busy(block insertion).
}

func NewLesServer(e *eth.Ethereum, config *eth.Config) (*LesServer, error) {
	// Collect les protocol version information supported by local node.
	lesTopics := make([]discv5.Topic, len(protocol.AdvertiseProtocolVersions))
	for i, pv := range protocol.AdvertiseProtocolVersions {
		lesTopics[i] = lesTopic(e.BlockChain().Genesis().Hash(), pv)
	}
	// Calculate the number of threads used to service the light client
	// requests based on the user-specified value.
	threads := config.LightServ * 4 / 100
	if threads < 4 {
		threads = 4
	}
	srv := &LesServer{
		genesis:          e.BlockChain().Genesis().Hash(),
		config:           config,
		chainConfig:      e.BlockChain().Config(),
		iConfig:          light.DefaultServerIndexerConfig,
		chainDb:          e.ChainDb(),
		peers:            newPeerSet(),
		chainReader:      e.BlockChain(),
		chtIndexer:       light.NewChtIndexer(e.ChainDb(), nil, params.CHTFrequency, params.HelperTrieProcessConfirmations),
		bloomTrieIndexer: light.NewBloomTrieIndexer(e.ChainDb(), nil, params.BloomBitsBlocks, params.BloomTrieFrequency),
		closeCh:          make(chan struct{}),
		archiveMode:      e.ArchiveMode(),
		lesTopics:        lesTopics,
		fcManager:        flowcontrol.NewClientManager(nil, &mclock.System{}),
		servingQueue:     newServingQueue(int64(time.Millisecond*10), float64(config.LightServ)/100),
		threadsBusy:      config.LightServ/100 + 1,
		threadsIdle:      threads,
	}
	srv.handler = newServerHandler(srv, e.BlockChain(), e.ChainDb(), e.TxPool(), e.Synced)
	srv.costTracker, srv.minCapacity = newCostTracker(e.ChainDb(), config)
	srv.freeCapacity = srv.minCapacity

	// Set up checkpoint oracle.
	oracle := config.CheckpointOracle
	if oracle == nil {
		oracle = params.CheckpointOracles[e.BlockChain().Genesis().Hash()]
	}
	srv.oracle = checkpointoracle.NewCheckpointOracle(oracle, srv.localCheckpoint)

	// Initialize server capacity management fields.
	srv.defParams = flowcontrol.ServerParams{
		BufLimit:    srv.freeCapacity * bufLimitRatio,
		MinRecharge: srv.freeCapacity,
	}
	// LES flow control tries to more or less guarantee the possibility for the
	// clients to send a certain amount of requests at any time and get a quick
	// response. Most of the clients want this guarantee but don't actually need
	// to send requests most of the time. Our goal is to serve as many clients as
	// possible while the actually used server capacity does not exceed the limits
	totalRecharge := srv.costTracker.totalRecharge()
	srv.maxCapacity = srv.freeCapacity * uint64(srv.config.LightPeers)
	if totalRecharge > srv.maxCapacity {
		srv.maxCapacity = totalRecharge
	}
	srv.fcManager.SetCapacityLimits(srv.freeCapacity, srv.maxCapacity, srv.freeCapacity*2)
	srv.clientPool = clientpool.NewClientPool(srv.chainDb, srv.freeCapacity, mclock.System{}, func(id enode.ID) { go srv.peers.unregister(peerIdToString(id)) })
	srv.clientPool.SetDefaultFactors(clientpool.PriceFactors{0, 1, 1}, clientpool.PriceFactors{0, 1, 1})

	checkpoint := srv.latestLocalCheckpoint()
	if !checkpoint.Empty() {
		log.Info("Loaded latest checkpoint", "section", checkpoint.SectionIndex, "head", checkpoint.SectionHead,
			"chtroot", checkpoint.CHTRoot, "bloomroot", checkpoint.BloomRoot)
	}
	srv.chtIndexer.Start(e.BlockChain())
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
		{
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPrivateDebugAPI(s),
			Public:    false,
		},
	}
}

func (s *LesServer) Protocols() []p2p.Protocol {
	ps := s.makeProtocols(protocol.ServerProtocolVersions, s.handler.runPeer, func(id enode.ID) interface{} {
		if p := s.peers.clientPeer(peerIdToString(id)); p != nil {
			return p.Info()
		}
		return nil
	})
	// Add "les" ENR entries.
	for i := range ps {
		ps[i].Attributes = []enr.Entry{&lesEntry{}}
	}
	return ps
}

// Start starts the LES server
func (s *LesServer) Start(srvr *p2p.Server) {
	s.privateKey = srvr.PrivateKey
	s.handler.start()

	s.wg.Add(1)
	go s.capacityManagement()

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

// Stop stops the LES service
func (s *LesServer) Stop() {
	close(s.closeCh)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	s.peers.close()

	s.fcManager.Stop()
	s.costTracker.stop()
	s.handler.stop()
	s.clientPool.Stop() // client pool should be closed after handler.
	s.servingQueue.stop()

	// Note, bloom trie indexer is closed by parent bloombits indexer.
	s.chtIndexer.Close()
	s.wg.Wait()
	log.Info("Les server stopped")
}

func (s *LesServer) SetBloomBitsIndexer(bloomIndexer *core.ChainIndexer) {
	bloomIndexer.AddChildIndexer(s.bloomTrieIndexer)
}

// SetClient sets the rpc client and starts running checkpoint contract if it is not yet watched.
func (s *LesServer) SetContractBackend(backend bind.ContractBackend) {
	if s.oracle == nil {
		return
	}
	s.oracle.Start(backend)
}

// capacityManagement starts an event handler loop that updates the recharge curve of
// the client manager and adjusts the client pool's size according to the total
// capacity updates coming from the client manager
func (s *LesServer) capacityManagement() {
	defer s.wg.Done()

	processCh := make(chan bool, 100)
	sub := s.handler.blockchain.SubscribeBlockProcessingEvent(processCh)
	defer sub.Unsubscribe()

	totalRechargeCh := make(chan uint64, 100)
	totalRecharge := s.costTracker.subscribeTotalRecharge(totalRechargeCh)

	totalCapacityCh := make(chan uint64, 100)
	totalCapacity := s.fcManager.SubscribeTotalCapacity(totalCapacityCh)
	s.clientPool.SetLimits(s.config.LightPeers, totalCapacity)

	var (
		busy         bool
		freePeers    uint64
		blockProcess mclock.AbsTime
	)
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
			if busy {
				blockProcess = mclock.Now()
			} else {
				metrics.BlockProcessingTimer.Update(time.Duration(mclock.Now() - blockProcess))
			}
			updateRecharge()
		case totalRecharge = <-totalRechargeCh:
			metrics.TotalRechargeGauge.Update(int64(totalRecharge))
			updateRecharge()
		case totalCapacity = <-totalCapacityCh:
			metrics.TotalCapacityGauge.Update(int64(totalCapacity))
			newFreePeers := totalCapacity / s.freeCapacity
			if newFreePeers < freePeers && newFreePeers < uint64(s.config.LightPeers) {
				log.Warn("Reduced free peer connections", "from", freePeers, "to", newFreePeers)
			}
			freePeers = newFreePeers
			s.clientPool.SetLimits(s.config.LightPeers, totalCapacity)
		case <-s.closeCh:
			return
		}
	}
}

// makeProtocols creates protocol descriptors for the given LES versions.
func (c *LesServer) makeProtocols(versions []uint, runPeer func(version uint, p *p2p.Peer, rw p2p.MsgReadWriter) error, peerInfo func(id enode.ID) interface{}) []p2p.Protocol {
	protos := make([]p2p.Protocol, len(versions))
	for i, version := range versions {
		version := version
		protos[i] = p2p.Protocol{
			Name:     "les",
			Version:  version,
			Length:   protocol.ProtocolLengths[version],
			NodeInfo: c.nodeInfo,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				return runPeer(version, peer, rw)
			},
			PeerInfo: peerInfo,
		}
	}
	return protos
}

// nodeInfo retrieves some protocol metadata about the running host node.
func (c *LesServer) nodeInfo() interface{} {
	head := c.chainReader.CurrentHeader()
	hash := head.Hash()
	return &NodeInfo{
		Network:    c.config.NetworkId,
		Difficulty: rawdb.ReadTd(c.chainDb, hash, head.Number.Uint64()),
		Genesis:    c.genesis,
		Config:     c.chainConfig,
		Head:       hash,
		CHT:        c.latestLocalCheckpoint(),
	}
}

// latestLocalCheckpoint finds the common stored section index and returns a set
// of post-processed trie roots (CHT and BloomTrie) associated with the appropriate
// section index and head hash as a local checkpoint package.
func (c *LesServer) latestLocalCheckpoint() params.TrustedCheckpoint {
	sections, _, _ := c.chtIndexer.Sections()
	sections2, _, _ := c.bloomTrieIndexer.Sections()
	// Cap the section index if the two sections are not consistent.
	if sections > sections2 {
		sections = sections2
	}
	if sections == 0 {
		// No checkpoint information can be provided.
		return params.TrustedCheckpoint{}
	}
	return c.localCheckpoint(sections - 1)
}

// localCheckpoint returns a set of post-processed trie roots (CHT and BloomTrie)
// associated with the appropriate head hash by specific section index.
//
// The returned checkpoint is only the checkpoint generated by the local indexers,
// not the stable checkpoint registered in the registrar contract.
func (c *LesServer) localCheckpoint(index uint64) params.TrustedCheckpoint {
	sectionHead := c.chtIndexer.SectionHead(index)
	return params.TrustedCheckpoint{
		SectionIndex: index,
		SectionHead:  sectionHead,
		CHTRoot:      light.GetChtRoot(c.chainDb, index, sectionHead),
		BloomRoot:    light.GetBloomTrieRoot(c.chainDb, index, sectionHead),
	}
}
