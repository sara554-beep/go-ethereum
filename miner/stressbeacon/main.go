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

// This file contains a miner stress test for the eth1/2 transition
package main

import (
	"crypto/ecdsa"
	"errors"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/catalyst"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/miner"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
)

type nodetype int

const (
	legacyMiningNode nodetype = iota
	legacyNormalNode
	eth2MiningNode
	eth2NormalNode
)

var (
	// transitionDifficulty is the target total difficulty for transition
	transitionDifficulty = new(big.Int).Mul(big.NewInt(15), params.MinimumDifficulty)
)

type ethNode struct {
	typ     nodetype
	api     *catalyst.ConsensusAPI
	backend *eth.Ethereum
	stack   *node.Node
	enode   *enode.Node
}

func newNode(typ nodetype, genesis *core.Genesis, enodes []*enode.Node) *ethNode {
	// Start the node and wait until it's up
	stack, ethBackend, api, err := makeMiner(genesis)
	if err != nil {
		panic(err)
	}
	for stack.Server().NodeInfo().Ports.Listener == 0 {
		time.Sleep(250 * time.Millisecond)
	}
	// Connect the node to all the previous ones
	for _, n := range enodes {
		stack.Server().AddPeer(n)
	}
	enode := stack.Server().Self()

	// Inject the signer key and start sealing with it
	store := stack.AccountManager().Backends(keystore.KeyStoreType)[0].(*keystore.KeyStore)
	if _, err := store.NewAccount(""); err != nil {
		panic(err)
	}
	return &ethNode{
		typ:     typ,
		api:     api,
		backend: ethBackend,
		stack:   stack,
		enode:   enode,
	}
}

func (n *ethNode) assembleBlock(parentHash common.Hash, parentTimestamp uint64) (*catalyst.ExecutableData, error) {
	if n.typ != eth2MiningNode {
		return nil, errors.New("invalid node type")
	}
	return n.api.AssembleBlock(catalyst.AssembleBlockParams{
		ParentHash: parentHash,
		Timestamp:  parentTimestamp + 3, // hardcode here
	})
}

func (n *ethNode) insertBlock(eb catalyst.ExecutableData) error {
	if n.typ != eth2MiningNode && n.typ != eth2NormalNode {
		return errors.New("invalid node type")
	}
	response, err := n.api.NewBlock(eb)
	if err != nil {
		return err
	}
	if !response.Valid {
		return errors.New("failed to insert block")
	}
	return nil
}

func (n *ethNode) insertBlockAndSetHead(ed catalyst.ExecutableData) error {
	if n.typ != eth2MiningNode && n.typ != eth2NormalNode {
		return errors.New("invalid node type")
	}
	if err := n.insertBlock(ed); err != nil {
		return err
	}
	block, err := catalyst.InsertBlockParamsToBlock(ed)
	if err != nil {
		return err
	}
	response, err := n.api.SetHead(block.Hash())
	if err != nil {
		return err
	}
	if !response.Success {
		return errors.New("failed to set head")
	}
	return nil
}

type nodeManager struct {
	genesis *core.Genesis
	nodes   []*ethNode
	enodes  []*enode.Node
	close   chan struct{}
}

func newNodeManager(genesis *core.Genesis) *nodeManager {
	return &nodeManager{
		close:   make(chan struct{}),
		genesis: genesis,
	}
}

func (mgr *nodeManager) createNode(typ nodetype) {
	node := newNode(typ, mgr.genesis, mgr.enodes)
	mgr.nodes = append(mgr.nodes, node)
	mgr.enodes = append(mgr.enodes, node.enode)
}

func (mgr *nodeManager) getNodes(typ nodetype) []*ethNode {
	var ret []*ethNode
	for _, node := range mgr.nodes {
		if node.typ == typ {
			ret = append(ret, node)
		}
	}
	return ret
}

func (mgr *nodeManager) startMining() {
	for _, node := range append(mgr.getNodes(eth2MiningNode), mgr.getNodes(legacyMiningNode)...) {
		if err := node.backend.StartMining(1); err != nil {
			panic(err)
		}
	}
}

func (mgr *nodeManager) shutdown() {
	close(mgr.close)
	for _, node := range mgr.nodes {
		node.stack.Close()
	}
}

func (mgr *nodeManager) run() {
	if len(mgr.nodes) == 0 {
		return
	}
	chain := mgr.nodes[0].backend.BlockChain()
	sink := make(chan core.ChainHeadEvent, 1024)
	sub := chain.SubscribeChainHeadEvent(sink)
	defer sub.Unsubscribe()

	var (
		transitioned bool
		parentBlock  *types.Block
	)
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // discard the initial tick

	for {
		select {
		case <-mgr.close:
			return

		case ev := <-sink:
			if transitioned {
				continue
			}
			td := chain.GetTd(ev.Block.Hash(), ev.Block.NumberU64())
			if td.Cmp(transitionDifficulty) < 0 {
				continue
			}
			transitioned, parentBlock = true, ev.Block
			timer.Reset(time.Second * 3)
			log.Info("Transition difficulty reached", "td", td, "target", transitionDifficulty)

		case <-timer.C:
			producers := mgr.getNodes(eth2MiningNode)
			if len(producers) == 0 {
				continue
			}
			ed, err := producers[0].assembleBlock(parentBlock.Hash(), parentBlock.Time())
			if err != nil {
				log.Error("Failed to assemble the block", "err", err)
				continue
			}
			block, _ := catalyst.InsertBlockParamsToBlock(*ed)
			for _, node := range append(mgr.getNodes(eth2MiningNode), mgr.getNodes(eth2NormalNode)...) {
				if err := node.insertBlockAndSetHead(*ed); err != nil {
					log.Error("Failed to insert block", "err", err)
				}
			}
			log.Info("Create and insert eth2 block", "number", ed.Number)
			parentBlock = block
			timer.Reset(time.Second * 3)
		}
	}
}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	fdlimit.Raise(2048)

	// Generate a batch of accounts to seal and fund with
	faucets := make([]*ecdsa.PrivateKey, 16)
	for i := 0; i < len(faucets); i++ {
		faucets[i], _ = crypto.GenerateKey()
	}
	// Pre-generate the ethash mining DAG so we don't race
	ethash.MakeDataset(1, filepath.Join(os.Getenv("HOME"), ".ethash"))

	// Create an Ethash network based off of the Ropsten config
	genesis := makeGenesis(faucets)
	manager := newNodeManager(genesis)
	defer manager.shutdown()

	manager.createNode(eth2NormalNode)
	manager.createNode(eth2MiningNode)
	manager.createNode(legacyMiningNode)
	manager.createNode(legacyNormalNode)

	// Iterate over all the nodes and start mining
	time.Sleep(3 * time.Second)
	manager.startMining()
	go manager.run()

	// Start injecting transactions from the faucets like crazy
	time.Sleep(3 * time.Second)
	nonces := make([]uint64, len(faucets))
	for {
		// Pick a random mining node
		nodes := manager.getNodes(eth2MiningNode)

		index := rand.Intn(len(faucets))
		node := nodes[index%len(nodes)]

		// Create a self transaction and inject into the pool
		tx, err := types.SignTx(types.NewTransaction(nonces[index], crypto.PubkeyToAddress(faucets[index].PublicKey), new(big.Int), 21000, big.NewInt(100000000000+rand.Int63n(65536)), nil), types.HomesteadSigner{}, faucets[index])
		if err != nil {
			panic(err)
		}
		if err := node.backend.TxPool().AddLocal(tx); err != nil {
			panic(err)
		}
		nonces[index]++

		// Wait if we're too saturated
		if pend, _ := node.backend.TxPool().Stats(); pend > 2048 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// makeGenesis creates a custom Ethash genesis block based on some pre-defined
// faucet accounts.
func makeGenesis(faucets []*ecdsa.PrivateKey) *core.Genesis {
	genesis := core.DefaultRopstenGenesisBlock()
	genesis.Difficulty = params.MinimumDifficulty
	genesis.GasLimit = 25000000

	genesis.Config.ChainID = big.NewInt(18)
	genesis.Config.EIP150Hash = common.Hash{}

	genesis.Alloc = core.GenesisAlloc{}
	for _, faucet := range faucets {
		genesis.Alloc[crypto.PubkeyToAddress(faucet.PublicKey)] = core.GenesisAccount{
			Balance: new(big.Int).Exp(big.NewInt(2), big.NewInt(128), nil),
		}
	}
	return genesis
}

func makeMiner(genesis *core.Genesis) (*node.Node, *eth.Ethereum, *catalyst.ConsensusAPI, error) {
	// Define the basic configurations for the Ethereum node
	datadir, _ := ioutil.TempDir("", "")

	config := &node.Config{
		Name:    "geth",
		Version: params.Version,
		DataDir: datadir,
		P2P: p2p.Config{
			ListenAddr:  "0.0.0.0:0",
			NoDiscovery: true,
			MaxPeers:    25,
		},
		UseLightweightKDF: true,
	}
	// Create the node and configure a full Ethereum node on it
	stack, err := node.New(config)
	if err != nil {
		return nil, nil, nil, err
	}
	ethBackend, err := eth.New(stack, &ethconfig.Config{
		Genesis:         genesis,
		NetworkId:       genesis.Config.ChainID.Uint64(),
		SyncMode:        downloader.FullSync,
		DatabaseCache:   256,
		DatabaseHandles: 256,
		TxPool:          core.DefaultTxPoolConfig,
		GPO:             ethconfig.Defaults.GPO,
		Ethash:          ethconfig.Defaults.Ethash,
		Miner: miner.Config{
			GasFloor: genesis.GasLimit * 9 / 10,
			GasCeil:  genesis.GasLimit * 11 / 10,
			GasPrice: big.NewInt(1),
			Recommit: 3 * time.Second,
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	err = stack.Start()
	return stack, ethBackend, catalyst.NewConsensusAPI(ethBackend), err
}
