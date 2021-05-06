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

package catalyst

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)

	testBalance = big.NewInt(2e10)
)

func generatePreMergeChain(n int) (*core.Genesis, []*types.Block) {
	db := rawdb.NewMemoryDatabase()
	config := params.AllEthashProtocolChanges
	genesis := &core.Genesis{
		Config:    config,
		Alloc:     core.GenesisAlloc{testAddr: {Balance: testBalance}},
		ExtraData: []byte("test genesis"),
		Timestamp: 9000,
	}
	testNonce := uint64(0)
	generate := func(i int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("test"))

		tx, _ := types.SignTx(types.NewTransaction(testNonce, testAddr, big.NewInt(0), params.TxGas, nil, nil), types.LatestSigner(config), testKey)
		g.AddTx(tx)
		testNonce++
	}
	gblock := genesis.ToBlock(db)
	engine := ethash.NewFaker()
	blocks, _ := core.GenerateChain(config, gblock, engine, db, n, generate)
	return genesis, blocks
}

func TestEth2AssembleBlock(t *testing.T) {
	genesis, blocks := generatePreMergeChain(10)
	n, ethservice := startEthService(t, genesis, blocks)
	defer n.Close()

	api := newConsensusAPI(ethservice)
	signer := types.NewEIP155Signer(ethservice.BlockChain().Config().ChainID)
	tx, err := types.SignTx(types.NewTransaction(uint64(10), blocks[9].Coinbase(), big.NewInt(1000), params.TxGas, nil, nil), signer, testKey)
	if err != nil {
		t.Fatalf("error signing transaction, err=%v", err)
	}
	ethservice.TxPool().AddLocal(tx)
	blockParams := assembleBlockParams{
		ParentHash: blocks[9].Hash(),
		Timestamp:  blocks[9].Time() + 5,
	}
	execData, err := api.AssembleBlock(blockParams)
	if err != nil {
		t.Fatalf("error producing block, err=%v", err)
	}
	if len(execData.Transactions) != 1 {
		t.Fatalf("invalid number of transactions %d != 1", len(execData.Transactions))
	}
}

func TestEth2AssembleBlockWithAnotherBlocksTxs(t *testing.T) {
	genesis, blocks := generatePreMergeChain(10)
	n, ethservice := startEthService(t, genesis, blocks[:9])
	defer n.Close()

	api := newConsensusAPI(ethservice)

	// Put the 10th block's tx in the pool and produce a new block
	api.addBlockTxs(blocks[9])
	blockParams := assembleBlockParams{
		ParentHash: blocks[8].Hash(),
		Timestamp:  blocks[8].Time() + 5,
	}
	execData, err := api.AssembleBlock(blockParams)
	if err != nil {
		t.Fatalf("error producing block, err=%v", err)
	}
	if len(execData.Transactions) != blocks[9].Transactions().Len() {
		t.Fatalf("invalid number of transactions %d != 1", len(execData.Transactions))
	}
}

func checkLogEvents(t *testing.T, logsCh <-chan []*types.Log, rmLogsCh <-chan core.RemovedLogsEvent, wantNew, wantRemoved int) {
	t.Helper()

	if len(logsCh) != wantNew {
		t.Fatalf("wrong number of log events: got %d, want %d", len(logsCh), wantNew)
	}
	if len(rmLogsCh) != wantRemoved {
		t.Fatalf("wrong number of removed log events: got %d, want %d", len(rmLogsCh), wantRemoved)
	}
	// Drain events.
	for i := 0; i < len(logsCh); i++ {
		<-logsCh
	}
	for i := 0; i < len(rmLogsCh); i++ {
		<-rmLogsCh
	}
}

func TestEth2NewBlock(t *testing.T) {
	genesis, preMergeBlocks := generatePreMergeChain(10)
	n, ethservice := startEthService(t, genesis, preMergeBlocks)
	defer n.Close()

	var (
		api    = newConsensusAPI(ethservice)
		parent = preMergeBlocks[len(preMergeBlocks)-1]

		// This EVM code generates a log when the contract is created.
		logCode = common.Hex2Bytes("60606040525b7f24ec1d3ff24c2f6ff210738839dbc339cd45a5294d85c79361016243157aae7b60405180905060405180910390a15b600a8060416000396000f360606040526008565b00")
	)
	// The event channels.
	newLogCh := make(chan []*types.Log, 10)
	rmLogsCh := make(chan core.RemovedLogsEvent, 10)
	ethservice.BlockChain().SubscribeLogsEvent(newLogCh)
	ethservice.BlockChain().SubscribeRemovedLogsEvent(rmLogsCh)

	for i := 0; i < 10; i++ {
		statedb, _ := ethservice.BlockChain().StateAt(parent.Root())
		nonce := statedb.GetNonce(testAddr)
		tx, err := types.SignTx(types.NewContractCreation(nonce, new(big.Int), 1000000, new(big.Int), logCode), types.LatestSigner(ethservice.BlockChain().Config()), testKey)
		ethservice.TxPool().AddLocal(tx)

		execData, err := api.AssembleBlock(assembleBlockParams{
			ParentHash: parent.Hash(),
			Timestamp:  parent.Time() + 5,
		})
		if err != nil {
			t.Fatalf("Failed to create the executable data %v", err)
		}
		block, err := insertBlockParamsToBlock(*execData)
		if err != nil {
			t.Fatalf("Failed to convert executable data to block %v", err)
		}
		newResp, err := api.NewBlock(*execData)
		if err != nil || !newResp.Valid {
			t.Fatalf("Failed to insert block: %v", err)
		}
		if ethservice.BlockChain().CurrentBlock().NumberU64() != block.NumberU64()-1 {
			t.Fatalf("Chain head shouldn't be updated")
		}
		checkLogEvents(t, newLogCh, rmLogsCh, 0, 0)

		setResp, err := api.SetHead(block.Hash())
		if err != nil || !setResp.Success {
			t.Fatalf("Failed to insert block: %v", err)
		}
		if ethservice.BlockChain().CurrentBlock().NumberU64() != block.NumberU64() {
			t.Fatalf("Chain head should be updated")
		}
		checkLogEvents(t, newLogCh, rmLogsCh, 1, 0)

		parent = block
	}

	// Introduce fork chain
	var (
		head = ethservice.BlockChain().CurrentBlock().NumberU64()
	)
	parent = preMergeBlocks[len(preMergeBlocks)-1]
	for i := 0; i < 10; i++ {
		execData, err := api.AssembleBlock(assembleBlockParams{
			ParentHash: parent.Hash(),
			Timestamp:  parent.Time() + 6,
		})
		if err != nil {
			t.Fatalf("Failed to create the executable data %v", err)
		}
		block, err := insertBlockParamsToBlock(*execData)
		if err != nil {
			t.Fatalf("Failed to convert executable data to block %v", err)
		}
		newResp, err := api.NewBlock(*execData)
		if err != nil || !newResp.Valid {
			t.Fatalf("Failed to insert block: %v", err)
		}
		if ethservice.BlockChain().CurrentBlock().NumberU64() != head {
			t.Fatalf("Chain head shouldn't be updated")
		}

		setResp, err := api.SetHead(block.Hash())
		if err != nil || !setResp.Success {
			t.Fatalf("Failed to insert block: %v", err)
		}
		if ethservice.BlockChain().CurrentBlock().NumberU64() != block.NumberU64() {
			t.Fatalf("Chain head should be updated")
		}
		parent, head = block, block.NumberU64()
	}
}

// startEthService creates a full node instance for testing.
func startEthService(t *testing.T, genesis *core.Genesis, blocks []*types.Block) (*node.Node, *eth.Ethereum) {
	t.Helper()

	n, err := node.New(&node.Config{})
	if err != nil {
		t.Fatal("can't create node:", err)
	}

	ethcfg := &ethconfig.Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}}
	ethservice, err := eth.New(n, ethcfg)
	if err != nil {
		t.Fatal("can't create eth service:", err)
	}
	if err := n.Start(); err != nil {
		t.Fatal("can't start node:", err)
	}
	if _, err := ethservice.BlockChain().InsertChain(blocks); err != nil {
		n.Close()
		t.Fatal("can't import test blocks:", err)
	}
	ethservice.SetEtherbase(testAddr)

	return n, ethservice
}
