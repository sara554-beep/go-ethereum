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

package eth

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/params"
)

// Tests that post eth protocol handshake, clients perform a mutual checkpoint
// challenge to validate each other's chains. Hash mismatches, or missing ones
// during a fast sync should lead to the peer getting dropped.
func TestCheckpointChallenge(t *testing.T) {
	tests := []struct {
		syncmode   downloader.SyncMode
		checkpoint bool
		timeout    bool
		empty      bool
		match      bool
		drop       bool
	}{
		// If checkpointing is not enabled locally, don't challenge and don't drop
		{downloader.FullSync, false, false, false, false, false},
		{downloader.FastSync, false, false, false, false, false},

		// If checkpointing is enabled locally and remote response is empty, only drop during fast sync
		{downloader.FullSync, true, false, true, false, false},
		{downloader.FastSync, true, false, true, false, true}, // Special case, fast sync, unsynced peer

		// If checkpointing is enabled locally and remote response mismatches, always drop
		{downloader.FullSync, true, false, false, false, true},
		{downloader.FastSync, true, false, false, false, true},

		// If checkpointing is enabled locally and remote response matches, never drop
		{downloader.FullSync, true, false, false, true, false},
		{downloader.FastSync, true, false, false, true, false},

		// If checkpointing is enabled locally and remote times out, always drop
		{downloader.FullSync, true, true, false, true, true},
		{downloader.FastSync, true, true, false, true, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("sync %v checkpoint %v timeout %v empty %v match %v", tt.syncmode, tt.checkpoint, tt.timeout, tt.empty, tt.match), func(t *testing.T) {
			testCheckpointChallenge(t, tt.syncmode, tt.checkpoint, tt.timeout, tt.empty, tt.match, tt.drop)
		})
	}
}

func testCheckpointChallenge(t *testing.T, syncmode downloader.SyncMode, checkpoint bool, timeout bool, empty bool, match bool, drop bool) {
	// Reduce the checkpoint handshake challenge timeout
	defer func(old time.Duration) { syncChallengeTimeout = old }(syncChallengeTimeout)
	syncChallengeTimeout = 250 * time.Millisecond

	// Initialize a chain and generate a fake CHT if checkpointing is enabled
	var (
		db     = rawdb.NewMemoryDatabase()
		config = new(params.ChainConfig)
	)
	(&core.Genesis{Config: config}).MustCommit(db) // Commit genesis block
	// If checkpointing is enabled, create and inject a fake CHT and the corresponding
	// chllenge response.
	var response *types.Header
	var cht *params.TrustedCheckpoint
	if checkpoint {
		index := uint64(rand.Intn(500))
		number := (index+1)*params.CHTFrequency - 1
		response = &types.Header{Number: big.NewInt(int64(number)), Extra: []byte("valid")}

		cht = &params.TrustedCheckpoint{
			SectionIndex: index,
			SectionHead:  response.Hash(),
		}
	}
	// Create a checkpoint aware protocol manager
	blockchain, err := core.NewBlockChain(db, nil, config, ethash.NewFaker(), vm.Config{}, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	pm, err := NewProtocolManager(config, cht, syncmode, DefaultConfig.NetworkId, new(event.TypeMux), &testTxPool{pool: make(map[common.Hash]*types.Transaction)}, ethash.NewFaker(), blockchain, db, 1, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	pm.Start(1000)
	defer pm.Stop()

	// Connect a new peer and check that we receive the checkpoint challenge
	peer, _ := newTestPeer("peer", eth.eth63, pm, true)
	defer peer.close()

	if checkpoint {
		challenge := &eth.getBlockHeadersData{
			Origin:  eth.hashOrNumber{Number: response.Number.Uint64()},
			Amount:  1,
			Skip:    0,
			Reverse: false,
		}
		if err := p2p.ExpectMsg(peer.app, eth.ethGetBlockHeadersMsg, challenge); err != nil {
			t.Fatalf("challenge mismatch: %v", err)
		}
		// Create a block to reply to the challenge if no timeout is simulated
		if !timeout {
			if empty {
				if err := p2p.Send(peer.app, eth.ethBlockHeadersMsg, []*types.Header{}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			} else if match {
				if err := p2p.Send(peer.app, eth.ethBlockHeadersMsg, []*types.Header{response}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			} else {
				if err := p2p.Send(peer.app, eth.ethBlockHeadersMsg, []*types.Header{{Number: response.Number}}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			}
		}
	}
	// Wait until the test timeout passes to ensure proper cleanup
	time.Sleep(syncChallengeTimeout + 300*time.Millisecond)

	// Verify that the remote peer is maintained or dropped
	if drop {
		if peers := pm.peers.Len(); peers != 0 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 0)
		}
	} else {
		if peers := pm.peers.Len(); peers != 1 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 1)
		}
	}
}

func TestBroadcastBlock(t *testing.T) {
	var tests = []struct {
		totalPeers        int
		broadcastExpected int
	}{
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{9, 3},
		{12, 3},
		{16, 4},
		{26, 5},
		{100, 10},
	}
	for _, test := range tests {
		testBroadcastBlock(t, test.totalPeers, test.broadcastExpected)
	}
}

func testBroadcastBlock(t *testing.T, totalPeers, broadcastExpected int) {
	var (
		evmux   = new(event.TypeMux)
		pow     = ethash.NewFaker()
		db      = rawdb.NewMemoryDatabase()
		config  = &params.ChainConfig{}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, pow, vm.Config{}, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	pm, err := NewProtocolManager(config, nil, downloader.FullSync, DefaultConfig.NetworkId, evmux, &testTxPool{pool: make(map[common.Hash]*types.Transaction)}, pow, blockchain, db, 1, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	pm.Start(1000)
	defer pm.Stop()
	var peers []*testPeer
	for i := 0; i < totalPeers; i++ {
		peer, _ := newTestPeer(fmt.Sprintf("peer %d", i), eth.eth63, pm, true)
		defer peer.close()

		peers = append(peers, peer)
	}
	chain, _ := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 1, func(i int, gen *core.BlockGen) {})
	pm.BroadcastBlock(chain[0], true /*propagate*/)

	errCh := make(chan error, totalPeers)
	doneCh := make(chan struct{}, totalPeers)
	for _, peer := range peers {
		go func(p *testPeer) {
			if err := p2p.ExpectMsg(p.app, eth.ethNewBlockMsg, &eth.newBlockData{Block: chain[0], TD: big.NewInt(131136)}); err != nil {
				errCh <- err
			} else {
				doneCh <- struct{}{}
			}
		}(peer)
	}
	var received int
	for {
		select {
		case <-doneCh:
			received++

		case <-time.After(100 * time.Millisecond):
			if received != broadcastExpected {
				t.Errorf("broadcast count mismatch: have %d, want %d", received, broadcastExpected)
			}
			return

		case err = <-errCh:
			t.Fatalf("broadcast failed: %v", err)
		}
	}

}

// Tests that a propagated malformed block (uncles or transactions don't match
// with the hashes in the header) gets discarded and not broadcast forward.
func TestBroadcastMalformedBlock(t *testing.T) {
	// Create a live node to test propagation with
	var (
		engine  = ethash.NewFaker()
		db      = rawdb.NewMemoryDatabase()
		config  = &params.ChainConfig{}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	pm, err := NewProtocolManager(config, nil, downloader.FullSync, DefaultConfig.NetworkId, new(event.TypeMux), new(testTxPool), engine, blockchain, db, 1, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	pm.Start(2)
	defer pm.Stop()

	// Create two peers, one to send the malformed block with and one to check
	// propagation
	source, _ := newTestPeer("source", eth.eth63, pm, true)
	defer source.close()

	sink, _ := newTestPeer("sink", eth.eth63, pm, true)
	defer sink.close()

	// Create various combinations of malformed blocks
	chain, _ := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 1, func(i int, gen *core.BlockGen) {})

	malformedUncles := chain[0].Header()
	malformedUncles.UncleHash[0]++
	malformedTransactions := chain[0].Header()
	malformedTransactions.TxHash[0]++
	malformedEverything := chain[0].Header()
	malformedEverything.UncleHash[0]++
	malformedEverything.TxHash[0]++

	// Keep listening to broadcasts and notify if any arrives
	notify := make(chan struct{}, 1)
	go func() {
		if _, err := sink.app.ReadMsg(); err == nil {
			notify <- struct{}{}
		}
	}()
	// Try to broadcast all malformations and ensure they all get discarded
	for _, header := range []*types.Header{malformedUncles, malformedTransactions, malformedEverything} {
		block := types.NewBlockWithHeader(header).WithBody(chain[0].Transactions(), chain[0].Uncles())
		if err := p2p.Send(source.app, eth.ethNewBlockMsg, []interface{}{block, big.NewInt(131136)}); err != nil {
			t.Fatalf("failed to broadcast block: %v", err)
		}
		select {
		case <-notify:
			t.Fatalf("malformed block forwarded")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
