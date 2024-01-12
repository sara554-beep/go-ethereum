// Copyright 2014 The go-ethereum Authors
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

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
)

// Backend wraps all methods required for mining. Only full node is capable
// to offer all the functions here.
type Backend interface {
	BlockChain() *core.BlockChain
	TxPool() *txpool.TxPool
}

// Config is the configuration parameters of mining.
type Config struct {
	Etherbase common.Address `toml:",omitempty"` // Public address for block mining rewards
	ExtraData hexutil.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	GasCeil   uint64         // Target gas ceiling for mined blocks.
	GasPrice  *big.Int       // Minimum gas price for mining a transaction

	Recommit time.Duration // The time interval for miner to re-create mining work.
}

// DefaultConfig contains default settings for miner.
var DefaultConfig = Config{
	GasCeil:  30_000_000,
	GasPrice: big.NewInt(params.GWei),

	// The default recommit time is chosen as two seconds since
	// consensus-layer usually will wait a half slot of time(6s)
	// for payload generation. It should be enough for Geth to
	// run 3 rounds.
	Recommit: 2 * time.Second,
}

// Miner is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type Miner struct {
	confMu      sync.RWMutex // The lock used to protect the config
	config      *Config
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	txpool      *txpool.TxPool
	chain       *core.BlockChain

	pendingMu    sync.Mutex // The lock used to protect the pending cache
	pendingCache *newPayloadResult
	cacheTime    time.Time

	// Feeds
	pendingLogsFeed event.Feed

	running atomic.Bool
}

// New creates a new miner.
func New(eth Backend, config *Config, engine consensus.Engine) *Miner {
	if config == nil {
		config = &DefaultConfig
	}
	worker := &Miner{
		config:      config,
		chainConfig: eth.BlockChain().Config(),
		engine:      engine,
		txpool:      eth.TxPool(),
		chain:       eth.BlockChain(),
	}
	worker.running.Store(true)
	return worker
}

func (miner *Miner) Close() {
	miner.running.Store(false)
}

func (miner *Miner) Mining() bool {
	return miner.running.Load()
}

// setExtra sets the content used to initialize the block extra field.
func (miner *Miner) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	miner.confMu.Lock()
	defer miner.confMu.Unlock()
	miner.config.ExtraData = extra
	return nil
}

// Pending returns the currently pending block and associated state. The returned
// values can be nil in case the pending block is not initialized
func (miner *Miner) Pending() (*types.Block, *state.StateDB) {
	block := miner.pending()
	return block.block, block.stateDB
}

// PendingBlock returns the currently pending block. The returned block can be
// nil in case the pending block is not initialized.
//
// Note, to access both the pending block and the pending state
// simultaneously, please use Pending(), as the pending state can
// change between multiple method calls
func (miner *Miner) PendingBlock() *types.Block {
	block := miner.pending()
	return block.block
}

// PendingBlockAndReceipts returns the currently pending block and corresponding receipts.
// The returned values can be nil in case the pending block is not initialized.
func (miner *Miner) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	block := miner.pending()
	return block.block, block.receipts
}

func (miner *Miner) SetEtherbase(addr common.Address) {
	miner.confMu.Lock()
	defer miner.confMu.Unlock()
	miner.config.Etherbase = addr
}

// SetGasCeil sets the gaslimit to strive for when mining blocks post 1559.
// For pre-1559 blocks, it sets the ceiling.
func (miner *Miner) SetGasCeil(ceil uint64) {
	miner.confMu.Lock()
	defer miner.confMu.Unlock()
	miner.config.GasCeil = ceil
}

// SubscribePendingLogs starts delivering logs from pending transactions
// to the given channel.
func (miner *Miner) SubscribePendingLogs(ch chan<- []*types.Log) event.Subscription {
	return miner.pendingLogsFeed.Subscribe(ch)
}

// BuildPayload builds the payload according to the provided parameters.
func (miner *Miner) BuildPayload(args *BuildPayloadArgs) (*Payload, error) {
	return miner.buildPayload(args)
}

// pending returns the pending state and corresponding block. The returned
// values can be nil in case the pending block is not initialized.
func (miner *Miner) pending() *newPayloadResult {
	// Read config
	miner.confMu.RLock()
	coinbase := miner.config.Etherbase
	miner.confMu.RUnlock()
	// Lock pending block
	miner.pendingMu.Lock()
	defer miner.pendingMu.Unlock()
	if time.Since(miner.cacheTime) < time.Second && coinbase == miner.pendingCache.block.Coinbase() {
		return miner.pendingCache
	}
	pending := miner.generateWork(&generateParams{
		timestamp:   uint64(time.Now().Unix()),
		forceTime:   true,
		parentHash:  miner.chain.CurrentBlock().Hash(),
		coinbase:    coinbase,
		random:      common.Hash{},
		withdrawals: nil,
		beaconRoot:  nil,
		noTxs:       false,
	})
	if pending.err != nil {
		// force subsequent calls to recreate pending block
		miner.cacheTime = time.Time{}
		return &newPayloadResult{}
	}
	miner.pendingCache = pending
	miner.cacheTime = time.Now()
	return pending
}
