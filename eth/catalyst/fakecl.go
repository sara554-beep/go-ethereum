// Copyright 2022 The go-ethereum Authors
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
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/beacon"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
)

type FakeCL struct {
	api *ConsensusAPI

	block       *types.Block        // Syncing target provided by users
	chainConfig *params.ChainConfig // Network related configs

	closed chan struct{}
	wg     sync.WaitGroup
}

func RegisterMockConsensusLayer(stack *node.Node, backend *eth.Ethereum, block *types.Block, chainConfig *params.ChainConfig) (*FakeCL, error) {
	// Sanitize that we are indeed on a post-merged network
	if !chainConfig.TerminalTotalDifficultyPassed || chainConfig.TerminalTotalDifficulty == nil {
		return nil, errors.New("network is not merged yet")
	}
	cl := &FakeCL{
		api:         NewConsensusAPI(backend),
		block:       block,
		chainConfig: chainConfig,
		closed:      make(chan struct{}),
	}
	stack.RegisterLifecycle(cl)

	return cl, nil
}

func (cl *FakeCL) Start() error {
	go cl.exchangeTransitionConfiguration()

	// Keep firing NewPayload-UpdateForkChoice combos with the provided
	// target block, it may or may not trigger the beacon sync depends
	// on if there are protocol peers connected.
	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		for {
			select {
			case <-time.NewTicker(time.Second * 5).C:
				data := beacon.BlockToExecutableData(cl.block)
				cl.api.NewPayloadV1(*data)
				cl.api.ForkchoiceUpdatedV1(beacon.ForkchoiceStateV1{
					HeadBlockHash:      cl.block.Hash(),
					SafeBlockHash:      cl.block.Hash(),
					FinalizedBlockHash: cl.block.Hash(),
				}, nil)
			case <-cl.closed:
				return
			}
		}
	}()
	return nil
}

// Stop stops the fake consensus layer to stop all background activities.
// This function can only be called for one time.
func (cl *FakeCL) Stop() error {
	close(cl.closed)
	cl.wg.Wait()

	return nil
}

// exchangeTransitionConfiguration sends transition config to engine API to let
// it confirm that consensus layer is still alive.
func (cl *FakeCL) exchangeTransitionConfiguration() {
	defer cl.wg.Done()

	for {
		select {
		case <-time.NewTicker(time.Second * 5).C:
			// Fire a transition call every 5 seconds on order to let node confirms
			// that consensus layer is still alive. Terminal block information is
			// left as empty since node may not have the terminal block at all.
			_, err := cl.api.ExchangeTransitionConfigurationV1(beacon.TransitionConfigurationV1{
				TerminalTotalDifficulty: (*hexutil.Big)(cl.chainConfig.TerminalTotalDifficulty),
			})
			if err != nil {
				log.Warn("Failed to exchange transition config", "err", err)
			}
		case <-cl.closed:
			return
		}
	}
}
