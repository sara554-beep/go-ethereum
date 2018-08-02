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
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
)

const EpochLength = 300

// syncer is responsible for periodically synchronising with the network, both
// downloading hashes and blocks as well as handling the announcement handler.
func (pm *ProtocolManager) syncer() {
	// Start and ensure cleanup of sync mechanisms
	//pm.fetcher.Start()
	//defer pm.fetcher.Stop()
	defer pm.downloader.Terminate()

	// Wait for different events to fire synchronisation operations
	//forceSync := time.Tick(forceSyncCycle)
	for {
		select {
		case <-pm.newPeerCh:
			/*			// Make sure we have peers to select from, then sync
						if pm.peers.Len() < minDesiredPeerCount {
							break
						}
						go pm.synchronise(pm.peers.BestPeer())
			*/
		/*case <-forceSync:
		// Force a sync even if not enough peers are present
		go pm.synchronise(pm.peers.BestPeer())
		*/
		case <-pm.noMorePeers:
			return
		}
	}
}

func (pm *ProtocolManager) needToSync(peerHead blockInfo) bool {
	head := pm.blockchain.CurrentHeader()
	currentTd := rawdb.ReadTd(pm.chainDb, head.Hash(), head.Number.Uint64())
	return currentTd != nil && peerHead.Td.Cmp(currentTd) > 0
}

// synchronise tries to sync up our local block chain with a remote peer.
func (pm *ProtocolManager) synchronise(peer *peer) {
	// Short circuit if no peers are available
	if peer == nil {
		return
	}
	// Make sure the peer's TD is higher than our own.
	if !pm.needToSync(peer.headBlockInfo()) {
		return
	}
	// Never try to sync with a peer that doesn't advertise stable checkpoint.
	checkpoint := peer.advertisedCheckpoint
	if checkpoint == (light.TrustedCheckpoint{}) {
		return
	}
	// Fetch the last block headers of each epoch which covered by peer's advertised checkpoint.
	checkpointHeight := (checkpoint.SectionIdx+1)*pm.indexerConfig.CheckpointSize - 1
	log.Info("Start synchronization", "target peer", peer.ID(), "checkpoint height", checkpointHeight)
	var numbers []uint64
	var i = uint64(1)
	for i*EpochLength-1 <= checkpointHeight {
		numbers = append(numbers, i*EpochLength-1)
		i += 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	headers, tds, err := light.UntrustedGetHeadersByNumber(ctx, pm.odr, checkpoint.ChtRoot, checkpoint.SectionIdx, numbers)
	if err != nil {
		log.Info("Synchronise failed", "err", err)
	}
	fmt.Println(headers, tds)

	var startHeader *types.Header
	var threshold = big.NewInt(0).Div(peer.headInfo.Td, big.NewInt(100))
	for i := len(headers) - 1; i >= 0; i-- {
		if big.NewInt(0).Sub(peer.headInfo.Td, tds[i]).Cmp(threshold) > 0 {
			startHeader = headers[i]
			break
		}

	}
	if startHeader == nil {
		log.Info("Invalid peer")
		return
	}
	pm.blockchain.(*light.LightChain).SetHeader(startHeader)
	// Sync start from the start point
	err = pm.downloader.Synchronise(peer.id, peer.Head(), peer.Td(), downloader.LightSync)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("sync from", startHeader.Number, "done")
	fmt.Println("current head", pm.blockchain.CurrentHeader().Number)

	// Total difficulty checking

}
