// Copyright 2018 The go-ethereum Authors
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
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/light"
)

func TestCheckpointSyncingLes1(t *testing.T) { testCheckpointSyncing(t, 1) }
func TestCheckpointSyncingLes2(t *testing.T) { testCheckpointSyncing(t, 2) }

func testCheckpointSyncing(t *testing.T, protocol int) {
	config := light.TestServerIndexerConfig
	waitIndexers := func(cIndexer, bIndexer, btIndexer *core.ChainIndexer) {
		for {
			cs, _, _ := cIndexer.Sections()
			bs, _, _ := bIndexer.Sections()
			bts, _, _ := btIndexer.Sections()
			if cs >= config.PairChtSize/config.ChtSize && bs >= config.PairChtSize/config.BloomSize &&
				bts >= config.PairChtSize/config.BloomTrieSize {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	server, client, tearDown := newClientServerEnv(t, int(config.PairChtSize+config.ChtConfirm), protocol, waitIndexers, false)
	defer tearDown()

	// Add trusted checkpoint for client side indexers.
	cs, _, head := server.chtIndexer.Sections()
	chtRoot := light.GetChtRoot(server.db, cs-1, head)
	bts, _, head := server.bloomTrieIndexer.Sections()
	btRoot := light.GetBloomTrieRoot(server.db, bts-1, head)
	// Register fake stable checkpoint
	ckp := &light.TrustedCheckpoint{
		SectionIdx:    0,
		SectionHead:   head,
		ChtRoot:       chtRoot,
		BloomTrieRoot: btRoot,
	}
	light.WriteTrustedCheckpoint(server.db, ckp)

	// Create connected peer pair.
	peer, err1, lPeer, err2 := newTestPeerPair("peer", protocol, server.pm, client.pm)
	select {
	case <-time.After(time.Millisecond * 100):
	case err := <-err1:
		t.Fatalf("peer 1 handshake error: %v", err)
	case err := <-err2:
		t.Fatalf("peer 2 handshake error: %v", err)
	}
	server.rPeer, client.rPeer = peer, lPeer

	time.Sleep(time.Second * 10)
}
