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

package snap

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// MakeProtocols constructs the P2P protocol definitions for `snap`.
func MakeProtocols(chain *core.BlockChain, dnsdisc enode.Iterator) []p2p.Protocol {
	protocols := make([]p2p.Protocol, len(protocolVersions))
	for i, version := range protocolVersions {
		protocols[i] = p2p.Protocol{
			Name:    protocolName,
			Version: version,
			Length:  protocolLengths[version],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				return handle(NewPeer(version, p, rw), chain)
			},
			NodeInfo: func() interface{} {
				return nodeInfo(chain)
			},
			PeerInfo: func(id enode.ID) interface{} {
				if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
					return p.Info()
				}
				return nil
			},
			Attributes:     []enr.Entry{&enrEntry{}},
			DialCandidates: dnsdisc,
		}
	}
	return protocols
}

// handle is the callback invoked to manage the life cycle of a `snap` peer.
// When this function terminates, the peer is disconnected.
func handle(p *Peer, chain *core.BlockChain) error {
	p.Log().Warn("Snapshot peer connected", "name", p.Name())
	for {
		if err := handleMessage(p, rw); err != nil {
			p.Log().Warn("Snapshot message handling failed", "err", err)
			return err
		}
	}
}

// handleMessage is invoked whenever an inbound message is received from a
// remote peer on the `spap` protocol. The remote connection is torn down upon
// returning any error.
func (pm *ProtocolManager) handleMessage(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > maxMessageSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, maxMessageSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == getAccountRangeMsg:
		// Decode the account retrieval request
		var req getAccountRangeData
		if err := msg.Decode(&req); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Retrieve the requested state and bail out if non existent
		state, err := pm.blockchain.StateAt(req.Root)
		if err != nil {
			return p2p.Send(rw, accountRangeMsg, &accountRangeData{ID: req.ID})
		}
		it, err := pm.blockchain.AccountIterator(req.Root, req.Origin)
		if err != nil {
			return p2p.Send(rw, accountRangeMsg, &accountRangeData{ID: req.ID})
		}
		defer it.Release()

		// Iterate over the requested range and pile account up
		var (
			accounts    []*accountData
			bytes       uint64
			first, last common.Hash
		)
		for it.Next() && bytes < req.Bytes && bytes < 1<<20 {
			hash, account := it.Hash(), it.Account()

			// Track the returned interval for the Merkle proofs
			if first == (common.Hash{}) {
				first = hash
			}
			last = hash

			// Assemble the reply item
			bytes += uint64(len(account))
			accounts = append(accounts, &accountData{
				Hash: hash,
				Body: account,
			})
		}
		if first == (common.Hash{}) {
			return p2p.Send(rw, accountRangeMsg, &accountRangeData{ID: req.ID})
		}
		// Generate the Merkle proofs for the first and last account
		firstProof, err := state.GetProofByHash(first)
		if err != nil {
			log.Warn("Failed to prove account range", "first", first, "err", err)
			return p2p.Send(rw, accountRangeMsg, &accountRangeData{ID: req.ID})
		}
		lastProof, err := state.GetProofByHash(last)
		if err != nil {
			log.Warn("Failed to prove account range", "last", last, "err", err)
			return p2p.Send(rw, accountRangeMsg, &accountRangeData{ID: req.ID})
		}
		// Send back anything accumulated
		return p2p.Send(rw, accountRangeMsg, &accountRangeData{
			ID:       req.ID,
			Accounts: accounts,
			Proof:    append(firstProof, lastProof...),
		})

	case msg.Code == getStorageRangeMsg:
		// Decode the storage retrieval request
		var req getStorageRangeData
		if err := msg.Decode(&req); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Not implemented, return an empty reply
		return p2p.Send(rw, storageRangeMsg, &storageRangeData{ID: req.ID})

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
}

// NodeInfo represents a short summary of the `snap` sub-protocol metadata
// known about the host peer.
type NodeInfo struct{}

// nodeInfo retrieves some `snap` protocol metadata about the running host node.
func nodeInfo(chain *core.BlockChain) *NodeInfo {
	return &NodeInfo{}
}
