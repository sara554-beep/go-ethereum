// Copyright 2019 The go-ethereum Authors
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

package lesclient

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/les/lesclient/fetcher"
	"github.com/ethereum/go-ethereum/p2p/enode"

	"github.com/ethereum/go-ethereum/les/lesclient/distributor"
	"github.com/ethereum/go-ethereum/les/metrics"
	"github.com/ethereum/go-ethereum/les/protocol"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/params"
)

// clientHandler is responsible for receiving and processing all incoming server
// responses.
type clientHandler struct {
	ulc        *fetcher.ULC
	checkpoint *params.TrustedCheckpoint
	fetcher    *fetcher.LightFetcher
	downloader *downloader.Downloader
	backend    *LightEthereum

	closeCh  chan struct{}
	wg       sync.WaitGroup // WaitGroup used to track all connected peers.
	syncDone func()         // Test hooks when syncing is done.
}

func newClientHandler(ulcServers []string, ulcFraction int, checkpoint *params.TrustedCheckpoint, backend *LightEthereum) *clientHandler {
	handler := &clientHandler{
		checkpoint: checkpoint,
		backend:    backend,
		closeCh:    make(chan struct{}),
	}
	if ulcServers != nil {
		ulc, err := fetcher.NewULC(ulcServers, ulcFraction)
		if err != nil {
			log.Error("Failed to initialize ultra light client")
		}
		handler.ulc = ulc
		log.Info("Enable ultra light client mode")
	}
	var height uint64
	if checkpoint != nil {
		height = (checkpoint.SectionIndex+1)*params.CHTFrequency - 1
	}
	handler.fetcher = fetcher.NewLightFetcher(handler, handler.backend.blockchain, handler.backend.reqDist, handler.ulc)
	handler.downloader = downloader.New(height, backend.chainDb, nil, backend.eventMux, nil, backend.blockchain, handler.removePeer)
	handler.backend.peers.Subscribe((*downloaderPeerNotify)(handler))
	return handler
}

func (h *clientHandler) stop() {
	close(h.closeCh)
	h.downloader.Terminate()
	h.fetcher.Close()
	h.wg.Wait()
}

func (h *clientHandler) ChainDB() ethdb.Database {
	return h.backend.chainDb
}

func (h *clientHandler) RemovePeer(id enode.ID) {
	h.backend.peers.Unregister(id.String())
}

func (h *clientHandler) TriggerSync(id enode.ID) {
}

// runPeer is the p2p protocol run function for the given version.
func (h *clientHandler) runPeer(version uint, p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := NewServerPeer(int(version), h.backend.config.NetworkId, false, p, metrics.NewMeteredMsgWriter(rw, int(version)))
	defer peer.Close()
	entry := h.backend.serverPool.Connect(peer, peer.Node())
	if entry == nil {
		return p2p.DiscRequested
	}
	h.wg.Add(1)
	defer h.wg.Done()
	err := h.handle(peer)
	h.backend.serverPool.Disconnect(entry)
	return err
}

func (h *clientHandler) handle(p *Peer) error {
	if h.backend.peers.Len() >= h.backend.config.LightPeers && !p.Peer.Info().Network.Trusted {
		return p2p.DiscTooManyPeers
	}
	p.Log().Debug("Light Ethereum peer connected", "name", p.Name())

	// Execute the LES handshake
	var (
		head   = h.backend.blockchain.CurrentHeader()
		hash   = head.Hash()
		number = head.Number.Uint64()
		td     = h.backend.blockchain.GetTd(hash, number)
	)
	if err := p.Handshake(td, hash, number, h.backend.blockchain.Genesis().Hash()); err != nil {
		p.Log().Debug("Light Ethereum handshake failed", "err", err)
		return err
	}
	// Register the peer locally
	if err := h.backend.peers.Register(p); err != nil {
		p.Log().Error("Light Ethereum peer registration failed", "err", err)
		return err
	}
	metrics.ServerConnectionGauge.Update(int64(h.backend.peers.Len()))

	connectedAt := mclock.Now()
	defer func() {
		h.backend.peers.Unregister(p.ID().String())
		metrics.ConnectionTimer.Update(time.Duration(mclock.Now() - connectedAt))
		metrics.ServerConnectionGauge.Update(int64(h.backend.peers.Len()))
	}()

	// h.fetcher.Announce(p, &protocol.AnnounceData{Hash: p.headInfo.Hash, Number: p.headInfo.Number, Td: p.headInfo.Td})

	// pool entry can be nil during the unit test.
	//if p.poolEntry != nil {
	//	h.backend.serverPool.registered(p.poolEntry)
	//}
	// Spawn a main loop to handle all incoming messages.
	for {
		if err := h.handleMsg(p); err != nil {
			p.Log().Debug("Light Ethereum message handling failed", "err", err)
			return err
		}
	}
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (h *clientHandler) handleMsg(p *Peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.ReadMsg()
	if err != nil {
		return err
	}
	p.Log().Trace("Light Ethereum message arrived", "code", msg.Code, "bytes", msg.Size)

	if msg.Size > protocol.ProtocolMaxMsgSize {
		return errResp(protocol.ErrMsgTooLarge, "%v > %v", msg.Size, protocol.ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	var deliverMsg *Msg

	// Handle the message depending on its contents
	switch msg.Code {
	case protocol.AnnounceMsg:
		p.Log().Trace("Received announce message")
		var req protocol.AnnounceData
		if err := msg.Decode(&req); err != nil {
			return errResp(protocol.ErrDecode, "%v: %v", msg, err)
		}
		if err := req.SanityCheck(); err != nil {
			return err
		}
		update, size := req.Update.Decode()
		if p.RejectUpdate(size) {
			return errResp(protocol.ErrRequestRejected, "")
		}
		p.UpdateFlowControl(update)

		if req.Hash != (common.Hash{}) {
			if p.AnnounceType == 0 {
				return errResp(protocol.ErrUnexpectedResponse, "")
			}
			if p.AnnounceType == 1 {
				if err := req.CheckSignature(p.ID(), update); err != nil {
					p.Log().Trace("Invalid announcement signature", "err", err)
					return err
				}
				p.Log().Trace("Valid announcement signature")
			}
			p.Log().Trace("Announce message content", "number", req.Number, "hash", req.Hash, "td", req.Td, "reorg", req.ReorgDepth)
			h.fetcher.Announce(p, &req)
		}
	case protocol.BlockHeadersMsg:
		p.Log().Trace("Received block header response message")
		var resp struct {
			ReqID, BV uint64
			Headers   []*types.Header
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		if h.fetcher.RequestedID(resp.ReqID) {
			h.fetcher.DeliverHeaders(p, resp.ReqID, resp.Headers)
		} else {
			if err := h.downloader.DeliverHeaders(p.ID().String(), resp.Headers); err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}
	case protocol.BlockBodiesMsg:
		p.Log().Trace("Received block bodies response")
		var resp struct {
			ReqID, BV uint64
			Data      []*types.Body
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgBlockBodies,
			ReqID:   resp.ReqID,
			Obj:     resp.Data,
		}
	case protocol.CodeMsg:
		p.Log().Trace("Received code response")
		var resp struct {
			ReqID, BV uint64
			Data      [][]byte
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgCode,
			ReqID:   resp.ReqID,
			Obj:     resp.Data,
		}
	case protocol.ReceiptsMsg:
		p.Log().Trace("Received receipts response")
		var resp struct {
			ReqID, BV uint64
			Receipts  []types.Receipts
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgReceipts,
			ReqID:   resp.ReqID,
			Obj:     resp.Receipts,
		}
	case protocol.ProofsV2Msg:
		p.Log().Trace("Received les/2 proofs response")
		var resp struct {
			ReqID, BV uint64
			Data      light.NodeList
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgProofsV2,
			ReqID:   resp.ReqID,
			Obj:     resp.Data,
		}
	case protocol.HelperTrieProofsMsg:
		p.Log().Trace("Received helper trie proof response")
		var resp struct {
			ReqID, BV uint64
			Data      HelperTrieResps
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgHelperTrieProofs,
			ReqID:   resp.ReqID,
			Obj:     resp.Data,
		}
	case protocol.TxStatusMsg:
		p.Log().Trace("Received tx status response")
		var resp struct {
			ReqID, BV uint64
			Status    []light.TxStatus
		}
		if err := msg.Decode(&resp); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.ReceivedReply(resp.ReqID, resp.BV)
		deliverMsg = &Msg{
			MsgType: MsgTxStatus,
			ReqID:   resp.ReqID,
			Obj:     resp.Status,
		}
	case protocol.StopMsg:
		p.Freeze()
		h.backend.retriever.frozen(p)
		p.Log().Debug("Service stopped")
	case protocol.ResumeMsg:
		var bv uint64
		if err := msg.Decode(&bv); err != nil {
			return errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
		}
		p.Unfreeze()
		p.Log().Debug("Service resumed")
	default:
		p.Log().Trace("Received invalid message", "code", msg.Code)
		return errResp(protocol.ErrInvalidMsgCode, "%v", msg.Code)
	}
	// Deliver the received response to retriever.
	if deliverMsg != nil {
		if err := h.backend.retriever.deliver(p, deliverMsg); err != nil {

		}
	}
	return nil
}

func (h *clientHandler) removePeer(id string) {
	h.backend.peers.Unregister(id)
}

type peerConnection struct {
	handler *clientHandler
	peer    *Peer
}

func (pc *peerConnection) Head() (common.Hash, *big.Int) {
	return pc.peer.HeadAndTd()
}

func (pc *peerConnection) RequestHeadersByHash(origin common.Hash, amount int, skip int, reverse bool) error {
	rq := &distributor.DistReq{
		GetCost: func(dp distributor.DistPeer) uint64 {
			peer := dp.(*Peer)
			return peer.GetRequestCost(protocol.GetBlockHeadersMsg, amount)
		},
		CanSend: func(dp distributor.DistPeer) bool {
			return dp.(*Peer) == pc.peer
		},
		Request: func(dp distributor.DistPeer) func() {
			reqID := genReqID()
			peer := dp.(*Peer)
			cost := peer.GetRequestCost(protocol.GetBlockHeadersMsg, amount)
			peer.QueuedRequest(reqID, cost)
			return func() { peer.RequestHeadersByHash(reqID, origin, amount, skip, reverse) }
		},
	}
	_, ok := <-pc.handler.backend.reqDist.Queue(rq)
	if !ok {
		return light.ErrNoPeers
	}
	return nil
}

func (pc *peerConnection) RequestHeadersByNumber(origin uint64, amount int, skip int, reverse bool) error {
	rq := &distributor.DistReq{
		GetCost: func(dp distributor.DistPeer) uint64 {
			peer := dp.(*Peer)
			return peer.GetRequestCost(protocol.GetBlockHeadersMsg, amount)
		},
		CanSend: func(dp distributor.DistPeer) bool {
			return dp.(*Peer) == pc.peer
		},
		Request: func(dp distributor.DistPeer) func() {
			reqID := genReqID()
			peer := dp.(*Peer)
			cost := peer.GetRequestCost(protocol.GetBlockHeadersMsg, amount)
			peer.QueuedRequest(reqID, cost)
			return func() { peer.RequestHeadersByNumber(reqID, origin, amount, skip, reverse) }
		},
	}
	_, ok := <-pc.handler.backend.reqDist.Queue(rq)
	if !ok {
		return light.ErrNoPeers
	}
	return nil
}

// downloaderPeerNotify implements peerSetNotify
type downloaderPeerNotify clientHandler

func (d *downloaderPeerNotify) registerPeer(p *Peer) {
	h := (*clientHandler)(d)
	pc := &peerConnection{
		handler: h,
		peer:    p,
	}
	h.downloader.RegisterLightPeer(p.ID().String(), protocol.EthVersion, pc)
}

func (d *downloaderPeerNotify) unregisterPeer(p *Peer) {
	h := (*clientHandler)(d)
	h.downloader.UnregisterPeer(p.ID().String())
}
