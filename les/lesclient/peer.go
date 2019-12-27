// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either Version 3 of the License, or
// (at your option) any later Version.
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
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/les/protocol"
	"github.com/ethereum/go-ethereum/les/utilities"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

func errResp(code protocol.ErrCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

var (
	ErrClosed            = errors.New("peer set is closed")
	ErrAlreadyRegistered = errors.New("peer is already registered")
	ErrNotRegistered     = errors.New("peer is not registered")

	s = rand.NewSource(time.Now().UnixNano())
	r = rand.New(s)
)

const (
	maxRequestErrors  = 20 // number of invalid requests tolerated (makes the protocol less brittle but still avoids spam)
	maxResponseErrors = 50 // number of invalid responses tolerated (makes the protocol less brittle but still avoids spam)

	allowedUpdateBytes = 100000                // initial/maximum allowed update size
	allowedUpdateRate  = time.Millisecond * 10 // time constant for recharging one byte of allowance

	freezeTimeBase    = time.Millisecond * 700 // fixed component of client freeze time
	freezeTimeRandom  = time.Millisecond * 600 // random component of client freeze time
	freezeCheckPeriod = time.Millisecond * 100 // buffer value recheck period after initial freeze time has elapsed

	// If the total encoded size of a sent transaction batch is over txSizeCostLimit
	// per transaction then the request cost is calculated as proportional to the
	// encoded size instead of the transaction count
	txSizeCostLimit = 0x4000

	// handshakeTimeout is the timeout LES handshake will be treated as failed.
	handshakeTimeout = 5 * time.Second

	// retrySendCachePeriod is the time interval a caching retry is performed.
	retrySendCachePeriod = time.Millisecond * 100
)

const (
	announceTypeNone = iota
	announceTypeSimple
	announceTypeSigned
)

// peerIdToString converts enode.ID to a string form
func peerIdToString(id enode.ID) string {
	return fmt.Sprintf("%x", id.Bytes())
}

// peerCommons contains fields needed by both server peer and client peer.
type peerCommons struct {
	*p2p.Peer
	rw p2p.MsgReadWriter

	id           string             // Peer identity.
	Version      int                // Protocol Version negotiated.
	network      uint64             // Network ID being on.
	frozen       uint32             // Flag whether the peer is frozen.
	AnnounceType uint64             // New block announcement type.
	HeadInfo     protocol.BlockInfo // Latest block information.

	// Background task queue for caching peer tasks and executing in order.
	sendQueue *utilities.ExecQueue

	// Flow control agreement.
	fcParams flowcontrol.ServerParams  // The config for token bucket.
	fcCosts  protocol.RequestCostTable // The Maximum request cost table.

	closeCh chan struct{}
	Lock    sync.RWMutex // Lock used to protect all thread-sensitive fields.
}

func (p *peerCommons) ReadMsg() (p2p.Msg, error) {
	return p.rw.ReadMsg()
}

// isFrozen returns true if the client is frozen or the server has put our
// client in frozen state
func (p *peerCommons) IsFrozen() bool {
	return atomic.LoadUint32(&p.frozen) != 0
}

func (p *peerCommons) IsOnlyAnnounced() bool {
	return true
}

// CanQueue returns an indicator whether the peer can queue a operation.
func (p *peerCommons) CanQueue() bool {
	return p.sendQueue.CanQueue() && !p.IsFrozen()
}

// QueueSend caches a peer operation in the background task queue.
// Please ensure to check `CanQueue` before call this function
func (p *peerCommons) QueueSend(f func()) bool {
	return p.sendQueue.Queue(f)
}

// mustQueueSend starts a for loop and retry the caching if failed.
// If the stopCh is closed, then it returns.
func (p *peerCommons) MustQueueSend(f func(), stopCh chan struct{}) {
	for {
		// Check whether the stopCh is closed.
		select {
		case <-stopCh:
			return
		default:
		}
		// If the function is successfully cached, return.
		if p.CanQueue() && p.QueueSend(f) {
			return
		}
		time.Sleep(retrySendCachePeriod)
	}
}

// String implements fmt.Stringer.
func (p *peerCommons) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id, fmt.Sprintf("les/%d", p.Version))
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *peerCommons) Info() *eth.PeerInfo {
	return &eth.PeerInfo{
		Version:    p.Version,
		Difficulty: p.Td(),
		Head:       fmt.Sprintf("%x", p.Head()),
	}
}

// Head retrieves a copy of the current head (most recent) hash of the peer.
func (p *peerCommons) Head() (hash common.Hash) {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	copy(hash[:], p.HeadInfo.Hash[:])
	return hash
}

// Td retrieves the current total difficulty of a peer.
func (p *peerCommons) Td() *big.Int {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	return new(big.Int).Set(p.HeadInfo.Td)
}

// HeadAndTd retrieves the current head hash and total difficulty of a peer.
func (p *peerCommons) HeadAndTd() (hash common.Hash, td *big.Int) {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	copy(hash[:], p.HeadInfo.Hash[:])
	return hash, new(big.Int).Set(p.HeadInfo.Td)
}

// sendReceiveHandshake exchanges handshake packet with remote peer and returns any error
// if failed to send or receive packet.
func (p *peerCommons) sendReceiveHandshake(sendList utilities.KeyValueList) (utilities.KeyValueList, error) {
	var (
		errc     = make(chan error, 2)
		recvList utilities.KeyValueList
	)
	// Send out own handshake in a new thread
	go func() {
		errc <- p2p.Send(p.rw, protocol.StatusMsg, sendList)
	}()
	go func() {
		// In the mean time retrieve the remote status message
		msg, err := p.rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		if msg.Code != protocol.StatusMsg {
			errc <- errResp(protocol.ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, protocol.StatusMsg)
			return
		}
		if msg.Size > protocol.ProtocolMaxMsgSize {
			errc <- errResp(protocol.ErrMsgTooLarge, "%v > %v", msg.Size, protocol.ProtocolMaxMsgSize)
			return
		}
		// Decode the handshake
		if err := msg.Decode(&recvList); err != nil {
			errc <- errResp(protocol.ErrDecode, "msg %v: %v", msg, err)
			return
		}
		errc <- nil
	}()
	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				return nil, err
			}
		case <-timeout.C:
			return nil, p2p.DiscReadTimeout
		}
	}
	return recvList, nil
}

// handshake executes the les protocol handshake, negotiating Version number,
// network IDs, difficulties, head and genesis blocks. Besides the basic handshake
// fields, server and client can exchange and resolve some specified fields through
// two callback functions.
func (p *peerCommons) myhandshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, sendCallback func(*utilities.KeyValueList), recvCallback func(utilities.KeyValueMap) error) error {
	p.Lock.Lock()
	defer p.Lock.Unlock()

	var send utilities.KeyValueList

	// Add some basic handshake fields
	send = send.Add("protocolVersion", uint64(p.Version))
	send = send.Add("networkId", p.network)
	send = send.Add("headTd", td)
	send = send.Add("headHash", head)
	send = send.Add("headNum", headNum)
	send = send.Add("genesisHash", genesis)

	// Add client-specified or server-specified fields
	if sendCallback != nil {
		sendCallback(&send)
	}
	// Exchange the handshake packet and resolve the received one.
	recvList, err := p.sendReceiveHandshake(send)
	if err != nil {
		return err
	}
	recv, size := recvList.Decode()
	if size > allowedUpdateBytes {
		return errResp(protocol.ErrRequestRejected, "")
	}
	var rGenesis, rHash common.Hash
	var rVersion, rNetwork, rNum uint64
	var rTd *big.Int
	if err := recv.Get("protocolVersion", &rVersion); err != nil {
		return err
	}
	if err := recv.Get("networkId", &rNetwork); err != nil {
		return err
	}
	if err := recv.Get("headTd", &rTd); err != nil {
		return err
	}
	if err := recv.Get("headHash", &rHash); err != nil {
		return err
	}
	if err := recv.Get("headNum", &rNum); err != nil {
		return err
	}
	if err := recv.Get("genesisHash", &rGenesis); err != nil {
		return err
	}
	if rGenesis != genesis {
		return errResp(protocol.ErrGenesisBlockMismatch, "%x (!= %x)", rGenesis[:8], genesis[:8])
	}
	if rNetwork != p.network {
		return errResp(protocol.ErrNetworkIdMismatch, "%d (!= %d)", rNetwork, p.network)
	}
	if int(rVersion) != p.Version {
		return errResp(protocol.ErrProtocolVersionMismatch, "%d (!= %d)", rVersion, p.Version)
	}
	p.HeadInfo = protocol.BlockInfo{Hash: rHash, Number: rNum, Td: rTd}
	if recvCallback != nil {
		return recvCallback(recv)
	}
	return nil
}

// close closes the channel and notifies all background routines to exit.
func (p *peerCommons) Close() {
	close(p.closeCh)
	p.sendQueue.Quit()
}

// Peer represents each node to which the client is connected.
// The node here refers to the les server.
type Peer struct {
	peerCommons

	// Status fields
	trusted                 bool   // The flag whether the server is selected as trusted server.
	onlyAnnounce            bool   // The flag whether the server sends announcement only.
	chainSince, chainRecent uint64 // The range of chain server peer can serve.
	stateSince, stateRecent uint64 // The range of state server peer can serve.

	// Advertised checkpoint fields
	CheckpointNumber uint64                   // The block height which the checkpoint is registered.
	Checkpoint       params.TrustedCheckpoint // The advertised checkpoint sent by server.

	// IN THE SERVERPOOL PART!!!!!!
	// poolEntry *poolEntry              // Statistic for server peer.
	fcServer *flowcontrol.ServerNode // Client side mirror token bucket.

	// Statistics
	errCount    int // Counter the invalid responses server has replied
	updateCount uint64
	updateTime  mclock.AbsTime

	// Callbacks
	hasBlock func(common.Hash, uint64, bool) bool // Used to determine whether the server has the specified block.
}

func NewServerPeer(version int, network uint64, trusted bool, p *p2p.Peer, rw p2p.MsgReadWriter) *Peer {
	return &Peer{
		peerCommons: peerCommons{
			Peer:      p,
			rw:        rw,
			id:        peerIdToString(p.ID()),
			Version:   version,
			network:   network,
			sendQueue: utilities.NewExecQueue(100),
			closeCh:   make(chan struct{}),
		},
		trusted: trusted,
	}
}

// RejectUpdate returns true if a parameter update has to be rejected because
// the size and/or rate of updates exceed the capacity limitation
func (p *Peer) RejectUpdate(size uint64) bool {
	now := mclock.Now()
	if p.updateCount == 0 {
		p.updateTime = now
	} else {
		dt := now - p.updateTime
		p.updateTime = now

		r := uint64(dt / mclock.AbsTime(allowedUpdateRate))
		if p.updateCount > r {
			p.updateCount -= r
		} else {
			p.updateCount = 0
		}
	}
	p.updateCount += size
	return p.updateCount > allowedUpdateBytes
}

// freeze processes Stop messages from the given server and set the status as
// frozen.
func (p *Peer) Freeze() {
	if atomic.CompareAndSwapUint32(&p.frozen, 0, 1) {
		p.sendQueue.Clear()
	}
}

// unfreeze processes Resume messages from the given server and set the status
// as unfrozen.
func (p *Peer) Unfreeze() {
	atomic.StoreUint32(&p.frozen, 0)
}

// sendRequest send a request to the server based on the given message type
// and content.
func sendRequest(w p2p.MsgWriter, msgcode, reqID uint64, data interface{}) error {
	type req struct {
		ReqID uint64
		Data  interface{}
	}
	return p2p.Send(w, msgcode, req{reqID, data})
}

// RequestHeadersByHash fetches a batch of blocks' headers corresponding to the
// specified header query, based on the hash of an origin block.
func (p *Peer) RequestHeadersByHash(reqID uint64, origin common.Hash, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromhash", origin, "skip", skip, "reverse", reverse)
	return sendRequest(p.rw, protocol.GetBlockHeadersMsg, reqID, &protocol.GetBlockHeadersData{Origin: protocol.HashOrNumber{Hash: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// RequestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin block.
func (p *Peer) RequestHeadersByNumber(reqID, origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return sendRequest(p.rw, protocol.GetBlockHeadersMsg, reqID, &protocol.GetBlockHeadersData{Origin: protocol.HashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// RequestBodies fetches a batch of blocks' bodies corresponding to the hashes
// specified.
func (p *Peer) RequestBodies(reqID uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of block bodies", "count", len(hashes))
	return sendRequest(p.rw, protocol.GetBlockBodiesMsg, reqID, hashes)
}

// RequestCode fetches a batch of arbitrary data from a node's known state
// data, corresponding to the specified hashes.
func (p *Peer) RequestCode(reqID uint64, reqs []protocol.CodeReq) error {
	p.Log().Debug("Fetching batch of codes", "count", len(reqs))
	return sendRequest(p.rw, protocol.GetCodeMsg, reqID, reqs)
}

// RequestReceipts fetches a batch of transaction receipts from a remote node.
func (p *Peer) RequestReceipts(reqID uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of receipts", "count", len(hashes))
	return sendRequest(p.rw, protocol.GetReceiptsMsg, reqID, hashes)
}

// RequestProofs fetches a batch of merkle proofs from a remote node.
func (p *Peer) RequestProofs(reqID uint64, reqs []protocol.ProofReq) error {
	p.Log().Debug("Fetching batch of proofs", "count", len(reqs))
	return sendRequest(p.rw, protocol.GetProofsV2Msg, reqID, reqs)
}

// RequestHelperTrieProofs fetches a batch of HelperTrie merkle proofs from a remote node.
func (p *Peer) RequestHelperTrieProofs(reqID uint64, reqs []protocol.HelperTrieReq) error {
	p.Log().Debug("Fetching batch of HelperTrie proofs", "count", len(reqs))
	return sendRequest(p.rw, protocol.GetHelperTrieProofsMsg, reqID, reqs)
}

// RequestTxStatus fetches a batch of transaction status records from a remote node.
func (p *Peer) RequestTxStatus(reqID uint64, txHashes []common.Hash) error {
	p.Log().Debug("Requesting transaction status", "count", len(txHashes))
	return sendRequest(p.rw, protocol.GetTxStatusMsg, reqID, txHashes)
}

// SendTxStatus creates a reply with a batch of transactions to be added to the remote transaction pool.
func (p *Peer) SendTxs(reqID uint64, txs rlp.RawValue) error {
	p.Log().Debug("Sending batch of transactions", "size", len(txs))
	return sendRequest(p.rw, protocol.SendTxV2Msg, reqID, txs)
}

// WaitBefore implements distPeer interface
func (p *Peer) WaitBefore(maxCost uint64) (time.Duration, float64) {
	return p.fcServer.CanSend(maxCost)
}

// getRequestCost returns an estimated request cost according to the flow control
// rules negotiated between the server and the client.
func (p *Peer) GetRequestCost(msgcode uint64, amount int) uint64 {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	costs := p.fcCosts[msgcode]
	if costs == nil {
		return 0
	}
	cost := costs.BaseCost + costs.ReqCost*uint64(amount)
	if cost > p.fcParams.BufLimit {
		cost = p.fcParams.BufLimit
	}
	return cost
}

// getTxRelayCost returns an estimated relay cost according to the flow control
// rules negotiated between the server and the client.
func (p *Peer) GetTxRelayCost(amount, size int) uint64 {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	costs := p.fcCosts[protocol.SendTxV2Msg]
	if costs == nil {
		return 0
	}
	cost := costs.BaseCost + costs.ReqCost*uint64(amount)
	sizeCost := costs.BaseCost + costs.ReqCost*uint64(size)/txSizeCostLimit
	if sizeCost > cost {
		cost = sizeCost
	}
	if cost > p.fcParams.BufLimit {
		cost = p.fcParams.BufLimit
	}
	return cost
}

func (p *Peer) QueuedRequest(reqID, maxCost uint64) {
	p.fcServer.QueuedRequest(reqID, maxCost)
}

func (p *Peer) ReceivedReply(reqID, bv uint64) {
	p.fcServer.ReceivedReply(reqID, bv)
}

// HasBlock checks if the peer has a given block
func (p *Peer) HasBlock(hash common.Hash, number uint64, hasState bool) bool {
	p.Lock.RLock()
	defer p.Lock.RUnlock()

	head := p.HeadInfo.Number
	var since, recent uint64
	if hasState {
		since = p.stateSince
		recent = p.stateRecent
	} else {
		since = p.chainSince
		recent = p.chainRecent
	}
	hasBlock := p.hasBlock

	return head >= number && number >= since && (recent == 0 || number+recent+4 > head) && hasBlock != nil && hasBlock(hash, number, hasState)
}

// updateFlowControl updates the flow control parameters belonging to the server
// node if the announced key/value set contains relevant fields
func (p *Peer) UpdateFlowControl(update utilities.KeyValueMap) {
	p.Lock.Lock()
	defer p.Lock.Unlock()

	// If any of the flow control params is nil, refuse to update.
	var params flowcontrol.ServerParams
	if update.Get("flowControl/BL", &params.BufLimit) == nil && update.Get("flowControl/MRR", &params.MinRecharge) == nil {
		// todo can light client set a minimal acceptable flow control params?
		p.fcParams = params
		p.fcServer.UpdateParams(params)
	}
	var MRC protocol.RequestCostList
	if update.Get("flowControl/MRC", &MRC) == nil {
		costUpdate := MRC.Decode(protocol.ProtocolLengths[uint(p.Version)])
		for code, cost := range costUpdate {
			p.fcCosts[code] = cost
		}
	}
}

// Handshake executes the les protocol handshake, negotiating Version number,
// network IDs, difficulties, head and genesis blocks.
func (p *Peer) Handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash) error {
	return p.myhandshake(td, head, headNum, genesis, func(lists *utilities.KeyValueList) {
		// Add some client-specific handshake fields
		//
		// Enable signed announcement randomly even the server is not trusted.
		p.AnnounceType = announceTypeSimple
		if p.trusted || r.Intn(10) > 3 {
			p.AnnounceType = announceTypeSigned
		}
		*lists = (*lists).Add("AnnounceType", p.AnnounceType)
	}, func(recv utilities.KeyValueMap) error {
		if recv.Get("serveChainSince", &p.chainSince) != nil {
			p.onlyAnnounce = true
		}
		if recv.Get("serveRecentChain", &p.chainRecent) != nil {
			p.chainRecent = 0
		}
		if recv.Get("serveStateSince", &p.stateSince) != nil {
			p.onlyAnnounce = true
		}
		if recv.Get("serveRecentState", &p.stateRecent) != nil {
			p.stateRecent = 0
		}
		if recv.Get("txRelay", nil) != nil {
			p.onlyAnnounce = true
		}
		if p.onlyAnnounce && !p.trusted {
			return errResp(protocol.ErrUselessPeer, "peer cannot serve requests")
		}
		// Parse flow control handshake packet.
		var sParams flowcontrol.ServerParams
		if err := recv.Get("flowControl/BL", &sParams.BufLimit); err != nil {
			return err
		}
		if err := recv.Get("flowControl/MRR", &sParams.MinRecharge); err != nil {
			return err
		}
		var MRC protocol.RequestCostList
		if err := recv.Get("flowControl/MRC", &MRC); err != nil {
			return err
		}
		p.fcParams = sParams
		p.fcServer = flowcontrol.NewServerNode(sParams, &mclock.System{})
		p.fcCosts = MRC.Decode(protocol.ProtocolLengths[uint(p.Version)])

		recv.Get("checkpoint/value", &p.Checkpoint)
		recv.Get("checkpoint/registerHeight", &p.CheckpointNumber)

		//if !p.onlyAnnounce {
		//	for msgCode := range reqAvgTimeCost {
		//		if p.fcCosts[msgCode] == nil {
		//			return errResp(ErrUselessPeer, "peer does not support message %d", msgCode)
		//		}
		//	}
		//}
		return nil
	})
}

// serverPeerSubscriber is a callback interface to notify services about added or
// removed server peers
type serverPeerSubscriber interface {
	RegisterPeer(*Peer)
	UnregisterPeer(*Peer)
}

// PeerSet represents the collection of active peers currently participating in
// the Light Ethereum sub-protocol.
type PeerSet struct {
	serverPeers map[string]*Peer
	peerFeed    event.Feed

	// sSubs is a batch of subscribers and peerset will notify these
	// subscribers when the peerset changes(new server peer is added
	// or removed)
	sSubs []serverPeerSubscriber

	closed bool
	lock   sync.RWMutex
}

// newPeerSet creates a new peer set to track the active participants.
func NewPeerSet() *PeerSet {
	set := &PeerSet{}
	set.serverPeers = make(map[string]*Peer)
	return set
}

// subscribe adds a service to be notified about added or removed
// peers and also register all active peers into the given service.
func (ps *PeerSet) Subscribe(s interface{}) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	sub := s.(serverPeerSubscriber)
	ps.sSubs = append(ps.sSubs, sub)
	for _, p := range ps.serverPeers {
		sub.RegisterPeer(p)
	}
}

// unSubscribe removes the specified service from the subscriber pool.
func (ps *PeerSet) UnSubscribe(s interface{}) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	sub := s.(serverPeerSubscriber)
	for i, ss := range ps.sSubs {
		if ss == sub {
			ps.sSubs = append(ps.sSubs[:i], ps.sSubs[i+1:]...)
		}
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known.
func (ps *PeerSet) Register(p interface{}) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if ps.closed {
		return ErrClosed
	}
	peer := p.(*Peer)
	if _, exist := ps.serverPeers[peer.id]; exist {
		return ErrAlreadyRegistered
	}
	ps.serverPeers[peer.id] = peer
	for _, sub := range ps.sSubs {
		sub.RegisterPeer(peer)
	}
	return nil
}

// Unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity. It also initiates disconnection at the networking layer.
func (ps *PeerSet) Unregister(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	p, ok := ps.serverPeers[id]
	if !ok {
		return ErrNotRegistered
	}
	delete(ps.serverPeers, id)
	for _, sub := range ps.sSubs {
		sub.UnregisterPeer(p)
	}
	p.Peer.Disconnect(p2p.DiscUselessPeer)
	return nil
}

// AllPeerIDs returns a list of all registered peer IDs
func (ps *PeerSet) AllPeerIds() []string {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var ids []string
	for id := range ps.serverPeers {
		ids = append(ids, id)
	}
	return ids
}

// Peer retrieves the registered peer with the given id.
func (ps *PeerSet) ServerPeer(id string) *Peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.serverPeers[id]
}

// Len returns if the current number of peers in the set.
func (ps *PeerSet) Len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.serverPeers)
}

// BestPeer retrieves the known peer with the currently highest total difficulty.
// If the peerset is "client peer set", then nothing meaningful will return. The
// reason is client peer never send back their latest status to server.
func (ps *PeerSet) BestPeer() *Peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var (
		bestPeer *Peer
		bestTd   *big.Int
	)
	for _, p := range ps.serverPeers {
		if td := p.Td(); bestTd == nil || td.Cmp(bestTd) > 0 {
			bestPeer, bestTd = p, td
		}
	}
	return bestPeer
}

// allServerPeers returns all server peers in a list.
func (ps *PeerSet) AllServerPeers() []*Peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*Peer, 0, len(ps.serverPeers))
	for _, p := range ps.serverPeers {
		list = append(list, p)
	}
	return list
}

// Close disconnects all peers.
// No new peers can be registered after Close has returned.
func (ps *PeerSet) Close() {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for _, p := range ps.serverPeers {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.closed = true
}
