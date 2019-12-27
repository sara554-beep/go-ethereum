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

package lesserver

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/les/protocol"
	"github.com/ethereum/go-ethereum/les/utilities"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	errClosed            = errors.New("peer set is closed")
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")

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

	id           string    // Peer identity.
	version      int       // Protocol version negotiated.
	network      uint64    // Network ID being on.
	frozen       uint32    // Flag whether the peer is frozen.
	announceType uint64    // New block announcement type.
	headInfo     blockInfo // Latest block information.

	// Background task queue for caching peer tasks and executing in order.
	sendQueue *utilities.ExecQueue

	// Flow control agreement.
	fcParams flowcontrol.ServerParams  // The config for token bucket.
	fcCosts  protocol.RequestCostTable // The Maximum request cost table.

	closeCh chan struct{}
	lock    sync.RWMutex // Lock used to protect all thread-sensitive fields.
}

// isFrozen returns true if the client is frozen or the server has put our
// client in frozen state
func (p *peerCommons) isFrozen() bool {
	return atomic.LoadUint32(&p.frozen) != 0
}

// canQueue returns an indicator whether the peer can queue a operation.
func (p *peerCommons) canQueue() bool {
	return p.sendQueue.CanQueue() && !p.isFrozen()
}

// queueSend caches a peer operation in the background task queue.
// Please ensure to check `canQueue` before call this function
func (p *peerCommons) queueSend(f func()) bool {
	return p.sendQueue.Queue(f)
}

// mustQueueSend starts a for loop and retry the caching if failed.
// If the stopCh is closed, then it returns.
func (p *peerCommons) mustQueueSend(f func(), stopCh chan struct{}) {
	for {
		// Check whether the stopCh is closed.
		select {
		case <-stopCh:
			return
		default:
		}
		// If the function is successfully cached, return.
		if p.canQueue() && p.queueSend(f) {
			return
		}
		time.Sleep(retrySendCachePeriod)
	}
}

// String implements fmt.Stringer.
func (p *peerCommons) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id, fmt.Sprintf("les/%d", p.version))
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *peerCommons) Info() *eth.PeerInfo {
	return &eth.PeerInfo{
		Version:    p.version,
		Difficulty: p.Td(),
		Head:       fmt.Sprintf("%x", p.Head()),
	}
}

// Head retrieves a copy of the current head (most recent) hash of the peer.
func (p *peerCommons) Head() (hash common.Hash) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	copy(hash[:], p.headInfo.Hash[:])
	return hash
}

// Td retrieves the current total difficulty of a peer.
func (p *peerCommons) Td() *big.Int {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return new(big.Int).Set(p.headInfo.Td)
}

// HeadAndTd retrieves the current head hash and total difficulty of a peer.
func (p *peerCommons) HeadAndTd() (hash common.Hash, td *big.Int) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	copy(hash[:], p.headInfo.Hash[:])
	return hash, new(big.Int).Set(p.headInfo.Td)
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

// handshake executes the les protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks. Besides the basic handshake
// fields, server and client can exchange and resolve some specified fields through
// two callback functions.
func (p *peerCommons) handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, sendCallback func(*utilities.KeyValueList), recvCallback func(utilities.KeyValueMap) error) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var send utilities.KeyValueList

	// Add some basic handshake fields
	send = send.Add("protocolVersion", uint64(p.version))
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
	if int(rVersion) != p.version {
		return errResp(protocol.ErrProtocolVersionMismatch, "%d (!= %d)", rVersion, p.version)
	}
	p.headInfo = blockInfo{Hash: rHash, Number: rNum, Td: rTd}
	if recvCallback != nil {
		return recvCallback(recv)
	}
	return nil
}

// close closes the channel and notifies all background routines to exit.
func (p *peerCommons) close() {
	close(p.closeCh)
	p.sendQueue.Quit()
}

// clientPeer represents each node to which the les server is connected.
// The node here refers to the light client.
type clientPeer struct {
	peerCommons

	// responseLock ensures that responses are queued in the same order as
	// RequestProcessed is called
	responseLock  sync.Mutex
	server        bool
	invalidCount  uint32 // Counter the invalid request the client peer has made.
	responseCount uint64 // Counter to generate an unique id for request processing.
	errCh         chan error
	fcClient      *flowcontrol.ClientNode // Server side mirror token bucket.
}

func newClientPeer(version int, network uint64, p *p2p.Peer, rw p2p.MsgReadWriter) *clientPeer {
	return &clientPeer{
		peerCommons: peerCommons{
			Peer:      p,
			rw:        rw,
			id:        peerIdToString(p.ID()),
			version:   version,
			network:   network,
			sendQueue: utilities.NewExecQueue(100),
			closeCh:   make(chan struct{}),
		},
		errCh: make(chan error, 1),
	}
}

// freeClientId returns a string identifier for the peer. Multiple peers with
// the same identifier can not be connected in free mode simultaneously.
func (p *clientPeer) FreeClientId() string {
	if addr, ok := p.RemoteAddr().(*net.TCPAddr); ok {
		if addr.IP.IsLoopback() {
			// using peer id instead of loopback ip address allows multiple free
			// connections from local machine to own server
			return p.id
		} else {
			return addr.IP.String()
		}
	}
	return p.id
}

// sendStop notifies the client about being in frozen state
func (p *clientPeer) sendStop() error {
	return p2p.Send(p.rw, protocol.StopMsg, struct{}{})
}

// sendResume notifies the client about getting out of frozen state
func (p *clientPeer) sendResume(bv uint64) error {
	return p2p.Send(p.rw, protocol.ResumeMsg, bv)
}

// freeze temporarily puts the client in a frozen state which means all unprocessed
// and subsequent requests are dropped. Unfreezing happens automatically after a short
// time if the client's buffer value is at least in the slightly positive region.
// The client is also notified about being frozen/unfrozen with a Stop/Resume message.
func (p *clientPeer) freeze() {
	if p.version < protocol.Lpv3 {
		// if Stop/Resume is not supported then just drop the peer after setting
		// its frozen status permanently
		atomic.StoreUint32(&p.frozen, 1)
		p.Peer.Disconnect(p2p.DiscUselessPeer)
		return
	}
	if atomic.SwapUint32(&p.frozen, 1) == 0 {
		go func() {
			p.sendStop()
			time.Sleep(freezeTimeBase + time.Duration(rand.Int63n(int64(freezeTimeRandom))))
			for {
				bufValue, bufLimit := p.fcClient.BufferStatus()
				if bufLimit == 0 {
					return
				}
				if bufValue <= bufLimit/8 {
					time.Sleep(freezeCheckPeriod)
					continue
				}
				atomic.StoreUint32(&p.frozen, 0)
				p.sendResume(bufValue)
				return
			}
		}()
	}
}

// reply struct represents a reply with the actual data already RLP encoded and
// only the bv (buffer value) missing. This allows the serving mechanism to
// calculate the bv value which depends on the data size before sending the reply.
type reply struct {
	w              p2p.MsgWriter
	msgcode, reqID uint64
	data           rlp.RawValue
}

// send sends the reply with the calculated buffer value
func (r *reply) send(bv uint64) error {
	type resp struct {
		ReqID, BV uint64
		Data      rlp.RawValue
	}
	return p2p.Send(r.w, r.msgcode, resp{r.reqID, bv, r.data})
}

// size returns the RLP encoded size of the message data
func (r *reply) size() uint32 {
	return uint32(len(r.data))
}

// replyBlockHeaders creates a reply with a batch of block headers
func (p *clientPeer) replyBlockHeaders(reqID uint64, headers []*types.Header) *reply {
	data, _ := rlp.EncodeToBytes(headers)
	return &reply{p.rw, protocol.BlockHeadersMsg, reqID, data}
}

// replyBlockBodiesRLP creates a reply with a batch of block contents from
// an already RLP encoded format.
func (p *clientPeer) replyBlockBodiesRLP(reqID uint64, bodies []rlp.RawValue) *reply {
	data, _ := rlp.EncodeToBytes(bodies)
	return &reply{p.rw, protocol.BlockBodiesMsg, reqID, data}
}

// replyCode creates a reply with a batch of arbitrary internal data, corresponding to the
// hashes requested.
func (p *clientPeer) replyCode(reqID uint64, codes [][]byte) *reply {
	data, _ := rlp.EncodeToBytes(codes)
	return &reply{p.rw, protocol.CodeMsg, reqID, data}
}

// replyReceiptsRLP creates a reply with a batch of transaction receipts, corresponding to the
// ones requested from an already RLP encoded format.
func (p *clientPeer) replyReceiptsRLP(reqID uint64, receipts []rlp.RawValue) *reply {
	data, _ := rlp.EncodeToBytes(receipts)
	return &reply{p.rw, protocol.ReceiptsMsg, reqID, data}
}

// replyProofsV2 creates a reply with a batch of merkle proofs, corresponding to the ones requested.
func (p *clientPeer) replyProofsV2(reqID uint64, proofs light.NodeList) *reply {
	data, _ := rlp.EncodeToBytes(proofs)
	return &reply{p.rw, protocol.ProofsV2Msg, reqID, data}
}

// replyHelperTrieProofs creates a reply with a batch of HelperTrie proofs, corresponding to the ones requested.
func (p *clientPeer) replyHelperTrieProofs(reqID uint64, resp HelperTrieResps) *reply {
	data, _ := rlp.EncodeToBytes(resp)
	return &reply{p.rw, protocol.HelperTrieProofsMsg, reqID, data}
}

// replyTxStatus creates a reply with a batch of transaction status records, corresponding to the ones requested.
func (p *clientPeer) replyTxStatus(reqID uint64, stats []light.TxStatus) *reply {
	data, _ := rlp.EncodeToBytes(stats)
	return &reply{p.rw, protocol.TxStatusMsg, reqID, data}
}

// sendAnnounce announces the availability of a number of blocks through
// a hash notification.
func (p *clientPeer) sendAnnounce(request protocol.AnnounceData) error {
	return p2p.Send(p.rw, protocol.AnnounceMsg, request)
}

// updateCapacity updates the request serving capacity assigned to a given client
// and also sends an announcement about the updated flow control parameters
func (p *clientPeer) UpdateCapacity(cap uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.fcParams = flowcontrol.ServerParams{MinRecharge: cap, BufLimit: cap * bufLimitRatio}
	p.fcClient.UpdateParams(p.fcParams)
	var kvList utilities.KeyValueList
	kvList = kvList.Add("flowControl/MRR", cap)
	kvList = kvList.Add("flowControl/BL", cap*bufLimitRatio)

	// todo(rjl493456442) please ensure the capacity upgrade function can be queued
	p.mustQueueSend(func() { p.sendAnnounce(protocol.AnnounceData{Update: kvList}) }, p.closeCh)
}

// freezeClient temporarily puts the client in a frozen state which means all
// unprocessed and subsequent requests are dropped. Unfreezing happens automatically
// after a short time if the client's buffer value is at least in the slightly positive
// region. The client is also notified about being frozen/unfrozen with a Stop/Resume
// message.
func (p *clientPeer) FreezeClient() {
	if p.version < protocol.Lpv3 {
		// if Stop/Resume is not supported then just drop the peer after setting
		// its frozen status permanently
		atomic.StoreUint32(&p.frozen, 1)
		p.Peer.Disconnect(p2p.DiscUselessPeer)
		return
	}
	if atomic.SwapUint32(&p.frozen, 1) == 0 {
		go func() {
			p.sendStop()
			time.Sleep(freezeTimeBase + time.Duration(rand.Int63n(int64(freezeTimeRandom))))
			for {
				bufValue, bufLimit := p.fcClient.BufferStatus()
				if bufLimit == 0 {
					return
				}
				if bufValue <= bufLimit/8 {
					time.Sleep(freezeCheckPeriod)
				} else {
					atomic.StoreUint32(&p.frozen, 0)
					p.sendResume(bufValue)
					break
				}
			}
		}()
	}
}

// Handshake executes the les protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
func (p *clientPeer) Handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, server *LesServer) error {
	return p.handshake(td, head, headNum, genesis, func(lists *utilities.KeyValueList) {
		// Add some information which services server can offer.
		if !server.config.UltraLightOnlyAnnounce {
			*lists = (*lists).Add("serveHeaders", nil)
			*lists = (*lists).Add("serveChainSince", uint64(0))
			*lists = (*lists).Add("serveStateSince", uint64(0))

			// If local ethereum node is running in archive mode, advertise ourselves we have
			// all version state data. Otherwise only recent state is available.
			stateRecent := uint64(core.TriesInMemory - 4)
			if server.archiveMode {
				stateRecent = 0
			}
			*lists = (*lists).Add("serveRecentState", stateRecent)
			*lists = (*lists).Add("txRelay", nil)
		}
		*lists = (*lists).Add("flowControl/BL", server.defParams.BufLimit)
		*lists = (*lists).Add("flowControl/MRR", server.defParams.MinRecharge)

		var costList protocol.RequestCostList
		if server.costTracker.testCostList != nil {
			costList = server.costTracker.testCostList
		} else {
			costList = server.costTracker.makeCostList(server.costTracker.globalFactor())
		}
		*lists = (*lists).Add("flowControl/MRC", costList)
		p.fcCosts = costList.Decode(protocol.ProtocolLengths[uint(p.version)])
		p.fcParams = server.defParams

		// Add advertised checkpoint and register block height which
		// client can verify the checkpoint validity.
		if server.oracle != nil && server.oracle.Running() {
			cp, height := server.oracle.StableCheckpoint()
			if cp != nil {
				*lists = (*lists).Add("checkpoint/value", cp)
				*lists = (*lists).Add("checkpoint/registerHeight", height)
			}
		}
	}, func(recv utilities.KeyValueMap) error {
		p.server = recv.Get("flowControl/MRR", nil) == nil
		if p.server {
			p.announceType = announceTypeNone // connected to another server, send no messages
		} else {
			if recv.Get("announceType", &p.announceType) != nil {
				// set default announceType on server side
				p.announceType = announceTypeSimple
			}
			p.fcClient = flowcontrol.NewClientNode(server.fcManager, server.defParams)
		}
		return nil
	})
}

// clientPeerSubscriber is a callback interface to notify services about added or
// removed client peers
type clientPeerSubscriber interface {
	registerPeer(*clientPeer)
	unregisterPeer(*clientPeer)
}

// peerSet represents the collection of active peers currently participating in
// the Light Ethereum sub-protocol.
type peerSet struct {
	clientPeers map[string]*clientPeer

	// cSubs is a batch of subscribers and peerset will notify these
	// subscribers when the peerset changes(new client peer is added
	// or removed)
	cSubs []clientPeerSubscriber

	closed bool
	lock   sync.RWMutex
}

// newPeerSet creates a new peer set to track the active participants.
func newPeerSet() *peerSet {
	set := &peerSet{}
	set.clientPeers = make(map[string]*clientPeer)
	return set
}

// subscribe adds a service to be notified about added or removed
// peers and also register all active peers into the given service.
func (ps *peerSet) subscribe(s interface{}) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	sub := s.(clientPeerSubscriber)
	ps.cSubs = append(ps.cSubs, sub)
	for _, p := range ps.clientPeers {
		sub.registerPeer(p)
	}
}

// unSubscribe removes the specified service from the subscriber pool.
func (ps *peerSet) unSubscribe(s interface{}) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	sub := s.(clientPeerSubscriber)
	for i, cs := range ps.cSubs {
		if cs == sub {
			ps.cSubs = append(ps.cSubs[:i], ps.cSubs[i+1:]...)
		}
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known.
func (ps *peerSet) register(p interface{}) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if ps.closed {
		return errClosed
	}
	peer := p.(*clientPeer)
	if _, exist := ps.clientPeers[peer.id]; exist {
		return errAlreadyRegistered
	}
	ps.clientPeers[peer.id] = peer
	for _, sub := range ps.cSubs {
		sub.registerPeer(peer)
	}
	return nil
}

// Unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity. It also initiates disconnection at the networking layer.
func (ps *peerSet) unregister(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	p, ok := ps.clientPeers[id]
	if !ok {
		return errNotRegistered
	}
	delete(ps.clientPeers, id)
	for _, sub := range ps.cSubs {
		sub.unregisterPeer(p)
	}
	p.Peer.Disconnect(p2p.DiscUselessPeer)
	return nil
}

// AllPeerIDs returns a list of all registered peer IDs
func (ps *peerSet) allPeerIds() []string {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var ids []string
	for id := range ps.clientPeers {
		ids = append(ids, id)
	}
	return ids
}

// Peer retrieves the registered peer with the given id.
func (ps *peerSet) clientPeer(id string) *clientPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.clientPeers[id]
}

// Len returns if the current number of peers in the set.
func (ps *peerSet) len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.clientPeers)
}

// allClientPeers returns all client peers in a list.
func (ps *peerSet) allClientPeers() []*clientPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*clientPeer, 0, len(ps.clientPeers))
	for _, p := range ps.clientPeers {
		list = append(list, p)
	}
	return list
}

// Close disconnects all peers.
// No new peers can be registered after Close has returned.
func (ps *peerSet) close() {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for _, p := range ps.clientPeers {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.closed = true
}
