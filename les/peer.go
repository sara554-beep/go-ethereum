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
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/les/flowcontrol"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	errClosed            = errors.New("peer set is closed")
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")
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

type keyValueEntry struct {
	Key   string
	Value rlp.RawValue
}

type keyValueList []keyValueEntry
type keyValueMap map[string]rlp.RawValue

func (l keyValueList) add(key string, val interface{}) keyValueList {
	var entry keyValueEntry
	entry.Key = key
	if val == nil {
		val = uint64(0)
	}
	enc, err := rlp.EncodeToBytes(val)
	if err == nil {
		entry.Value = enc
	}
	return append(l, entry)
}

func (l keyValueList) decode() (keyValueMap, uint64) {
	m := make(keyValueMap)
	var size uint64
	for _, entry := range l {
		m[entry.Key] = entry.Value
		size += uint64(len(entry.Key)) + uint64(len(entry.Value)) + 8
	}
	return m, size
}

func (m keyValueMap) get(key string, val interface{}) error {
	enc, ok := m[key]
	if !ok {
		return errResp(ErrMissingKey, "%s", key)
	}
	if val == nil {
		return nil
	}
	return rlp.DecodeBytes(enc, val)
}

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
	active       bool

	// Background task queue for caching peer tasks and executing in order.
	sendQueue *execQueue

	// Flow control agreement.
	fcParams flowcontrol.ServerParams // The config for token bucket.
	fcCosts  requestCostTable         // The Maximum request cost table.

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
	return p.sendQueue.canQueue() && !p.isFrozen()
}

// queueSend caches a peer operation in the background task queue.
// Please ensure to check `canQueue` before call this function
func (p *peerCommons) queueSend(f func()) bool {
	return p.sendQueue.queue(f)
}

// mustQueueSend starts a for loop and retry the caching if failed.
// If the stopCh is closed, then it returns.
func (p *peerCommons) mustQueueSend(f func()) {
	for {
		// Check whether the stopCh is closed.
		select {
		case <-p.closeCh:
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

	return p.headInfo.Hash
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

	return p.headInfo.Hash, new(big.Int).Set(p.headInfo.Td)
}

// sendReceiveHandshake exchanges handshake packet with remote peer and returns any error
// if failed to send or receive packet.
func (p *peerCommons) sendReceiveHandshake(sendList keyValueList) (keyValueList, error) {
	var (
		errc     = make(chan error, 2)
		recvList keyValueList
	)
	// Send out own handshake in a new thread
	go func() {
		errc <- p2p.Send(p.rw, StatusMsg, sendList)
	}()
	go func() {
		// In the mean time retrieve the remote status message
		msg, err := p.rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		if msg.Code != StatusMsg {
			errc <- errResp(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, StatusMsg)
			return
		}
		if msg.Size > ProtocolMaxMsgSize {
			errc <- errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
			return
		}
		// Decode the handshake
		if err := msg.Decode(&recvList); err != nil {
			errc <- errResp(ErrDecode, "msg %v: %v", msg, err)
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
func (p *peerCommons) handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, sendCallback func(*keyValueList), recvCallback func(keyValueMap) error) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var send keyValueList

	// Add some basic handshake fields
	send = send.add("protocolVersion", uint64(p.version))
	send = send.add("networkId", p.network)
	send = send.add("headTd", td)
	send = send.add("headHash", head)
	send = send.add("headNum", headNum)
	send = send.add("genesisHash", genesis)

	// Add client-specified or server-specified fields
	if sendCallback != nil {
		sendCallback(&send)
	}
	// Exchange the handshake packet and resolve the received one.
	recvList, err := p.sendReceiveHandshake(send)
	if err != nil {
		return err
	}
	recv, size := recvList.decode()
	if size > allowedUpdateBytes {
		return errResp(ErrRequestRejected, "")
	}
	var rGenesis, rHash common.Hash
	var rVersion, rNetwork, rNum uint64
	var rTd *big.Int
	if err := recv.get("protocolVersion", &rVersion); err != nil {
		return err
	}
	if err := recv.get("networkId", &rNetwork); err != nil {
		return err
	}
	if err := recv.get("headTd", &rTd); err != nil {
		return err
	}
	if err := recv.get("headHash", &rHash); err != nil {
		return err
	}
	if err := recv.get("headNum", &rNum); err != nil {
		return err
	}
	if err := recv.get("genesisHash", &rGenesis); err != nil {
		return err
	}
	if rGenesis != genesis {
		return errResp(ErrGenesisBlockMismatch, "%x (!= %x)", rGenesis[:8], genesis[:8])
	}
	if rNetwork != p.network {
		return errResp(ErrNetworkIdMismatch, "%d (!= %d)", rNetwork, p.network)
	}
	if int(rVersion) != p.version {
		return errResp(ErrProtocolVersionMismatch, "%d (!= %d)", rVersion, p.version)
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
	p.sendQueue.quit()
}

// serverPeer represents each node to which the client is connected.
// The node here refers to the les server.
type serverPeer struct {
	peerCommons

	// Status fields
	trusted                 bool   // The flag whether the server is selected as trusted server.
	onlyAnnounce            bool   // The flag whether the server sends announcement only.
	chainSince, chainRecent uint64 // The range of chain server peer can serve.
	stateSince, stateRecent uint64 // The range of state server peer can serve.

	// Advertised checkpoint fields
	checkpointNumber uint64                   // The block height which the checkpoint is registered.
	checkpoint       params.TrustedCheckpoint // The advertised checkpoint sent by server.

	poolEntry *poolEntry              // Statistic for server peer.
	fcServer  *flowcontrol.ServerNode // Client side mirror token bucket.

	// Statistics
	errCount    int // Counter the invalid responses server has replied
	updateCount uint64
	updateTime  mclock.AbsTime

	// Callbacks
	hasBlock func(common.Hash, uint64, bool) bool // Used to determine whether the server has the specified block.
}

func newServerPeer(version int, network uint64, trusted bool, p *p2p.Peer, rw p2p.MsgReadWriter) *serverPeer {
	return &serverPeer{
		peerCommons: peerCommons{
			Peer:      p,
			rw:        rw,
			id:        peerIdToString(p.ID()),
			version:   version,
			network:   network,
			sendQueue: newExecQueue(100),
			closeCh:   make(chan struct{}),
		},
		trusted: trusted,
	}
}

// rejectUpdate returns true if a parameter update has to be rejected because
// the size and/or rate of updates exceed the capacity limitation
func (p *serverPeer) rejectUpdate(size uint64) bool {
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
func (p *serverPeer) freeze() {
	if atomic.CompareAndSwapUint32(&p.frozen, 0, 1) {
		p.sendQueue.clear()
	}
}

// unfreeze processes Resume messages from the given server and set the status
// as unfrozen.
func (p *serverPeer) unfreeze() {
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

// requestHeadersByHash fetches a batch of blocks' headers corresponding to the
// specified header query, based on the hash of an origin block.
func (p *serverPeer) requestHeadersByHash(reqID uint64, origin common.Hash, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromhash", origin, "skip", skip, "reverse", reverse)
	return sendRequest(p.rw, GetBlockHeadersMsg, reqID, &getBlockHeadersData{Origin: hashOrNumber{Hash: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// requestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin block.
func (p *serverPeer) requestHeadersByNumber(reqID, origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return sendRequest(p.rw, GetBlockHeadersMsg, reqID, &getBlockHeadersData{Origin: hashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// requestBodies fetches a batch of blocks' bodies corresponding to the hashes
// specified.
func (p *serverPeer) requestBodies(reqID uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of block bodies", "count", len(hashes))
	return sendRequest(p.rw, GetBlockBodiesMsg, reqID, hashes)
}

// requestCode fetches a batch of arbitrary data from a node's known state
// data, corresponding to the specified hashes.
func (p *serverPeer) requestCode(reqID uint64, reqs []CodeReq) error {
	p.Log().Debug("Fetching batch of codes", "count", len(reqs))
	return sendRequest(p.rw, GetCodeMsg, reqID, reqs)
}

// requestReceipts fetches a batch of transaction receipts from a remote node.
func (p *serverPeer) requestReceipts(reqID uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of receipts", "count", len(hashes))
	return sendRequest(p.rw, GetReceiptsMsg, reqID, hashes)
}

// requestProofs fetches a batch of merkle proofs from a remote node.
func (p *serverPeer) requestProofs(reqID uint64, reqs []ProofReq) error {
	p.Log().Debug("Fetching batch of proofs", "count", len(reqs))
	return sendRequest(p.rw, GetProofsV2Msg, reqID, reqs)
}

// requestHelperTrieProofs fetches a batch of HelperTrie merkle proofs from a remote node.
func (p *serverPeer) requestHelperTrieProofs(reqID uint64, reqs []HelperTrieReq) error {
	p.Log().Debug("Fetching batch of HelperTrie proofs", "count", len(reqs))
	return sendRequest(p.rw, GetHelperTrieProofsMsg, reqID, reqs)
}

// requestTxStatus fetches a batch of transaction status records from a remote node.
func (p *serverPeer) requestTxStatus(reqID uint64, txHashes []common.Hash) error {
	p.Log().Debug("Requesting transaction status", "count", len(txHashes))
	return sendRequest(p.rw, GetTxStatusMsg, reqID, txHashes)
}

// sendTxs creates a reply with a batch of transactions to be added to the remote transaction pool.
func (p *serverPeer) sendTxs(reqID uint64, txs rlp.RawValue) error {
	p.Log().Debug("Sending batch of transactions", "size", len(txs))
	return sendRequest(p.rw, SendTxV2Msg, reqID, txs)
}

// sendLespay sends a set of commands to the service token sale module
func (p *serverPeer) sendLespay(reqID uint64, cmd []byte) error {
	p.Log().Debug("Sending batch of lespay commands", "size", len(cmd))
	return sendRequest(p.rw, LespayMsg, reqID, cmd)
}

// waitBefore implements distPeer interface
func (p *serverPeer) waitBefore(maxCost uint64) (time.Duration, float64) {
	return p.fcServer.CanSend(maxCost)
}

// getRequestCost returns an estimated request cost according to the flow control
// rules negotiated between the server and the client.
func (p *serverPeer) getRequestCost(msgcode uint64, amount int) uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	costs := p.fcCosts[msgcode]
	if costs == nil {
		return 0
	}
	cost := costs.baseCost + costs.reqCost*uint64(amount)
	if cost > p.fcParams.BufLimit {
		cost = p.fcParams.BufLimit
	}
	return cost
}

// getTxRelayCost returns an estimated relay cost according to the flow control
// rules negotiated between the server and the client.
func (p *serverPeer) getTxRelayCost(amount, size int) uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	costs := p.fcCosts[SendTxV2Msg]
	if costs == nil {
		return 0
	}
	cost := costs.baseCost + costs.reqCost*uint64(amount)
	sizeCost := costs.baseCost + costs.reqCost*uint64(size)/txSizeCostLimit
	if sizeCost > cost {
		cost = sizeCost
	}
	if cost > p.fcParams.BufLimit {
		cost = p.fcParams.BufLimit
	}
	return cost
}

// HasBlock checks if the peer has a given block
func (p *serverPeer) HasBlock(hash common.Hash, number uint64, hasState bool) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()

	head := p.headInfo.Number
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
func (p *serverPeer) updateFlowControl(update keyValueMap) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// If any of the flow control params is nil, refuse to update.
	var params flowcontrol.ServerParams
	updated := false
	if update.get("flowControl/BL", &params.BufLimit) == nil && update.get("flowControl/MRR", &params.MinRecharge) == nil {
		// todo can light client set a minimal acceptable flow control params?
		p.fcParams = params
		p.fcServer.UpdateParams(params)
		updated = true
	}
	var MRC RequestCostList
	if update.get("flowControl/MRC", &MRC) == nil {
		costUpdate := MRC.decode(ProtocolLengths[uint(p.version)])
		for code, cost := range costUpdate {
			p.fcCosts[code] = cost
		}
		updated = true
	}
	if updated {
		p.active = p.paramsUseful()
	}
}

// paramsUseful returns true if the server parameters ensure the minimum required
// buffer limit and recharge
func (p *serverPeer) paramsUseful() bool {
	reqRecharge, reqBufLimit := p.fcCosts.reqParams()
	return p.fcParams.MinRecharge >= reqRecharge && p.fcParams.BufLimit >= reqBufLimit
}

// Handshake executes the les protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
func (p *serverPeer) Handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, server *LesServer) error {
	return p.handshake(td, head, headNum, genesis, func(lists *keyValueList) {
		// Add some client-specific handshake fields
		//
		// Enable signed announcement randomly even the server is not trusted.
		p.announceType = announceTypeSimple
		if p.trusted {
			p.announceType = announceTypeSigned
		}
		*lists = (*lists).add("announceType", p.announceType)
	}, func(recv keyValueMap) error {
		if recv.get("serveChainSince", &p.chainSince) != nil {
			p.onlyAnnounce = true
		}
		if recv.get("serveRecentChain", &p.chainRecent) != nil {
			p.chainRecent = 0
		}
		if recv.get("serveStateSince", &p.stateSince) != nil {
			p.onlyAnnounce = true
		}
		if recv.get("serveRecentState", &p.stateRecent) != nil {
			p.stateRecent = 0
		}
		if recv.get("txRelay", nil) != nil {
			p.onlyAnnounce = true
		}
		if p.onlyAnnounce && !p.trusted {
			return errResp(ErrUselessPeer, "peer cannot serve requests")
		}
		// Parse flow control handshake packet.
		var sParams flowcontrol.ServerParams
		if err := recv.get("flowControl/BL", &sParams.BufLimit); err != nil {
			return err
		}
		if err := recv.get("flowControl/MRR", &sParams.MinRecharge); err != nil {
			return err
		}
		var MRC RequestCostList
		if err := recv.get("flowControl/MRC", &MRC); err != nil {
			return err
		}
		p.fcParams = sParams
		p.fcServer = flowcontrol.NewServerNode(sParams, &mclock.System{})
		p.fcCosts = MRC.decode(ProtocolLengths[uint(p.version)])
		p.active = p.paramsUseful()

		recv.get("checkpoint/value", &p.checkpoint)
		recv.get("checkpoint/registerHeight", &p.checkpointNumber)

		if !p.onlyAnnounce {
			for msgCode := range reqAvgTimeCost {
				if p.fcCosts[msgCode] == nil {
					return errResp(ErrUselessPeer, "peer does not support message %d", msgCode)
				}
			}
		}
		return nil
	})
}

// clientPeer represents each node to which the les server is connected.
// The node here refers to the light client.
type clientPeer struct {
	peerCommons

	activate, deactivate func()
	getBalance           func() uint64

	// responseLock ensures that responses are queued in the same order as
	// RequestProcessed is called
	responseLock   sync.Mutex
	server         bool
	invalidCount   uint32 // Counter the invalid request the client peer has made.
	responseCount  uint64 // Counter to generate an unique id for request processing.
	errCh          chan error
	fcClient       *flowcontrol.ClientNode // Server side mirror token bucket.
	balanceTracker *balanceTracker         // set by clientPool.connect, used and removed by serverHandler
}

func newClientPeer(version int, network uint64, p *p2p.Peer, rw p2p.MsgReadWriter) *clientPeer {
	return &clientPeer{
		peerCommons: peerCommons{
			Peer:      p,
			rw:        rw,
			id:        peerIdToString(p.ID()),
			version:   version,
			network:   network,
			sendQueue: newExecQueue(100),
			closeCh:   make(chan struct{}),
		},
		errCh: make(chan error, 1),
	}
}

// freeClientId returns a string identifier for the peer. Multiple peers with
// the same identifier can not be connected in free mode simultaneously.
func (p *clientPeer) freeClientId() string {
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
	return p2p.Send(p.rw, StopMsg, struct{}{})
}

// sendResume notifies the client about getting out of frozen state
func (p *clientPeer) sendResume(sf stateFeedback) error {
	return p2p.Send(p.rw, ResumeMsg, sf)
}

// freeze temporarily puts the client in a frozen state which means all unprocessed
// and subsequent requests are dropped. Unfreezing happens automatically after a short
// time if the client's buffer value is at least in the slightly positive region.
// The client is also notified about being frozen/unfrozen with a Stop/Resume message.
func (p *clientPeer) freeze() {
	if p.version < lpv3 {
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
				var balance uint64
				if p.getBalance != nil {
					balance = p.getBalance()
				}
				sf := stateFeedback{
					protocolVersion: p.version,
					stateFeedbackV4: stateFeedbackV4{
						BV:           bufValue,
						RealCost:     0,
						TokenBalance: balance,
					},
				}
				p.sendResume(sf)
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
func (r *reply) send(sf stateFeedback) error {
	type resp struct {
		ReqID uint64
		SF    stateFeedback
		Data  rlp.RawValue
	}
	return p2p.Send(r.w, r.msgcode, resp{r.reqID, sf, r.data})
}

// size returns the RLP encoded size of the message data
func (r *reply) size() uint32 {
	return uint32(len(r.data))
}

// replyBlockHeaders creates a reply with a batch of block headers
func (p *clientPeer) replyBlockHeaders(reqID uint64, headers []*types.Header) *reply {
	data, _ := rlp.EncodeToBytes(headers)
	return &reply{p.rw, BlockHeadersMsg, reqID, data}
}

// replyBlockBodiesRLP creates a reply with a batch of block contents from
// an already RLP encoded format.
func (p *clientPeer) replyBlockBodiesRLP(reqID uint64, bodies []rlp.RawValue) *reply {
	data, _ := rlp.EncodeToBytes(bodies)
	return &reply{p.rw, BlockBodiesMsg, reqID, data}
}

// replyCode creates a reply with a batch of arbitrary internal data, corresponding to the
// hashes requested.
func (p *clientPeer) replyCode(reqID uint64, codes [][]byte) *reply {
	data, _ := rlp.EncodeToBytes(codes)
	return &reply{p.rw, CodeMsg, reqID, data}
}

// replyReceiptsRLP creates a reply with a batch of transaction receipts, corresponding to the
// ones requested from an already RLP encoded format.
func (p *clientPeer) replyReceiptsRLP(reqID uint64, receipts []rlp.RawValue) *reply {
	data, _ := rlp.EncodeToBytes(receipts)
	return &reply{p.rw, ReceiptsMsg, reqID, data}
}

// replyProofsV2 creates a reply with a batch of merkle proofs, corresponding to the ones requested.
func (p *clientPeer) replyProofsV2(reqID uint64, proofs light.NodeList) *reply {
	data, _ := rlp.EncodeToBytes(proofs)
	return &reply{p.rw, ProofsV2Msg, reqID, data}
}

// replyHelperTrieProofs creates a reply with a batch of HelperTrie proofs, corresponding to the ones requested.
func (p *clientPeer) replyHelperTrieProofs(reqID uint64, resp HelperTrieResps) *reply {
	data, _ := rlp.EncodeToBytes(resp)
	return &reply{p.rw, HelperTrieProofsMsg, reqID, data}
}

// replyTxStatus creates a reply with a batch of transaction status records, corresponding to the ones requested.
func (p *clientPeer) replyTxStatus(reqID uint64, stats []light.TxStatus) *reply {
	data, _ := rlp.EncodeToBytes(stats)
	return &reply{p.rw, TxStatusMsg, reqID, data}
}

// replyLespay sends a set of replies to lespay commands
func (p *clientPeer) replyLespay(reqID uint64, reply []byte, delay uint) error {
	p.Log().Debug("Sending batch of lespay replies", "size", len(reply))
	return sendRequest(p.rw, LespayReplyMsg, reqID, lespayReply{reply, delay})
}

// sendAnnounce announces the availability of a number of blocks through
// a hash notification.
func (p *clientPeer) sendAnnounce(request announceData) error {
	return p2p.Send(p.rw, AnnounceMsg, request)
}

// updateCapacity updates the request serving capacity assigned to a given client
// and also sends an announcement about the updated flow control parameters
func (p *clientPeer) updateCapacity(cap uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.active && cap != 0 && p.activate != nil {
		p.activate()
	}
	if cap != 0 || p.version >= lpv4 {
		p.fcParams = flowcontrol.ServerParams{MinRecharge: cap, BufLimit: cap * bufLimitRatio}
		p.fcClient.UpdateParams(p.fcParams)
		var kvList keyValueList
		kvList = kvList.add("flowControl/BL", cap*bufLimitRatio)
		kvList = kvList.add("flowControl/MRR", cap)
		p.mustQueueSend(func() { p.sendAnnounce(announceData{Update: kvList}) })
	}
	if p.active && cap == 0 && p.deactivate != nil {
		p.deactivate()
	}
}

// Handshake executes the les protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
func (p *clientPeer) Handshake(td *big.Int, head common.Hash, headNum uint64, genesis common.Hash, server *LesServer) error {
	return p.handshake(td, head, headNum, genesis, func(lists *keyValueList) {
		// Add some information which services server can offer.
		if !server.config.UltraLightOnlyAnnounce {
			*lists = (*lists).add("serveHeaders", nil)
			*lists = (*lists).add("serveChainSince", uint64(0))
			*lists = (*lists).add("serveStateSince", uint64(0))

			// If local ethereum node is running in archive mode, advertise ourselves we have
			// all version state data. Otherwise only recent state is available.
			stateRecent := uint64(core.TriesInMemory - 4)
			if server.archiveMode {
				stateRecent = 0
			}
			*lists = (*lists).add("serveRecentState", stateRecent)
			*lists = (*lists).add("txRelay", nil)
		}
		p.active = p.version < lpv4
		if p.active {
			p.fcParams = server.defParams
		} else {
			p.fcParams = flowcontrol.ServerParams{}
		}
		*lists = (*lists).add("flowControl/BL", p.fcParams.BufLimit)
		*lists = (*lists).add("flowControl/MRR", p.fcParams.MinRecharge)

		var costList RequestCostList
		if server.costTracker.testCostList != nil {
			costList = server.costTracker.testCostList
		} else {
			costList = server.costTracker.makeCostList(server.costTracker.globalFactor())
		}
		*lists = (*lists).add("flowControl/MRC", costList)
		p.fcCosts = costList.decode(ProtocolLengths[uint(p.version)])

		// Add advertised checkpoint and register block height which
		// client can verify the checkpoint validity.
		if server.oracle != nil && server.oracle.IsRunning() {
			cp, height := server.oracle.StableCheckpoint()
			if cp != nil {
				*lists = (*lists).add("checkpoint/value", cp)
				*lists = (*lists).add("checkpoint/registerHeight", height)
			}
		}
	}, func(recv keyValueMap) error {
		p.server = recv.get("flowControl/MRR", nil) == nil
		if p.server {
			p.announceType = announceTypeNone // connected to another server, send no messages
		} else {
			if recv.get("announceType", &p.announceType) != nil {
				// set default announceType on server side
				p.announceType = announceTypeSimple
			}
			p.fcClient = flowcontrol.NewClientNode(server.fcManager, p.fcParams)
		}
		return nil
	})
}

// serverPeerSubscriber is an interface to notify services about added or
// removed server peers
type serverPeerSubscriber interface {
	registerPeer(*serverPeer)
	unregisterPeer(*serverPeer)
}

// clientPeerSubscriber is an interface to notify services about added or
// removed client peers
type clientPeerSubscriber interface {
	registerPeer(*clientPeer)
	unregisterPeer(*clientPeer)
}

// clientPeerSet represents the set of active client peers currently
// participating in the Light Ethereum sub-protocol.
type clientPeerSet struct {
	active, inactive map[string]*clientPeer
	// subscribers is a batch of subscribers and peerset will notify
	// these subscribers when the peerset changes(new client peer is
	// added or removed)
	subscribers []clientPeerSubscriber
	closed      bool
	lock        sync.RWMutex
}

// newClientPeerSet creates a new peer set to track the client peers.
func newClientPeerSet() *clientPeerSet {
	return &clientPeerSet{
		active:   make(map[string]*clientPeer),
		inactive: make(map[string]*clientPeer),
	}
}

// subscribe adds a service to be notified about added or removed
// peers and also register all active peers into the given service.
func (ps *clientPeerSet) subscribe(sub clientPeerSubscriber) {
	ps.lock.Lock()
	ps.subscribers = append(ps.subscribers, sub)
	notify := make([]*clientPeer, 0, len(ps.active))
	for _, p := range ps.active {
		notify = append(notify, p)
	}
	ps.lock.Unlock()
	for _, p := range notify {
		sub.registerPeer(p)
	}
}

// unSubscribe removes the specified service from the subscriber pool.
func (ps *clientPeerSet) unSubscribe(sub clientPeerSubscriber) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for i, s := range ps.subscribers {
		if s == sub {
			ps.subscribers = append(ps.subscribers[:i], ps.subscribers[i+1:]...)
			return
		}
	}
}

// register adds a new peer into the peer set, or returns an error if the
// peer is already known.
func (ps *clientPeerSet) register(p *clientPeer) error {
	ps.lock.Lock()
	if ps.closed {
		ps.lock.Unlock()
		return errClosed
	}
	if _, ok := ps.active[p.id]; ok {
		ps.lock.Unlock()
		return errAlreadyRegistered
	}
	delete(ps.inactive, p.id)
	ps.active[p.id] = p

	peers := make([]clientPeerSubscriber, len(ps.subscribers))
	copy(peers, ps.subscribers)
	ps.lock.Unlock()

	for _, n := range peers {
		n.registerPeer(p)
	}
	return nil
}

// unregister removes a remote peer from the peer set, disabling any further
// actions to/from that particular entity. It also initiates disconnection
// at the networking layer.
func (ps *clientPeerSet) unregister(p *clientPeer) error {
	ps.lock.Lock()
	if _, ok := ps.active[p.id]; !ok {
		ps.lock.Unlock()
		return errNotRegistered
	} else {
		delete(ps.active, p.id)
		ps.inactive[p.id] = p
		peers := make([]clientPeerSubscriber, len(ps.subscribers))
		copy(peers, ps.subscribers)
		ps.lock.Unlock()

		for _, n := range peers {
			n.unregisterPeer(p)
		}
		return nil
	}
}

// disconnect removes a remote peer from either the active or inactive set and
// initiates disconnection at the networking layer.
func (ps *clientPeerSet) disconnect(id string) error {
	ps.lock.Lock()

	var (
		peers []clientPeerSubscriber
		p     *clientPeer
		ok    bool
	)
	if p, ok = ps.active[id]; ok {
		delete(ps.active, p.id)
		peers = make([]clientPeerSubscriber, len(ps.subscribers))
		copy(peers, ps.subscribers)
	} else if p, ok = ps.inactive[id]; ok {
		delete(ps.inactive, id)
	} else {
		ps.lock.Unlock()
		return errNotRegistered
	}
	ps.lock.Unlock()
	for _, n := range peers {
		n.unregisterPeer(p)
	}
	p.Peer.Disconnect(p2p.DiscUselessPeer)
	return nil
}

// ids returns a list of all active peer IDs
func (ps *clientPeerSet) ids() []string {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var ids []string
	for id := range ps.active {
		ids = append(ids, id)
	}
	return ids
}

// peer retrieves the registered peer with the given id.
func (ps *clientPeerSet) peer(id string) *clientPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	if p, ok := ps.active[id]; ok {
		return p
	}
	return ps.inactive[id]
}

// len returns if the current number of peers in the active set.
func (ps *clientPeerSet) len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.active)
}

// allClientPeers returns all active client peers in a list.
func (ps *clientPeerSet) allPeers() []*clientPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*clientPeer, 0, len(ps.active))
	for _, p := range ps.active {
		list = append(list, p)
	}
	return list
}

// close disconnects all peers. No new peers can be registered
// after close has returned.
func (ps *clientPeerSet) close() {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for _, p := range ps.active {
		p.Disconnect(p2p.DiscQuitting)
	}
	for _, p := range ps.inactive {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.closed = true
}

// serverPeerSet represents the set of active server peers currently
// participating in the Light Ethereum sub-protocol.
type serverPeerSet struct {
	active, inactive map[string]*serverPeer
	// subscribers is a batch of subscribers and peerset will notify
	// these subscribers when the peerset changes(new server peer is
	// added or removed)
	subscribers []serverPeerSubscriber
	closed      bool
	lock        sync.RWMutex
}

// newServerPeerSet creates a new peer set to track the active server peers.
func newServerPeerSet() *serverPeerSet {
	return &serverPeerSet{
		active:   make(map[string]*serverPeer),
		inactive: make(map[string]*serverPeer),
	}
}

// subscribe adds a service to be notified about added or removed
// peers and also register all active peers into the given service.
func (ps *serverPeerSet) subscribe(sub serverPeerSubscriber) {
	ps.lock.Lock()
	ps.subscribers = append(ps.subscribers, sub)
	notify := make([]*serverPeer, 0, len(ps.active))
	for _, p := range ps.active {
		notify = append(notify, p)
	}
	ps.lock.Unlock()
	for _, p := range notify {
		sub.registerPeer(p)
	}
}

// unSubscribe removes the specified service from the subscriber pool.
func (ps *serverPeerSet) unSubscribe(sub serverPeerSubscriber) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for i, s := range ps.subscribers {
		if s == sub {
			ps.subscribers = append(ps.subscribers[:i], ps.subscribers[i+1:]...)
			return
		}
	}
}

// register adds a new server peer into the set, or returns an error if the
// peer is already known.
func (ps *serverPeerSet) register(p *serverPeer) error {
	ps.lock.Lock()
	if ps.closed {
		ps.lock.Unlock()
		return errClosed
	}
	if _, ok := ps.active[p.id]; ok {
		ps.lock.Unlock()
		return errAlreadyRegistered
	}
	delete(ps.inactive, p.id)
	ps.active[p.id] = p

	peers := make([]serverPeerSubscriber, len(ps.subscribers))
	copy(peers, ps.subscribers)
	ps.lock.Unlock()

	for _, n := range peers {
		n.registerPeer(p)
	}
	return nil
}

// unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity. It also initiates disconnection at
// the networking layer.
func (ps *serverPeerSet) unregister(p *serverPeer) error {
	ps.lock.Lock()
	if _, ok := ps.active[p.id]; !ok {
		ps.lock.Unlock()
		return errNotRegistered
	} else {
		delete(ps.active, p.id)
		ps.inactive[p.id] = p
		peers := make([]serverPeerSubscriber, len(ps.subscribers))
		copy(peers, ps.subscribers)
		ps.lock.Unlock()

		for _, n := range peers {
			n.unregisterPeer(p)
		}
		return nil
	}
}

// disconnect removes a remote peer from either the active or inactive set and
// initiates disconnection at the networking layer.
func (ps *serverPeerSet) disconnect(id string) error {
	ps.lock.Lock()

	var (
		peers []serverPeerSubscriber
		p     *serverPeer
		ok    bool
	)
	if p, ok = ps.active[id]; ok {
		delete(ps.active, p.id)
		peers = make([]serverPeerSubscriber, len(ps.subscribers))
		copy(peers, ps.subscribers)
	} else if p, ok = ps.inactive[id]; ok {
		delete(ps.inactive, id)
	} else {
		ps.lock.Unlock()
		return errNotRegistered
	}
	ps.lock.Unlock()
	for _, n := range peers {
		n.unregisterPeer(p)
	}
	p.Peer.Disconnect(p2p.DiscUselessPeer)
	return nil
}

// ids returns a list of all active peer IDs
func (ps *serverPeerSet) ids() []string {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var ids []string
	for id := range ps.active {
		ids = append(ids, id)
	}
	return ids
}

// peer retrieves the registered peer with the given id.
func (ps *serverPeerSet) peer(id string) *serverPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	if p, ok := ps.active[id]; ok {
		return p
	}
	return ps.inactive[id]
}

// len returns if the current number of peers in the active set.
func (ps *serverPeerSet) len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.active)
}

// bestPeer retrieves the known peer with the currently highest total difficulty.
// If the peerset is "client peer set", then nothing meaningful will return. The
// reason is client peer never send back their latest status to server.
func (ps *serverPeerSet) bestPeer() *serverPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var (
		bestPeer *serverPeer
		bestTd   *big.Int
	)
	for _, p := range ps.active {
		if td := p.Td(); bestTd == nil || td.Cmp(bestTd) > 0 {
			bestPeer, bestTd = p, td
		}
	}
	return bestPeer
}

// allPeers returns all active server peers in a list.
func (ps *serverPeerSet) allPeers() []*serverPeer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*serverPeer, 0, len(ps.active))
	for _, p := range ps.active {
		list = append(list, p)
	}
	return list
}

// close disconnects all peers. No new peers can be registered
// after close has returned.
func (ps *serverPeerSet) close() {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for _, p := range ps.active {
		p.Disconnect(p2p.DiscQuitting)
	}
	for _, p := range ps.inactive {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.closed = true
}
