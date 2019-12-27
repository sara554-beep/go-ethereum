package distributor

import (
	"container/list"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"time"
)

// DistPeer is an LES server peer interface for the Request distributor.
// WaitBefore returns either the necessary waiting time before sending a Request
// with the given upper estimated cost or the estimated remaining relative buffer
// value after sending such a Request (in which case the Request can be sent
// immediately). At least one of these values is always zero.
type DistPeer interface {
	ID() enode.ID
	WaitBefore(uint64) (time.Duration, float64)
	CanQueue() bool
	QueueSend(f func()) bool
}

// DistReq is the Request abstraction used by the distributor. It is based on
// three callback functions:
// - GetCost returns the upper estimate of the cost of sending the Request to a given peer
// - CanSend tells if the server peer is suitable to serve the Request
// - Request prepares sending the Request to the given peer and returns a function that
// does the actual sending. Request order should be preserved but the callback itself should not
// block until it is sent because other peers might still be able to receive requests while
// one of them is blocking. Instead, the returned function is put in the peer's send Queue.
type DistReq struct {
	// COST
	//
	GetCost func(DistPeer) uint64
	CanSend func(DistPeer) bool
	Request func(DistPeer) func()

	reqOrder     uint64
	sentChn      chan DistPeer
	element      *list.Element
	waitForPeers mclock.AbsTime
	enterQueue   mclock.AbsTime
}
