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

package clientpool

import "github.com/ethereum/go-ethereum/p2p/enode"

// clientStatus represents the connection status of clients.
type connectionStatus int

const (
	connected connectionStatus = iota
	active
	inactive
	disconnected
)

type clientStatus struct {
	status connectionStatus

	// Additional "immutable" fields. These fields should be
	// set when the structure is created.
	ipaddr string
}

func (s *clientStatus) withStatus(status connectionStatus) *clientStatus {
	s.status = status
	return s
}

// clientPeer represents a client peer in the pool.
// Positive balances are assigned to node key while negative balances are assigned
// to IPAddr. Currently network IP address without port is used because
// clients have a limited access to IP addresses while new node keys can be easily
// generated so it would be useless to assign a negative value to them.
type clientPeer interface {
	Node() *enode.Node
	IPAddr() string
	UpdateCapacity(uint64)
	Freeze()
	AllowInactive() bool
}
