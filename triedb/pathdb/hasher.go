// Copyright 2024 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gballet/go-verkle"
)

// nodeHasher is the interface for computing the unique hash of a provided
// node blob. It should be implemented for merkle and verkle tree.
type nodeHasher interface {
	hash([]byte) common.Hash
}

// merkleHasher implements the nodeHasher interface, computing node hash
// in merkle tree manner.
type merkleHasher struct {
	lock  sync.Mutex
	state crypto.KeccakState
}

func newMerkleHasher() nodeHasher {
	return &merkleHasher{
		state: crypto.NewKeccakState(),
	}
}

func (h *merkleHasher) hash(blob []byte) common.Hash {
	h.lock.Lock()
	defer h.lock.Unlock()

	return crypto.HashData(h.state, blob)
}

// verkleHasher implements the nodeHasher interface, computing node hash
// in verkle tree manner.
type verkleHasher struct{}

func newVerkleHasher() nodeHasher {
	return &verkleHasher{}
}

func (h *verkleHasher) hash(blob []byte) common.Hash {
	n, err := verkle.ParseNode(blob, 0)
	if err != nil {
		return common.Hash{}
	}
	return n.Commit().Bytes()
}
