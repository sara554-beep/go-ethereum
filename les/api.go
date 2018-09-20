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
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	errNoCheckpoint = errors.New("no local checkpoint provided")
	errNotActivated = errors.New("checkpoint registrar is not activated")
)

// PrivateLesServerAPI provides a private API to access the les server.
type PrivateLesServerAPI struct {
	server *LesServer
	reg    *checkpointRegistrar
}

// NewPrivateLesServerAPI creates a new les server API.
func NewPrivateLesServerAPI(server *LesServer) *PrivateLesServerAPI {
	return &PrivateLesServerAPI{
		server: server,
		reg:    server.protocolManager.reg,
	}
}

// LatestCheckpoint returns the latest local checkpoint package.
//
// The checkpoint package consists of 4 strings:
//   result[0], hex encoded latest section index
//   result[1], 32 bytes hex encoded latest section head hash
//   result[2], 32 bytes hex encoded latest section canonical hash trie root hash
//   result[3], 32 bytes hex encoded latest section bloom trie root hash
func (api *PrivateLesServerAPI) LatestCheckpoint() ([4]string, error) {
	// Short circuit if registrar instance is nil
	if api.reg == nil {
		return [4]string{}, errNotActivated
	}
	var res [4]string
	cp := api.reg.latestLocalCheckpoint()
	if cp.Empty() {
		return res, errNoCheckpoint
	}
	res[0] = hexutil.Encode(big.NewInt(int64(cp.SectionIndex)).Bytes())
	res[1], res[2], res[3] = cp.SectionHead.Hex(), cp.CHTRoot.Hex(), cp.BloomRoot.Hex()
	return res, nil
}

// GetLocalCheckpoint returns the specific local checkpoint package.
//
// The checkpoint package consists of 3 strings:
//   result[0], 32 bytes hex encoded latest section head hash
//   result[1], 32 bytes hex encoded latest section canonical hash trie root hash
//   result[2], 32 bytes hex encoded latest section bloom trie root hash
func (api *PrivateLesServerAPI) GetCheckpoint(index uint64) ([3]string, error) {
	// Short circuit if registrar instance is nil
	if api.reg == nil {
		return [3]string{}, errNotActivated
	}
	var res [3]string
	cp := api.reg.getLocalCheckpoint(index)
	if cp.Empty() {
		return res, errNoCheckpoint
	}
	res[0], res[1], res[2] = cp.SectionHead.Hex(), cp.CHTRoot.Hex(), cp.BloomRoot.Hex()
	return res, nil
}
