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

package ethflare

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

type EthflareClient struct {
	url string // The URL of http server for data retrieval
}

func NewClient(url string) *EthflareClient {
	return &EthflareClient{url: url}
}

func (client *EthflareClient) GetBlockHeader(ctx context.Context, hash common.Hash) (*types.Header, error) {
	var header types.Header
	res, err := httpDo(ctx, fmt.Sprintf("%s/chain/0x%x/header", client.url, hash))
	if err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(res, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func (client *EthflareClient) GetStateTile(ctx context.Context, hash common.Hash) ([][]byte, error) {
	res, err := httpDo(ctx, fmt.Sprintf("%s/state/0x%x?target=%d&limit=%d&barrier=%d", client.url, hash, 16, 256, 2))
	if err != nil {
		return nil, err
	}
	var nodes [][]byte
	if err := rlp.DecodeBytes(res, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// httpDo sends a http request based on the given URL, extracts the
// response and return.
func httpDo(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	blob, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	res.Body.Close()
	return blob, nil
}
