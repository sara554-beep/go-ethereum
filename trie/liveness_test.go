// Copyright 2021 The go-ethereum Authors
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

package trie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestCheckLiveness(t *testing.T) {
	triedb := NewDatabase(memorydb.New())
	triedb.cleans = fastcache.New(16 * 1024 * 1024)
	trie, _ := NewSecure(common.Hash{}, triedb)

	// Fill it with some arbitrary data
	content := make(map[string][]byte)
	for i := byte(0); i < 255; i++ {
		// Map the same data under multiple keys
		key, val := common.LeftPadBytes([]byte{1, i}, 32), []byte{i}
		content[string(key)] = val
		trie.Update(key, val)

		key, val = common.LeftPadBytes([]byte{2, i}, 32), []byte{i}
		content[string(key)] = val
		trie.Update(key, val)

		// Add some other data to inflate the trie
		for j := byte(3); j < 13; j++ {
			key, val = common.LeftPadBytes([]byte{j, i}, 32), []byte{j, i}
			content[string(key)] = val
			trie.Update(key, val)
		}
	}
	hash, _ := trie.Commit(nil)
	var keys [][]byte
	triedb.Commit(hash, false, func(key []byte) {
		keys = append(keys, key)
	})
	traverse := &traverser{
		db:    triedb,
		state: &traverserState{hash: hash, node: hashNode(hash[:])},
	}
	for _, key := range keys {
		owner, path, hash := DecodeNodeKey(key)
		exist := traverse.live(owner, hash, path)
		if !exist {
			t.Fatalf("Missing existent node")
		}
	}
}

func myFunc(input []byte) (common.Hash, []byte) {
	if index := bytes.Index(input, []byte{0xff}); index == -1 {
		return common.Hash{}, input
	} else {
		return common.BytesToHash(HexToKeybytes(input[:index])), input[index+1:]
	}
}

func TestMy(t *testing.T) {
	var b = common.Hex2Bytes("000e0f06010d0701070f0307090d08010c0c040b0706060309090700070a0a0f0204000d0907000e0609060a010d010f000a04020d0007070f060d0b0c0a040010ff0e0c0101")

	owner, path := myFunc(b)
	fmt.Println(owner.Hex())
	fmt.Println(path)
}
