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

import "github.com/ethereum/go-ethereum/common"

// EncodeNodeKey combines the node path and node hash together to act as
// the unique database key for the trie node.
//
// The path part is encoded as the SIMPLE-COMPACT format. It encodes all
// the nibbles into the hexary format and right-pads the path to 32 bytes
// for alignment. The raw key length will be encoded in a single byte and
// put it in end of the entire key.
//
// The benefits of this key scheme are that:
// - it can group all the relevant trie nodes together to have data locality
//   in the database perspective.
// - it's space efficient. Although the path prefix is added compared with
//   the legacy scheme(raw node hash as the key), but the underlying database
//   will do the key compression by sharing the key prefix with the preceding
//   key. And all the padded zero bytes can be compressed well by the compression
//   algorithm of database(e.g. Snappy), so the overhead is acceptable.
// - it's pruning friendly. A list of trie nodes with same trie path can be
//   easily obtained for pruning.
// - all the trie nodes can be iterated in the key path ordering.
//
// What's more, the prefix(a few bytes) of node hash is necessary for identifying
// the trie node since the hash collision under the same path prefix is very low.
// And even the collision happens it's also super easy to fix it. TODO(rjl493456442) explore this idea later.
func EncodeNodeKey(owner common.Hash, path []byte, hash common.Hash) []byte {
	if owner == (common.Hash{}) && hash == (common.Hash{}) && len(path) == 0 {
		return nil // special case, metaroot
	}
	var ret []byte
	if owner != (common.Hash{}) {
		ret = append(ret, owner.Bytes()...)
	}
	compact, length := hexToSimpleCompact(path, common.HashLength)
	return append(append(append(ret, compact...), hash.Bytes()...), byte(length))
}

// DecodeNodeKey returns the composing hashes of a trie node key.
// The key is composed by two parts:
// - the trie node owner
// - the trie node path
// - the trie node hash
func DecodeNodeKey(key []byte) (common.Hash, []byte, common.Hash) {
	if len(key) == 0 {
		return common.Hash{}, nil, common.Hash{} // special case, metaroot
	}
	if len(key) != 1+2*common.HashLength && len(key) != 1+3*common.HashLength {
		return common.Hash{}, nil, common.Hash{} // invalid key
	}
	flag := key[len(key)-1]
	key = key[:len(key)-1]

	hash := common.BytesToHash(key[len(key)-common.HashLength:])
	path := key[:len(key)-common.HashLength]

	// Single trie node(account)
	if len(path) == common.HashLength {
		return common.Hash{}, simpleCompactToHex(path, int(flag)), hash
	}
	// Layered trie node(storage)
	return common.BytesToHash(path[:common.HashLength]), simpleCompactToHex(path[common.HashLength:], int(flag)), hash
}

// TrieRootKey returns the composed trie node key for trie root node.
func TrieRootKey(owner common.Hash, root common.Hash) []byte {
	return EncodeNodeKey(owner, nil, root)
}

// encodeNodePath returns the encoded node path with given parameters.
// Compared with the trie nodedatabase key the hash and path length
// flag are omitted here.
func encodeNodePath(owner common.Hash, path []byte) []byte {
	var prefix []byte
	if owner != (common.Hash{}) {
		prefix = append(prefix, owner.Bytes()...)
	}
	encoded, _ := hexToSimpleCompact(path, common.HashLength)
	return append(prefix, encoded...)
}
