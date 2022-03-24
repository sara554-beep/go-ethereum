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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

const (
	HashScheme = "hashScheme" // Identifier of hash based node scheme
	PathScheme = "pathScheme" // Identifier of path based node scheme
)

// NodeScheme describes the scheme for interacting nodes in disk.
type NodeScheme interface {
	// Name returns the identifier of node scheme.
	Name() string

	// HasTrieNode checks the trie node presence with the provided node info and
	// the associated node hash.
	HasTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) bool

	// ReadTrieNode retrieves the trie node from database with the provided node info
	// and the associated node hash.
	ReadTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) []byte

	// WriteTrieNode writes the trie node into database with the provided node info
	// and associated node hash.
	WriteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash, node []byte)

	// DeleteTrieNode deletes the trie node from database with the provided node info
	// and associated node hash.
	DeleteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash)

	// IsTrieNode returns an indicator if the given key is the key of trie node
	// according to the scheme.
	IsTrieNode(key []byte) (bool, []byte)
}

type hashScheme struct{}

// Name returns the identifier of hash based scheme.
func (scheme *hashScheme) Name() string {
	return HashScheme
}

// HasTrieNode checks the trie node presence with the provided node info and
// the associated node hash.
func (scheme *hashScheme) HasTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) bool {
	return rawdb.HasLegacyTrieNode(db, hash)
}

// ReadTrieNode retrieves the trie node from database with the provided node info
// and associated node hash.
func (scheme *hashScheme) ReadTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) []byte {
	return rawdb.ReadLegacyTrieNode(db, hash)
}

// WriteTrieNode writes the trie node into database with the provided node info
// and associated node hash.
func (scheme *hashScheme) WriteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash, node []byte) {
	rawdb.WriteLegacyTrieNode(db, hash, node)
}

// DeleteTrieNode deletes the trie node from database with the provided node info
// and associated node hash.
func (scheme *hashScheme) DeleteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash) {
	rawdb.DeleteLegacyTrieNode(db, hash)
}

// IsTrieNode returns an indicator if the given key is the key of trie node
// according to the scheme.
func (scheme *hashScheme) IsTrieNode(key []byte) (bool, []byte) {
	if len(key) == common.HashLength {
		return true, key
	}
	return false, nil
}

type pathScheme struct{}

// Name returns the identifier of path based scheme.
func (scheme *pathScheme) Name() string {
	return PathScheme
}

// HasTrieNode checks the trie node presence with the provided node info and
// the associated node hash.
func (scheme *pathScheme) HasTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) bool {
	return rawdb.HasTrieNode(db, encodeStorageKey(owner, path), hash)
}

// ReadTrieNode retrieves the trie node from database with the provided node info
// and associated node hash.
func (scheme *pathScheme) ReadTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash) []byte {
	blob, h := rawdb.ReadTrieNode(db, encodeStorageKey(owner, path))
	if h != hash {
		return nil
	}
	return blob
}

// WriteTrieNode writes the trie node into database with the provided node info
// and associated node hash.
func (scheme *pathScheme) WriteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash, node []byte) {
	rawdb.WriteTrieNode(db, encodeStorageKey(owner, path), node)
}

// DeleteTrieNode deletes the trie node from database with the provided node info
// and associated node hash.
func (scheme *pathScheme) DeleteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash) {
	rawdb.DeleteTrieNode(db, encodeStorageKey(owner, path))
}

// IsTrieNode returns an indicator if the given key is the key of trie node
// according to the scheme.
func (scheme *pathScheme) IsTrieNode(key []byte) (bool, []byte) {
	return rawdb.IsTrieNodeKey(key)
}

// encodeStorageKey combines the node owner and node path together to act as
// the unique database key for the trie node.
//
// The path part is encoded as the SUFFIX-COMPACT format. It encodes all
// the nibbles into the hexary format and put the oddness flag in the end.
//
// The benefits of this key scheme are that:
// - it can group all the relevant trie nodes together to have data locality
//   in the database perspective.
// - it's space efficient. The keys obtained after encoding of adjacent nodes
//   have the same prefix which can cut down the real data size stored in the
//   underlying database
//
// The drawback of this scheme is: trie nodes can't be iterated in the key path
// ordering. Need to twist this format a bit if this property is needed.
func encodeStorageKey(owner common.Hash, path []byte) []byte {
	var ret []byte
	if owner != (common.Hash{}) {
		ret = append(ret, owner.Bytes()...)
	}
	return append(ret, hexToSuffixCompact(path)...)
}

// decodeStorageKey decodes the storage format node key and returns all the
// key components. The returned key is in hex nibbles.
func decodeStorageKey(key []byte) (common.Hash, []byte) {
	if len(key) <= common.HashLength {
		return common.Hash{}, suffixCompactToHex(key)
	}
	return common.BytesToHash(key[:common.HashLength]), suffixCompactToHex(key[common.HashLength:])
}

// MaxStorageKeyLen returns the maximum storage key length. In practice,
// it's impossible to reach this length since valueNode is always embedded
// in the parent node. This function can be used to calculate the upper
// bound length of encoded storage key.
func MaxStorageKeyLen() int {
	key := encodeStorageKey(common.HexToHash("deadbeef"), keybytesToHex(common.Hash{}.Bytes()))
	return len(key)
}
