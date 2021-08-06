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
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// traverser is a stateful trie traversal data structure used by the pruner to
// verify the liveness of a node within a specific trie. The reason for having
// a separate data structure is to allow reusing previous traversals to check
// the liveness of nested nodes (i.e. entire subtried during pruning).
type traverser struct {
	db    *Database       // Trie database for accessing dirty and clean data
	state *traverserState // Leftover state from the previous traversals
}

// traverserState is the internal state of a trie traverser.
type traverserState struct {
	parent *traverserState // Parent traverser to allow backtracking
	prefix []byte          // Path leading up to the root of this traverser
	node   node            // Trie node where this traverser is currently at
	hash   common.Hash     // Hash of the trie node at the traversed position
}

// ownerAndPath resolves the node path referenced by the traverser.
func (state *traverserState) ownerAndPath() (common.Hash, []byte) {
	if index := bytes.Index(state.prefix, []byte{0xff}); index == -1 {
		return common.Hash{}, state.prefix
	} else {
		return common.BytesToHash(state.prefix[:index]), state.prefix[index+1:]
	}
}

// live checks whether the trie iterated by this traverser contains the hashnode
// at the given path, minimizing data access and processing by reusing previous
// state instead of starting fresh.
//
// The path is a full canonical path from the account trie root down to the node
// potentially crossing over into a storage trie. The account and storage trie
// paths are separated by a 0xff byte (nibbles range from 0x00-0x10). This byte
// is needed to differentiate between the leaf of the account trie and the root
// of a storage trie (which otherwise would have the same traversal path).
func (t *traverser) live(owner common.Hash, hash common.Hash, path []byte) bool {
	// Rewind the traverser until it's prefix is actually a prefix of the path
	for !bytes.HasPrefix(path, t.state.prefix) {
		t.state = t.state.parent
	}
	// Traverse downward until the prefix matches the path completely
	remain := path[len(t.state.prefix):]
	for len(remain) > 0 {
		// If we're at a hash node, expand before continuing
		if n, ok := t.state.node.(hashNode); ok {
			t.state.hash = common.BytesToHash(n)

			// Replace the node in the traverser with the expanded one
			sowner, spath := t.state.ownerAndPath()
			key := EncodeNodeKey(sowner, spath, t.state.hash)
			if enc := t.db.cleans.Get(nil, key); enc != nil {
				t.state.node = mustDecodeNode(t.state.hash[:], enc)
			} else if node := t.db.dirties[string(key)]; node != nil {
				t.state.node = node.node
			} else {
				blob, err := t.db.diskdb.Get(key)
				if blob == nil || err != nil {
					log.Error("Missing referenced node", "owner", owner, "hash", t.state.hash.Hex(),
						"remain", fmt.Sprintf("%x", remain), "path", fmt.Sprintf("%x", path))
					return false
				}
				t.state.node = mustDecodeNode(t.state.hash[:], blob)
				t.db.cleans.Set(key, blob)
			}
		}
		// If we reached an account node, extract the storage trie root to continue on
		if remain[0] == 0xff {
			// Retrieve the storage trie root and abort if empty
			if have, ok := t.state.node.(valueNode); ok {
				var account struct {
					Nonce    uint64
					Balance  *big.Int
					Root     common.Hash
					CodeHash []byte
				}
				if err := rlp.DecodeBytes(have, &account); err != nil {
					panic(err)
				}
				if account.Root == emptyRoot {
					return false
				}
				// Create a new nesting in the traversal and continue on that depth
				t.state, remain = &traverserState{
					parent: t.state,
					prefix: append(t.state.prefix, 0xff),
					node:   hashNode(account.Root[:]),
				}, remain[1:]
				continue
			}
			panic(fmt.Sprintf("liveness check path swap terminated on non value node: %T", t.state.node))
		}
		// Descend into the trie following the specified path. This code segment must
		// be able to handle both simplified raw nodes kept in this cache as well as
		// cold nodes loaded directly from disk.
		switch n := t.state.node.(type) {
		case *rawShortNode:
			if prefixLen(n.Key, remain) == len(n.Key) {
				t.state, remain = &traverserState{
					parent: t.state,
					prefix: append(t.state.prefix, remain[:len(n.Key)]...),
					node:   n.Val,
				}, remain[len(n.Key):]
				continue
			}
			return false

		case *shortNode:
			if prefixLen(n.Key, remain) == len(n.Key) {
				t.state, remain = &traverserState{
					parent: t.state,
					prefix: append(t.state.prefix, remain[:len(n.Key)]...),
					node:   n.Val,
				}, remain[len(n.Key):]
				continue
			}
			return false

		case rawFullNode:
			if child := n[remain[0]]; child != nil {
				t.state, remain = &traverserState{
					parent: t.state,
					prefix: append(t.state.prefix, remain[0]),
					node:   child,
				}, remain[1:]
				continue
			}
			return false

		case *fullNode:
			if child := n.Children[remain[0]]; child != nil {
				t.state, remain = &traverserState{
					parent: t.state,
					prefix: append(t.state.prefix, remain[0]),
					node:   child,
				}, remain[1:]
				continue
			}
			return false

		default:
			panic(fmt.Sprintf("unknown node type: %T", n))
		}
	}
	// The prefix should match perfectly here, check if the hashes matches
	if t.state.hash != (common.Hash{}) { // expanded/cached hash node
		return t.state.hash == hash
	}
	if have, ok := t.state.node.(hashNode); ok { // collapsed hash node
		t.state.hash = common.BytesToHash(have)
		return t.state.hash == hash
	}
	return false
}

// unref marks the current traversal nodes as *not* containing the specific trie
// node having been searched for. It is used by searches in subsequent tries to
// avoid reiterating the exact same sub-tries.
func (t *traverser) unref(count int, unrefs map[common.Hash]bool) {
	state := t.state
	for state != nil && count > 0 {
		// If we've found a hash node, store it as a subresult
		if state.hash != (common.Hash{}) {
			unrefs[state.hash] = true
			count--
		}
		// Traverse further up to the next hash node
		state = state.parent
	}
}
