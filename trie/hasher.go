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

package trie

import (
	"bytes"
	"hash"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/rlp"
)

// calculator is a utility used by the hasher to calculate the hash value of the tree node.
type calculator struct {
	sha    hash.Hash
	buffer *bytes.Buffer
}

func (c *calculator) reset() {
	c.sha.Reset()
	c.buffer.Reset()
}

// calculatorPool is a set of temporary calculators that may be individually saved and retrieved.
var calculatorPool = sync.Pool{
	New: func() interface{} {
		return &calculator{buffer: new(bytes.Buffer), sha: sha3.NewKeccak256()}
	},
}

// hasher hasher is used to calculate the hash value of the whole tree.
type hasher struct {
	cachegen   uint16
	cachelimit uint16
	threaded   bool
	onleaf     LeafCallback
}

func newHasher(cachegen, cachelimit uint16, onleaf LeafCallback) *hasher {
	h := &hasher{
		cachegen:   cachegen,
		cachelimit: cachelimit,
		onleaf:     onleaf,
	}
	return h
}

// newCalculator retrieves a cleaned calculator from calculator pool.
func (h *hasher) newCalculator() *calculator {
	calculator := calculatorPool.Get().(*calculator)
	calculator.reset()
	return calculator
}

// returnCalculator returns a no longer used calculator to the pool.
func (h *hasher) returnCalculator(calculator *calculator) {
	calculatorPool.Put(calculator)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hash(n node, calculator *calculator, db *Database, force bool) (node, node, error) {
	// If we're not storing the node, just hashing, use available cached data
	if hash, dirty := n.cache(); hash != nil {
		if db == nil {
			return hash, n, nil
		}
		if n.canUnload(h.cachegen, h.cachelimit) {
			// Unload the node from cache. All of its subnodes will have a lower or equal
			// cache generation number.
			cacheUnloadCounter.Inc(1)
			return hash, hash, nil
		}
		if !dirty {
			return hash, n, nil
		}
	}
	// Trie not processed yet or needs storage, walk the children
	collapsed, cached, err := h.hashChildren(n, calculator, db)
	if err != nil {
		return hashNode{}, n, err
	}
	hashed, err := h.store(collapsed, calculator, db, force)
	if err != nil {
		return hashNode{}, n, err
	}
	// Cache the hash of the node for later reuse and remove
	// the dirty flag in commit mode. It's fine to assign these values directly
	// without copying the node first because hashChildren copies it.
	cachedHash, _ := hashed.(hashNode)
	switch cn := cached.(type) {
	case *shortNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	case *fullNode:
		cn.flags.hash = cachedHash
		if db != nil {
			cn.flags.dirty = false
		}
	}
	return hashed, cached, nil
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
func (h *hasher) hashChildren(original node, calculator *calculator, db *Database) (node, node, error) {
	var err error

	switch n := original.(type) {
	case *shortNode:
		// Hash the short node's child, caching the newly hashed subtree
		collapsed, cached := n.copy(), n.copy()
		collapsed.Key = hexToCompact(n.Key)
		cached.Key = common.CopyBytes(n.Key)

		if _, ok := n.Val.(valueNode); !ok {
			collapsed.Val, cached.Val, err = h.hash(n.Val, calculator, db, false)
			if err != nil {
				return original, original, err
			}
		}
		if collapsed.Val == nil {
			collapsed.Val = valueNode(nil) // Ensure that nil children are encoded as empty strings.
		}
		return collapsed, cached, nil

	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		collapsed, cached := n.copy(), n.copy()

		// hashChild is a helper to hash a single child, which is called either on the
		// same thread as the caller or in a goroutine for the toplevel branching.
		hashChild := func(index int, wg *sync.WaitGroup, alloc bool) {
			if wg != nil {
				defer wg.Done()
			}
			// Ensure that nil children are encoded as empty strings.
			if collapsed.Children[index] == nil {
				collapsed.Children[index] = valueNode(nil)
				return
			}
			// Allocate a new calculator only when the goroutine is threaded.
			calculator := calculator
			if alloc {
				calculator = h.newCalculator()
				defer h.returnCalculator(calculator)
			}
			// Hash all other children properly
			var herr error
			collapsed.Children[index], cached.Children[index], herr = h.hash(n.Children[index], calculator, db, false)
			if herr != nil {
				err = herr
			}
		}
		// If we're not running in threaded mode yet, span a goroutine for each child
		if !h.threaded {
			// Disable further threading
			h.threaded = true

			// Hash all the children concurrently
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go hashChild(i, &wg, true)
			}
			wg.Wait()

			// Reenable threading for subsequent hash calls
			h.threaded = false
		} else {
			for i := 0; i < 16; i++ {
				hashChild(i, nil, false)
			}
		}
		if err != nil {
			return original, original, err
		}
		cached.Children[16] = n.Children[16]
		if collapsed.Children[16] == nil {
			collapsed.Children[16] = valueNode(nil)
		}
		return collapsed, cached, nil
	default:
		// Value and hash nodes don't have children so they're left as were
		return n, original, nil
	}
}

// store hashes the node n and if we have a storage layer specified, it writes
// the key/value pair to it and tracks any node->child references as well as any
// node->external trie references.
func (h *hasher) store(n node, calculator *calculator, db *Database, force bool) (node, error) {
	// Don't store hashes or empty nodes.
	if _, isHash := n.(hashNode); n == nil || isHash {
		return n, nil
	}
	calculator.reset()
	if err := rlp.Encode(calculator.buffer, n); err != nil {
		panic("encode error: " + err.Error())
	}
	if calculator.buffer.Len() < 32 && !force {
		return n, nil // Nodes smaller than 32 bytes are stored inside their parent
	}
	// Larger nodes are replaced by their hash and stored in the database.
	hash, _ := n.cache()
	if hash == nil {
		calculator.sha.Write(calculator.buffer.Bytes())
		hash = hashNode(calculator.sha.Sum(nil))
	}
	if db != nil {
		// We are pooling the trie nodes into an intermediate memory cache
		db.lock.Lock()

		hash := common.BytesToHash(hash)
		db.insert(hash, calculator.buffer.Bytes())

		// Track all direct parent->child node references
		switch n := n.(type) {
		case *shortNode:
			if child, ok := n.Val.(hashNode); ok {
				db.reference(common.BytesToHash(child), hash)
			}
		case *fullNode:
			for i := 0; i < 16; i++ {
				if child, ok := n.Children[i].(hashNode); ok {
					db.reference(common.BytesToHash(child), hash)
				}
			}
		}
		db.lock.Unlock()

		// Track external references from account->storage trie
		if h.onleaf != nil {
			switch n := n.(type) {
			case *shortNode:
				if child, ok := n.Val.(valueNode); ok && child != nil {
					h.onleaf(child, hash)
				}
			case *fullNode:
				for i := 0; i < 16; i++ {
					if child, ok := n.Children[i].(valueNode); ok && child != nil {
						h.onleaf(child, hash)
					}
				}
			}
		}
	}
	return hash, nil
}
