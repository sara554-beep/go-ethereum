// Copyright 2015 The go-ethereum Authors
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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// Prove constructs a merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root node), ending
// with the node that proves the absence of the key.
func (t *Trie) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	// Collect all nodes on the path to key.
	key = keybytesToHex(key)
	var nodes []node
	tn := t.root
	for len(key) > 0 && tn != nil {
		switch n := tn.(type) {
		case *shortNode:
			if len(key) < len(n.Key) || !bytes.Equal(n.Key, key[:len(n.Key)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(n.Key):]
			}
			nodes = append(nodes, n)
		case *fullNode:
			tn = n.Children[key[0]]
			key = key[1:]
			nodes = append(nodes, n)
		case hashNode:
			var err error
			tn, err = t.resolveHash(n, nil)
			if err != nil {
				log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
				return err
			}
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	for i, n := range nodes {
		if fromLevel > 0 {
			fromLevel--
			continue
		}
		var hn node
		n, hn = hasher.proofHash(n)
		if hash, ok := hn.(hashNode); ok || i == 0 {
			// If the node's database encoding is a hash (or is the
			// root node), it becomes a proof element.
			enc, _ := rlp.EncodeToBytes(n)
			if !ok {
				hash = hasher.hashData(enc)
			}
			proofDb.Put(hash, enc)
		}
	}
	return nil
}

// Prove constructs a merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root node), ending
// with the node that proves the absence of the key.
func (t *SecureTrie) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	return t.trie.Prove(key, fromLevel, proofDb)
}

// VerifyProof checks merkle proofs. The given proof must contain the value for
// key in a trie with the given root hash. VerifyProof returns an error if the
// proof contains invalid trie nodes or the wrong value.
func VerifyProof(rootHash common.Hash, key []byte, proofDb ethdb.KeyValueReader) (value []byte, err error) {
	key = keybytesToHex(key)
	wantHash := rootHash
	for i := 0; ; i++ {
		buf, _ := proofDb.Get(wantHash[:])
		if buf == nil {
			return nil, fmt.Errorf("proof node %d (hash %064x) missing", i, wantHash)
		}
		n, err := decodeNode(wantHash[:], buf)
		if err != nil {
			return nil, fmt.Errorf("bad proof node %d: %v", i, err)
		}
		keyrest, cld := get(n, key, true)
		switch cld := cld.(type) {
		case nil:
			// The trie doesn't contain the key.
			return nil, nil
		case hashNode:
			key = keyrest
			copy(wantHash[:], cld)
		case valueNode:
			return cld, nil
		}
	}
}

// proofToPath converts a merkle proof to trie node path.
// The main purpose of this function is recovering a node
// path from the merkle proof stream. All necessary nodes
// will be resolved and leave the remaining as hashnode.
func proofToPath(rootHash common.Hash, root node, key []byte, proofDb ethdb.KeyValueReader) (node, error) {
	// resolveNode retrieves and resolves trie node from merkle proof stream
	resolveNode := func(hash common.Hash) (node, error) {
		buf, _ := proofDb.Get(hash[:])
		if buf == nil {
			return nil, fmt.Errorf("proof node (hash %064x) missing", hash)
		}
		n, err := decodeNode(hash[:], buf)
		if err != nil {
			return nil, fmt.Errorf("bad proof node %v", err)
		}
		return n, err
	}
	// If the root node is empty, resolve it first
	if root == nil {
		n, err := resolveNode(rootHash)
		if err != nil {
			return nil, err
		}
		root = n
	}
	var (
		err           error
		child, parent node
		keyrest       []byte
		terminate     bool
	)
	key, parent = keybytesToHex(key), root
	for {
		keyrest, child = get(parent, key, false)
		switch cld := child.(type) {
		case nil:
			// The trie doesn't contain the key.
			return nil, fmt.Errorf("the node is not contained in trie")
		case *shortNode:
			key, parent = keyrest, child // Already resolved
			continue
		case *fullNode:
			key, parent = keyrest, child // Already resolved
			continue
		case hashNode:
			child, err = resolveNode(common.BytesToHash(cld))
			if err != nil {
				return nil, err
			}
		case valueNode:
			terminate = true
		}
		// Link the parent and child.
		switch pnode := parent.(type) {
		case *shortNode:
			pnode.Val = child
		case *fullNode:
			pnode.Children[key[0]] = child
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", pnode, pnode))
		}
		if terminate {
			return root, nil // The whole path is resolved
		}
		key, parent = keyrest, child
	}
}

// unsetRefs removes all internal node references(hashnode). It
// should be called after a trie is constructed with two edge proofs.
//
// It's the key step for range proof. All visited nodes should be
// marked dirty since the node content might be modified. Besides
// it can happen that some fullnodes only have one child which is
// disallowed. But if the proof is valid, the missing children will
// be filled, otherwise the entire trie will be thrown anyway.
func unsetRefs(root node, remove bool, removeLeft bool) {
	switch rn := root.(type) {
	case *fullNode:
		first, last := -1, -1
		for i := 0; i < 16; i++ {
			switch rn.Children[i].(type) {
			case *fullNode, *shortNode:
				hasher := newHasher(false)
				defer returnHasherToPool(hasher)

				// Filter out small embedded node
				// todo(rjl493456442) it's quite expensive
				// to calculate the hash for each node.
				_, hashed := hasher.proofHash(rn.Children[i])
				if _, ok := hashed.(hashNode); ok {
					if first != -1 && last != -1 {
						panic("more than two extension children")
					}
					if first == -1 {
						first = i
					} else {
						last = i
					}
				}
			}
		}
		// It can happen that all children of parent node(fullnode)
		// are embedded small nodes. Skip in this case.
		//
		// todo(493456442) e.g. fullnode [0: v0, 1: v1, ..., 15: v15]
		// but in the response range, only v0-v5, v7-v15 are given,
		// in this case the re-constructued hash is still correct.
		if first == -1 && last == -1 {
			return
		}
		if first != -1 && last != -1 {
			// Find the fork point! Unset all intermediate references
			for i := first + 1; i < last; i++ {
				if _, ok := rn.Children[i].(hashNode); ok {
					rn.Children[i] = nil
				}
			}
			rn.flags = nodeFlag{dirty: true}
			unsetRefs(rn.Children[first], true, false)
			unsetRefs(rn.Children[last], true, true)
		} else if remove {
			if removeLeft {
				for i := 0; i < first; i++ {
					if _, ok := rn.Children[i].(hashNode); ok {
						rn.Children[i] = nil
					}
				}
				rn.flags = nodeFlag{dirty: true}
				unsetRefs(rn.Children[first], true, true)
			} else {
				for i := first + 1; i < 16; i++ {
					if _, ok := rn.Children[i].(hashNode); ok {
						rn.Children[i] = nil
					}
				}
				rn.flags = nodeFlag{dirty: true}
				unsetRefs(rn.Children[first], true, false)
			}
		} else {
			// Step down, the fork hasn't been found
			rn.flags = nodeFlag{dirty: true}
			unsetRefs(rn.Children[first], false, false)
		}
	case *shortNode:
		rn.flags = nodeFlag{dirty: true}
		unsetRefs(rn.Val, remove, removeLeft)
	case valueNode:
		return
	case hashNode:
		panic("it shouldn't happen")
	}
}

// VerifyRangeProof checks whether the given leave nodes and edge proofs
// can prove the given trie leaves range is matched with given root hash
// and the range is consecutive(no gap inside).
func VerifyRangeProof(rootHash common.Hash, keys [][]byte, values [][]byte, firstProof ethdb.KeyValueReader, lastProof ethdb.KeyValueReader) error {
	if len(keys) == 0 {
		return fmt.Errorf("nothing to verify")
	}
	if len(keys) != len(values) {
		return fmt.Errorf("inconsistent proof data, keys: %d, values: %d", len(keys), len(values))
	}
	if len(keys) == 1 {
		value, err := VerifyProof(rootHash, keys[0], firstProof)
		if err != nil {
			return err
		}
		if !bytes.Equal(value, values[0]) {
			return fmt.Errorf("correct proof but invalid data")
		}
		return nil
	}
	// Convert the edge proofs to edge trie paths. Then we can
	// have the same tree architecture with the original one.
	root, err := proofToPath(rootHash, nil, keys[0], firstProof)
	if err != nil {
		return err
	}
	// Pass the root node here, the second path will be merged
	// with the first one.
	root, err = proofToPath(rootHash, root, keys[len(keys)-1], lastProof)
	if err != nil {
		return err
	}
	unsetRefs(root, false, false)
	// Rebuild the trie with the leave stream, the shape of trie
	// should be same with the original one.
	newtrie, err := NewWithRoot(root, NewDatabase(memorydb.New()))
	if err != nil {
		return err
	}
	for index, key := range keys {
		newtrie.TryUpdate(key, values[index])
	}
	if newtrie.Hash() != rootHash {
		return fmt.Errorf("invalid proof, wanthash %x, got %x", rootHash, newtrie.Hash())
	}
	return nil
}

// get returns the child of the given node. Return nil if the
// node with specified key doesn't exist at all.
//
// There is an additional flag `skipResolved`. If it's set then
// all resolved nodes won't be returned.
func get(tn node, key []byte, skipResolved bool) ([]byte, node) {
	for {
		switch n := tn.(type) {
		case *shortNode:
			if len(key) < len(n.Key) || !bytes.Equal(n.Key, key[:len(n.Key)]) {
				return nil, nil
			}
			tn = n.Val
			key = key[len(n.Key):]
			if !skipResolved {
				return key, tn
			}
		case *fullNode:
			tn = n.Children[key[0]]
			key = key[1:]
			if !skipResolved {
				return key, tn
			}
		case hashNode:
			return key, n
		case nil:
			return key, nil
		case valueNode:
			return nil, n
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
}
