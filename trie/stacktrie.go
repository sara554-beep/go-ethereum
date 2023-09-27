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

package trie

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

var (
	stPool = sync.Pool{
		New: func() interface{} {
			return NewStackTrie(nil)
		},
	}
)

type (
	// NodeWriteFunc is used to provide all the information about a dirty node
	// for committing, allowing callers to flush nodes into the database using
	// their desired scheme.
	NodeWriteFunc = func(owner common.Hash, path []byte, hash common.Hash, blob []byte)
)

// StackTrieOptions contains the configured options for manipulating the stackTrie.
type StackTrieOptions struct {
	Owner        common.Hash   // The associated account hash serves as the trie namespace, empty for non-storage trie
	Writer       NodeWriteFunc // The function to commit the dirty nodes
	SkipBoundary bool          // Flag if boundary trie nodes are ignored for committing
}

// NewStackTrieOptions initializes an empty options for stackTrie.
func NewStackTrieOptions() *StackTrieOptions { return &StackTrieOptions{} }

// WithOwner configures trie owner within the options.
func (o *StackTrieOptions) WithOwner(owner common.Hash) *StackTrieOptions {
	o.Owner = owner
	return o
}

// WithWriter configures trie node writer within the options.
func (o *StackTrieOptions) WithWriter(writer NodeWriteFunc) *StackTrieOptions {
	o.Writer = writer
	return o
}

// WithSkipBoundary sets the skipBoundary flag.
func (o *StackTrieOptions) WithSkipBoundary() *StackTrieOptions {
	o.SkipBoundary = true
	return o
}

// commit writes the provided trie node into the underlying storage, or ignore
// the write operation if the associated writer is not configured.
func (o *StackTrieOptions) commit(path []byte, hash common.Hash, blob []byte) {
	if o.Writer == nil {
		return
	}
	o.Writer(o.Owner, path, hash, blob)
}

func stackTrieFromPool(options *StackTrieOptions) *StackTrie {
	st := stPool.Get().(*StackTrie)
	st.options = options
	return st
}

func returnToPool(st *StackTrie) {
	st.Reset()
	stPool.Put(st)
}

// StackTrie is a trie implementation that expects keys to be inserted
// in order. Once it determines that a subtree will no longer be inserted
// into, it will hash it and free up the memory it uses.
type StackTrie struct {
	nodeType     uint8             // node type (as in branch, ext, leaf)
	key          []byte            // key chunk covered by this (leaf|ext) node
	val          []byte            // value contained by this node if it's a leaf
	children     [16]*StackTrie    // list of children (for branch and exts)
	leftBoundary bool              // flag whether the node is on the left boundary of trie
	options      *StackTrieOptions // a set of options to configure stackTrie
}

// NewStackTrie allocates and initializes an empty trie.
func NewStackTrie(options *StackTrieOptions) *StackTrie {
	if options == nil {
		options = NewStackTrieOptions()
	}
	return &StackTrie{
		nodeType: emptyNode,
		options:  options,
	}
}

// NewFromBinary initialises a serialized stacktrie with the given options.
func NewFromBinary(data []byte, options *StackTrieOptions) (*StackTrie, error) {
	var st StackTrie
	if err := st.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if options == nil {
		options = NewStackTrieOptions()
	}
	st.setOptions(options)
	return &st, nil
}

// MarshalBinary implements encoding.BinaryMarshaler
func (st *StackTrie) MarshalBinary() (data []byte, err error) {
	var (
		b bytes.Buffer
		w = bufio.NewWriter(&b)
	)
	if err := gob.NewEncoder(w).Encode(struct {
		NodeType uint8
		Val      []byte
		Key      []byte
		Left     bool
	}{
		st.nodeType,
		st.val,
		st.key,
		st.leftBoundary,
	}); err != nil {
		return nil, err
	}
	for _, child := range st.children {
		if child == nil {
			w.WriteByte(0)
			continue
		}
		w.WriteByte(1)
		if childData, err := child.MarshalBinary(); err != nil {
			return nil, err
		} else {
			w.Write(childData)
		}
	}
	w.Flush()
	return b.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (st *StackTrie) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	return st.unmarshalBinary(r)
}

func (st *StackTrie) unmarshalBinary(r io.Reader) error {
	var dec struct {
		NodeType uint8
		Val      []byte
		Key      []byte
		Left     bool
	}
	if err := gob.NewDecoder(r).Decode(&dec); err != nil {
		return err
	}
	st.nodeType = dec.NodeType
	st.val = dec.Val
	st.key = dec.Key
	st.leftBoundary = dec.Left

	var hasChild = make([]byte, 1)
	for i := range st.children {
		if _, err := r.Read(hasChild); err != nil {
			return err
		} else if hasChild[0] == 0 {
			continue
		}
		var child StackTrie
		if err := child.unmarshalBinary(r); err != nil {
			return err
		}
		st.children[i] = &child
	}
	return nil
}

func (st *StackTrie) setOptions(options *StackTrieOptions) {
	st.options = options
	for _, child := range st.children {
		if child != nil {
			child.setOptions(options)
		}
	}
}

func newLeaf(key, val []byte, leftBoundary bool, options *StackTrieOptions) *StackTrie {
	st := stackTrieFromPool(options)
	st.nodeType = leafNode
	st.key = append(st.key, key...)
	st.val = val
	st.leftBoundary = leftBoundary
	return st
}

func newExt(key []byte, child *StackTrie, leftBoundary bool, options *StackTrieOptions) *StackTrie {
	st := stackTrieFromPool(options)
	st.nodeType = extNode
	st.key = append(st.key, key...)
	st.children[0] = child
	st.leftBoundary = leftBoundary
	return st
}

// List all values that StackTrie#nodeType can hold
const (
	emptyNode = iota
	branchNode
	extNode
	leafNode
	hashedNode
)

// Update inserts a (key, value) pair into the stack trie.
func (st *StackTrie) Update(key, value []byte) error {
	k := keybytesToHex(key)
	k = k[:len(k)-1] // chop the terminal flag
	if len(value) == 0 {
		panic("deletion not supported")
	}
	st.insert(k, value, nil, st.nodeType == emptyNode)
	return nil
}

// MustUpdate is a wrapper of Update and will omit any encountered error but
// just print out an error message.
func (st *StackTrie) MustUpdate(key, value []byte) {
	if err := st.Update(key, value); err != nil {
		log.Error("Unhandled trie error in StackTrie.Update", "err", err)
	}
}

// Reset clears the cached fields of stackTrie node, but options is still left
// serving as a global config.
func (st *StackTrie) Reset() {
	st.nodeType = emptyNode
	st.key = st.key[:0]
	st.val = nil
	for i := range st.children {
		st.children[i] = nil
	}
	st.leftBoundary = false
}

// Helper function that, given a full key, determines the index
// at which the chunk pointed by st.keyOffset is different from
// the same chunk in the full key.
func (st *StackTrie) getDiffIndex(key []byte) int {
	for idx, nibble := range st.key {
		if nibble != key[idx] {
			return idx
		}
	}
	return len(st.key)
}

// linkChildren links two children within the stackTrie node based on the provided
// shared key prefix, child indexes, and children.
func (st *StackTrie) linkChildren(diffIndex int, sharedKey []byte, oldIndex byte, oldChild *StackTrie, newIndex byte, newChild *StackTrie) {
	// Reset the current stackTrie as the first step.
	st.Reset()

	// Check if the split occurs at the first nibble of the chunk. In that
	// case, no prefix external node is necessary; otherwise, create that.
	if diffIndex == 0 {
		st.nodeType = branchNode
		st.children[oldIndex] = oldChild
		st.children[newIndex] = newChild
		st.leftBoundary = oldChild.leftBoundary // mark parent as boundary if child is
		return
	}
	// Construct an internal branch node as the parent of two leaves.
	branch := NewStackTrie(st.options)
	branch.nodeType = branchNode
	branch.children[oldIndex] = oldChild
	branch.children[newIndex] = newChild
	branch.leftBoundary = oldChild.leftBoundary // mark parent as boundary if child is

	// Convert current node into an extension, and link the branch node as child.
	st.nodeType = extNode
	st.key = sharedKey
	st.children[0] = branch
	st.leftBoundary = branch.leftBoundary // mark parent as boundary if child is
}

// Helper function to that inserts a (key, value) pair into
// the trie.
func (st *StackTrie) insert(key, value []byte, prefix []byte, leftBoundary bool) {
	switch st.nodeType {
	case branchNode: /* Branch */
		index := int(key[0])

		// Un-resolve elder siblings
		for i := index - 1; i >= 0; i-- {
			if st.children[i] != nil {
				if st.children[i].nodeType != hashedNode {
					st.children[i].hash(append(prefix, byte(i)))
				}
				break
			}
		}
		// New sibling must be non-left boundary node
		if st.children[index] == nil {
			st.children[index] = newLeaf(key[1:], value, false, st.options)
		} else {
			st.children[index].insert(key[1:], value, append(prefix, key[0]), false)
		}

	case extNode: /* Ext */
		// Compare both key chunks and see where they differ
		diffIndex := st.getDiffIndex(key)

		// Check if chunks are identical. If so, recurse into the child node.
		// Otherwise, the key has to be split into 1) an optional common prefix,
		// 2) the full node representing the two differing path, and 3) a leaf
		// for each of the differentiated subtrees.
		if diffIndex == len(st.key) {
			// Ext key and key segment are identical, recurse into the child
			// node. The new node is surely not on the left boundary because
			// of existent nodes.
			st.children[0].insert(key[diffIndex:], value, append(prefix, key[:diffIndex]...), false)
			return
		}
		// Hash the original child to free up memory. Depending on whether the
		// break is at the extension's last byte or not, create an intermediate
		// extension or use the extension's child node directly.
		var (
			oldChild  *StackTrie
			sharedKey = st.key[:diffIndex]
			oldIndex  = st.key[diffIndex]
		)
		if diffIndex < len(st.key)-1 {
			// Break on the non-last byte, insert an intermediate extension. The
			// path prefix of the newly-inserted extension should also contain the
			// different byte and should be marked as left-boundary if child is.
			child := st.children[0]
			oldChild = newExt(st.key[diffIndex+1:], child, child.leftBoundary, st.options)
			oldChild.hash(append(prefix, st.key[:diffIndex+1]...))
		} else {
			// Break on the last byte, no need to insert an extension node: reuse
			// the current node. The path prefix of the original part should still
			// be same.
			oldChild = st.children[0]
			oldChild.hash(append(prefix, st.key...))
		}
		// Create the new leaf node for newly inserted value
		var (
			newIndex = key[diffIndex]
			newKey   = key[diffIndex+1:]
		)
		newChild := newLeaf(newKey, value, false, st.options) // new sibling must be non-left boundary node

		// Link two children into the existent stackTrie.
		st.linkChildren(diffIndex, sharedKey, oldIndex, oldChild, newIndex, newChild)

	case leafNode: /* Leaf */
		// Compare both key chunks and see where they differ
		diffIndex := st.getDiffIndex(key)

		// Overwriting a key isn't supported, which means that the current leaf
		// is expected to be split into 1) an optional extension for the common
		// prefix of these 2 keys, 2) a fullnode selecting the path on which the
		// keys differ, and 3) one leaf for the differentiated component of each key.
		if diffIndex >= len(st.key) {
			panic("Trying to insert into existing key")
		}
		// Hash the original leaf node to free up memory
		var (
			sharedKey = st.key[:diffIndex]
			oldIndex  = st.key[diffIndex]
			oldKey    = st.key[diffIndex+1:]
			oldVal    = st.val
			oldLeft   = st.leftBoundary
			oldPath   = append(prefix, st.key[:diffIndex+1]...) // include the pos byte in branch
		)
		oldChild := newLeaf(oldKey, oldVal, oldLeft, st.options)
		oldChild.hash(oldPath)

		// Create the new leaf node for newly inserted value
		var (
			newIndex = key[diffIndex]
			newKey   = key[diffIndex+1:]
		)
		newChild := newLeaf(newKey, value, false, st.options) // new sibling must be non-left boundary node

		// Link two children into the existent stackTrie.
		st.linkChildren(diffIndex, sharedKey, oldIndex, oldChild, newIndex, newChild)

	case emptyNode: /* Empty */
		st.nodeType = leafNode
		st.key = key
		st.val = value
		st.leftBoundary = leftBoundary // set boundary flag for first inserted node

	case hashedNode:
		panic("trying to insert into hash")

	default:
		panic("invalid type")
	}
}

// hash converts st into a 'hashedNode', if possible. Possible outcomes:
//
// 1. The rlp-encoded value was >= 32 bytes:
//   - Then the 32-byte `hash` will be accessible in `st.val`.
//   - And the 'st.type' will be 'hashedNode'
//
// 2. The rlp-encoded value was < 32 bytes
//   - Then the <32 byte rlp-encoded value will be accessible in 'st.val'.
//   - And the 'st.type' will be 'hashedNode' AGAIN
//
// This method also sets 'st.type' to hashedNode, and clears 'st.key'.
func (st *StackTrie) hash(path []byte) {
	h := newHasher(false)
	defer returnHasherToPool(h)

	st.hashRec(h, path, false)
}

func (st *StackTrie) hashRec(hasher *hasher, path []byte, rightBoundary bool) {
	// The switch below sets this to the RLP-encoding of this node.
	var encoded []byte

	switch st.nodeType {
	case hashedNode:
		return

	case emptyNode:
		st.val = types.EmptyRootHash.Bytes()
		st.key = st.key[:0]
		st.nodeType = hashedNode
		return

	case branchNode:
		var nodes fullNode
		for i, child := range st.children {
			if child == nil {
				nodes.Children[i] = nilValueNode
				continue
			}
			// Recursively hashing child, all the children except the last one
			// should already be hashed and freed up, thus the last child is
			// regarded as on right boundary if parent is.
			child.hashRec(hasher, append(path, byte(i)), rightBoundary)

			if len(child.val) < 32 {
				nodes.Children[i] = rawNode(child.val)
			} else {
				nodes.Children[i] = hashNode(child.val)
			}
			// Release child back to pool.
			st.children[i] = nil
			returnToPool(child)
		}
		nodes.encode(hasher.encbuf)
		encoded = hasher.encodedBytes()

	case extNode:
		// Recursively hashing child, the child is regarded as on the right
		// boundary if the parent is.
		st.children[0].hashRec(hasher, append(path, st.key...), rightBoundary)

		n := shortNode{Key: hexToCompactInPlace(st.key)}
		if len(st.children[0].val) < 32 {
			n.Val = rawNode(st.children[0].val)
		} else {
			n.Val = hashNode(st.children[0].val)
		}
		n.encode(hasher.encbuf)
		encoded = hasher.encodedBytes()

		// Release child back to pool.
		returnToPool(st.children[0])
		st.children[0] = nil

	case leafNode:
		st.key = append(st.key, byte(16))
		n := shortNode{Key: hexToCompactInPlace(st.key), Val: valueNode(st.val)}

		n.encode(hasher.encbuf)
		encoded = hasher.encodedBytes()

	default:
		panic("invalid node type")
	}

	st.nodeType = hashedNode
	st.key = st.key[:0]
	if len(encoded) < 32 {
		st.val = common.CopyBytes(encoded)
		return
	}
	// Write the hash to the 'val'. We allocate a new val here to not mutate
	// input values
	st.val = hasher.hashData(encoded)

	// Skip boundary nodes if they are requested to not commit
	if (st.leftBoundary || rightBoundary) && st.options.SkipBoundary {
		return
	}
	st.options.commit(path, common.BytesToHash(st.val), encoded)
}

// Hash returns the hash of the current node.
func (st *StackTrie) Hash() (h common.Hash) {
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	st.hashRec(hasher, nil, true)
	if len(st.val) == 32 {
		copy(h[:], st.val)
		return h
	}
	// If the node's RLP isn't 32 bytes long, the node will not be hashed,
	// and instead contain the rlp-encoding of the node. For the top level
	// node, we need to force the hashing.
	hasher.sha.Reset()
	hasher.sha.Write(st.val)
	hasher.sha.Read(h[:])
	return h
}

// Commit will firstly hash the entire trie if it's still not hashed
// and then commit all nodes to the associated database. Actually most
// of the trie nodes MAY have been committed already. The main purpose
// here is to commit the root node.
//
// The associated database is expected, otherwise the whole commit
// functionality should be disabled.
func (st *StackTrie) Commit() (h common.Hash, err error) {
	if st.options.Writer == nil {
		return common.Hash{}, errors.New("no database for committing")
	}
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	st.hashRec(hasher, nil, true)
	if len(st.val) == 32 {
		copy(h[:], st.val)
		return h, nil
	}
	// If the node's RLP isn't 32 bytes long, the node will not
	// be hashed (and committed), and instead contain the rlp-encoding of the
	// node. For the top level node, we need to force the hashing+commit.
	hasher.sha.Reset()
	hasher.sha.Write(st.val)
	hasher.sha.Read(h[:])

	st.options.commit(nil, h, st.val)
	return h, nil
}
