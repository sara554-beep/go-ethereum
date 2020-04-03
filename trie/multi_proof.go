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
	"errors"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// opcode is the unique indicator for each instruction
type opcode byte

const (
	Branch opcode = iota
	Hasher
	Leaf
	Extension
	Add
)

func (code opcode) String() string {
	switch code {
	case Branch:
		return "Branch"
	case Hasher:
		return "Hasher"
	case Leaf:
		return "Leaf"
	case Extension:
		return "Extension"
	case Add:
		return "Add"
	default:
		return "Undefined"
	}
}

// Instruction represents a step for trie re-construction. Now there are 5
// instructions defined.
// - Branch <index>
//   pop a node from node stack, create a fullnode and set the node as the
//   index-th child, push the fullnode to the node stack
// - Hasher
//   pop a hash from the hash tape and push to the node stack(hash node)
// - Leaf:
//   pop a leaf from the leaves tape and push to the node stack(short node)
// - Extension <key>:
//   pop a node n from stack, create a short node with the n as value and
//   key from instruction as the node key, push the short node back
// - Add <index>:
//   pop two nodes from the stack, former one is child and latter one is parent.
//   Set the child node as the index-th child of the parent and push the parent back
type Instruction struct {
	code  opcode
	index uint8
	key   []byte
}

type Instructions []Instruction

func (instrs *Instructions) String() string {
	var str string
	str += "######## Instructions ########\n"
	for _, instr := range *instrs {
		str += fmt.Sprintf("Opcode: %v", instr.code)
		switch instr.code {
		case Branch, Add:
			str += fmt.Sprintf(" index: %v", instr.index)
		case Extension:
			str += fmt.Sprintf(" key: %v", instr.key)
		}
		str += "\n"
	}
	str += "######## End ########\n"
	return str
}

func (instrs *Instructions) EncodeRLP(w io.Writer) error {
	var enc []interface{}
	for _, instr := range *instrs {
		enc = append(enc, instr.code)
		switch instr.code {
		case Branch, Add:
			enc = append(enc, instr.index)
		case Extension:
			enc = append(enc, instr.key)
		}
	}
	return rlp.Encode(w, enc)
}

func (instrs *Instructions) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	for {
		var instr Instruction
		typ, err := s.Uint()
		if err != nil {
			if err == rlp.EOL {
				break
			}
			return err
		}
		instr.code = opcode(typ)
		switch instr.code {
		case Branch, Add:
			index, err := s.Uint()
			if err != nil {
				return err
			}
			instr.index = uint8(index)
		case Extension:
			keys, err := s.Bytes()
			if err != nil {
				return err
			}
			instr.key = common.CopyBytes(keys)
		}
		*instrs = append(*instrs, instr)
	}
	if err := s.ListEnd(); err != nil {
		return err
	}
	return nil
}

type MultiProof struct {
	// Instructions is a list of instructions to tell the proof requestor how
	// to re-construct trie with given pieces of nodes. Instructions can also
	// be understood as the structural information.
	Instructions Instructions
	Hashes       []common.Hash // The list of node hashes used to re-construct the trie.
	Leaves       [][]byte      // The list of leaf nodes used to re-construct the trie.
	iindex       int           // Iterator position for instruction
	hindex       int           // Iterator position for hash tape
	lindex       int           // Iterator position for leaf tape
}

// nextInstr returns the next instruction for multi-proof verification
func (mp *MultiProof) nextInstr() *Instruction {
	if len(mp.Instructions) <= mp.iindex {
		return nil
	}
	instr := mp.Instructions[mp.iindex]
	mp.iindex += 1
	return &instr
}

// nextHash returns the next hash from hash tape for multi-proof verification
func (mp *MultiProof) nextHash() common.Hash {
	if len(mp.Hashes) <= mp.hindex {
		return common.Hash{}
	}
	hash := mp.Hashes[mp.hindex]
	mp.hindex += 1
	return hash
}

// nextLeaf returns the next leaf node from leaf tape for multi-proof verification
func (mp *MultiProof) nextLeaf() *leafnode {
	if len(mp.Leaves) <= mp.lindex {
		return nil
	}
	blob := mp.Leaves[mp.lindex]

	var ln leafnode
	if err := rlp.DecodeBytes(blob, &ln); err != nil {
		return nil
	}
	mp.lindex += 1
	return &ln
}

// MultiProve generates the multi-proof for the given keyset against the
// current trie.
func (t *Trie) MultiProve(keys [][]byte) (*MultiProof, error) {
	// Short circuit if nothing to be proved
	if len(keys) == 0 {
		return nil, errors.New("nothing to be proven")
	}
	// Convert the raw format keys to hex format.
	var hexKeys = make([][]byte, len(keys))
	for _, key := range keys {
		hexKeys = append(hexKeys, keybytesToHex(key))
	}
	instructions, hashes, leaves, err := t.multiProve(t.root, hexKeys, true)
	if err != nil {
		return nil, err
	}
	return &MultiProof{
		Instructions: instructions,
		Hashes:       hashes,
		Leaves:       leaves,
	}, nil
}

type leafnode struct {
	Key []byte
	Val []byte
}

// nodehash calculates the hash of the given node
func nodehash(n node, force bool) common.Hash {
	if hn, _ := n.cache(); hn != nil {
		return common.BytesToHash(hn)
	}
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	n, hn := hasher.proofHash(n)
	if hashnode, ok := hn.(hashNode); ok {
		return common.BytesToHash(hashnode)
	} else if force {
		enc, _ := rlp.EncodeToBytes(n)
		return common.BytesToHash(hasher.hashData(enc))
	}
	return common.Hash{} // the node is too small, no corresponding hash
}

func (t *Trie) multiProve(root node, hexkeys [][]byte, topmost bool) (instructions []Instruction, hashes []common.Hash, leaves [][]byte, err error) {
	switch rn := root.(type) {
	case *fullNode:
		// The whole keyset should overlap with the fullnode
		var dispatch = make([][][]byte, 16)
		for _, hk := range hexkeys {
			if len(hk) <= 1 {
				panic("invalid key") // Should never happen
			}
			dispatch[hk[0]] = append(dispatch[hk[0]], hk[1:])
		}
		branch := true
		for index, subkeys := range dispatch {
			if len(subkeys) == 0 {
				if rn.Children[index] != nil {
					hash := nodehash(rn.Children[index], false)
					if hash == (common.Hash{}) {
						// It's an embedded node in the fullnode
						//
						// todo(rjl493456442) it can be fullnode actually, although
						// it's impossible in ethereum.
						child := rn.Children[index].(*shortNode)
						leaf := leafnode{
							Key: child.Key,
							Val: child.Val.(valueNode),
						}
						blob, err := rlp.EncodeToBytes(leaf)
						if err != nil {
							return nil, nil, nil, err
						}
						leaves = append(leaves, blob)
						instructions = append(instructions, Instruction{code: Leaf})
					} else {
						hashes = append(hashes, hash)
						instructions = append(instructions, Instruction{code: Hasher})
					}
					if branch {
						branch = false
						instructions = append(instructions, Instruction{code: Branch, index: uint8(index)})
					} else {
						instructions = append(instructions, Instruction{code: Add, index: uint8(index)})
					}
				}
			} else {
				// Overlap with the existent path, prove it recursively.
				is, hs, ls, err := t.multiProve(rn.Children[index], subkeys, false)
				if err != nil {
					return nil, nil, nil, err
				}
				instructions = append(instructions, is...)
				hashes = append(hashes, hs...)
				leaves = append(leaves, ls...)
				if branch {
					branch = false
					instructions = append(instructions, Instruction{code: Branch, index: uint8(index)})
				} else {
					instructions = append(instructions, Instruction{code: Add, index: uint8(index)})
				}
			}
		}
	case *shortNode:
		if hasTerm(rn.Key) {
			if len(hexkeys) != 1 {
				return nil, nil, nil, errors.New("invalid keyset")
			}
			blob, err := rlp.EncodeToBytes(rn)
			if err != nil {
				return nil, nil, nil, err
			}
			leaves = append(leaves, blob) // key,value
			instructions = append(instructions, Instruction{code: Leaf})
		} else {
			var truncated [][]byte
			for _, key := range hexkeys {
				prefix := prefixLen(key, rn.Key)
				if prefix == len(rn.Key) {
					truncated = append(truncated, key[prefix:])
				}
			}
			if len(truncated) == 0 {
				if topmost {
					return nil, nil, nil, nil // Nothing to prove
				}
				hash := nodehash(rn, false)
				if hash == (common.Hash{}) {
					// Special case, it's an embedded node, push the whole
					// short node into the leaf tape.
					leaf := leafnode{
						Key: rn.Key,
						Val: rn.Val.(valueNode),
					}
					blob, err := rlp.EncodeToBytes(leaf)
					if err != nil {
						return nil, nil, nil, err
					}
					leaves = append(leaves, blob)
					instructions = append(instructions, Instruction{code: Leaf})
				} else {
					hashes = append(hashes, hash)
					instructions = append(instructions, Instruction{code: Hasher})
					instructions = append(instructions, Instruction{code: Extension, key: rn.Key})
				}
			} else {
				is, hs, ls, err := t.multiProve(rn.Val, truncated, false)
				if err != nil {
					return nil, nil, nil, err
				}
				instructions = append(instructions, is...)
				hashes = append(hashes, hs...)
				leaves = append(leaves, ls...)
				instructions = append(instructions, Instruction{code: Extension, key: rn.Key})
			}
		}
	case hashNode:
		n, err := t.resolveHash(rn, nil)
		if err != nil {
			return nil, nil, nil, err
		}
		is, hs, ls, err := t.multiProve(n, hexkeys, false)
		if err != nil {
			return nil, nil, nil, err
		}
		instructions = append(instructions, is...)
		hashes = append(hashes, hs...)
		leaves = append(leaves, ls...)

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", rn, rn))
	}
	return
}

type nodeStack struct {
	nodes []node
}

func (ns *nodeStack) push(n node) {
	ns.nodes = append(ns.nodes, n)
}

func (ns *nodeStack) pop() node {
	if len(ns.nodes) == 0 {
		return nil
	}
	n := ns.nodes[len(ns.nodes)-1]
	ns.nodes = ns.nodes[:len(ns.nodes)-1]
	return n
}

func (ns *nodeStack) peek() node {
	if len(ns.nodes) == 0 {
		return nil
	}
	return ns.nodes[len(ns.nodes)-1]
}

func (ns *nodeStack) len() int {
	return len(ns.nodes)
}

func VerifyMultiProof(root common.Hash, proof *MultiProof) bool {
	var ns = &nodeStack{}
	for instr := proof.nextInstr(); instr != nil; instr = proof.nextInstr() {
		switch instr.code {
		case Branch:
			child := ns.pop()
			parent := &fullNode{}
			parent.Children[instr.index] = child
			ns.push(parent)
		case Hasher:
			hash := proof.nextHash()
			if hash == (common.Hash{}) {
				panic("empty hash")
			}
			ns.push(hashNode(hash.Bytes()))
		case Leaf:
			leaf := proof.nextLeaf()
			if leaf == nil {
				panic("empty leaf")
			}
			ns.push(&shortNode{
				Key:   leaf.Key,
				Val:   valueNode(leaf.Val),
			})
		case Extension:
			ns.push(&shortNode{Key: instr.key, Val: ns.pop()})
		case Add:
			child, parent := ns.pop(), ns.pop()
			fn, ok := parent.(*fullNode)
			if !ok {
				panic("fullnode required")
			}
			fn.Children[instr.index] = child
			ns.push(fn)
		}
	}
	rn := ns.pop()
	hash := nodehash(rn, true)
	return hash == root
}
