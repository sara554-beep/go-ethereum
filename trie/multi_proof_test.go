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
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
)


func TestEncodeDecodeInstrutions(t *testing.T) {
	var instrs Instructions
	instrs = append(instrs, Instruction{code: Branch, index: 1})
	instrs = append(instrs, Instruction{code: Add, index: 1})
	instrs = append(instrs, Instruction{code: Extension, key: []byte{0x01, 0x02}})
	instrs = append(instrs, Instruction{code: Leaf})
	instrs = append(instrs, Instruction{code: Hasher})

	blob, err := rlp.EncodeToBytes(&instrs)
	if err != nil {
		t.Fatalf("Failed to encode instructions %v", err)
	}
	var dec *Instructions
	if err := rlp.DecodeBytes(blob, &dec); err != nil {
		t.Fatalf("Failed to decode instructions %v", err)
	}
	if !reflect.DeepEqual(instrs, *dec) {
		t.Fatalf("Failed to decode instructions")
	}
}

func TestGenerateMultiProof(t *testing.T) {
	trie := newEmpty()
	updateString(trie, "k1", "a")
	updateString(trie, "k2", "b")
	updateString(trie, "k3", "c")
	proof, err := trie.MultiProve([][]byte{[]byte("k1"), []byte("k3")})
	if err != nil {
		t.Fatalf("Failed to generate multiproof %v", err)
	}
	if !VerifyMultiProof(trie.Hash(), proof) {
		t.Fatalf("Failed to verify multiproof")
	}
}
