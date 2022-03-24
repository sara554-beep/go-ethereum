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
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

var (
	testOwner = common.HexToHash("0x65710c2c33ddfda00132ce3ab21de97bfa01ea7a1403cfa8a8e3a9dccbb66422")
)

func TestEncodeStorageKey(t *testing.T) {
	var cases = []struct {
		owner  common.Hash
		path   []byte
		expect []byte
	}{
		// empty keys, with or without owner
		{common.HexToHash(""), []byte{}, append(bytes.Repeat([]byte{0x0}, 32), []byte{0x0}...)},
		{testOwner, []byte{}, append(testOwner.Bytes(), append(bytes.Repeat([]byte{0x0}, 32), []byte{0x0}...)...)},

		// odd length keys, with or without owner
		{common.HexToHash(""), bytes.Repeat([]byte{1}, 15),
			append(
				append(bytes.Repeat([]byte{0x11}, 7), []byte{0x10}...),
				append(bytes.Repeat([]byte{0x0}, 24), []byte{0x0f}...)...,
			),
		},
		{testOwner, bytes.Repeat([]byte{1}, 15),
			append(
				testOwner.Bytes(),
				append(
					append(bytes.Repeat([]byte{0x11}, 7), []byte{0x10}...),
					append(bytes.Repeat([]byte{0x0}, 24), []byte{0x0f}...)...,
				)...,
			),
		},

		// even length keys, with or without owner
		{common.HexToHash(""), bytes.Repeat([]byte{1}, 16),
			append(
				bytes.Repeat([]byte{0x11}, 8),
				append(bytes.Repeat([]byte{0x0}, 24), []byte{0x10}...)...,
			),
		},
		{testOwner, bytes.Repeat([]byte{1}, 16),
			append(
				testOwner.Bytes(),
				append(
					bytes.Repeat([]byte{0x11}, 8),
					append(bytes.Repeat([]byte{0x0}, 24), []byte{0x10}...)...,
				)...,
			),
		},
	}
	for _, c := range cases {
		got := encodeStorageKey(c.owner, c.path)
		if !bytes.Equal(got, c.expect) {
			t.Fatal("Encoding result mismatch", "want", c.expect, "got", got)
		}
	}
}

func TestDecodeStorageKey(t *testing.T) {
	var cases = []struct {
		owner common.Hash
		path  []byte
		input []byte
	}{
		// empty keys, with or without owner
		{common.HexToHash(""), []byte{}, append(bytes.Repeat([]byte{0x0}, 32), []byte{0x0}...)},
		{testOwner, []byte{}, append(testOwner.Bytes(), append(bytes.Repeat([]byte{0x0}, 32), []byte{0x0}...)...)},

		// odd length keys, with or without owner
		{common.HexToHash(""), bytes.Repeat([]byte{1}, 15),
			append(
				append(bytes.Repeat([]byte{0x11}, 7), []byte{0x10}...),
				append(bytes.Repeat([]byte{0x0}, 24), []byte{0x0f}...)...,
			),
		},
		{testOwner, bytes.Repeat([]byte{1}, 15),
			append(
				testOwner.Bytes(),
				append(
					append(bytes.Repeat([]byte{0x11}, 7), []byte{0x10}...),
					append(bytes.Repeat([]byte{0x0}, 24), []byte{0x0f}...)...,
				)...,
			),
		},

		// even length keys, with or without owner
		{common.HexToHash(""), bytes.Repeat([]byte{1}, 16),
			append(
				bytes.Repeat([]byte{0x11}, 8),
				append(bytes.Repeat([]byte{0x0}, 24), []byte{0x10}...)...,
			),
		},
		{testOwner, bytes.Repeat([]byte{1}, 16),
			append(
				testOwner.Bytes(),
				append(
					bytes.Repeat([]byte{0x11}, 8),
					append(bytes.Repeat([]byte{0x0}, 24), []byte{0x10}...)...,
				)...,
			),
		},
	}
	for _, c := range cases {
		owner, path := decodeStorageKey(c.input)
		if !bytes.Equal(owner.Bytes(), c.owner.Bytes()) {
			t.Fatal("Decode owner mismatch", "want", c.owner, "got", owner)
		}
		if !bytes.Equal(path, c.path) {
			t.Fatal("Decode path mismatch", "want", c.path, "got", path)
		}
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkEncodeStorageKey
// BenchmarkEncodeStorageKey-8   	17090445	        66.05 ns/op
func BenchmarkEncodeStorageKey(b *testing.B) {
	var (
		owner = randomHash()
		path  = []byte{0, 1, 2, 3, 4, 5}
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeStorageKey(owner, path)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkDecodeStorageKey
// BenchmarkDecodeStorageKey-8   	49469064	        22.92 ns/op
func BenchmarkDecodeStorageKey(b *testing.B) {
	var (
		owner   = randomHash()
		path    = []byte{0, 1, 2, 3, 4, 5}
		storage = encodeStorageKey(owner, path)
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decodeStorageKey(storage)
	}
}
