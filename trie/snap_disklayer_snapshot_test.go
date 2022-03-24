// Copyright 2022 The go-ethereum Authors
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
	"github.com/ethereum/go-ethereum/crypto"
)

func TestGetSnapshot(t *testing.T) {
	defer func(origin int) {
		defaultCacheSize = origin
	}(defaultCacheSize)
	defaultCacheSize = 1024 * 256 // Lower the dirty cache size

	var (
		index int
		env   = fillDB(t, PathScheme)
		db    = env.db.backend.(*snapDatabase)
		dl    = db.tree.bottom().(*diskLayer)
	)
	for index = 0; index < len(env.roots); index++ {
		if env.roots[index] == dl.root {
			break
		}
	}
	for i := 0; i < index; i++ {
		layer, err := dl.GetSnapshot(env.roots[i], db.freezer)
		if err != nil {
			t.Fatalf("Failed to retrieve snapshot %v", err)
		}
		defer layer.Release()

		paths, blobs := env.paths[i], env.blobs[i]
		for j, path := range paths {
			if len(blobs[j]) == 0 {
				// deleted node, expect error
				blob, _ := layer.NodeBlob(common.Hash{}, path, crypto.Keccak256Hash(blobs[j])) // error can occur
				if len(blob) != 0 {
					t.Error("Unexpected state", "path", path, "got", blob)
				}
			} else {
				// normal node, expect correct value
				blob, err := layer.NodeBlob(common.Hash{}, path, crypto.Keccak256Hash(blobs[j]))
				if err != nil {
					t.Error("Failed to retrieve state", "err", err)
				}
				if !bytes.Equal(blob, blobs[j]) {
					t.Error("Unexpected state", "path", path, "want", blobs[j], "got", blob)
				}
			}
		}
	}
}
