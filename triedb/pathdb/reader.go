// Copyright 2024 The go-ethereum Authors
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

package pathdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/triedb/database"
)

const (
	locDirtyCache = "dirty"
	locCleanCache = "clean"
	locDisk       = "disk"
	locDiffLayer  = "diff"
)

// nodeLoc is a helpful structure that contains the location where the node
// is found, as it's useful for debugging purposes.
type nodeLoc struct {
	loc   string
	depth int
}

// string returns the string representation of node location.
func (loc *nodeLoc) string() string {
	return fmt.Sprintf("loc: %s, depth: %d", loc.loc, loc.depth)
}

// readOption contains the configurations for reader.
type readOption struct {
	checkHash bool
	hasher    func([]byte) common.Hash
}

// reader implements the Reader interface, providing the functionalities to
// retrieve trie nodes by wrapping the internal state layer.
type reader struct {
	layer  layer
	option *readOption
}

// Node implements trie.Reader interface, retrieving the node with specified
// node info. Don't modify the returned byte slice since it's not deep-copied
// and still be referenced by database.
func (r *reader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	blob, loc, err := r.layer.node(owner, path, 0)
	if err != nil {
		return nil, err
	}
	// Skip the hash comparison if it's disabled. Normally it will be configured
	// in the verkle context because slim format is used in verkle which doesn't
	// store the node hash at all to reduce read/write amplification.
	if !r.option.checkHash {
		return blob, nil
	}
	if got := r.option.hasher(blob); got != hash {
		switch loc.loc {
		case locCleanCache:
			cleanFalseMeter.Mark(1)
		case locDirtyCache:
			dirtyFalseMeter.Mark(1)
		case locDiffLayer:
			diffFalseMeter.Mark(1)
		case locDisk:
			diskFalseMeter.Mark(1)
		}
		return nil, fmt.Errorf("unexpected node: (%x %v), %x!=%x, %s", owner, path, hash, got, loc.string())
	}
	return blob, nil
}

// Account directly retrieves the account RLP associated with a particular
// hash in the slim data format. An error will be returned if the read
// operation exits abnormally. Specifically, if the layer is already stale.
//
// Note:
// - the returned account is not a copy, please don't modify it.
// - no error will be returned if the requested account is not found in database.
func (r *reader) Account(hash common.Hash) ([]byte, error) {
	return r.layer.account(hash, 0)
}

// Storage directly retrieves the storage data associated with a particular hash,
// within a particular account. An error will be returned if the read operation
// exits abnormally. Specifically, if the layer is already stale.
//
// Note:
// - the returned storage data is not a copy, please don't modify it.
// - no error will be returned if the requested slot is not found in database.
func (r *reader) Storage(accountHash, storageHash common.Hash) ([]byte, error) {
	return r.layer.storage(accountHash, storageHash, 0)
}

// NodeReader retrieves a layer belonging to the given state root.
func (db *Database) NodeReader(root common.Hash) (database.NodeReader, error) {
	layer := db.tree.get(root)
	if layer == nil {
		return nil, fmt.Errorf("state %#x is not available", root)
	}
	return &reader{layer: layer, option: db.readOption}, nil
}

// StateReader retrieves a layer belonging to the given state root.
func (db *Database) StateReader(root common.Hash) (database.StateReader, error) {
	layer := db.tree.get(root)
	if layer == nil {
		return nil, fmt.Errorf("state %#x is not available", root)
	}
	return &reader{layer: layer, option: db.readOption}, nil
}
