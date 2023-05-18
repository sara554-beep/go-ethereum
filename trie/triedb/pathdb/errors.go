// Copyright 2023 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

var (
	// errSnapshotReadOnly is returned if the database is opened in read only mode
	// and mutation is requested.
	errSnapshotReadOnly = errors.New("read only")

	// errSnapshotStale is returned from data accessors if the underlying layer
	// layer had been invalidated due to the chain progressing forward far enough
	// to not maintain the layer's original state.
	errSnapshotStale = errors.New("layer stale")

	// errUnexpectedTrieHistory is returned if an unmatched trie history is applied
	// to the database for state rollback.
	errUnexpectedTrieHistory = errors.New("unexpected trie history")

	// errStateUnrecoverable is returned if state is required to be reverted to
	// a destination without associated trie history available.
	errStateUnrecoverable = errors.New("state is unrecoverable")
)

// UnexpectedNodeError is returned if the requested node with specified path is
// not hash matched.
type UnexpectedNodeError struct {
	typ      string
	expected common.Hash
	hash     common.Hash
	owner    common.Hash
	path     []byte
}

func (err *UnexpectedNodeError) Error() string {
	return fmt.Sprintf("%s: unexpected node %x!=%x(%x %v)", err.typ, err.expected, err.hash, err.owner, err.path)
}
