// Copyright 2018 The go-ethereum Authors
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

// Package database defines the interfaces for an Ethereum data store.
package ethdb

import "io"

// Reader wraps the Has and Get method of a backing data store.
type Reader interface {
	// Has retrieves if a key is present in the key-value data store.
	Has(key []byte) (bool, error)

	// Get retrieves the given key if it's present in the key-value data store.
	Get(key []byte) ([]byte, error)
}

// Writer wraps the Put method of a backing data store.
type Writer interface {
	// Put inserts the given value into the key-value data store.
	Put(key []byte, value []byte) error
}

// Deleter wraps the Delete method of a backing data store.
type Deleter interface {
	// Delete removes the key from the key-value data store.
	Delete(key []byte) error
}

// Stater wraps the Stat method of a backing data store.
type Stater interface {
	// Stat returns a particular internal stat of the database.
	Stat(property string) (string, error)
}

// Compacter wraps the Compact method of a backing data store.
type Compacter interface {
	// Compact flattens the underlying data store for the given key range. In essence,
	// deleted and overwritten versions are discarded, and the data is rearranged to
	// reduce the cost of operations needed to access them.
	//
	// A nil start is treated as a key before all keys in the data store; a nil limit
	// is treated as a key after all keys in the data store. If both is nil then it
	// will compact entire data store.
	Compact(start []byte, limit []byte) error
}

// KeyValueStore contains all the methods required to allow handling different
// key-value data stores backing the high level database.
type KeyValueStore interface {
	Reader
	Writer
	Deleter
	Batcher
	Iteratee
	Stater
	Compacter
	io.Closer
}

// AncientReader contains the methods required to read from immutable ancient data.
type AncientReader interface {
	// Ancient retrieves an ancient binary blob from the append-only immutable files.
	Ancient(kind string, number uint64) ([]byte, error)
}

// AncientWriter contains the methods required to write to immutable ancient data.
type AncientWriter interface {
	// Append injects a binary blob at the end of the append-only immutable table files.
	Append(kind string, number uint64, blob []byte) error
}

type AncientDeleter interface {
	Truncate(items uint64) error
}

type AncientLengther interface {
	Items() (uint64, error)
}

// DatabaseReader contains the methods required to read data from both key-value as well as
// immutable ancient data.
type DatabaseReader interface {
	Reader
	AncientReader
}

// DatabaseWriter contains the methods required to write data to both key-value as well as
// immutable ancient data.
type DatabaseWriter interface {
	Writer
	AncientWriter
}

type DatabaseDeleter interface {
	Deleter
	AncientDeleter
}

// KeyValueStore contains all the methods required to allow handling different
// ancient data stores backing immutable chain data store
type AncientStore interface {
	AncientReader
	AncientWriter
	AncientDeleter
	AncientLengther
	io.Closer
}

// Database contains all the methods required by the high level database to not
// only access the key-value data store but also the chain freezer.
type Database interface {
	DatabaseReader
	DatabaseWriter
	DatabaseDeleter
	AncientLengther
	Batcher
	Iteratee
	Stater
	Compacter
	io.Closer
}
