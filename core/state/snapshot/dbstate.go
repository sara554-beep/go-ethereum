// Copyright 2024 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package snapshot

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type basicIt struct {
	iter ethdb.Iterator
}

// Next steps the iterator forward one element, returning false if exhausted,
// or an error if iteration failed for some reason (e.g. root being iterated
// becomes stale and garbage collected).
func (it *basicIt) Next() bool {
	return it.iter.Next()
}

// Error returns any failure that occurred during iteration, which might have
// caused a premature iteration exit (e.g. snapshot stack becoming stale).
func (it *basicIt) Error() error {
	return it.iter.Error()
}

// Release releases associated resources. Release should always succeed and
// can be called multiple times without causing error.
func (it *basicIt) Release() {
	it.iter.Release()
}

type accountIt struct {
	*basicIt
}

// Hash returns the hash of the account or storage slot the iterator is
// currently at.
func (it *accountIt) Hash() common.Hash {
	key := it.iter.Key()
	if !bytes.HasPrefix(key, rawdb.StateAccountPrefix) {
		return common.Hash{}
	}
	if len(key) != len(rawdb.StateAccountPrefix)+common.HashLength {
		return common.Hash{}
	}
	return common.BytesToHash(key[len(rawdb.StateAccountPrefix):])
}

// Account returns the RLP encoded slim account the iterator is currently at.
// An error will be returned if the iterator becomes invalid
func (it *accountIt) Account() []byte {
	return it.iter.Value()
}

type storageIt struct {
	*basicIt
}

// Hash returns the hash of the account or storage slot the iterator is
// currently at.
func (it *storageIt) Hash() common.Hash {
	key := it.iter.Key()
	if !bytes.HasPrefix(key, rawdb.StateStoragePrefix) {
		return common.Hash{}
	}
	if len(key) != len(rawdb.StateStoragePrefix)+2*common.HashLength {
		return common.Hash{}
	}
	return common.BytesToHash(key[len(rawdb.StateStoragePrefix)+common.HashLength:])
}

// Slot returns the storage slot the iterator is currently at. An error will
// be returned if the iterator becomes invalid
func (it *storageIt) Slot() []byte {
	return it.iter.Value()
}

// VerifyState iterates the whole state(all the accounts as well as the corresponding storages)
// with the specific root and compares the re-computed hash with the original one.
func VerifyState(disk ethdb.Database, root common.Hash) error {
	acctIt := &accountIt{
		basicIt: &basicIt{rawdb.IterateAccountStates(disk)},
	}
	defer acctIt.Release()

	got, err := GenerateTrieRoot(nil, "", acctIt, common.Hash{}, stackTrieGenerate, func(db ethdb.KeyValueWriter, accountHash, codeHash common.Hash, stat *generateStats) (common.Hash, error) {
		it := &storageIt{
			basicIt: &basicIt{rawdb.IterateStorageStates(disk, accountHash)},
		}
		defer it.Release()

		hash, err := GenerateTrieRoot(nil, "", it, accountHash, stackTrieGenerate, nil, stat, false)
		if err != nil {
			return common.Hash{}, err
		}
		return hash, nil
	}, newGenerateStats(), true)

	if err != nil {
		return err
	}
	if got != root {
		return fmt.Errorf("state root hash mismatch: got %x, want %x", got, root)
	}
	return nil
}
