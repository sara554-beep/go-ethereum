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

package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// multiReader is the aggregation of a list of Reader interface, providing state
// access by leveraging all readers. The checking priority is determined by the
// position in the reader list.
type multiReader struct {
	readers []Reader // List of readers, sorted by checking priority
}

// newMultiReader constructs a multiReader instance with the given readers. The
// priority among readers is assumed to be sorted. Note, it must contains at least
// one reader for constructing a multiReader.
func newMultiReader(readers ...Reader) (*multiReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("empty reader set")
	}
	return &multiReader{
		readers: readers,
	}, nil
}

// Account implementing AccountReader interface, retrieving the account associated
// with a particular address.
//
//	(a) A nil account is returned if it's not existent.
//	(b) An aggregated error will be returned if all readers are failed
//	    to retrieve the account.
//	(c) The returned account is safe to modify
func (r *multiReader) Account(addr common.Address) (*types.StateAccount, error) {
	var errs []error
	for _, reader := range r.readers {
		acct, err := reader.Account(addr)
		if err == nil {
			return acct, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

// Storage implementing StorageReader interface, retrieving the storage slot
// associated with a particular account address and slot key.
//
//	(a) An empty slot is returned if it's not existent
//	(b) An aggregated error will be returned if all readers are failed
//	    to retrieve the storage slot
//	(c) The returned storage slot is safe to modify
func (r *multiReader) Storage(addr common.Address, storageRoot common.Hash, slot common.Hash) (common.Hash, error) {
	var errs []error
	for _, reader := range r.readers {
		slot, err := reader.Storage(addr, storageRoot, slot)
		if err == nil {
			return slot, nil
		}
		errs = append(errs, err)
	}
	return common.Hash{}, errors.Join(errs...)
}

// Copy implementing Reader interface, returning a deep-copied state reader.
func (r *multiReader) Copy() Reader {
	var readers []Reader
	for _, reader := range r.readers {
		readers = append(readers, reader.Copy())
	}
	return &multiReader{readers: readers}
}
