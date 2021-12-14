// Copyright 2019 The go-ethereum Authors
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

package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Version is the current version tag of freezer table.
	// The legacy freezer table doesn't have version information,
	// it's added when tail truncation is implemented.
	version = uint16(0)

	// metaSize is the size of meta index in the index file.
	// It occupies the first 256 bytes in the index file and leaves
	// a lot of unused space for future expansion.
	metaSize = 256
)

var (
	errUnexpectedMetaSize = errors.New("unexpected meta index size")
)

// metaEntry contains the meta information of the freezer table.
type metaIndex struct {
	version uint16 // Version indicator of freezer table.

	// tailDeleted represents the number of data entries deleted
	// from the tail. The default value is 0 (nothing deleted).
	tailDeleted uint64

	// tailHidden represents the number of data entries hidden
	// from the tail. Hidden means the data entry is marked as deleted
	// but it's not removed from the file system yet because of the
	// lazy-deletion character of freezer table.
	//
	// It will be reset once the real deletion happens. The default
	// value is 0 (nothing hidden).
	tailHidden uint64

	// tailFile is the number of the tail data file.
	tailFile uint32
}

// unmarshalBinary deserializes binary b into the meta index entry.
func (meta *metaIndex) unmarshalBinary(b []byte) error {
	if len(b) != metaSize {
		return fmt.Errorf("%w, want %d, got %d", errUnexpectedMetaSize, metaSize, len(b))
	}
	meta.version = binary.BigEndian.Uint16(b[:2])       // The first 2 bytes
	meta.tailDeleted = binary.BigEndian.Uint64(b[2:10]) // The next 8 bytes
	meta.tailHidden = binary.BigEndian.Uint64(b[10:18]) // The next 8 bytes
	meta.tailFile = binary.BigEndian.Uint32(b[18:22])   // The last 4 bytes
	return nil

	// The remaining bytes can be extended in the future.
}

// marshalBinary adds the encoded meta entry to the end of b.
func (meta *metaIndex) marshalBinary(b []byte) []byte {
	offset := len(b)
	out := append(b, make([]byte, metaSize)...)
	binary.BigEndian.PutUint16(out[offset:], meta.version) // The first 2 bytes
	binary.BigEndian.PutUint64(out[offset+2:], meta.tailDeleted) // The next 8 bytes
	binary.BigEndian.PutUint64(out[offset+10:], meta.tailHidden) // The next 8 bytes
	binary.BigEndian.PutUint32(out[offset+18:], meta.tailFile) // The last 4 bytes
	return out
}