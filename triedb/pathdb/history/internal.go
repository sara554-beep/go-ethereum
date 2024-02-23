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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package history

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

const (
	accountIndexSize = common.AddressLength + 13 // The length of encoded account index
	slotIndexSize    = common.HashLength + 5     // The length of encoded slot index
	metaSize         = 9 + 2*common.HashLength   // The length of meta object

	version = uint8(0) // initial version of state history structure.
)

// meta describes the meta data of state history object.
type meta struct {
	version uint8       // version tag of history object
	parent  common.Hash // prev-state root before the state transition
	root    common.Hash // post-state root after the state transition
	block   uint64      // associated block number
}

// encode packs the meta object into byte stream.
func (m *meta) encode() []byte {
	buf := make([]byte, metaSize)
	buf[0] = m.version
	copy(buf[1:1+common.HashLength], m.parent.Bytes())
	copy(buf[1+common.HashLength:1+2*common.HashLength], m.root.Bytes())
	binary.BigEndian.PutUint64(buf[1+2*common.HashLength:metaSize], m.block)
	return buf[:]
}

// decode unpacks the meta object from byte stream.
func (m *meta) decode(blob []byte) error {
	if len(blob) < 1 {
		return fmt.Errorf("no version tag")
	}
	switch blob[0] {
	case version:
		if len(blob) != metaSize {
			return fmt.Errorf("invalid state history meta, len: %d", len(blob))
		}
		m.version = blob[0]
		m.parent = common.BytesToHash(blob[1 : 1+common.HashLength])
		m.root = common.BytesToHash(blob[1+common.HashLength : 1+2*common.HashLength])
		m.block = binary.BigEndian.Uint64(blob[1+2*common.HashLength : metaSize])
		return nil
	default:
		return fmt.Errorf("unknown version %d", blob[0])
	}
}

// accountIndex describes the metadata belonging to an account.
type accountIndex struct {
	address       common.Address // The address of account
	length        uint8          // The length of account data, size limited by 255
	offset        uint32         // The offset of item in account data table
	storageOffset uint32         // The offset of storage index in storage index table
	storageSlots  uint32         // The number of mutated storage slots belonging to the account
}

// encode packs account index into byte stream.
func (i *accountIndex) encode() []byte {
	var buf [accountIndexSize]byte
	copy(buf[:], i.address.Bytes())
	buf[common.AddressLength] = i.length
	binary.BigEndian.PutUint32(buf[common.AddressLength+1:], i.offset)
	binary.BigEndian.PutUint32(buf[common.AddressLength+5:], i.storageOffset)
	binary.BigEndian.PutUint32(buf[common.AddressLength+9:], i.storageSlots)
	return buf[:]
}

// decode unpacks account index from byte stream.
func (i *accountIndex) decode(blob []byte) {
	i.address = common.BytesToAddress(blob[:common.AddressLength])
	i.length = blob[common.AddressLength]
	i.offset = binary.BigEndian.Uint32(blob[common.AddressLength+1:])
	i.storageOffset = binary.BigEndian.Uint32(blob[common.AddressLength+5:])
	i.storageSlots = binary.BigEndian.Uint32(blob[common.AddressLength+9:])
}

// slotIndex describes the metadata belonging to a storage slot.
type slotIndex struct {
	hash   common.Hash // The hash of slot key
	length uint8       // The length of storage slot, up to 32 bytes defined in protocol
	offset uint32      // The offset of item in storage slot data table
}

// encode packs slot index into byte stream.
func (i *slotIndex) encode() []byte {
	var buf [slotIndexSize]byte
	copy(buf[:common.HashLength], i.hash.Bytes())
	buf[common.HashLength] = i.length
	binary.BigEndian.PutUint32(buf[common.HashLength+1:], i.offset)
	return buf[:]
}

// decode unpack slot index from the byte stream.
func (i *slotIndex) decode(blob []byte) {
	i.hash = common.BytesToHash(blob[:common.HashLength])
	i.length = blob[common.HashLength]
	i.offset = binary.BigEndian.Uint32(blob[common.HashLength+1:])
}
