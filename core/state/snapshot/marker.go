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

package snapshot

import (
	"bytes"
	"errors"
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

const genMarkerVersion = byte(0) // Current snapshot generation marker version

// encodeMarker tries to encode the given generation progress marker into the
// binary format. Note only 32 byte account marker and 64 byte storage markers
// can be accepted.
func encodeMarker(account []byte, storages [][]byte) ([]byte, error) {
	if len(account) == 0 {
		return nil, nil
	}
	var marker []byte
	marker = append(marker, genMarkerVersion)

	if len(account) != common.HashLength {
		return nil, errors.New("invalid account marker")
	}
	marker = append(marker, account...)

	for _, storage := range storages {
		if len(storage) != 2*common.HashLength {
			return nil, errors.New("invalid storage marker")
		}
		marker = append(marker, storage...)
	}
	return marker, nil
}

// decodeMarker tries to parse the generation marker in binary format.
// There are two versions of marker:
// (a) Legacy marker format:
//     Account Hash (32 bytes) or Account Hash + Slot Hash(64 bytes)
//     nil means no marker
// (b) Versionized marker format:
//     Verison(1 byte) + Account Hash (32 byte) + [Account_i Hash + Slot_i Hash ...](Optional)
//     nil means no marker
// The returned value includes:
// (a) the last iterated account hash
// (b) a list of last iterated storage marker.
// (c) flag whether the marker is valid
func decodeMarker(marker []byte) ([]byte, [][]byte, error) {
	if len(marker) == 0 { // Nil or []byte{}
		return nil, nil, nil // empty
	}
	// Parse the legacy marker
	if len(marker) == common.HashLength {
		return marker, nil, nil
	}
	if len(marker) == 2*common.HashLength {
		return marker[:common.HashLength], [][]byte{marker}, nil
	}
	// Parse versionized marker
	if len(marker) < 1 || (len(marker)-1)%common.HashLength != 0 {
		return nil, nil, errors.New("invalid generation marker")
	}
	switch marker[0] {
	case genMarkerVersion:
		var (
			account  []byte
			storages [][]byte
			remain   = marker[1:]
		)
		if len(remain) >= common.HashLength {
			account = remain[:common.HashLength]
			remain = remain[common.HashLength:]
		}
		if len(remain)%2*common.HashLength != 0 {
			return nil, nil, errors.New("invalid generation marker 2")
		}
		for len(remain) > 0 {
			storages = append(storages, remain[:2*common.HashLength])
			remain = remain[2*common.HashLength:]
		}
		return account, storages, nil
	default:
		return nil, nil, errors.New("invalid generation marker(unknown verison)")
	}
}

// slotCovered reports that whether the given storage slot is already covered
// in the snapshot.
func slotCovered(account []byte, slot []byte, accountMarker []byte, storageMarkers [][]byte) bool {
	// There is no "iterating" storage, compare the account marker only.
	if len(storageMarkers) == 0 {
		return bytes.Compare(account, accountMarker) < 0
	}
	sortByteArrays(storageMarkers)

	searchKey := append(account, slot...)
	index := sort.Search(len(storageMarkers), func(i int) bool {
		return bytes.Compare(searchKey, storageMarkers[i]) > 0
	})
	// If the key is larger than any iterated storage marker,
	// then it's not covered for sure.
	if index == len(storageMarkers)-1 {
		return false
	}
	// If the key is in the middle of iterated storage marker,
	// analysis the coverage a bit further. Otherwise it's covered
	// for sure(less than any storage marker).
	if index < len(storageMarkers) {
		// Match with the previous marker, it means the slot is not covered yet.
		if bytes.Equal(storageMarkers[index][:common.HashLength], account) {
			return false
		}
		// Otherwise, there are two cases all of them can prove the slot
		// is already covered.
		// - it's matched with the next marker but the required slot is
		//   less than the latest generated slot(although the entire
		//   storage is not generated yet).
		//
		// - it doesn't match any marker, it means the relevant storage
		//   is already generated.
	}
	return true
}

// slotCoveredByMarker is the wrap function of slotCovered which accepts
// the marker as the parameter and decodes internally.
func slotCoveredByMarker(account []byte, slot []byte, marker []byte) (bool, error) {
	accountMarker, storageMarkers, err := decodeMarker(marker)
	if err != nil {
		return false, err
	}
	return slotCovered(account, slot, accountMarker, storageMarkers), nil
}

// sortByteArrays is the helper function to sort the two-dimensional byte arrays.
func sortByteArrays(src [][]byte) {
	sort.Slice(src, func(i, j int) bool { return bytes.Compare(src[i], src[j]) < 0 })
}
