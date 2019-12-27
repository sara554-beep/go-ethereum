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

package utilities

import (
	"fmt"

	"github.com/ethereum/go-ethereum/rlp"
)

type KeyValueEntry struct {
	Key   string
	Value rlp.RawValue
}

type KeyValueList []KeyValueEntry
type KeyValueMap map[string]rlp.RawValue

func (l KeyValueList) Add(key string, val interface{}) KeyValueList {
	var entry KeyValueEntry
	entry.Key = key
	if val == nil {
		val = uint64(0)
	}
	enc, err := rlp.EncodeToBytes(val)
	if err == nil {
		entry.Value = enc
	}
	return append(l, entry)
}

func (l KeyValueList) Decode() (KeyValueMap, uint64) {
	m := make(KeyValueMap)
	var size uint64
	for _, entry := range l {
		m[entry.Key] = entry.Value
		size += uint64(len(entry.Key)) + uint64(len(entry.Value)) + 8
	}
	return m, size
}

func (m KeyValueMap) Get(key string, val interface{}) error {
	enc, ok := m[key]
	if !ok {
		return fmt.Errorf("missing key %s", key)
	}
	if val == nil {
		return nil
	}
	return rlp.DecodeBytes(enc, val)
}
