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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/rlp"
)

type historicSnapshot struct {
	snaps *Tree
	root  common.Hash
	id    uint64

	// TODO cache
}

func (t *Tree) NewHistoricSnapshot(root common.Hash) (*historicSnapshot, error) {
	id, valid := rawdb.ReadStateLookup(t.diskdb, root)
	if !valid {
		return nil, errors.New("state not existent")
	}
	return &historicSnapshot{
		snaps: t,
		root:  root,
		id:    id,
	}, nil
}

// Root returns the root hash for which this snapshot was made.
func (l *historicSnapshot) Root() common.Hash {
	return l.root
}

// Account directly retrieves the account associated with a particular hash in
// the snapshot slim data format.
func (l *historicSnapshot) Account(hash common.Hash) (*Account, error) {
	data, err := l.AccountRLP(hash)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 { // can be both nil and []byte{}
		return nil, nil
	}
	account := new(Account)
	if err := rlp.DecodeBytes(data, account); err != nil {
		panic(err)
	}
	return account, nil
}

// AccountRLP directly retrieves the account RLP associated with a particular
// hash in the snapshot slim data format.
func (l *historicSnapshot) AccountRLP(hash common.Hash) ([]byte, error) {
	// TODO no atomicity guarantee, flush can happen
	blob := rawdb.ReadAccountIndex(l.snaps.diskdb, hash)
	if len(blob)%8 != 0 {
		return nil, errors.New("invalid")
	}
	n := len(blob) / 8
	pos := sort.Search(n, func(i int) bool {
		number := binary.BigEndian.Uint64(blob[8*i : 8*(i+1)])
		return number > l.id
	})
	if pos == n {
		return l.snaps.tree.bottom().AccountRLP(hash)
	}
	number := binary.BigEndian.Uint64(blob[8*pos : 8*(pos+1)])
	return l.readAccount(hash, number)
}

func (l *historicSnapshot) readAccount(accountHash common.Hash, number uint64) ([]byte, error) {
	meta := rawdb.ReadStateAccountIndex(l.snaps.freezer, number)
	if len(meta)%accountIndexSize != 0 {
		return nil, errors.New("invalid")
	}
	n := len(meta) / accountIndexSize

	pos := sort.Search(n, func(i int) bool {
		hash := meta[accountIndexSize*i : accountIndexSize*i+common.HashLength]
		return bytes.Compare(hash, accountHash.Bytes()) >= 0
	})
	if pos == n {
		return nil, errors.New("not found")
	}
	hash := common.BytesToHash(meta[accountIndexSize*pos : accountIndexSize*pos+common.HashLength])
	if hash != accountHash {
		return nil, errors.New("not found")
	}
	o := accountIndexSize*pos + common.HashLength
	offset := binary.BigEndian.Uint32(meta[o : o+4])
	length := binary.BigEndian.Uint32(meta[o+4 : o+8])
	data := rawdb.ReadStateAccountHistory(l.snaps.freezer, number)
	if len(data) < int(offset+length) {
		return nil, errors.New("corrupted data")
	}
	return data[offset : offset+length], nil
}

// Storage directly retrieves the storage data associated with a particular hash,
// within a particular account.
func (l *historicSnapshot) Storage(accountHash, storageHash common.Hash) ([]byte, error) {
	// TODO no atomicity guarantee, flush can happen
	blob := rawdb.ReadStorageIndex(l.snaps.diskdb, accountHash, storageHash)
	size := 8
	if len(blob)%size != 0 {
		return nil, errors.New("invalid")
	}
	n := len(blob) / size
	pos := sort.Search(n, func(i int) bool {
		number := binary.BigEndian.Uint64(blob[size*i : size*(i+1)])
		return number > l.id
	})
	if pos == n {
		return l.snaps.tree.bottom().Storage(accountHash, storageHash)
	}
	number := binary.BigEndian.Uint64(blob[size*pos : size*(pos+1)])
	return l.readStorage(accountHash, storageHash, number)
}

func (l *historicSnapshot) readStorage(accountHash, storageHash common.Hash, number uint64) ([]byte, error) {
	// Find account
	accountMeta := rawdb.ReadStateAccountIndex(l.snaps.freezer, number)
	accountSize := common.HashLength + 16
	if len(accountMeta)%accountSize != 0 {
		return nil, errors.New("invalid")
	}
	n := len(accountMeta) / accountSize

	pos := sort.Search(n, func(i int) bool {
		hash := accountMeta[accountSize*i : accountSize*i+common.HashLength]
		return bytes.Compare(hash, accountHash.Bytes()) >= 0
	})
	if pos == n {
		return nil, errors.New("not found")
	}
	hash := common.BytesToHash(accountMeta[accountSize*pos : accountSize*pos+common.HashLength])
	if hash != accountHash {
		return nil, errors.New("not found")
	}
	slotOffset := int(binary.BigEndian.Uint32(accountMeta[accountSize*pos+common.HashLength+8 : accountSize*pos+common.HashLength+12]))
	slotLength := int(binary.BigEndian.Uint32(accountMeta[accountSize*pos+common.HashLength+12 : accountSize*pos+common.HashLength+16]))

	// Find storage
	storageMeta := rawdb.ReadStateStorageIndex(l.snaps.freezer, number)
	if len(storageMeta)%storageIndexSize != 0 {
		return nil, errors.New("invalid")
	}
	if storageIndexSize*(slotOffset+slotLength) > len(storageMeta) {
		return nil, errors.New("invalid")
	}
	storageSubMeta := storageMeta[storageIndexSize*slotOffset : storageIndexSize*(slotOffset+slotLength)]

	slotPos := sort.Search(slotLength, func(i int) bool {
		slotHash := storageSubMeta[storageIndexSize*i : storageIndexSize*i+common.HashLength]
		return bytes.Compare(slotHash, storageHash.Bytes()) >= 0
	})
	if slotPos == slotLength {
		return nil, errors.New("not found")
	}
	slotHash := common.BytesToHash(storageSubMeta[storageIndexSize*pos : storageIndexSize*pos+common.HashLength])
	if slotHash != storageHash {
		return nil, errors.New("not found")
	}

	o := storageIndexSize*slotPos + common.HashLength
	offset := binary.BigEndian.Uint32(storageSubMeta[o : o+4])
	length := binary.BigEndian.Uint32(storageSubMeta[o+4 : o+8])
	data := rawdb.ReadStateStorageHistory(l.snaps.freezer, number)
	if len(data) < int(offset+length) {
		return nil, errors.New("corrupted data")
	}
	return data[offset : offset+length], nil
}
