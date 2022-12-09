package trie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

type HistoricStateReader struct {
	db *snapDatabase
}

func NewHistoricStateReader(db *Database) (*HistoricStateReader, error) {
	snap, ok := db.backend.(*snapDatabase)
	if !ok {
		return nil, errors.New("unsupported")
	}
	return &HistoricStateReader{db: snap}, nil
}

func (r *HistoricStateReader) readAccount(accountHash common.Hash, number uint64) ([]byte, error) {
	index := rawdb.ReadStateAccountIndex(r.db.stateHistory, number)
	position := sort.Search(len(index)/accountIndexLength, func(i int) bool {
		hash := index[accountIndexLength*i : accountIndexLength*i+common.HashLength]
		return bytes.Compare(hash, accountHash.Bytes()) >= 0
	})
	hash := common.BytesToHash(index[accountIndexLength*position : accountIndexLength*position+common.HashLength])
	if hash != accountHash {
		return nil, errors.New("not found")
	}
	o := accountIndexLength*position + common.HashLength
	offset, length := binary.BigEndian.Uint32(index[o:o+4]), binary.BigEndian.Uint32(index[o+4:o+8])
	data := rawdb.ReadStateAccountData(r.db.stateHistory, number)
	return data[offset : offset+length], nil
}

func (r *HistoricStateReader) ReadAccount(accountHash common.Hash, root common.Hash) ([]byte, error) {
	data, err := r.db.tree.bottom().(*diskLayer).state(common.Hash{}, accountHash)
	if err != nil {
		return nil, err
	}
	id, exist := rawdb.ReadStateLookup(r.db.diskdb, root)
	if !exist {
		return nil, errors.New("no")
	}
	blob := rawdb.ReadAccountIndex(r.db.diskdb, accountHash)
	if len(blob)%8 != 0 {
		return nil, errors.New("invalid")
	}
	for i := 0; i < len(blob)/8; i++ {
		number := binary.BigEndian.Uint64(blob[8*i : 8*(i+1)])
		if number > id {
			return r.readAccount(accountHash, number)
		}
	}
	return data, nil // todo return latest
}
