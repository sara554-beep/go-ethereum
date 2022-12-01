package snapshot

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type ArchiveSnapshot struct {
	root   common.Hash
	reader *trie.HistoricStateReader
}

func NewArchiveSnapshot(root common.Hash, reader *trie.HistoricStateReader) *ArchiveSnapshot {
	return &ArchiveSnapshot{
		root:   root,
		reader: reader,
	}
}

func (snap *ArchiveSnapshot) Root() common.Hash {
	return snap.root
}

// Account directly retrieves the account associated with a particular hash in
// the snapshot slim data format.
func (snap *ArchiveSnapshot) Account(hash common.Hash) (*Account, error) {
	data, err := snap.reader.ReadAccount(hash, snap.root)
	if err != nil {
		return nil, err
	}
	account := new(Account)
	if err := rlp.DecodeBytes(data, account); err != nil {
		panic(err)
	}
	return account, nil
}

// AccountRLP directly retrieves the account RLP associated with a particular
// hash in the snapshot slim data format.
func (snap *ArchiveSnapshot) AccountRLP(hash common.Hash) ([]byte, error) {
	return snap.reader.ReadAccount(hash, snap.root)
}

// Storage directly retrieves the storage data associated with a particular hash,
// within a particular account.
func (snap *ArchiveSnapshot) Storage(accountHash, storageHash common.Hash) ([]byte, error) {
	return nil, nil
}
