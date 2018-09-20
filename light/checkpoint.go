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

package light

import (
	"encoding/binary"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	// checkpointPrefix + index (uint64 big endian) -> checkpoint
	checkpointPrefix = []byte("cp")

	// headCheckpointKey tracks the latest know stable checkpoint
	headCheckpointKey = []byte("LastCheckpoint")

	// EmptyCheckpoint used to represent an empty checkpoint.
	EmptyCheckpoint = &TrustedCheckpoint{}
)

// TrustedCheckpoints associates each known checkpoint with the genesis hash of the chain it belongs to
var TrustedCheckpoints = map[common.Hash]*params.TrustedCheckpoint{
	params.MainnetGenesisHash: params.MainnetTrustedCheckpoint,
	params.TestnetGenesisHash: params.TestnetTrustedCheckpoint,
	params.RinkebyGenesisHash: params.RinkebyTrustedCheckpoint,
}

// TrustedCheckpoint represents a set of post-processed trie roots (CHT and BloomTrie) associated with
// the appropriate section index and head hash.
//
// It is used to start light syncing from this checkpoint and avoid downloading the entire header chain
// while still being able to securely access old headers/logs.
type TrustedCheckpoint params.TrustedCheckpoint

type trustCheckpointRLP struct {
	SectionIndex uint64
	SectionHead  common.Hash
	CHTRoot      common.Hash
	BloomRoot    common.Hash
}

// EncodeRLP implements rlp.Encoder, and flattens the necessary fields of a checkpoint
// into an RLP stream.
func (c *TrustedCheckpoint) EncodeRLP(w io.Writer) (err error) {
	return rlp.Encode(w, &trustCheckpointRLP{c.SectionIndex, c.SectionHead, c.CHTRoot, c.BloomRoot})
}

// DecodeRLP implements rlp.Decoder, and loads the necessary fields of a checkpoint
// from an RLP stream.
func (c *TrustedCheckpoint) DecodeRLP(s *rlp.Stream) error {
	var dec trustCheckpointRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	c.SectionIndex, c.SectionHead, c.CHTRoot, c.BloomRoot = dec.SectionIndex, dec.SectionHead, dec.CHTRoot, dec.BloomRoot
	return nil
}

// HashEqual returns an indicator comparing the itself hash with given one.
func (c *TrustedCheckpoint) HashEqual(hash common.Hash) bool {
	if c.Empty() {
		return hash == common.Hash{}
	}
	return c.Hash() == hash
}

// Hash returns the hash of checkpoint's four key fields(index, sectionHead, chtRoot and bloomTrieRoot).
func (c *TrustedCheckpoint) Hash() common.Hash {
	buf := make([]byte, 8+3*common.HashLength)
	binary.BigEndian.PutUint64(buf, c.SectionIndex)
	copy(buf[8:], c.SectionHead.Bytes())
	copy(buf[8+common.HashLength:], c.CHTRoot.Bytes())
	copy(buf[8+2*common.HashLength:], c.BloomRoot.Bytes())
	return crypto.Keccak256Hash(buf)
}

func (c *TrustedCheckpoint) Empty() bool {
	return c.SectionHead == (common.Hash{}) || c.CHTRoot == (common.Hash{}) || c.BloomRoot == (common.Hash{})
}

// checkpointsKey = checkpointPrefix + index (uint64 big endian)
func checkpointsKey(index uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, index)
	return append(checkpointPrefix, enc...)
}

// ReadTrustedCheckpoint retrieves the checkpoint from the database.
func ReadTrustedCheckpoint(db ethdb.Database, index uint64) *TrustedCheckpoint {
	data, err := db.Get(checkpointsKey(index))
	if err != nil {
		return nil
	}
	c := new(TrustedCheckpoint)
	if err := rlp.DecodeBytes(data, c); err != nil {
		log.Error("Invalid checkpoint RLP", "err", err)
		return nil
	}
	return c
}

// WriteTrustedCheckpoint stores an RLP encoded checkpoint into the database.
func WriteTrustedCheckpoint(db ethdb.Putter, index uint64, checkpoint *TrustedCheckpoint) {
	data, err := rlp.EncodeToBytes(checkpoint)
	if err != nil {
		log.Crit("Failed to RLP encode checkpoint", err)
	}
	if err := db.Put(checkpointsKey(index), data); err != nil {
		log.Crit("Failed to store checkpoint", "err", err)
	}
}

// ReadHeadCheckpoint retrieves the index of the current stable checkpoint
func ReadHeadCheckpoint(db ethdb.Database) uint64 {
	data, _ := db.Get(headCheckpointKey)
	if len(data) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// WriteHeadCheckpoint stores the index of the current stable checkpoint
func WriteHeadCheckpoint(db ethdb.Putter, index uint64) {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, index)
	if err := db.Put(headCheckpointKey, enc); err != nil {
		log.Crit("Failed to store last checkpoint index", "err", err)
	}
}
