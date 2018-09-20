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
	// checkpointKey tracks the latest stable checkpoint.
	checkpointKey = []byte("Checkpoint")

	// EmptyCheckpoint used to represent an empty checkpoint.
	EmptyCheckpoint = &TrustedCheckpoint{}
)

// TrustedCheckpoint represents a set of post-processed trie roots (CHT and BloomTrie) associated with
// the appropriate section index and head hash.
//
// It is used to start light syncing from this checkpoint and avoid downloading the entire header chain
// while still being able to securely access old headers/logs.
type TrustedCheckpoint struct {
	SectionIdx  uint64      // Section index
	SectionHead common.Hash // Block Hash for the last block in the section
	CHTRoot     common.Hash // CHT(Canonical Hash Trie) root associated to the section
	BloomRoot   common.Hash // Bloom Trie root associated to the section

	// Non-hash related fields
	Name        string // Indicator which chain the checkpoint belongs to
	BlockNumber uint64 // Corresponding block number when checkpoint is registered.
}

type trustCheckpointRLP struct {
	BlockNumber uint64
	SectionIdx  uint64
	SectionHead common.Hash
	CHTRoot     common.Hash
	BloomRoot   common.Hash
}

// EncodeRLP implements rlp.Encoder, and flattens the necessary fields of a checkpoint
// into an RLP stream.
func (c *TrustedCheckpoint) EncodeRLP(w io.Writer) (err error) {
	return rlp.Encode(w, &trustCheckpointRLP{c.BlockNumber, c.SectionIdx, c.SectionHead, c.CHTRoot, c.BloomRoot})
}

// DecodeRLP implements rlp.Decoder, and loads the necessary fields of a checkpoint
// from an RLP stream.
func (c *TrustedCheckpoint) DecodeRLP(s *rlp.Stream) error {
	var dec trustCheckpointRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	c.BlockNumber, c.SectionIdx, c.SectionHead, c.CHTRoot, c.BloomRoot = dec.BlockNumber, dec.SectionIdx, dec.SectionHead, dec.CHTRoot, dec.BloomRoot
	return nil
}

// HashEqual returns an indicator comparing the itself hash with given one.
func (c *TrustedCheckpoint) HashEqual(hash common.Hash) bool {
	if c.Empty() {
		return hash == common.Hash{}
	}
	return c.Hash() == hash
}

// Hash returns the hash of checkpoint's three key fields(sectionHead, chtRoot and bloomTrieRoot).
func (c *TrustedCheckpoint) Hash() common.Hash {
	buf := make([]byte, 8+3*common.HashLength)
	binary.BigEndian.PutUint64(buf, c.SectionIdx)
	copy(buf[8:], c.SectionHead.Bytes())
	copy(buf[8+common.HashLength:], c.CHTRoot.Bytes())
	copy(buf[8+2*common.HashLength:], c.BloomRoot.Bytes())
	return crypto.Keccak256Hash(buf)
}

func (c *TrustedCheckpoint) Empty() bool {
	return c.SectionHead == (common.Hash{}) || c.CHTRoot == (common.Hash{}) || c.BloomRoot == (common.Hash{})
}

// ReadTrustedCheckpoint retrieves the checkpoint from the database.
func ReadTrustedCheckpoint(db ethdb.Database) *TrustedCheckpoint {
	data, err := db.Get(checkpointKey)
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
func WriteTrustedCheckpoint(db ethdb.Putter, checkpoint *TrustedCheckpoint) {
	data, err := rlp.EncodeToBytes(checkpoint)
	if err != nil {
		log.Crit("Failed to RLP encode checkpoint", err)
	}
	if err := db.Put(checkpointKey, data); err != nil {
		log.Crit("Failed to store checkpoint", "err", err)
	}
}

// TrustedCheckpoints associates all known checkpoints with the genesis hash of the chain it belongs to
//
// After the registrar contract is enabled, all new checkpoint information will be published via contract.
// To ensure compatibility with previously hardcoded checkpoint information, all checkpoint history is
// listed here.
var TrustedCheckpoints = map[common.Hash]TrustedCheckpoint{
	params.MainnetGenesisHash: {
		Name:        "mainnet",
		SectionIdx:  193,
		SectionHead: common.HexToHash("0xc2d574295ecedc4d58530ae24c31a5a98be7d2b3327fba0dd0f4ed3913828a55"),
		CHTRoot:     common.HexToHash("0x5d1027dfae688c77376e842679ceada87fd94738feb9b32ef165473bfbbb317b"),
		BloomRoot:   common.HexToHash("0xd38be1a06aabd568e10957fee4fcc523bc64996bcf31bae3f55f86e0a583919f"),
	},

	params.TestnetGenesisHash: {
		Name:        "testnet",
		SectionIdx:  123,
		SectionHead: common.HexToHash("0xa372a53decb68ce453da12bea1c8ee7b568b276aa2aab94d9060aa7c81fc3dee"),
		CHTRoot:     common.HexToHash("0x6b02e7fada79cd2a80d4b3623df9c44384d6647fc127462e1c188ccd09ece87b"),
		BloomRoot:   common.HexToHash("0xf2d27490914968279d6377d42868928632573e823b5d1d4a944cba6009e16259"),
	},
	params.RinkebyGenesisHash: {
		Name:        "rinkeby",
		SectionIdx:  91,
		SectionHead: common.HexToHash("0x435b7b2d8a7922f3b9a522f2fb02730e95e0e1782f0f5443894d5415bba37154"),
		CHTRoot:     common.HexToHash("0x0664bf7ecccfb6775c4eca6f0f264fb5282a22754a2135a1ac4bff2ef02898dd"),
		BloomRoot:   common.HexToHash("0x2a64df2400c3a2cb6400639bb6ed29389abdb4d93e2e525aa7c21f38767cd96f"),
	},
}
