// Copyright 2023 The go-ethereum Authors
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

	"github.com/crate-crypto/go-ipa/banderwagon"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
)

const (
	// commitmentSize is the size of commitment stored in cache.
	commitmentSize = banderwagon.UncompressedSize

	// Cache item granted for caching commitment results.
	commitmentCacheItems = 64 * 1024 * 1024 / (commitmentSize + common.AddressLength)
)

// merkleHasher implements Hasher interface, providing the state hash
// functionalities using Merkle-Patricia-Trie rules.
type merkleHasher struct {
	db         *trie.Database                     // Database for loading trie(s)
	root       common.Hash                        // Roof of the prestate
	mainTrie   *trie.StateTrie                    // Account trie, cached when it's resolved
	subTries   map[common.Address]*trie.StateTrie // group of storage tries, cached when it's resolved.
	prefetcher *triePrefetcher                    // prefetcher which does trie preloading
}

// newMerkleHasher constructs the merkle hasher with the optional prefetcher,
// an error will be returned if the corresponding trie is not available.
func newMerkleHasher(db *trie.Database, root common.Hash, prefetcher *triePrefetcher) (*merkleHasher, error) {
	var (
		err      error
		mainTrie *trie.StateTrie
	)
	if prefetcher != nil {
		tr := prefetcher.trie(newTrieID(typeMerkle, root, common.Hash{}, root))
		if tr != nil {
			mainTrie = tr.(*trie.StateTrie)
		}
	}
	if mainTrie == nil {
		mainTrie, err = trie.NewStateTrie(trie.StateTrieID(root), db)
		if err != nil {
			return nil, err
		}
	}
	return &merkleHasher{
		db:         db,
		root:       root,
		mainTrie:   mainTrie,
		subTries:   make(map[common.Address]*trie.StateTrie),
		prefetcher: prefetcher,
	}, nil
}

// loadAccountTrie returns the cached storage trie, or resolves it from either
// prefetcher or database.
func (h *merkleHasher) loadStorageTrie(address common.Address, root common.Hash) (*trie.StateTrie, error) {
	if tr := h.subTries[address]; tr != nil {
		return tr, nil
	}
	var (
		err      error
		subTrie  *trie.StateTrie
		addrHash = crypto.Keccak256Hash(address.Bytes())
	)
	if h.prefetcher != nil {
		tr := h.prefetcher.trie(newTrieID(typeMerkle, h.root, addrHash, root))
		if tr != nil {
			subTrie = tr.(*trie.StateTrie)
		}
	}
	if subTrie == nil {
		subTrie, err = trie.NewStateTrie(trie.StorageTrieID(h.root, addrHash, root), h.db)
		if err != nil {
			return nil, err
		}
	}
	h.subTries[address] = subTrie
	return subTrie, nil
}

// UpdateAccount abstracts an account write to the trie. It encodes the
// provided account object with associated algorithm and then updates it
// in the trie with provided address.
func (h *merkleHasher) UpdateAccount(address common.Address, account *types.StateAccount) error {
	return h.mainTrie.UpdateAccount(address, account)
}

// UpdateStorage associates key with value in the trie. If value has length zero,
// any existing value is deleted from the trie. The value bytes must not be modified
// by the caller while they are stored in the trie. If a node was not found in the
// database, a trie.MissingNodeError is returned.
func (h *merkleHasher) UpdateStorage(address common.Address, root common.Hash, key, value []byte) error {
	tr, err := h.loadStorageTrie(address, root)
	if err != nil {
		return err
	}
	return tr.UpdateStorage(address, key, value)
}

// DeleteAccount abstracts an account deletion from the trie.
func (h *merkleHasher) DeleteAccount(address common.Address) error {
	return h.mainTrie.DeleteAccount(address)
}

// DeleteStorage removes any existing value for key from the trie. If a node
// was not found in the database, a trie.MissingNodeError is returned.
func (h *merkleHasher) DeleteStorage(address common.Address, root common.Hash, key []byte) error {
	tr, err := h.loadStorageTrie(address, root)
	if err != nil {
		return err
	}
	return tr.DeleteStorage(address, key)
}

// UpdateContractCode abstracts code write to the trie. It is expected
// to be moved to the stateWriter interface when the latter is ready.
func (h *merkleHasher) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

// Hash returns the root hash of the trie. It does not write to the database and
// can be used even if the trie doesn't have one.
func (h *merkleHasher) Hash(address *common.Address, origin common.Hash) (common.Hash, error) {
	if address == nil {
		return h.mainTrie.Hash(), nil
	}
	tr, ok := h.subTries[*address]
	if !ok {
		return common.Hash{}, errors.New("storage trie is not loaded yet")
	}
	return tr.Hash(), nil
}

// Commit collects all dirty nodes in the trie and replace them with the
// corresponding node hash. All collected nodes(including dirty leaves if
// collectLeaf is true) will be encapsulated into a nodeset for return.
// The returned nodeset can be nil if the trie is clean(nothing to commit).
// Once the trie is committed, it's not usable anymore. A new trie must
// be created with new root and updated trie database for following usage
func (h *merkleHasher) Commit(address *common.Address, root common.Hash, collectLeaf bool) (*trienode.NodeSet, error) {
	var (
		err       error
		set       *trienode.NodeSet
		localHash common.Hash
	)
	if address == nil {
		localHash, set, err = h.mainTrie.Commit(collectLeaf)
	} else {
		tr, ok := h.subTries[*address]
		if !ok {
			return nil, errors.New("storage trie is not loaded yet")
		}
		localHash, set, err = tr.Commit(collectLeaf)
	}
	if err != nil {
		return nil, err
	}
	if localHash != root {
		return nil, errors.New("trie hash is not matched")
	}
	return set, nil
}

// Copy returns a deep-copied state hasher.
func (h *merkleHasher) Copy() Hasher {
	var mainTrie *trie.StateTrie
	if h.mainTrie != nil {
		mainTrie = h.mainTrie.Copy()
	}
	subTries := make(map[common.Address]*trie.StateTrie, len(h.subTries))
	for addr, tr := range h.subTries {
		subTries[addr] = tr.Copy()
	}
	return &merkleHasher{
		db:       h.db,
		root:     h.root,
		mainTrie: mainTrie,
		subTries: subTries,
	}
}

// verkleHasher implements Hasher interface, providing the hash functionalities
// using Verkle-Trie rules.
type verkleHasher struct {
	db   *trie.Database   // Database for loading verkle tree
	root common.Hash      // Associated prestate root
	tr   *trie.VerkleTrie // Associated verkle tree for state hashing
}

// newVerkleHasher constructs the verkle hasher with the optional prefetcher.
func newVerkleHasher(db *trie.Database, root common.Hash, prefetcher *triePrefetcher) (*verkleHasher, error) {
	var (
		err      error
		mainTrie *trie.VerkleTrie
	)
	if prefetcher != nil {
		tr := prefetcher.trie(newTrieID(typeVerkle, root, common.Hash{}, common.Hash{}))
		if tr != nil {
			mainTrie = tr.(*trie.VerkleTrie)
		}
	}
	if mainTrie == nil {
		mainTrie, err = trie.NewVerkleTrie(root, db, utils.NewPointCache(commitmentCacheItems))
		if err != nil {
			return nil, err
		}
	}
	return &verkleHasher{
		db:   db,
		root: root,
		tr:   mainTrie,
	}, nil
}

// UpdateAccount abstracts an account write to the trie. It encodes the
// provided account object with associated algorithm and then updates it
// in the trie with provided address.
func (h *verkleHasher) UpdateAccount(address common.Address, account *types.StateAccount) error {
	return h.tr.UpdateAccount(address, account)
}

// UpdateStorage associates key with value in the trie. If value has length zero,
// any existing value is deleted from the trie. The value bytes must not be modified
// by the caller while they are stored in the trie. If a node was not found in the
// database, a trie.MissingNodeError is returned.
func (h *verkleHasher) UpdateStorage(address common.Address, root common.Hash, key, value []byte) error {
	return h.tr.UpdateStorage(address, key, value)
}

// DeleteAccount abstracts an account deletion from the trie.
func (h *verkleHasher) DeleteAccount(address common.Address) error {
	return h.tr.DeleteAccount(address)
}

// DeleteStorage removes any existing value for key from the trie. If a node
// was not found in the database, a trie.MissingNodeError is returned.
func (h *verkleHasher) DeleteStorage(address common.Address, root common.Hash, key []byte) error {
	return h.tr.DeleteStorage(address, key)
}

// UpdateContractCode abstracts code write to the trie. It is expected
// to be moved to the stateWriter interface when the latter is ready.
func (h *verkleHasher) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return h.tr.UpdateContractCode(address, codeHash, code)
}

// Hash returns the root hash of the trie. It does not write to the database and
// can be used even if the trie doesn't have one.
func (h *verkleHasher) Hash(address *common.Address, origin common.Hash) (common.Hash, error) {
	if address != nil {
		return origin, nil
	}
	return h.tr.Hash(), nil
}

// Commit collects all dirty nodes in the trie and replace them with the
// corresponding node hash. All collected nodes(including dirty leaves if
// collectLeaf is true) will be encapsulated into a nodeset for return.
// The returned nodeset can be nil if the trie is clean(nothing to commit).
// Once the trie is committed, it's not usable anymore. A new trie must
// be created with new root and updated trie database for following usage
func (h *verkleHasher) Commit(address *common.Address, root common.Hash, collectLeaf bool) (*trienode.NodeSet, error) {
	if address != nil {
		return nil, nil
	}
	localHash, set, err := h.tr.Commit(collectLeaf)
	if err != nil {
		return nil, err
	}
	if localHash != root {
		return nil, errors.New("trie hash is not matched")
	}
	return set, nil
}

// Copy returns a deep-copied state hasher.
func (h *verkleHasher) Copy() Hasher {
	var tr *trie.VerkleTrie
	if h.tr != nil {
		tr = tr.Copy()
	}
	return &verkleHasher{
		root: h.root,
		db:   h.db,
		tr:   tr,
	}
}
