// Copyright 2015 The go-ethereum Authors
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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

func NewState(ctx context.Context, head *types.Header, odr OdrBackend, cdnURL string, cdnSwitch time.Duration) *state.StateDB {
	state, _ := state.New(head.Root, NewStateDatabase(ctx, head, odr, cdnURL, cdnSwitch))
	return state
}

func NewStateDatabase(ctx context.Context, head *types.Header, odr OdrBackend, cdnURL string, cdnSwitch time.Duration) state.Database {
	return &odrDatabase{ctx, StateTrieID(head), odr, cdnURL, cdnSwitch}
}

type odrDatabase struct {
	ctx       context.Context
	id        *TrieID
	backend   OdrBackend
	cdnURL    string
	cdnSwitch time.Duration
}

func (db *odrDatabase) OpenTrie(root common.Hash) (state.Trie, error) {
	return &odrTrie{db: db, id: db.id, cdnURL: db.cdnURL, cdnSwitch: db.cdnSwitch}, nil
}

func (db *odrDatabase) OpenStorageTrie(addrHash, root common.Hash) (state.Trie, error) {
	return &odrTrie{db: db, id: StorageTrieID(db.id, addrHash, root), cdnURL: db.cdnURL, cdnSwitch: db.cdnSwitch}, nil
}

func (db *odrDatabase) CopyTrie(t state.Trie) state.Trie {
	switch t := t.(type) {
	case *odrTrie:
		cpy := &odrTrie{db: t.db, id: t.id, cdnURL: t.cdnURL, cdnSwitch: t.cdnSwitch}
		if t.trie != nil {
			cpytrie := *t.trie
			cpy.trie = &cpytrie
		}
		return cpy
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

func (db *odrDatabase) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	if codeHash == sha3Nil {
		return nil, nil
	}
	if code, err := db.backend.Database().Get(codeHash[:]); err == nil {
		return code, nil
	}
	ctx, cancel := context.WithCancel(db.ctx)
	fetchData(db.cdnURL, db.cdnSwitch, func() error {
		id := &TrieID{
			BlockNumber: db.id.BlockNumber,
			BlockHash:   db.id.BlockHash,
			Root:        db.id.Root,
			AccKey:      addrHash.Bytes(),
		}
		req := &CodeRequest{Id: id, Hash: codeHash}
		return db.backend.Retrieve(ctx, req)
	}, func() error {
		return fetchCodeFromCDN(db.cdnURL, codeHash, db.backend.Database())
	}, func() {
		cancel()
	})
	return db.backend.Database().Get(codeHash[:])
}

func (db *odrDatabase) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	code, err := db.ContractCode(addrHash, codeHash)
	return len(code), err
}

func (db *odrDatabase) TrieDB() *trie.Database {
	return nil
}

type odrTrie struct {
	db        *odrDatabase
	id        *TrieID
	trie      *trie.Trie
	cdnURL    string
	cdnSwitch time.Duration
}

func (t *odrTrie) TryGet(key []byte) ([]byte, error) {
	key = crypto.Keccak256(key)
	var res []byte
	err := t.do(key, func() (err error) {
		res, err = t.trie.TryGet(key)
		return err
	})
	return res, err
}

func (t *odrTrie) TryUpdate(key, value []byte) error {
	key = crypto.Keccak256(key)
	return t.do(key, func() error {
		return t.trie.TryUpdate(key, value)
	})
}

func (t *odrTrie) TryDelete(key []byte) error {
	key = crypto.Keccak256(key)
	return t.do(key, func() error {
		return t.trie.TryDelete(key)
	})
}

func (t *odrTrie) Commit(onleaf trie.LeafCallback) (common.Hash, error) {
	if t.trie == nil {
		return t.id.Root, nil
	}
	return t.trie.Commit(onleaf)
}

func (t *odrTrie) Hash() common.Hash {
	if t.trie == nil {
		return t.id.Root
	}
	return t.trie.Hash()
}

func (t *odrTrie) NodeIterator(startkey []byte) trie.NodeIterator {
	return newNodeIterator(t, startkey)
}

func (t *odrTrie) GetKey(sha []byte) []byte {
	return nil
}

func (t *odrTrie) Prove(key []byte, fromLevel uint, proofDb ethdb.KeyValueWriter) error {
	return errors.New("not implemented, needs client/server interface split")
}

// do tries and retries to execute a function until it returns with no error or
// an error type other than MissingNodeError
func (t *odrTrie) do(key []byte, fn func() error) error {
	for {
		var err error
		if t.trie == nil {
			t.trie, err = trie.New(t.id.Root, trie.NewDatabase(t.db.backend.Database()))
		}
		if err == nil {
			err = fn()
		}
		trieError, ok := err.(*trie.MissingNodeError)
		if !ok {
			return err
		}
		ctx, cancel := context.WithCancel(t.db.ctx)
		if err := fetchData(t.cdnURL, t.cdnSwitch, func() error {
			r := &TrieRequest{Id: t.id, Key: key}
			return t.db.backend.Retrieve(ctx, r)
		}, func() error {
			return fetchNodesFromCDN(t.cdnURL, trieError.NodeHash, t.db.backend.Database())
		}, func() {
			cancel()
		}); err != nil {
			return err
		}
	}
}

type nodeIterator struct {
	trie.NodeIterator
	t   *odrTrie
	err error
}

func newNodeIterator(t *odrTrie, startkey []byte) trie.NodeIterator {
	it := &nodeIterator{t: t}
	// Open the actual non-ODR trie if that hasn't happened yet.
	if t.trie == nil {
		it.do(func() error {
			t, err := trie.New(t.id.Root, trie.NewDatabase(t.db.backend.Database()))
			if err == nil {
				it.t.trie = t
			}
			return err
		})
	}
	it.do(func() error {
		it.NodeIterator = it.t.trie.NodeIterator(startkey)
		return it.NodeIterator.Error()
	})
	return it
}

func (it *nodeIterator) Next(descend bool) bool {
	var ok bool
	it.do(func() error {
		ok = it.NodeIterator.Next(descend)
		return it.NodeIterator.Error()
	})
	return ok
}

// do runs fn and attempts to fill in missing nodes by retrieving.
func (it *nodeIterator) do(fn func() error) {
	var lasthash common.Hash
	for {
		it.err = fn()
		missing, ok := it.err.(*trie.MissingNodeError)
		if !ok {
			return
		}
		if missing.NodeHash == lasthash {
			it.err = fmt.Errorf("retrieve loop for trie node %x", missing.NodeHash)
			return
		}
		lasthash = missing.NodeHash
		r := &TrieRequest{Id: it.t.id, Key: nibblesToKey(missing.Path)}
		if it.err = it.t.db.backend.Retrieve(it.t.db.ctx, r); it.err != nil {
			return
		}
	}
}

func (it *nodeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.NodeIterator.Error()
}

func nibblesToKey(nib []byte) []byte {
	if len(nib) > 0 && nib[len(nib)-1] == 0x10 {
		nib = nib[:len(nib)-1] // drop terminator
	}
	if len(nib)&1 == 1 {
		nib = append(nib, 0) // make even
	}
	key := make([]byte, len(nib)/2)
	for bi, ni := 0, 0; ni < len(nib); bi, ni = bi+1, ni+2 {
		key[bi] = nib[ni]<<4 | nib[ni+1]
	}
	return key
}

func fetchCodeFromCDN(url string, codeHash common.Hash, db ethdb.Database) error {
	res, err := http.Get(fmt.Sprintf("%s/state/0x%x?target=%d&limit=%d&barrier=%d", url, codeHash, 16, 256, 2))
	if err != nil {
		return err
	}
	blob, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	res.Body.Close()
	var nodes [][]byte
	if err := rlp.DecodeBytes(blob, &nodes); err != nil {
		return err
	}
	db.Put(codeHash.Bytes(), nodes[0])
	return nil
}

func fetchNodesFromCDN(url string, root common.Hash, db ethdb.Database) error {
	res, err := http.Get(fmt.Sprintf("%s/state/0x%x?target=%d&limit=%d&barrier=%d", url, root, 16, 256, 2))
	if err != nil {
		return err
	}
	blob, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	res.Body.Close()

	var nodes [][]byte
	if err := rlp.DecodeBytes(blob, &nodes); err != nil {
		return err
	}
	// Write all retrieved nodes in the specified tile into database
	for _, node := range nodes {
		db.Put(crypto.Keccak256(node), node)
	}
	return nil
}

func fetchData(cdnURL string, cdnSwitch time.Duration, p2pFetcher func() error, cdnFetcher func() error, stopP2P func()) (err error) {
	defer func(start time.Time) {
		log.Debug("Retrieved data", "elapsed", common.PrettyDuration(time.Since(start)))
	}(time.Now())

	var (
		count   int
		wg      sync.WaitGroup
		errorCh = make(chan error, 2)
		closeCh = make(chan struct{})
	)
	// If no external CDN is configured or p2p request is not disabled,
	// spin up a p2p fetcher.
	if (cdnURL == "" || cdnSwitch != 0) && p2pFetcher != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p2pFetcher()
			select {
			case errorCh <- err:
			case <-closeCh:
			}
		}()
	}
	// If external CDN is configured, spin up a cdn fetcher.
	if cdnURL != "" && cdnFetcher != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-time.NewTimer(cdnSwitch).C:
			case <-closeCh:
				return
			}
			select {
			case errorCh <- cdnFetcher():
				stopP2P()
			case <-closeCh:
			}
		}()
	}

loop:
	for {
		select {
		case cerr := <-errorCh:
			if cerr != nil {
				count += 1
				if count == 2 {
					break loop
				}
				continue
			}
			err = cerr
			break loop
		}
	}
	close(closeCh)
	wg.Wait()
	return
}
