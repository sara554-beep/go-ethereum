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

package fetcher

import (
	"crypto/ecdsa"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

func makeTransactions(key *ecdsa.PrivateKey, target int) []*types.Transaction {
	var txs []*types.Transaction

	for i := 0; i < target; i++ {
		random := rand.Uint32()
		tx := types.NewTransaction(uint64(random), common.Address{0x1, 0x2, 0x3}, big.NewInt(int64(random)), 100, big.NewInt(int64(random)), nil)
		tx, _ = types.SignTx(tx, types.NewEIP155Signer(big.NewInt(1)), key)
		txs = append(txs, tx)
	}
	return txs
}

func makeUnsignedTransactions(key *ecdsa.PrivateKey, target int) []*types.Transaction {
	var txs []*types.Transaction

	for i := 0; i < target; i++ {
		random := rand.Uint32()
		tx := types.NewTransaction(uint64(random), common.Address{0x1, 0x2, 0x3}, big.NewInt(int64(random)), 100, big.NewInt(int64(random)), nil)
		txs = append(txs, tx)
	}
	return txs
}

type txfetcherTester struct {
	fetcher *TxFetcher

	sender     *ecdsa.PrivateKey
	senderAddr common.Address
	signer     types.Signer
	txs        map[common.Hash]*types.Transaction
	dropped    map[string]struct{}
	lock       sync.RWMutex
}

func newTxFetcherTester() *txfetcherTester {
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)
	t := &txfetcherTester{
		sender:     key,
		senderAddr: addr,
		signer:     types.NewEIP155Signer(big.NewInt(1)),
		txs:        make(map[common.Hash]*types.Transaction),
		dropped:    make(map[string]struct{}),
	}
	t.fetcher = NewTxFetcher(t.hasTx, t.addTxs, t.dropPeer)
	t.fetcher.Start()
	return t
}

func (t *txfetcherTester) hasTx(hash common.Hash) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.txs[hash] != nil
}

func (t *txfetcherTester) addTxs(txs []*types.Transaction) []error {
	t.lock.Lock()
	defer t.lock.Unlock()

	var errors []error
	for _, tx := range txs {
		// Make sure the transaction is signed properly
		_, err := types.Sender(t.signer, tx)
		if err != nil {
			errors = append(errors, core.ErrInvalidSender)
		} else {
			t.txs[tx.Hash()] = tx
		}
	}
	return errors
}

func (t *txfetcherTester) dropPeer(id string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.dropped[id] = struct{}{}
}

// makeTxFetcher retrieves a batch of transaction associated with a simulated peer.
func (t *txfetcherTester) makeTxFetcher(peer string, txs []*types.Transaction) func(hashes []common.Hash) error {
	closure := make(map[common.Hash]*types.Transaction)
	for _, tx := range txs {
		closure[tx.Hash()] = tx
	}
	return func(hashes []common.Hash) error {
		var txs []*types.Transaction
		for _, hash := range hashes {
			tx := closure[hash]
			if tx == nil {
				continue
			}
			txs = append(txs, tx)
		}
		// Return on a new thread
		go t.fetcher.EnqueueTxs(peer, txs)
		return nil
	}
}

func TestSequentialTxAnnouncements(t *testing.T) {
	tester := newTxFetcherTester()
	txs := makeTransactions(tester.sender, txAnnounceLimit)

	retrieveTxs := tester.makeTxFetcher("peer", txs)

	newTxsCh := make(chan struct{})
	tester.fetcher.importTxsHook = func(transactions []*types.Transaction) {
		newTxsCh <- struct{}{}
	}

	for _, tx := range txs {
		tester.fetcher.Notify("peer", tx.Hash(), time.Now().Add(-arriveTimeout), retrieveTxs)
		select {
		case <-newTxsCh:
		case <-time.NewTimer(time.Second).C:
			t.Fatalf("timeout")
		}
	}
	if len(tester.txs) != len(txs) {
		t.Fatalf("Imported transaction number mismatch, want %d, got %d", len(txs), len(tester.txs))
	}
}

func TestConcurrentAnnouncements(t *testing.T) {
	tester := newTxFetcherTester()
	txs := makeTransactions(tester.sender, txAnnounceLimit)

	txFetcherFn1 := tester.makeTxFetcher("peer1", txs)
	txFetcherFn2 := tester.makeTxFetcher("peer2", txs)

	var (
		count uint32
		done  = make(chan struct{})
	)
	tester.fetcher.importTxsHook = func(transactions []*types.Transaction) {
		if len(transactions) > MaxTransactionFetch {
			t.Fatalf("Requested transaction number exceeds the limit, now: %d, limit: %d", len(transactions), MaxTransactionFetch)
		}
		atomic.AddUint32(&count, uint32(len(transactions)))

		if atomic.LoadUint32(&count) >= txAnnounceLimit {
			done <- struct{}{}
		}
	}

	for _, tx := range txs {
		tester.fetcher.Notify("peer1", tx.Hash(), time.Now().Add(-arriveTimeout), txFetcherFn1)
		tester.fetcher.Notify("peer2", tx.Hash(), time.Now().Add(-arriveTimeout+time.Millisecond), txFetcherFn2)
		tester.fetcher.Notify("peer2", tx.Hash(), time.Now().Add(-arriveTimeout-time.Millisecond), txFetcherFn2)
	}
	select {
	case <-done:
	case <-time.NewTimer(time.Second).C:
		t.Fatalf("timeout")
	}
}

func TestBatchAnnouncements(t *testing.T) {
	tester := newTxFetcherTester()
	txs := makeTransactions(tester.sender, txAnnounceLimit*2)

	retrieveTxs := tester.makeTxFetcher("peer", txs)

	for _, tx := range txs {
		tester.fetcher.Notify("peer", tx.Hash(), time.Now(), retrieveTxs)
	}

	var (
		count uint32
		done  = make(chan struct{})
	)
	tester.fetcher.importTxsHook = func(transactions []*types.Transaction) {
		if len(transactions) > MaxTransactionFetch {
			t.Fatalf("Requested transaction number exceeds the limit, now: %d, limit: %d", len(transactions), MaxTransactionFetch)
		}
		atomic.AddUint32(&count, uint32(len(transactions)))

		if atomic.LoadUint32(&count) >= txAnnounceLimit {
			done <- struct{}{}
		}
	}

	time.Sleep(arriveTimeout)
	select {
	case <-done:
	case <-time.NewTimer(time.Second).C:
		t.Fatalf("timeout")
	}
}

func TestEnqueueTransactions(t *testing.T) {
	tester := newTxFetcherTester()
	txs := makeTransactions(tester.sender, txAnnounceLimit)

	tester.fetcher.EnqueueTxs("peer", txs)

	done := make(chan struct{})
	tester.fetcher.importTxsHook = func(transactions []*types.Transaction) {
		if len(transactions) == txAnnounceLimit {
			done <- struct{}{}
		}
	}
	select {
	case <-done:
	case <-time.NewTimer(time.Second).C:
		t.Fatalf("timeout")
	}
}

func TestInvalidTxAnnounces(t *testing.T) {
	tester := newTxFetcherTester()
	txs := makeUnsignedTransactions(tester.sender, 1)

	txFetcherFn := tester.makeTxFetcher("peer", txs)

	dropped := make(chan string, 1)
	tester.fetcher.dropHook = func(s string) { dropped <- s }

	for _, tx := range txs {
		tester.fetcher.Notify("peer", tx.Hash(), time.Now(), txFetcherFn)
	}
	select {
	case s := <-dropped:
		if s != "peer" {
			t.Fatalf("invalid dropped peer")
		}
	case <-time.NewTimer(time.Second).C:
		t.Fatalf("timeout")
	}
}
