// Copyright 2021 The go-ethereum Authors
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

package snap

import (
	"container/list"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	// GenerationTrigger is the minimum threshold at which a generation task is
	// triggered. The value shouldn't be too small in order to group as many
	// duplicated accounts as possible.
	// For a single generation task, the size is 32(1 + n) where n is the duplication
	// factor. So the value is picked to get a reasonable duplication factor.
	generationTrigger = 1024 * 1024

	// The hard limit of the memory allowance for caching storage generation tasks.
	// All the new tasks will be dropped silently if the memory usage is exceeded.
	maxCacheSize = 128 * 1024 * 1024
)

type storageSnap struct {
	root     common.Hash
	accounts []common.Hash
	node     *list.Element
}

func (s *storageSnap) size() int {
	return common.HashLength + len(s.accounts)*common.HashLength
}

func (s *storageSnap) run(db *trie.Database, batch ethdb.Batch) (int, int, int, error) {
	tr, err := trie.New(s.root, db)
	if err != nil {
		return 0, 0, 0, err
	}
	var (
		read  int
		write int
		iter  = trie.NewIterator(tr.NodeIterator(nil))
	)
	for iter.Next() {
		for _, account := range s.accounts {
			rawdb.WriteStorageSnapshot(batch, account, common.BytesToHash(iter.Key), iter.Value)
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return 0, 0, 0, err
			}
			batch.Reset()
		}
		read += len(iter.Key) + len(iter.Value)
		write = write + len(s.accounts)*(1+common.HashLength*2+len(iter.Value))
	}
	if iter.Err != nil {
		return 0, 0, 0, iter.Err
	}
	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			return 0, 0, 0, err
		}
	}
	return read, write, len(s.accounts), nil
}

type storageReq struct {
	root    common.Hash
	account common.Hash
}

type storageSnapScheduer struct {
	triedb *trie.Database
	disk   ethdb.KeyValueStore

	entries map[common.Hash]*storageSnap
	list    *list.List
	size    int

	reqs   chan *storageReq
	stop   chan chan struct{}
	closed chan struct{}
}

func newStorageSnapScheduler(triedb *trie.Database, disk ethdb.KeyValueStore) *storageSnapScheduer {
	sche := &storageSnapScheduer{
		triedb:  triedb,
		disk:    disk,
		entries: make(map[common.Hash]*storageSnap),
		list:    list.New(),
		reqs:    make(chan *storageReq),
		stop:    make(chan chan struct{}),
		closed:  make(chan struct{}),
	}
	go sche.loop()
	return sche
}

func (sche *storageSnapScheduer) close() {
	signal := make(chan struct{})
	sche.stop <- signal
	<-signal
}

func (sche *storageSnapScheduer) loop() {
	var (
		totalWrite    int
		totalRead     int
		totalAccounts int
		totalFailures int
		totalDropped  int
		done          chan struct{} // Non-nil if background executor is active.
	)
	for {
		if sche.size > generationTrigger && done == nil && sche.list.Len() != 0 {
			done = make(chan struct{})

			root := sche.list.Remove(sche.list.Front()).(common.Hash)
			task := sche.entries[root]
			go func(root common.Hash, task *storageSnap) {
				defer close(done)

				write, read, count, err := task.run(sche.triedb, sche.disk.NewBatch())
				if err != nil {
					totalFailures += 1
				} else {
					totalWrite += write
					totalRead += read
					totalAccounts += count
				}
				log.Trace("Storage snapshot generated", "root", root, "read", common.StorageSize(read), "write", common.StorageSize(write), "count", count, "err", err)
			}(root, task)
			delete(sche.entries, root)
			sche.size -= task.size()
		}
		select {
		case req := <-sche.reqs:
			if sche.size > maxCacheSize {
				totalDropped += 1
				continue
			}
			sche.add(req.root, req.account)

		case <-done:
			done = nil

		case signal := <-sche.stop:
			if done != nil {
				<-done
			}
			for _, task := range sche.entries {
				write, read, count, err := task.run(sche.triedb, sche.disk.NewBatch())
				if err != nil {
					totalFailures += 1
				} else {
					totalWrite += write
					totalRead += read
					totalAccounts += count
				}
			}
			sche.entries, sche.list = nil, nil
			close(sche.closed)
			close(signal)
			log.Trace("Generated storage snapshot", "read", common.StorageSize(totalRead), "write", common.StorageSize(totalWrite), "accounts", totalAccounts, "failures", totalFailures, "dropped", totalDropped)
			return
		}
	}
}

func (sche *storageSnapScheduer) add(root common.Hash, account common.Hash) {
	if entry, ok := sche.entries[root]; ok {
		entry.accounts = append(entry.accounts, account)
		sche.list.MoveToBack(entry.node)
		sche.size += common.HashLength
		return
	}
	entry := &storageSnap{root: root, accounts: []common.Hash{account}}
	entry.node = sche.list.PushBack(root)
	sche.entries[root] = entry
}

func (sche *storageSnapScheduer) tryAdd(root common.Hash, account common.Hash) {
	select {
	case sche.reqs <- &storageReq{root: root, account: account}:
	case <-sche.closed:
	}
}
