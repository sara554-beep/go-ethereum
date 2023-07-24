package pathdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/exp/slices"
)

type topAccount struct {
	hash   common.Hash
	modify uint64

	descSize common.StorageSize
	dataSize common.StorageSize
}

type acctStats struct {
	topN     int
	accounts []topAccount

	totalAccounts int
	totalModifies uint64
	totalSize     common.StorageSize
}

func (t *acctStats) addAccount(hash common.Hash, modify uint64, descSize, dataSize common.StorageSize) {
	t.totalAccounts += 1
	t.totalModifies += modify
	t.totalSize += descSize + dataSize

	if len(t.accounts) < t.topN {
		t.accounts = append(t.accounts, topAccount{
			hash:     hash,
			modify:   modify,
			descSize: descSize,
			dataSize: dataSize,
		})
	} else {
		if t.accounts[len(t.accounts)-1].modify > modify {
			return
		}
		t.accounts = t.accounts[:len(t.accounts)-1]
		t.accounts = append(t.accounts, topAccount{hash: hash, modify: modify})
	}
	slices.SortFunc(t.accounts, func(a, b topAccount) bool {
		return a.modify >= b.modify
	})
}

func (t *acctStats) Report() {
	for i := 0; i < len(t.accounts); i++ {
		log.Info("Top account",
			"i", i+1, "account", t.accounts[i].hash.Hex(), "modify", t.accounts[i].modify,
			"descsize", t.accounts[i].descSize, "datasize", t.accounts[i].dataSize)
	}
	log.Info("Total accounts", "number", t.totalAccounts,
		"average-modify", int(float64(t.totalModifies)/float64(t.totalAccounts)),
		"average-size", t.totalSize/common.StorageSize(t.totalAccounts))
}

type Traverser struct {
	db      ethdb.KeyValueStore
	freezer *rawdb.ResettableFreezer
}

func NewTraverser(db ethdb.KeyValueStore, freezer *rawdb.ResettableFreezer) *Traverser {
	return &Traverser{
		db:      db,
		freezer: freezer,
	}
}

func increaseKey(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		key[i]++
		if key[i] != 0x0 {
			break
		}
	}
	return key
}

func (t *Traverser) processAccount(stats *acctStats, account common.Hash) {
	index, ids, blocks := rawdb.ReadAllAccountState(t.db, account)
	descList, err := parseIndex(index)
	if err != nil {
		log.Error("Failed to decode account meta", "err", err)
		return
	}
	if len(descList) != len(ids) {
		log.Error("account index blocks are not aligned", "meta", len(descList), "blocks", len(ids))
		return
	}
	var (
		blockSize   common.StorageSize
		accountList []uint64
	)
	for i, desc := range descList {
		if desc.id != ids[i] {
			log.Error("account index block id is not aligned", "meta", desc.id, "disk", ids[i])
			return
		}
		blockSize += common.StorageSize(len(blocks[i]))

		restarts, blob, err := parseIndexBlock(blocks[i])
		if err != nil {
			log.Error("Failed to decode account block", "err", err)
			return
		}
		var (
			cursor int
			list   []uint64
		)
		for cursor < len(blob) {
			if cursor != int(restarts[0]) {
				panic(fmt.Sprintf("Uncontinous segment, cursor: %d, restart: %d", cursor, restarts[0]))
			}
			v, nn := binary.Uvarint(blob[restarts[0]:])
			if nn < 0 {
				panic("failed to read item")
			}
			restarts = restarts[1:]
			list = append(list, v)
			cursor += nn

			limit := len(blob)
			if len(restarts) > 0 {
				limit = int(restarts[0])
			}
			for cursor < limit {
				delta, nn := binary.Uvarint(blob[cursor:])
				if nn < 0 {
					panic("failed to read item")
				}
				v = v + delta
				list = append(list, v)
				cursor += nn
			}
		}
		accountList = append(accountList, list...)
	}
	for i, n := range accountList {
		if i == 0 {
			continue
		}
		if n <= accountList[i-1] {
			panic("invalid state index")
		}
	}
	stats.addAccount(account, uint64(len(accountList)), common.StorageSize(len(index)), blockSize)
}

func (t *Traverser) Traverse() {
	head := rawdb.ReadStateIndexHead(t.db)
	if head == nil {
		log.Info("Unindexed state history")
		return
	}
	tail, err := t.freezer.Tail()
	if err != nil {
		log.Error("Failed to retrieve freezer tail", "err", err)
	}
	log.Info("Indexing range", "tail", tail, "head", *head)

	var (
		top   = &acctStats{topN: 20}
		start []byte
	)
	for {
		accounts, _, _ := rawdb.ReadAllStateIndex(t.db, start)
		if len(accounts) <= 1 {
			break
		}
		start = increaseKey(accounts[len(accounts)-1].Bytes())
		if len(start) > 0 {
			log.Info("Read accounts", "number", len(accounts), "at", start[0])
		}
		for _, account := range accounts {
			t.processAccount(top, account)
		}
	}
	top.Report()
}
