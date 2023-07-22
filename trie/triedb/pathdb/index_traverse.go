package pathdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"math"
)

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
		accountN          int
		accountIndexMax   int
		accountIndexMin   = math.MaxInt
		accountIndexTotal uint64
		start             []byte
	)
	for {
		accounts, _, _ := rawdb.ReadAllStateIndex(t.db, start)
		if len(accounts) <= 1 {
			break
		}
		accountN += len(accounts)
		start = increaseKey(accounts[len(accounts)-1].Bytes())

		log.Info("Total accounts", "number", len(accounts))

		for _, account := range accounts {
			index, ids, blocks := rawdb.ReadAllAccountState(t.db, account)

			descList, err := parseIndex(index)
			if err != nil {
				log.Error("Failed to decode account meta", "err", err)
				continue
			}
			if len(descList) != len(ids) {
				log.Error("account index blocks are not aligned", "meta", len(descList), "blocks", len(ids))
				continue
			}
			var accountList []uint64
			for i, desc := range descList {
				if desc.id != ids[i] {
					log.Error("account index block id is not aligned", "meta", desc.id, "disk", ids[i])
				}
				restarts, blob, err := parseIndexBlock(blocks[i])
				if err != nil {
					log.Error("Failed to decode account block", "err", err)
					continue
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
			for xx, ii := range accountList {
				if xx == 0 {
					continue
				}
				if ii <= accountList[xx-1] {
					log.Error("Invalid state index")
				}
			}
			if len(accountList) > accountIndexMax {
				accountIndexMax = len(accountList)
			}
			if len(accountList) < accountIndexMin {
				accountIndexMin = len(accountList)
			}
			accountIndexTotal += uint64(len(accountList))
		}
	}

	log.Info("State history index statistics",
		"accounts", accountN, "average account index", accountIndexTotal/uint64(accountN),
		"max account index", accountIndexMax, "min account index", accountIndexMin)
}
