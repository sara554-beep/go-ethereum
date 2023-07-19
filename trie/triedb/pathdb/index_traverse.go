package pathdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
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

	accounts, _, _ := rawdb.ReadAllStateIndex(t.db)
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
			//xx := 0
			//var yy []uint64
			//for xx < len(blob) {
			//	v, nn := binary.Uvarint(blob[xx:])
			//	if nn < 0 {
			//		panic("failed to read item")
			//	}
			//	xx += nn
			//	yy = append(yy, v)
			//}
			//log.Info("blob data", "list", yy, "restarts", restarts, "blob", blob)
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
					//if delta == 0 {
					//	panic(fmt.Sprintf("invalid delta, nn: %d", nn))
					//}
					v = v + delta
					list = append(list, v)
					cursor += nn
				}
			}
			accountList = append(accountList, list...)
		}
		log.Info("Account change list", "account", account.Hex(), "list", accountList)
	}
}
