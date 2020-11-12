// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"github.com/ethereum/go-ethereum/ethdb"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/pruner"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256(nil)
)

var (
	snapshotCommand = cli.Command{
		Name:        "snapshot",
		Usage:       "A set of commands based on the snapshot",
		Category:    "MISCELLANEOUS COMMANDS",
		Description: "",
		Subcommands: []cli.Command{
			{
				Name:      "prune-state",
				Usage:     "Prune stale ethereum state data based on the snapshot",
				ArgsUsage: "<root>",
				Action:    utils.MigrateFlags(pruneState),
				Category:  "MISCELLANEOUS COMMANDS",
				Flags: []cli.Flag{
					utils.DataDirFlag,
					utils.RopstenFlag,
					utils.RinkebyFlag,
					utils.GoerliFlag,
					utils.LegacyTestnetFlag,
					utils.CacheTrieJournalFlag,
					utils.BloomFilterSizeFlag,
				},
				Description: `
geth snapshot prune-state <state-root>
will prune historical state data with the help of the state snapshot.
All trie nodes and contract codes that do not belong to the specified
version state will be deleted from the database. After pruning, only
two version states are available: genesis and the specific one.

The default pruning target is the HEAD-127 state.

WARNING: It's necessary to delete the trie clean cache after the pruning.
If you specify another directory for the trie clean cache via "--cache.trie.journal"
during the use of Geth, please also specify it here for correct deletion. Otherwise
the trie clean cache with default directory will be deleted.
`,
			},
			{
				Name:      "verify-state",
				Usage:     "Recalculate state hash based on the snapshot for verification",
				ArgsUsage: "<root>",
				Action:    utils.MigrateFlags(verifyState),
				Category:  "MISCELLANEOUS COMMANDS",
				Flags: []cli.Flag{
					utils.DataDirFlag,
					utils.RopstenFlag,
					utils.RinkebyFlag,
					utils.GoerliFlag,
					utils.LegacyTestnetFlag,
				},
				Description: `
geth snapshot verify-state <state-root>
will traverse the whole accounts and storages set based on the specified
snapshot and recalculate the root hash of state for verification.
In other words, this command does the snapshot to trie conversion.
`,
			},
			{
				Name:      "repair-state",
				Usage:     "Repair the broken state based on the snapshot",
				ArgsUsage: "<path> <root> [accounthash]",
				Action:    utils.MigrateFlags(repairState),
				Category:  "MISCELLANEOUS COMMANDS",
				Flags: []cli.Flag{
					utils.DataDirFlag,
					utils.RopstenFlag,
					utils.RinkebyFlag,
					utils.GoerliFlag,
					utils.LegacyTestnetFlag,
				},
				Description: `
geth snapshot repair-state <path> <state-root> [account-hash]
will repair the broken state with the speific path by regenerating the missing
state. The assumption is held that the snapshot data is not broken, otherwise 
there is no guarantee the regenerated states are correct.

In order to make sure the snapshot is still usable, please run the 
"geth snapshot verify-state [target_state_root]" first to see if any corruption
is detected.
`,
			},
			{
				Name:      "traverse-state",
				Usage:     "Traverse the state with given root hash for verification",
				ArgsUsage: "<root>",
				Action:    utils.MigrateFlags(traverseState),
				Category:  "MISCELLANEOUS COMMANDS",
				Flags: []cli.Flag{
					utils.DataDirFlag,
					utils.RopstenFlag,
					utils.RinkebyFlag,
					utils.GoerliFlag,
					utils.LegacyTestnetFlag,
				},
				Description: `
geth snapshot traverse-state <state-root>
will traverse the whole state from the given state root and will abort if any
referenced trie node or contract code is missing. This command can be used for
state integrity verification. The default checking target is the HEAD state.

It's also usable without snapshot enabled.
`,
			},
			{
				Name:      "traverse-rawstate",
				Usage:     "Traverse the state with given root hash for verification",
				ArgsUsage: "<root>",
				Action:    utils.MigrateFlags(traverseRawState),
				Category:  "MISCELLANEOUS COMMANDS",
				Flags: []cli.Flag{
					utils.DataDirFlag,
					utils.RopstenFlag,
					utils.RinkebyFlag,
					utils.GoerliFlag,
					utils.LegacyTestnetFlag,
				},
				Description: `
geth snapshot traverse-rawstate <state-root>
will traverse the whole state from the given root and will abort if any referenced
trie node or contract code is missing. This command can be used for state integrity
verification. The default checking target is the HEAD state. It's basically identical
to traverse-state, but the check granularity is smaller. 

It's also usable without snapshot enabled.
`,
			},
		},
	}
)

func pruneState(ctx *cli.Context) error {
	stack, config := makeConfigNode(ctx)
	defer stack.Close()

	chain, chaindb := utils.MakeChain(ctx, stack, true)
	defer chaindb.Close()

	pruner, err := pruner.NewPruner(chaindb, chain.CurrentBlock().Header(), stack.ResolvePath(""), stack.ResolvePath(config.Eth.TrieCleanCacheJournal), ctx.GlobalUint64(utils.BloomFilterSizeFlag.Name))
	if err != nil {
		utils.Fatalf("Failed to open snapshot tree %v", err)
	}
	if ctx.NArg() > 1 {
		utils.Fatalf("Too many arguments given")
	}
	var targetRoot common.Hash
	if ctx.NArg() == 1 {
		targetRoot, err = parseRoot(ctx.Args()[0])
		if err != nil {
			utils.Fatalf("Failed to resolve state root %v", err)
		}
	}
	if err = pruner.Prune(targetRoot); err != nil {
		utils.Fatalf("Failed to prune state %v", err)
	}
	return nil
}

func verifyState(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, chaindb := utils.MakeChain(ctx, stack, true)
	defer chaindb.Close()

	snaptree, err := snapshot.New(chaindb, trie.NewDatabase(chaindb), chain.CurrentBlock().Root(), snapshot.Config{
		Recovery:   false,
		AsyncBuild: false,
		NoBuild:    true,
		Cache:      256,
	})
	if err != nil {
		log.Crit("Failed to open the snapshot tree", "error", err)
	}
	if ctx.NArg() > 1 {
		log.Crit("Too many arguments given")
	}
	var root = chain.CurrentBlock().Root()
	if ctx.NArg() == 1 {
		root, err = parseRoot(ctx.Args()[0])
		if err != nil {
			utils.Fatalf("Failed to resolve state root %v", err)
		}
	}
	if err := snaptree.Verify(root); err != nil {
		log.Crit("Failed to verfiy state", "error", err)
	} else {
		log.Info("Verified the state")
	}
	return nil
}

// getStateRange retrieves a batch of states(accounts or slots) with the
// target key range.
//
// TODO(rjl493456442) we have to limit the range, otherwise the OOM will happen.
func getStateRange(snaptree *snapshot.Tree, root common.Hash, accountHash common.Hash, start, limit []byte) ([][]byte, [][]byte, error) {
	var (
		err        error
		iterator   snapshot.Iterator
		keys, vals [][]byte
	)
	if accountHash != (common.Hash{}) {
		iterator, err = snaptree.StorageIterator(root, accountHash, common.Hash{})
		if err != nil {
			return nil, nil, err
		}
		for iterator.Next() {
			keys = append(keys, common.CopyBytes(iterator.Hash().Bytes()))
			vals = append(vals, common.CopyBytes(iterator.(snapshot.StorageIterator).Slot()))
		}
	} else {
		iterator, err = snaptree.AccountIterator(root, common.Hash{})
		if err != nil {
			return nil, nil, err
		}
		for iterator.Next() {
			keys = append(keys, common.CopyBytes(iterator.Hash().Bytes()))
			vals = append(vals, common.CopyBytes(iterator.(snapshot.AccountIterator).Account()))
		}
	}
	log.Info("Retrieved the state in the specific range", "entries", len(keys))
	return keys, vals, nil
}

// pathNeighbors calculates the neighbors of the specific path.
func pathNeighbors(path []byte, minPrefix int) ([]byte, []byte) {
	var (
		prev []byte
		next []byte
	)
	// Calculate the predecessor
	prefix := 0
	for i, c := range path {
		if prefix < minPrefix {
			prefix += 1
			continue
		}
		if c != 0x0 {
			prev = append(prev, path[:i+1]...)
			prev[len(prev)-1]--
			break
		}
	}
	// Calculate the successor
	prefix = 0
	for i, c := range path {
		if prefix < minPrefix {
			prefix += 1
			continue
		}
		if c != 0xff {
			next = append(next, path[:i+1]...)
			next[len(next)-1]++
			break
		}
	}
	log.Info("Resolved the path neighbors", "path", path, "prev", prev, "next", next)
	return prev, next
}

func commitFixedState(fixdb ethdb.KeyValueStore, maindb ethdb.Database) int {
	var committed int
	fixBatch := maindb.NewBatch()
	fixIter := fixdb.NewIterator(nil, nil)
	for fixIter.Next() {
		fixBatch.Put(fixIter.Key(), fixIter.Value())
		if fixBatch.ValueSize() > ethdb.IdealBatchSize {
			fixBatch.Write()
			fixBatch.Reset()
		}
		committed += 1
	}
	if fixBatch.ValueSize() > 0 {
		fixBatch.Write()
		fixBatch.Reset()
	}
	fixIter.Release()
	return committed
}

func repairState(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, chaindb := utils.MakeChain(ctx, stack, true)
	defer chaindb.Close()

	// Initialize the snapshot tree in recovery mode. The state database
	// may already be broken. So it's totally fine to heal the state from
	// the existent snapshot even they are not exactly paired.
	snaptree, err := snapshot.New(chaindb, trie.NewDatabase(chaindb), chain.CurrentBlock().Root(), snapshot.Config{
		Recovery:   true,
		AsyncBuild: false,
		NoBuild:    true,
		Cache:      256,
	})
	if err != nil {
		log.Crit("Failed to open the snapshot tree", "error", err)
	}
	var (
		path        []byte
		root        common.Hash
		accountHash common.Hash
		stateRoot   common.Hash
	)
	if ctx.NArg() < 2 {
		log.Crit("At least two arguments are expected")
	}
	path = common.FromHex(ctx.Args()[0])
	root, err = parseRoot(ctx.Args()[1])
	if err != nil {
		utils.Fatalf("Failed to resolve state root %v", err)
	}
	if len(ctx.Args()) > 2 {
		accountHash, err = parseRoot(ctx.Args()[2])
		if err != nil {
			utils.Fatalf("Failed to resolve account hash %v", err)
		}
	}
	// Open the specific state trie for repairing by default.
	triedb := trie.NewDatabase(chaindb)

	// TODO what if the root is missing(rjl493456442). Regenerate the
	// entire tree by snapshot?
	t, err := trie.NewSecure(root, triedb)
	if err != nil {
		log.Crit("Failed to open trie", "root", root, "error", err)
	}
	stateRoot = root

	// If the storage trie is specified, switch to the storage trie
	// for repairing.
	if accountHash != (common.Hash{}) {
		// TODO what if the sepcifed account is unaceesable(rjl493456442)?
		blob := t.Get(accountHash.Bytes())
		if len(blob) == 0 {
			log.Crit("Failed to load the target account")
		}
		var acc state.Account
		if err := rlp.DecodeBytes(blob, &acc); err != nil {
			log.Crit("Failed to decode the account", "error", err)
		}
		// TODO what if the root is missing(rjl493456442). Regenerate the
		// entire tree by snapshot?
		t, err = trie.NewSecure(acc.Root, triedb)
		if err != nil {
			log.Crit("Failed to open storage trie", "root", root, "account", accountHash, "error", err)
		}
		stateRoot = acc.Root
	}
	// Derive the neighbors of the target path, calculate the edge proofs.
	// The non-existent proofs are also valid.
	prev, next := pathNeighbors(path, len(path)-1)
	proofdb := rawdb.NewMemoryDatabase()
	if err := t.Prove(prev, 0, proofdb); err != nil {
		log.Crit("Failed to generate edge proof", "error", err)
	}
	if err := t.Prove(next, 0, proofdb); err != nil {
		log.Crit("Failed to generate edge proof", "error", err)
	}
	keys, vals, err := getStateRange(snaptree, root, accountHash, prev, next)
	if err != nil {
		log.Crit("Failed to get state range", "error", err)
	}
	fixdb, _, _, err := trie.VerifyRangeProof(stateRoot, prev, next, keys, vals, proofdb)
	if err != nil {
		log.Crit("Failed to verify range proof", "error", err)
	}
	committed := commitFixedState(fixdb, chaindb)
	log.Info("Corrupted database is fixed", "committed", committed)
	return nil
}

// traverseState is a helper function used for pruning verification.
// Basically it just iterates the trie, ensure all nodes and associated
// contract codes are present.
func traverseState(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, chaindb := utils.MakeChain(ctx, stack, true)
	defer chaindb.Close()

	if ctx.NArg() > 1 {
		log.Crit("Too many arguments given")
	}
	// Use the HEAD root as the default
	head := chain.CurrentBlock()
	if head == nil {
		log.Crit("Head block is missing")
	}
	var (
		root common.Hash
		err  error
	)
	if ctx.NArg() == 1 {
		root, err = parseRoot(ctx.Args()[0])
		if err != nil {
			utils.Fatalf("Failed to resolve state root %v", err)
		}
		log.Info("Start traversing the state", "root", root)
	} else {
		root = head.Root()
		log.Info("Start traversing the state", "root", root, "number", head.NumberU64())
	}
	triedb := trie.NewDatabase(chaindb)
	t, err := trie.NewSecure(root, triedb)
	if err != nil {
		log.Crit("Failed to open trie", "root", root, "error", err)
	}
	var (
		accounts   int
		slots      int
		codes      int
		lastReport time.Time
		start      = time.Now()
	)
	accIter := trie.NewIterator(t.NodeIterator(nil))
	for accIter.Next() {
		accounts += 1
		var acc state.Account
		if err := rlp.DecodeBytes(accIter.Value, &acc); err != nil {
			log.Crit("Invalid account encountered during traversal", "error", err)
		}
		if acc.Root != emptyRoot {
			storageTrie, err := trie.NewSecure(acc.Root, triedb)
			if err != nil {
				log.Crit("Failed to open storage trie", "root", acc.Root, "error", err)
			}
			storageIter := trie.NewIterator(storageTrie.NodeIterator(nil))
			for storageIter.Next() {
				slots += 1
			}
			if storageIter.Err != nil {
				log.Crit("Failed to traverse storage trie", "root", acc.Root, "error", storageIter.Err)
			}
		}
		if !bytes.Equal(acc.CodeHash, emptyCode) {
			code := rawdb.ReadCode(chaindb, common.BytesToHash(acc.CodeHash))
			if len(code) == 0 {
				log.Crit("Code is missing", "hash", common.BytesToHash(acc.CodeHash))
			}
			codes += 1
		}
		if time.Since(lastReport) > time.Second*8 {
			log.Info("Traversing state", "accounts", accounts, "slots", slots, "codes", codes, "elapsed", common.PrettyDuration(time.Since(start)))
			lastReport = time.Now()
		}
	}
	if accIter.Err != nil {
		log.Crit("Failed to traverse state trie", "root", root, "error", accIter.Err)
	}
	log.Info("State is complete", "accounts", accounts, "slots", slots, "codes", codes, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// traverseRawState is a helper function used for pruning verification.
// Basically it just iterates the trie, ensure all nodes and associated
// contract codes are present. It's basically identical to traverseState
// but it will check each trie node.
func traverseRawState(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, chaindb := utils.MakeChain(ctx, stack, true)
	defer chaindb.Close()

	if ctx.NArg() > 1 {
		log.Crit("Too many arguments given")
	}
	// Use the HEAD root as the default
	head := chain.CurrentBlock()
	if head == nil {
		log.Crit("Head block is missing")
	}
	var (
		root common.Hash
		err  error
	)
	if ctx.NArg() == 1 {
		root, err = parseRoot(ctx.Args()[0])
		if err != nil {
			utils.Fatalf("Failed to resolve state root %v", err)
		}
		log.Info("Start traversing the state", "root", root)
	} else {
		root = head.Root()
		log.Info("Start traversing the state", "root", root, "number", head.NumberU64())
	}
	triedb := trie.NewDatabase(chaindb)
	t, err := trie.NewSecure(root, triedb)
	if err != nil {
		log.Crit("Failed to open trie", "root", root, "error", err)
	}
	var (
		nodes      int
		accounts   int
		slots      int
		codes      int
		lastReport time.Time
		start      = time.Now()
	)
	accIter := t.NodeIterator(nil)
	for accIter.Next(true) {
		nodes += 1
		node := accIter.Hash()

		if node != (common.Hash{}) {
			// Check the present for non-empty hash node(embedded node doesn't
			// have their own hash).
			blob := rawdb.ReadTrieNode(chaindb, node)
			if len(blob) == 0 {
				log.Crit("Missing trie node(account)", "hash", node)
			}
		}
		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			accounts += 1
			var acc state.Account
			if err := rlp.DecodeBytes(accIter.LeafBlob(), &acc); err != nil {
				log.Crit("Invalid account encountered during traversal", "error", err)
			}
			if acc.Root != emptyRoot {
				storageTrie, err := trie.NewSecure(acc.Root, triedb)
				if err != nil {
					log.Crit("Failed to open storage trie", "root", acc.Root, "error", err)
				}
				storageIter := storageTrie.NodeIterator(nil)
				for storageIter.Next(true) {
					nodes += 1
					node := storageIter.Hash()

					// Check the present for non-empty hash node(embedded node doesn't
					// have their own hash).
					if node != (common.Hash{}) {
						blob := rawdb.ReadTrieNode(chaindb, node)
						if len(blob) == 0 {
							log.Crit("Missing trie node(storage)", "hash", node)
						}
					}
					// Bump the counter if it's leaf node.
					if storageIter.Leaf() {
						slots += 1
					}
				}
				if storageIter.Error() != nil {
					log.Crit("Failed to traverse storage trie", "root", acc.Root, "error", storageIter.Error())
				}
			}
			if !bytes.Equal(acc.CodeHash, emptyCode) {
				code := rawdb.ReadCode(chaindb, common.BytesToHash(acc.CodeHash))
				if len(code) == 0 {
					log.Crit("Code is missing", "account", common.BytesToHash(accIter.LeafKey()))
				}
				codes += 1
			}
			if time.Since(lastReport) > time.Second*8 {
				log.Info("Traversing state", "nodes", nodes, "accounts", accounts, "slots", slots, "codes", codes, "elapsed", common.PrettyDuration(time.Since(start)))
				lastReport = time.Now()
			}
		}
	}
	if accIter.Error() != nil {
		log.Crit("Failed to traverse state trie", "root", root, "error", accIter.Error())
	}
	log.Info("State is complete", "nodes", nodes, "accounts", accounts, "slots", slots, "codes", codes, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

func parseRoot(input string) (common.Hash, error) {
	var h common.Hash
	if err := h.UnmarshalText([]byte(input)); err != nil {
		return h, err
	}
	return h, nil
}
