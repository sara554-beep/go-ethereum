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

package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/olekukonko/tablewriter"
)

var (
	// errUndefinedType is returned if non-existent freezer is required
	// to be accessed.
	errUndefinedType = errors.New("freezer type is not defined")
)

const (
	reverseDiffPrefix = "rdiffs" // The path of reverse diff freezer
)

// freezerdb is a database wrapper that enabled freezer data retrievals.
type freezerdb struct {
	ethdb.KeyValueStore
	chain *freezer
	rdiff *freezer
}

// HasAncient returns an indicator whether the specified data exists in the
// ancient store.
func (frdb *freezerdb) HasAncient(typ string, kind string, id uint64) (bool, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.HasAncient(kind, id)
	case ReverseDiffFreezer:
		return frdb.rdiff.HasAncient(kind, id)
	default:
		return false, errUndefinedType
	}
}

// Ancient retrieves an ancient binary blob from the append-only immutable files.
func (frdb *freezerdb) Ancient(typ string, kind string, id uint64) ([]byte, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.Ancient(kind, id)
	case ReverseDiffFreezer:
		return frdb.rdiff.Ancient(kind, id)
	default:
		return nil, errUndefinedType
	}
}

// AncientRange retrieves multiple items in sequence, starting from the index 'start'.
func (frdb *freezerdb) AncientRange(typ string, kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.AncientRange(kind, start, max, maxByteSize)
	case ReverseDiffFreezer:
		return frdb.rdiff.AncientRange(kind, start, max, maxByteSize)
	default:
		return nil, errUndefinedType
	}
}

// Ancients returns the ancient item numbers in the ancient store.
func (frdb *freezerdb) Ancients(typ string) (uint64, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.Ancients()
	case ReverseDiffFreezer:
		return frdb.rdiff.Ancients()
	default:
		return 0, errUndefinedType
	}
}

// Tail returns the number of first stored item in the freezer.
func (frdb *freezerdb) Tail(typ string) (uint64, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.Tail()
	case ReverseDiffFreezer:
		return frdb.rdiff.Tail()
	default:
		return 0, errUndefinedType
	}
}

// AncientSize returns the ancient size of the specified category.
func (frdb *freezerdb) AncientSize(typ string, kind string) (uint64, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.AncientSize(kind)
	case ReverseDiffFreezer:
		return frdb.rdiff.AncientSize(kind)
	default:
		return 0, errUndefinedType
	}
}

// ModifyAncients runs a write operation on the ancient store.
// If the function returns an error, any changes to the underlying store are reverted.
// The integer return value is the total size of the written data.
func (frdb *freezerdb) ModifyAncients(typ string, fn func(ethdb.AncientWriteOp) error) (int64, error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.ModifyAncients(fn)
	case ReverseDiffFreezer:
		return frdb.rdiff.ModifyAncients(fn)
	default:
		return 0, errUndefinedType
	}
}

// TruncateHead discards all but the first n ancient data from the ancient store.
func (frdb *freezerdb) TruncateHead(typ string, items uint64) error {
	switch typ {
	case ChainFreezer:
		return frdb.chain.TruncateHead(items)
	case ReverseDiffFreezer:
		return frdb.rdiff.TruncateHead(items)
	default:
		return errUndefinedType
	}
}

// TruncateTail discards the first n ancient data from the ancient store.
func (frdb *freezerdb) TruncateTail(typ string, tail uint64) error {
	switch typ {
	case ChainFreezer:
		return frdb.chain.TruncateTail(tail)
	case ReverseDiffFreezer:
		return frdb.rdiff.TruncateTail(tail)
	default:
		return errUndefinedType
	}
}

// Sync flushes all in-memory ancient store data to disk.
func (frdb *freezerdb) Sync(typ string) error {
	switch typ {
	case ChainFreezer:
		return frdb.chain.Sync()
	case ReverseDiffFreezer:
		return frdb.rdiff.Sync()
	default:
		return errUndefinedType
	}
}

// ReadAncients runs the given read operation while ensuring that no writes take place
// on the underlying freezer.
func (frdb *freezerdb) ReadAncients(typ string, fn func(reader ethdb.AncientReadOp) error) (err error) {
	switch typ {
	case ChainFreezer:
		return frdb.chain.ReadAncients(fn)
	case ReverseDiffFreezer:
		return frdb.rdiff.ReadAncients(fn)
	default:
		return errUndefinedType
	}
}

// Close implements io.Closer, closing both the fast key-value store as well as
// the slow ancient tables.
func (frdb *freezerdb) Close() error {
	var errs []error
	if err := frdb.KeyValueStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := frdb.chain.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := frdb.rdiff.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// Freeze is a helper method used for external testing to trigger and block until
// a freeze cycle completes, without having to sleep for a minute to trigger the
// automatic background run.
func (frdb *freezerdb) Freeze(threshold uint64) error {
	if frdb.chain.readonly {
		return errReadOnly
	}
	// Set the freezer threshold to a temporary value
	defer func(old uint64) {
		atomic.StoreUint64(&frdb.chain.threshold, old)
	}(atomic.LoadUint64(&frdb.chain.threshold))
	atomic.StoreUint64(&frdb.chain.threshold, threshold)

	// Trigger a freeze cycle and block until it's done
	trigger := make(chan struct{}, 1)
	frdb.chain.trigger <- trigger
	<-trigger
	return nil
}

// nofreezedb is a database wrapper that disables freezer data retrievals.
type nofreezedb struct {
	ethdb.KeyValueStore
}

// HasAncient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) HasAncient(typ string, kind string, number uint64) (bool, error) {
	return false, errNotSupported
}

// Ancient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancient(typ string, kind string, number uint64) ([]byte, error) {
	return nil, errNotSupported
}

// AncientRange returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientRange(typ string, kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return nil, errNotSupported
}

// Ancients returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancients(typ string) (uint64, error) {
	return 0, errNotSupported
}

// Tail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Tail(typ string) (uint64, error) {
	return 0, errNotSupported
}

// AncientSize returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientSize(typ string, kind string) (uint64, error) {
	return 0, errNotSupported
}

// ModifyAncients returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) ModifyAncients(typ string, fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, errNotSupported
}

// TruncateHead returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateHead(typ string, items uint64) error {
	return errNotSupported
}

// TruncateTail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateTail(typ string, tail uint64) error {
	return errNotSupported
}

// Sync returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Sync(typ string) error {
	return errNotSupported
}

type nullAncientReadOp struct{}

// HasAncient returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) HasAncient(kind string, number uint64) (bool, error) {
	return false, errNotSupported
}

// Ancient returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, errNotSupported
}

// AncientRange returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) AncientRange(kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return nil, errNotSupported
}

// Ancients returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// Tail returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) Tail() (uint64, error) {
	return 0, errNotSupported
}

// AncientSize returns an error as we don't have a backing chain freezer.
func (db *nullAncientReadOp) AncientSize(kind string) (uint64, error) {
	return 0, errNotSupported
}

func (db *nofreezedb) ReadAncients(typ string, fn func(reader ethdb.AncientReadOp) error) (err error) {
	// Unlike other ancient-related methods, this method does not return
	// errNotSupported when invoked.
	// The reason for this is that the caller might want to do several things:
	// 1. Check if something is in freezer,
	// 2. If not, check leveldb.
	//
	// This will work, since the ancient-checks inside 'fn' will return errors,
	// and the leveldb work will continue.
	//
	// If we instead were to return errNotSupported here, then the caller would
	// have to explicitly check for that, having an extra clause to do the
	// non-ancient operations.
	return fn(&nullAncientReadOp{})
}

// NewDatabase creates a high level database on top of a given key-value data
// store without a freezer moving immutable chain segments into cold storage.
func NewDatabase(db ethdb.KeyValueStore) ethdb.Database {
	return &nofreezedb{KeyValueStore: db}
}

// NewDatabaseWithFreezer creates a high level database on top of a given key-
// value data store with a freezer moving immutable chain segments into cold
// storage.
func NewDatabaseWithFreezer(db ethdb.KeyValueStore, freezer string, namespace string, readonly bool) (ethdb.Database, error) {
	// Create the ancient chain freezer
	ancientChain, err := newFreezer(freezer, namespace, readonly, freezerTableSize, ChainFreezerNoSnappy)
	if err != nil {
		return nil, err
	}
	rdiff, err := newFreezer(path.Join(freezer, reverseDiffPrefix), namespace, readonly, freezerTableSize, ReveseDiffFreezerNoSnappy)
	if err != nil {
		return nil, err
	}
	// Since the freezer can be stored separately from the user's key-value database,
	// there's a fairly high probability that the user requests invalid combinations
	// of the freezer and database. Ensure that we don't shoot ourselves in the foot
	// by serving up conflicting data, leading to both datastores getting corrupted.
	//
	//   - If both the freezer and key-value store is empty (no genesis), we just
	//     initialized a new empty freezer, so everything's fine.
	//   - If the key-value store is empty, but the freezer is not, we need to make
	//     sure the user's genesis matches the freezer. That will be checked in the
	//     blockchain, since we don't have the genesis block here (nor should we at
	//     this point care, the key-value/freezer combo is valid).
	//   - If neither the key-value store nor the freezer is empty, cross validate
	//     the genesis hashes to make sure they are compatible. If they are, also
	//     ensure that there's no gap between the freezer and sunsequently leveldb.
	//   - If the key-value store is not empty, but the freezer is we might just be
	//     upgrading to the freezer release, or we might have had a small chain and
	//     not frozen anything yet. Ensure that no blocks are missing yet from the
	//     key-value store, since that would mean we already had an old freezer.

	// If the genesis hash is empty, we have a new key-value store, so nothing to
	// validate in this method. If, however, the genesis hash is not nil, compare
	// it to the freezer content.
	if kvgenesis, _ := db.Get(headerHashKey(0)); len(kvgenesis) > 0 {
		if frozen, _ := ancientChain.Ancients(); frozen > 0 {
			// If the freezer already contains something, ensure that the genesis blocks
			// match, otherwise we might mix up freezers across chains and destroy both
			// the freezer and the key-value store.
			frgenesis, err := ancientChain.Ancient(freezerHashTable, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve genesis from ancient %v", err)
			} else if !bytes.Equal(kvgenesis, frgenesis) {
				return nil, fmt.Errorf("genesis mismatch: %#x (leveldb) != %#x (ancients)", kvgenesis, frgenesis)
			}
			// Key-value store and freezer belong to the same network. Ensure that they
			// are contiguous, otherwise we might end up with a non-functional freezer.
			if kvhash, _ := db.Get(headerHashKey(frozen)); len(kvhash) == 0 {
				// Subsequent header after the freezer limit is missing from the database.
				// Reject startup is the database has a more recent head.
				if *ReadHeaderNumber(db, ReadHeadHeaderHash(db)) > frozen-1 {
					return nil, fmt.Errorf("gap (#%d) in the chain between ancients and leveldb", frozen)
				}
				// Database contains only older data than the freezer, this happens if the
				// state was wiped and reinited from an existing freezer.
			}
			// Otherwise, key-value store continues where the freezer left off, all is fine.
			// We might have duplicate blocks (crash after freezer write but before key-value
			// store deletion, but that's fine).
		} else {
			// If the freezer is empty, ensure nothing was moved yet from the key-value
			// store, otherwise we'll end up missing data. We check block #1 to decide
			// if we froze anything previously or not, but do take care of databases with
			// only the genesis block.
			if ReadHeadHeaderHash(db) != common.BytesToHash(kvgenesis) {
				// Key-value store contains more data than the genesis block, make sure we
				// didn't freeze anything yet.
				if kvblob, _ := db.Get(headerHashKey(1)); len(kvblob) == 0 {
					return nil, errors.New("ancient chain segments already extracted, please set --datadir.ancient to the correct path")
				}
				// Block #1 is still in the database, we're allowed to init a new feezer
			}
			// Otherwise, the head header is still the genesis, we're allowed to init a new
			// freezer.
		}
	}
	// Freezer is consistent with the key-value database, permit combining the two
	if !ancientChain.readonly {
		ancientChain.wg.Add(1)
		go func() {
			ancientChain.freeze(db)
			ancientChain.wg.Done()
		}()
	}
	return &freezerdb{
		KeyValueStore: db,
		chain:         ancientChain,
		rdiff:         rdiff,
	}, nil
}

// NewMemoryDatabase creates an ephemeral in-memory key-value database without a
// freezer moving immutable chain segments into cold storage.
func NewMemoryDatabase() ethdb.Database {
	return NewDatabase(memorydb.New())
}

// NewMemoryDatabaseWithCap creates an ephemeral in-memory key-value database
// with an initial starting capacity, but without a freezer moving immutable
// chain segments into cold storage.
func NewMemoryDatabaseWithCap(size int) ethdb.Database {
	return NewDatabase(memorydb.NewWithCap(size))
}

// NewLevelDBDatabase creates a persistent key-value database without a freezer
// moving immutable chain segments into cold storage.
func NewLevelDBDatabase(file string, cache int, handles int, namespace string, readonly bool) (ethdb.Database, error) {
	db, err := leveldb.New(file, cache, handles, namespace, readonly)
	if err != nil {
		return nil, err
	}
	return NewDatabase(db), nil
}

// NewLevelDBDatabaseWithFreezer creates a persistent key-value database with a
// freezer moving immutable chain segments into cold storage.
func NewLevelDBDatabaseWithFreezer(file string, cache int, handles int, freezer string, namespace string, readonly bool) (ethdb.Database, error) {
	kvdb, err := leveldb.New(file, cache, handles, namespace, readonly)
	if err != nil {
		return nil, err
	}
	frdb, err := NewDatabaseWithFreezer(kvdb, freezer, namespace, readonly)
	if err != nil {
		kvdb.Close()
		return nil, err
	}
	return frdb, nil
}

type counter uint64

func (c counter) String() string {
	return fmt.Sprintf("%d", c)
}

func (c counter) Percentage(current uint64) string {
	return fmt.Sprintf("%d", current*100/uint64(c))
}

// stat stores sizes and count for a parameter
type stat struct {
	size  common.StorageSize
	count counter
}

// Add size to the stat and increase the counter by 1
func (s *stat) Add(size common.StorageSize) {
	s.size += size
	s.count++
}

func (s *stat) Size() string {
	return s.size.String()
}

func (s *stat) Count() string {
	return s.count.String()
}

// InspectDatabase traverses the entire database and checks the size
// of all different categories of data.
func InspectDatabase(db ethdb.Database, keyPrefix, keyStart []byte) error {
	it := db.NewIterator(keyPrefix, keyStart)
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()

		// Key-value store statistics
		headers            stat
		bodies             stat
		receipts           stat
		tds                stat
		numHashPairings    stat
		hashNumPairings    stat
		tries              stat
		trieSnaps          stat
		legacyTries        stat
		reverseDiffLookups stat
		codes              stat
		txLookups          stat
		accountSnaps       stat
		storageSnaps       stat
		preimages          stat
		bloomBits          stat
		cliqueSnaps        stat

		// Ancient store statistics
		ancientHeadersSize  common.StorageSize
		ancientBodiesSize   common.StorageSize
		ancientReceiptsSize common.StorageSize
		ancientTdsSize      common.StorageSize
		ancientHashesSize   common.StorageSize

		reverseDiffsSize      common.StorageSize
		reverseDiffHashesSize common.StorageSize

		// Les statistic
		chtTrieNodes   stat
		bloomTrieNodes stat

		// Meta- and unaccounted data
		metadata    stat
		unaccounted stat

		// Totals
		total common.StorageSize
	)
	// Inspect key-value database first.
	for it.Next() {
		var (
			key  = it.Key()
			size = common.StorageSize(len(key) + len(it.Value()))
		)
		total += size
		switch {
		case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
			headers.Add(size)
		case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
			bodies.Add(size)
		case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
			receipts.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
			tds.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
			numHashPairings.Add(size)
		case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
			hashNumPairings.Add(size)
		case bytes.HasPrefix(key, TrieNodePrefix) && len(key) > len(TrieNodePrefix) && len(key) < len(TrieNodePrefix)+maxTrieNodeStorageKeyLen:
			tries.Add(size)
		case bytes.HasPrefix(key, TrieNodeSnapshotPrefix) && len(key) > len(TrieNodeSnapshotPrefix) && len(key) < len(TrieNodeSnapshotPrefix)+maxTrieNodeStorageKeyLen:
			trieSnaps.Add(size)
		case len(key) == common.HashLength:
			legacyTries.Add(size)
		case bytes.HasPrefix(key, ReverseDiffLookupPrefix) && len(key) == len(ReverseDiffLookupPrefix)+common.HashLength:
			reverseDiffLookups.Add(size)
		case bytes.HasPrefix(key, CodePrefix) && len(key) == len(CodePrefix)+common.HashLength:
			codes.Add(size)
		case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
			txLookups.Add(size)
		case bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength):
			accountSnaps.Add(size)
		case bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength):
			storageSnaps.Add(size)
		case bytes.HasPrefix(key, PreimagePrefix) && len(key) == (len(PreimagePrefix)+common.HashLength):
			preimages.Add(size)
		case bytes.HasPrefix(key, configPrefix) && len(key) == (len(configPrefix)+common.HashLength):
			metadata.Add(size)
		case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, BloomBitsIndexPrefix):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, []byte("clique-")) && len(key) == 7+common.HashLength:
			cliqueSnaps.Add(size)
		case bytes.HasPrefix(key, []byte("cht-")) ||
			bytes.HasPrefix(key, []byte("chtIndexV2-")) ||
			bytes.HasPrefix(key, []byte("chtRootV2-")): // Canonical hash trie
			chtTrieNodes.Add(size)
		case bytes.HasPrefix(key, []byte("blt-")) ||
			bytes.HasPrefix(key, []byte("bltIndex-")) ||
			bytes.HasPrefix(key, []byte("bltRoot-")): // Bloomtrie sub
			bloomTrieNodes.Add(size)
		default:
			var accounted bool
			for _, meta := range [][]byte{
				databaseVersionKey, headHeaderKey, headBlockKey, headFastBlockKey, lastPivotKey,
				fastTrieProgressKey, snapshotDisabledKey, SnapshotRootKey, snapshotJournalKey,
				snapshotGeneratorKey, snapshotRecoveryKey, snapshotSyncStatusKey, triesJournalKey,
				txIndexTailKey, fastTxLookupLimitKey, uncleanShutdownKey, badBlockKey, ReverseDiffHeadKey,
				transitionStatusKey,
			} {
				if bytes.Equal(key, meta) {
					metadata.Add(size)
					accounted = true
					break
				}
			}
			if !accounted {
				unaccounted.Add(size)
			}
		}
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	// Inspect append-only file store then.
	var (
		ancientSizes = []*common.StorageSize{&ancientHeadersSize, &ancientBodiesSize, &ancientReceiptsSize, &ancientHashesSize, &ancientTdsSize, &reverseDiffsSize, &reverseDiffHashesSize}
		categories   = []string{freezerHeaderTable, freezerBodiesTable, freezerReceiptTable, freezerHashTable, freezerDifficultyTable, freezerReverseDiffTable, freezerReverseDiffHashTable}
		freezers     = []string{ChainFreezer, ChainFreezer, ChainFreezer, ChainFreezer, ChainFreezer, ReverseDiffFreezer, ReverseDiffFreezer}
	)
	for i, category := range categories {
		if size, err := db.AncientSize(freezers[i], category); err == nil {
			*ancientSizes[i] += common.StorageSize(size)
			total += common.StorageSize(size)
		}
	}
	// Get number of ancient rows inside the freezer
	var (
		chainAncients = counter(0)
		rdiffAncients = counter(0)
	)
	if count, err := db.Ancients(ChainFreezer); err == nil {
		chainAncients = counter(count)
	}
	if count, err := db.Ancients(ReverseDiffFreezer); err == nil {
		rdiffAncients = counter(count)
	}
	// Display the database statistic.
	stats := [][]string{
		{"Key-Value store", "Headers", headers.Size(), headers.Count()},
		{"Key-Value store", "Bodies", bodies.Size(), bodies.Count()},
		{"Key-Value store", "Receipt lists", receipts.Size(), receipts.Count()},
		{"Key-Value store", "Difficulties", tds.Size(), tds.Count()},
		{"Key-Value store", "Block number->hash", numHashPairings.Size(), numHashPairings.Count()},
		{"Key-Value store", "Block hash->number", hashNumPairings.Size(), hashNumPairings.Count()},
		{"Key-Value store", "Transaction index", txLookups.Size(), txLookups.Count()},
		{"Key-Value store", "Bloombit index", bloomBits.Size(), bloomBits.Count()},
		{"Key-Value store", "Contract codes", codes.Size(), codes.Count()},
		{"Key-Value store", "Trie nodes", tries.Size(), tries.Count()},
		{"Key-Value store", "Trie node snapshots", trieSnaps.Size(), trieSnaps.Count()},
		{"Key-Value store", "Legacy trie nodes", legacyTries.Size(), legacyTries.Count()},
		{"Key-Value store", "Reverse diff lookups", reverseDiffLookups.Size(), reverseDiffLookups.Count()},
		{"Key-Value store", "Trie preimages", preimages.Size(), preimages.Count()},
		{"Key-Value store", "Account snapshot", accountSnaps.Size(), accountSnaps.Count()},
		{"Key-Value store", "Storage snapshot", storageSnaps.Size(), storageSnaps.Count()},
		{"Key-Value store", "Clique snapshots", cliqueSnaps.Size(), cliqueSnaps.Count()},
		{"Key-Value store", "Singleton metadata", metadata.Size(), metadata.Count()},
		{"Ancient store", "Headers", ancientHeadersSize.String(), chainAncients.String()},
		{"Ancient store", "Bodies", ancientBodiesSize.String(), chainAncients.String()},
		{"Ancient store", "Receipt lists", ancientReceiptsSize.String(), chainAncients.String()},
		{"Ancient store", "Difficulties", ancientTdsSize.String(), chainAncients.String()},
		{"Ancient store", "Block number->hash", ancientHashesSize.String(), chainAncients.String()},
		{"Ancient store", "Reverse diffs", reverseDiffsSize.String(), rdiffAncients.String()},
		{"Ancient store", "State root->reverse diff id", reverseDiffHashesSize.String(), rdiffAncients.String()},
		{"Light client", "CHT trie nodes", chtTrieNodes.Size(), chtTrieNodes.Count()},
		{"Light client", "Bloom trie nodes", bloomTrieNodes.Size(), bloomTrieNodes.Count()},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Size", "Items"})
	table.SetFooter([]string{"", "Total", total.String(), " "})
	table.AppendBulk(stats)
	table.Render()

	if unaccounted.size > 0 {
		log.Error("Database contains unaccounted data", "size", unaccounted.size, "count", unaccounted.count)
	}

	return nil
}
