// Copyright 2020 The go-ethereum Authors
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
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
)

var (
	// triePrefetchMetricsPrefix is the prefix under which to publish the metrics.
	triePrefetchMetricsPrefix = "trie/prefetch/"
)

type trieType int

const (
	typeMerkle trieType = iota
	typeVerkle
)

type trieID struct {
	typ   trieType
	owner common.Hash
	root  common.Hash
}

func newTrieID(typ trieType, state common.Hash, owner common.Hash, root common.Hash) *trieID {
	if typ == typeVerkle {
		return &trieID{typ: typeVerkle, owner: common.Hash{}, root: state}
	}
	return &trieID{typ: typeMerkle, owner: owner, root: root}
}

// string returns the string serialized trie id.
func (id *trieID) string() string {
	return id.owner.Hex() + id.root.Hex()
}

// triePrefetcher is an active prefetcher, which receives accounts or storage
// items and does trie-loading of them. The goal is to get as much useful
// content into the caches as possible.
//
// Note, the prefetcher's API is not thread safe.
type triePrefetcher struct {
	typ      trieType               // The type of trie to preload
	db       *trie.Database         // Database to fetch trie nodes through
	root     common.Hash            // Root hash of the state for metrics
	fetches  map[string]Trie        // Partially or fully fetched tries. Only populated for inactive copies.
	fetchers map[string]*subfetcher // Subfetchers for each trie

	deliveryMissMeter metrics.Meter
	accountLoadMeter  metrics.Meter
	accountDupMeter   metrics.Meter
	accountSkipMeter  metrics.Meter
	accountWasteMeter metrics.Meter
	storageLoadMeter  metrics.Meter
	storageDupMeter   metrics.Meter
	storageSkipMeter  metrics.Meter
	storageWasteMeter metrics.Meter
}

func newTriePrefetcher(db *trie.Database, root common.Hash, namespace string) *triePrefetcher {
	var (
		typ    = typeMerkle
		prefix = triePrefetchMetricsPrefix + namespace
	)
	if db.IsVerkle() {
		typ = typeVerkle
	}
	return &triePrefetcher{
		typ:      typ,
		db:       db,
		root:     root,
		fetchers: make(map[string]*subfetcher), // Active prefetchers use the fetchers map

		deliveryMissMeter: metrics.GetOrRegisterMeter(prefix+"/deliverymiss", nil),
		accountLoadMeter:  metrics.GetOrRegisterMeter(prefix+"/account/load", nil),
		accountDupMeter:   metrics.GetOrRegisterMeter(prefix+"/account/dup", nil),
		accountSkipMeter:  metrics.GetOrRegisterMeter(prefix+"/account/skip", nil),
		accountWasteMeter: metrics.GetOrRegisterMeter(prefix+"/account/waste", nil),
		storageLoadMeter:  metrics.GetOrRegisterMeter(prefix+"/storage/load", nil),
		storageDupMeter:   metrics.GetOrRegisterMeter(prefix+"/storage/dup", nil),
		storageSkipMeter:  metrics.GetOrRegisterMeter(prefix+"/storage/skip", nil),
		storageWasteMeter: metrics.GetOrRegisterMeter(prefix+"/storage/waste", nil),
	}
}

// close iterates over all the subfetchers, aborts any that were left spinning
// and reports the stats to the metrics subsystem.
func (p *triePrefetcher) close() {
	for _, fetcher := range p.fetchers {
		fetcher.abort() // safe to do multiple times

		if metrics.Enabled {
			if fetcher.root == p.root {
				p.accountLoadMeter.Mark(int64(len(fetcher.seen)))
				p.accountDupMeter.Mark(int64(fetcher.dups))
				p.accountSkipMeter.Mark(int64(len(fetcher.tasks)))

				for _, key := range fetcher.used {
					delete(fetcher.seen, string(key))
				}
				p.accountWasteMeter.Mark(int64(len(fetcher.seen)))
			} else {
				p.storageLoadMeter.Mark(int64(len(fetcher.seen)))
				p.storageDupMeter.Mark(int64(fetcher.dups))
				p.storageSkipMeter.Mark(int64(len(fetcher.tasks)))

				for _, key := range fetcher.used {
					delete(fetcher.seen, string(key))
				}
				p.storageWasteMeter.Mark(int64(len(fetcher.seen)))
			}
		}
	}
	// Clear out all fetchers (will crash on a second call, deliberate)
	p.fetchers = nil
}

// copy creates a deep-but-inactive copy of the trie prefetcher. Any trie data
// already loaded will be copied over, but no goroutines will be started. This
// is mostly used in the miner which creates a copy of it's actively mutated
// state to be sealed while it may further mutate the state.
func (p *triePrefetcher) copy() *triePrefetcher {
	copy := &triePrefetcher{
		db:      p.db,
		root:    p.root,
		fetches: make(map[string]Trie), // Active prefetchers use the fetches map

		deliveryMissMeter: p.deliveryMissMeter,
		accountLoadMeter:  p.accountLoadMeter,
		accountDupMeter:   p.accountDupMeter,
		accountSkipMeter:  p.accountSkipMeter,
		accountWasteMeter: p.accountWasteMeter,
		storageLoadMeter:  p.storageLoadMeter,
		storageDupMeter:   p.storageDupMeter,
		storageSkipMeter:  p.storageSkipMeter,
		storageWasteMeter: p.storageWasteMeter,
	}
	// If the prefetcher is already a copy, duplicate the data
	if p.fetches != nil {
		for root, fetch := range p.fetches {
			if fetch == nil {
				continue
			}
			copy.fetches[root] = mustCopyTrie(fetch)
		}
		return copy
	}
	// Otherwise we're copying an active fetcher, retrieve the current states
	for id, fetcher := range p.fetchers {
		copy.fetches[id] = fetcher.peek()
	}
	return copy
}

// prefetch schedules a batch of trie items to prefetch.
func (p *triePrefetcher) prefetch(owner common.Hash, root common.Hash, addr common.Address, keys [][]byte) {
	// If the prefetcher is an inactive one, bail out
	if p.fetches != nil {
		return
	}
	// Active fetcher, schedule the retrievals
	id := newTrieID(p.typ, p.root, owner, root)
	fetcher := p.fetchers[id.string()]
	if fetcher == nil {
		fetcher = newSubfetcher(p.db, p.typ, p.root, owner, root, addr)
		p.fetchers[id.string()] = fetcher
	}
	fetcher.schedule(keys)
}

// trie returns the trie matching the root hash, or nil if the prefetcher doesn't
// have it.
func (p *triePrefetcher) trie(id *trieID) Trie {
	// If the prefetcher is inactive, return from existing deep copies
	if p.fetches != nil {
		trie := p.fetches[id.string()]
		if trie == nil {
			p.deliveryMissMeter.Mark(1)
			return nil
		}
		return mustCopyTrie(trie)
	}
	// Otherwise the prefetcher is active, bail if no trie was prefetched for this root
	fetcher := p.fetchers[id.string()]
	if fetcher == nil {
		p.deliveryMissMeter.Mark(1)
		return nil
	}
	// Interrupt the prefetcher if it's by any chance still running and return
	// a copy of any pre-loaded trie.
	fetcher.abort() // safe to do multiple times

	trie := fetcher.peek()
	if trie == nil {
		p.deliveryMissMeter.Mark(1)
		return nil
	}
	return trie
}

// hasher constructs a state hasher by using all the cached pre-loaded tries.
func (p *triePrefetcher) hasher() (Hasher, error) {
	if p.typ == typeVerkle {
		return newVerkleHasher(p.db, p.root, p)
	}
	return newMerkleHasher(p.db, p.root, p)
}

// used marks a batch of state items used to allow creating statistics as to
// how useful or wasteful the prefetcher is.
func (p *triePrefetcher) used(owner common.Hash, root common.Hash, used [][]byte) {
	id := newTrieID(p.typ, p.root, owner, root)
	if fetcher := p.fetchers[id.string()]; fetcher != nil {
		fetcher.used = used
	}
}

// subfetcher is a trie fetcher goroutine responsible for pulling entries for a
// single trie. It is spawned when a new root is encountered and lives until the
// main prefetcher is paused and either all requested items are processed or if
// the trie being worked on is retrieved from the prefetcher.
type subfetcher struct {
	typ   trieType       // The type of the trie to preload
	db    *trie.Database // Database to load trie nodes through
	state common.Hash    // Root hash of the state to prefetch
	owner common.Hash    // Owner of the trie, usually account hash
	root  common.Hash    // Root hash of the trie to prefetch
	addr  common.Address // Address of the account that the trie belongs to
	trie  Trie           // Trie being populated with nodes

	tasks [][]byte   // Items queued up for retrieval
	lock  sync.Mutex // Lock protecting the task queue

	wake chan struct{}  // Wake channel if a new task is scheduled
	stop chan struct{}  // Channel to interrupt processing
	term chan struct{}  // Channel to signal interruption
	copy chan chan Trie // Channel to request a copy of the current trie

	seen map[string]struct{} // Tracks the entries already loaded
	dups int                 // Number of duplicate preload tasks
	used [][]byte            // Tracks the entries used in the end
}

// newSubfetcher creates a goroutine to prefetch state items belonging to a
// particular root hash.
func newSubfetcher(db *trie.Database, typ trieType, state common.Hash, owner common.Hash, root common.Hash, addr common.Address) *subfetcher {
	sf := &subfetcher{
		db:    db,
		typ:   typ,
		state: state,
		owner: owner,
		root:  root,
		addr:  addr,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		term:  make(chan struct{}),
		copy:  make(chan chan Trie),
		seen:  make(map[string]struct{}),
	}
	go sf.loop()
	return sf
}

// schedule adds a batch of trie keys to the queue to prefetch.
func (sf *subfetcher) schedule(keys [][]byte) {
	// Append the tasks to the current queue
	sf.lock.Lock()
	sf.tasks = append(sf.tasks, keys...)
	sf.lock.Unlock()

	// Notify the prefetcher, it's fine if it's already terminated
	select {
	case sf.wake <- struct{}{}:
	default:
	}
}

// peek tries to retrieve a deep copy of the fetcher's trie in whatever form it
// is currently.
func (sf *subfetcher) peek() Trie {
	ch := make(chan Trie)
	select {
	case sf.copy <- ch:
		// Subfetcher still alive, return copy from it
		return <-ch

	case <-sf.term:
		// Subfetcher already terminated, return a copy directly
		if sf.trie == nil {
			return nil
		}
		return mustCopyTrie(sf.trie)
	}
}

// abort interrupts the subfetcher immediately. It is safe to call abort multiple
// times but it is not thread safe.
func (sf *subfetcher) abort() {
	select {
	case <-sf.stop:
	default:
		close(sf.stop)
	}
	<-sf.term
}

func (sf *subfetcher) openTrie() (Trie, error) {
	if sf.typ == typeVerkle {
		return trie.NewVerkleTrie(sf.state, sf.db, utils.NewPointCache(commitmentCacheItems))
	}
	// Start by opening the trie and stop processing if it fails
	if sf.owner == (common.Hash{}) {
		return trie.NewStateTrie(trie.StateTrieID(sf.root), sf.db)
	}
	return trie.NewStateTrie(trie.StorageTrieID(sf.root, crypto.Keccak256Hash(sf.addr.Bytes()), sf.root), sf.db)
}

// loop waits for new tasks to be scheduled and keeps loading them until it runs
// out of tasks or its underlying trie is retrieved for committing.
func (sf *subfetcher) loop() {
	// No matter how the loop stops, signal anyone waiting that it's terminated
	defer close(sf.term)

	// Trie opened successfully, keep prefetching items
	tr, err := sf.openTrie()
	if err != nil {
		var contexts []interface{}
		if sf.typ == typeVerkle {
			contexts = append(contexts, "type", "verkle")
			contexts = append(contexts, "root", sf.state)
		} else {
			contexts = append(contexts, "type", "merkle")
			if sf.owner != (common.Hash{}) {
				contexts = append(contexts, "owner", sf.owner)
			}
			contexts = append(contexts, "root", sf.root)
		}
		contexts = append(contexts, "err", err)
		log.Warn("Trie prefetcher failed opening trie", contexts)
		return
	}
	sf.trie = tr

	for {
		select {
		case <-sf.wake:
			// Subfetcher was woken up, retrieve any tasks to avoid spinning the lock
			sf.lock.Lock()
			tasks := sf.tasks
			sf.tasks = nil
			sf.lock.Unlock()

			// Prefetch any tasks until the loop is interrupted
			for i, task := range tasks {
				select {
				case <-sf.stop:
					// If termination is requested, add any leftover back and return
					sf.lock.Lock()
					sf.tasks = append(sf.tasks, tasks[i:]...)
					sf.lock.Unlock()
					return

				case ch := <-sf.copy:
					// Somebody wants a copy of the current trie, grant them
					ch <- mustCopyTrie(sf.trie)

				default:
					// No termination request yet, prefetch the next entry
					if _, ok := sf.seen[string(task)]; ok {
						sf.dups++
					} else {
						if len(task) == common.AddressLength {
							sf.trie.GetAccount(common.BytesToAddress(task))
						} else {
							sf.trie.GetStorage(sf.addr, task)
						}
						sf.seen[string(task)] = struct{}{}
					}
				}
			}

		case ch := <-sf.copy:
			// Somebody wants a copy of the current trie, grant them
			ch <- mustCopyTrie(sf.trie)

		case <-sf.stop:
			// Termination is requested, abort and leave remaining tasks
			return
		}
	}
}

// mustCopyTrie creates a deep-copied trie and panic if the trie is unknown type.
func mustCopyTrie(tr Trie) Trie {
	switch t := tr.(type) {
	case *trie.StateTrie:
		return t.Copy()
	case *trie.VerkleTrie:
		return t.Copy()
	default:
		panic(fmt.Sprintf("Unknown trie type %T", tr))
	}
}
