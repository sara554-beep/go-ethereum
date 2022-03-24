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

package trie

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// makeTestTrie create a sample test trie to test node-wise reconstruction.
func makeTestTrie(scheme string) (*Database, *SecureTrie, map[string][]byte) {
	// Create an empty trie
	triedb := NewDatabase(rawdb.NewMemoryDatabase(), &Config{Scheme: scheme})
	trie, _ := NewStateTrie(common.Hash{}, common.Hash{}, common.Hash{}, triedb)

	// Fill it with some arbitrary data
	content := make(map[string][]byte)
	for i := byte(0); i < 255; i++ {
		// Map the same data under multiple keys
		key, val := common.LeftPadBytes([]byte{1, i}, 32), []byte{i}
		content[string(key)] = val
		trie.Update(key, val)

		key, val = common.LeftPadBytes([]byte{2, i}, 32), []byte{i}
		content[string(key)] = val
		trie.Update(key, val)

		// Add some other data to inflate the trie
		for j := byte(3); j < 13; j++ {
			key, val = common.LeftPadBytes([]byte{j, i}, 32), []byte{j, i}
			content[string(key)] = val
			trie.Update(key, val)
		}
	}
	root, nodes, err := trie.Commit(false)
	if err != nil {
		panic(err)
	}
	if err := triedb.Update(root, common.Hash{}, NewWithNodeSet(nodes)); err != nil {
		panic(err)
	}
	if err := triedb.Commit(root); err != nil {
		panic(err)
	}
	// Re-create the trie based on the new state
	trie, _ = NewStateTrie(root, common.Hash{}, root, triedb)
	return triedb, trie, content
}

// checkTrieContents cross references a reconstructed trie with an expected data
// content map.
func checkTrieContents(t *testing.T, db ethdb.Database, scheme NodeScheme, root []byte, content map[string][]byte) {
	// Check root availability and trie contents
	ndb := NewDatabase(db, &Config{Scheme: scheme.Name()})
	trie, err := NewStateTrie(common.BytesToHash(root), common.Hash{}, common.BytesToHash(root), ndb)
	if err != nil {
		t.Fatalf("failed to create trie at %x: %v", root, err)
	}
	if err := checkTrieConsistency(db, scheme, common.BytesToHash(root)); err != nil {
		t.Fatalf("inconsistent trie at %x: %v", root, err)
	}
	for key, val := range content {
		if have := trie.Get([]byte(key)); !bytes.Equal(have, val) {
			t.Errorf("entry %x: content mismatch: have %x, want %x", key, have, val)
		}
	}
}

// checkTrieConsistency checks that all nodes in a trie are indeed present.
func checkTrieConsistency(db ethdb.Database, scheme NodeScheme, root common.Hash) error {
	ndb := NewDatabase(db, &Config{Scheme: scheme.Name()})
	trie, err := NewStateTrie(root, common.Hash{}, root, ndb)
	if err != nil {
		return nil // Consider a non existent state consistent
	}
	it := trie.NodeIterator(nil)
	for it.Next(true) {
	}
	return it.Error()
}

// trieElement represents the element in the state trie(bytecode or trie node).
type trieElement struct {
	path     string
	hash     common.Hash
	syncPath SyncPath
}

// Tests that an empty trie is not scheduled for syncing.
func TestEmptySync(t *testing.T) {
	dbA := NewDatabase(rawdb.NewMemoryDatabase(), &Config{Scheme: HashScheme})
	dbB := NewDatabase(rawdb.NewMemoryDatabase(), &Config{Scheme: HashScheme})
	dbC := NewDatabase(rawdb.NewMemoryDatabase(), &Config{Scheme: PathScheme})
	dbD := NewDatabase(rawdb.NewMemoryDatabase(), &Config{Scheme: PathScheme})

	emptyA := NewEmpty(dbA)
	emptyB, _ := New(emptyRoot, common.Hash{}, emptyRoot, dbB)
	emptyC := NewEmpty(dbC)
	emptyD, _ := New(emptyRoot, common.Hash{}, emptyRoot, dbD)

	for i, trie := range []*Trie{emptyA, emptyB, emptyC, emptyD} {
		sync := NewSync(trie.Hash(), memorydb.New(), nil, []*Database{dbA, dbB, dbC, dbD}[i].Scheme())
		if paths, nodes, codes := sync.Missing(1); len(paths) != 0 || len(nodes) != 0 || len(codes) != 0 {
			t.Errorf("test %d: content requested for empty trie: %v, %v, %v", i, paths, nodes, codes)
		}
	}
}

// Tests that given a root hash, a trie can sync iteratively on a single thread,
// requesting retrieval tasks and returning all of them in one go.
func TestIterativeSyncIndividualHashBased(t *testing.T) {
	testIterativeSync(t, 1, false, HashScheme)
}
func TestIterativeSyncBatchedHashBased(t *testing.T) {
	testIterativeSync(t, 100, false, HashScheme)
}
func TestIterativeSyncIndividualByPathHashBased(t *testing.T) {
	testIterativeSync(t, 1, true, HashScheme)
}
func TestIterativeSyncBatchedByPathHashBased(t *testing.T) {
	testIterativeSync(t, 100, true, HashScheme)
}
func TestIterativeSyncIndividualPathBased(t *testing.T) {
	testIterativeSync(t, 1, false, PathScheme)
}
func TestIterativeSyncBatchedPathBased(t *testing.T) {
	testIterativeSync(t, 100, false, PathScheme)
}
func TestIterativeSyncIndividualByPathPathBased(t *testing.T) {
	testIterativeSync(t, 1, true, PathScheme)
}
func TestIterativeSyncBatchedByPathPathBased(t *testing.T) {
	testIterativeSync(t, 100, true, PathScheme)
}

func testIterativeSync(t *testing.T, count int, bypath bool, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(count)
	var elements []trieElement
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
	}
	for len(elements) > 0 {
		results := make([]NodeSyncResult, len(elements))
		if !bypath {
			for i, element := range elements {
				owner, inner := ParsePath([]byte(element.path))
				data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
				if err != nil {
					t.Fatalf("failed to retrieve node data for hash %x: %v", element.hash, err)
				}
				results[i] = NodeSyncResult{element.path, data}
			}
		} else {
			for i, element := range elements {
				data, _, err := srcTrie.TryGetNode(element.syncPath[len(element.syncPath)-1])
				if err != nil {
					t.Fatalf("failed to retrieve node data for path %x: %v", element.path, err)
				}
				results[i] = NodeSyncResult{element.path, data}
			}
		}
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(count)
		elements = elements[:0]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)
}

// Tests that the trie scheduler can correctly reconstruct the state even if only
// partial results are returned, and the others sent only later.
func TestIterativeDelayedSyncHashBased(t *testing.T) { testIterativeDelayedSync(t, HashScheme) }
func TestIterativeDelayedSyncPathBased(t *testing.T) { testIterativeDelayedSync(t, PathScheme) }

func testIterativeDelayedSync(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(10000)
	var elements []trieElement
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
	}
	for len(elements) > 0 {
		// Sync only half of the scheduled nodes
		results := make([]NodeSyncResult, len(elements)/2+1)
		for i, element := range elements[:len(results)] {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			results[i] = NodeSyncResult{element.path, data}
		}
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(10000)
		elements = elements[len(results):]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)
}

// Tests that given a root hash, a trie can sync iteratively on a single thread,
// requesting retrieval tasks and returning all of them in one go, however in a
// random order.
func TestIterativeRandomSyncIndividualHashBased(t *testing.T) {
	testIterativeRandomSync(t, 1, HashScheme)
}
func TestIterativeRandomSyncBatchedHashBased(t *testing.T) {
	testIterativeRandomSync(t, 100, HashScheme)
}
func TestIterativeRandomSyncIndividualPathBased(t *testing.T) {
	testIterativeRandomSync(t, 1, PathScheme)
}
func TestIterativeRandomSyncBatchedPathBased(t *testing.T) {
	testIterativeRandomSync(t, 100, PathScheme)
}

func testIterativeRandomSync(t *testing.T, count int, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(count)
	queue := make(map[string]trieElement)
	for i, path := range paths {
		queue[path] = trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		}
	}
	for len(queue) > 0 {
		// Fetch all the queued nodes in a random order
		results := make([]NodeSyncResult, 0, len(queue))
		for path, element := range queue {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			results = append(results, NodeSyncResult{path, data})
		}
		// Feed the retrieved results back and queue new tasks
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(count)
		queue = make(map[string]trieElement)
		for i, path := range paths {
			queue[path] = trieElement{
				path:     path,
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(path)),
			}
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)
}

// Tests that the trie scheduler can correctly reconstruct the state even if only
// partial results are returned (Even those randomly), others sent only later.
func TestIterativeRandomDelayedSyncHashBased(t *testing.T) {
	testIterativeRandomDelayedSync(t, HashScheme)
}
func TestIterativeRandomDelayedSyncPathBased(t *testing.T) {
	testIterativeRandomDelayedSync(t, PathScheme)
}

func testIterativeRandomDelayedSync(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(10000)
	queue := make(map[string]trieElement)
	for i, path := range paths {
		queue[path] = trieElement{
			path:     path,
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(path)),
		}
	}
	for len(queue) > 0 {
		// Sync only half of the scheduled nodes, even those in random order
		results := make([]NodeSyncResult, 0, len(queue)/2+1)
		for path, element := range queue {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			results = append(results, NodeSyncResult{path, data})

			if len(results) >= cap(results) {
				break
			}
		}
		// Feed the retrieved results back and queue new tasks
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()
		for _, result := range results {
			delete(queue, result.Path)
		}
		paths, nodes, _ = sched.Missing(10000)
		for i, path := range paths {
			queue[path] = trieElement{
				path:     path,
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(path)),
			}
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)
}

// Tests that a trie sync will not request nodes multiple times, even if they
// have such references.
func TestDuplicateAvoidanceSyncHashBased(t *testing.T) { testDuplicateAvoidanceSync(t, HashScheme) }
func TestDuplicateAvoidanceSyncPathBased(t *testing.T) { testDuplicateAvoidanceSync(t, PathScheme) }

func testDuplicateAvoidanceSync(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(0)
	var elements []trieElement
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
	}
	requested := make(map[common.Hash]struct{})

	for len(elements) > 0 {
		results := make([]NodeSyncResult, len(elements))
		for i, element := range elements {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			if _, ok := requested[element.hash]; ok {
				t.Errorf("hash %x already requested once", element.hash)
			}
			requested[element.hash] = struct{}{}

			results[i] = NodeSyncResult{element.path, data}
		}
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(0)
		elements = elements[:0]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)
}

// Tests that at any point in time during a sync, only complete sub-tries are in
// the database.
func TestIncompleteSyncHashBased(t *testing.T) { testIncompleteSync(t, HashScheme) }
func TestIncompleteSyncPathBased(t *testing.T) { testIncompleteSync(t, PathScheme) }

func testIncompleteSync(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, _ := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	var (
		addedKeys   []string
		addedHashes []common.Hash
		elements    []trieElement
		root        = srcTrie.Hash()
	)
	paths, nodes, _ := sched.Missing(1)
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
	}
	for len(elements) > 0 {
		// Fetch a batch of trie nodes
		results := make([]NodeSyncResult, len(elements))
		for i, element := range elements {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			results[i] = NodeSyncResult{element.path, data}
		}
		// Process each of the trie nodes
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		for _, result := range results {
			hash := crypto.Keccak256Hash(result.Data)
			if hash != root {
				addedKeys = append(addedKeys, result.Path)
				addedHashes = append(addedHashes, crypto.Keccak256Hash(result.Data))
			}
		}
		// Fetch the next batch to retrieve
		paths, nodes, _ = sched.Missing(1)
		elements = elements[:0]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
		}
	}
	// Sanity check that removing any node from the database is detected
	for i, path := range addedKeys {
		owner, inner := ParsePath([]byte(path))
		nodeHash := addedHashes[i]
		value := srcDb.Scheme().ReadTrieNode(diskdb, owner, inner, nodeHash)
		srcDb.Scheme().DeleteTrieNode(diskdb, owner, inner, nodeHash)
		if err := checkTrieConsistency(diskdb, srcDb.Scheme(), root); err == nil {
			t.Fatalf("trie inconsistency not caught, missing: %x", path)
		}
		srcDb.Scheme().WriteTrieNode(diskdb, owner, inner, nodeHash, value)
	}
}

// Tests that trie nodes get scheduled lexicographically when having the same
// depth.
func TestSyncOrderingHashBased(t *testing.T) { testSyncOrdering(t, HashScheme) }
func TestSyncOrderingPathBased(t *testing.T) { testSyncOrdering(t, PathScheme) }

func testSyncOrdering(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler, tracking the requests
	diskdb := rawdb.NewMemoryDatabase()
	sched := NewSync(srcTrie.Hash(), diskdb, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	var (
		reqs     []SyncPath
		elements []trieElement
	)
	paths, nodes, _ := sched.Missing(1)
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
		reqs = append(reqs, NewSyncPath([]byte(paths[i])))
	}

	for len(elements) > 0 {
		results := make([]NodeSyncResult, len(elements))
		for i, element := range elements {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(srcTrie.Hash()).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for %x: %v", element.hash, err)
			}
			results[i] = NodeSyncResult{element.path, data}
		}
		for _, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result %v", err)
			}
		}
		batch := diskdb.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(1)
		elements = elements[:0]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
			reqs = append(reqs, NewSyncPath([]byte(paths[i])))
		}
	}
	// Cross check that the two tries are in sync
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)

	// Check that the trie nodes have been requested path-ordered
	for i := 0; i < len(reqs)-1; i++ {
		if len(reqs[i]) > 1 || len(reqs[i+1]) > 1 {
			// In the case of the trie tests, there's no storage so the tuples
			// must always be single items. 2-tuples should be tested in state.
			t.Errorf("Invalid request tuples: len(%v) or len(%v) > 1", reqs[i], reqs[i+1])
		}
		if bytes.Compare(compactToHex(reqs[i][0]), compactToHex(reqs[i+1][0])) > 0 {
			t.Errorf("Invalid request order: %v before %v", compactToHex(reqs[i][0]), compactToHex(reqs[i+1][0]))
		}
	}
}

func syncWith(t *testing.T, root common.Hash, db ethdb.Database, srcDb *Database) {
	// Create a destination trie and sync with the scheduler
	sched := NewSync(root, db, nil, srcDb.Scheme())

	// The code requests are ignored here since there is no code
	// at the testing trie.
	paths, nodes, _ := sched.Missing(1)
	var elements []trieElement
	for i := 0; i < len(paths); i++ {
		elements = append(elements, trieElement{
			path:     paths[i],
			hash:     nodes[i],
			syncPath: NewSyncPath([]byte(paths[i])),
		})
	}
	for len(elements) > 0 {
		results := make([]NodeSyncResult, len(elements))
		for i, element := range elements {
			owner, inner := ParsePath([]byte(element.path))
			data, err := srcDb.GetReader(root).NodeBlob(owner, inner, element.hash)
			if err != nil {
				t.Fatalf("failed to retrieve node data for hash %x: %v", element.hash, err)
			}
			results[i] = NodeSyncResult{element.path, data}
		}
		for index, result := range results {
			if err := sched.ProcessNode(result); err != nil {
				t.Fatalf("failed to process result[%d][%v] data %v %v", index, []byte(result.Path), result.Data, err)
			}
		}
		batch := db.NewBatch()
		if err := sched.Commit(batch); err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}
		batch.Write()

		paths, nodes, _ = sched.Missing(1)
		elements = elements[:0]
		for i := 0; i < len(paths); i++ {
			elements = append(elements, trieElement{
				path:     paths[i],
				hash:     nodes[i],
				syncPath: NewSyncPath([]byte(paths[i])),
			})
		}
	}
}

// Tests that the syncing target is keeping moving which may overwrite the stale
// states synced in the last cycle.
func TestSyncWithDynamicTargetHashBased(t *testing.T) { testSyncWithDynamicTarget(t, HashScheme) }
func TestSyncWithDynamicTargetPathBased(t *testing.T) { testSyncWithDynamicTarget(t, PathScheme) }

func testSyncWithDynamicTarget(t *testing.T, scheme string) {
	// Create a random trie to copy
	srcDb, srcTrie, srcData := makeTestTrie(scheme)

	// Create a destination trie and sync with the scheduler
	diskdb := rawdb.NewMemoryDatabase()
	syncWith(t, srcTrie.Hash(), diskdb, srcDb)
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), srcData)

	// Push more modifications into the src trie, to see if dest trie can still
	// sync with it(overwrite stale states)
	var (
		preRoot = srcTrie.Hash()
		diff    = make(map[string][]byte)
	)
	for i := byte(0); i < 10; i++ {
		key, val := randBytes(32), randBytes(32)
		srcTrie.Update(key, val)
		diff[string(key)] = val
	}
	root, nodes, err := srcTrie.Commit(false)
	if err != nil {
		panic(err)
	}
	if err := srcDb.Update(root, preRoot, NewWithNodeSet(nodes)); err != nil {
		panic(err)
	}
	if err := srcDb.Commit(root); err != nil {
		panic(err)
	}
	preRoot = root
	syncWith(t, srcTrie.Hash(), diskdb, srcDb)
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), diff)

	// Revert added modifications from the src trie, to see if dest trie can still
	// sync with it(overwrite reverted states)
	var reverted = make(map[string][]byte)
	for k := range diff {
		srcTrie.Delete([]byte(k))
		reverted[k] = nil
	}
	for k := range srcData {
		val := randBytes(32)
		srcTrie.Update([]byte(k), val)
		reverted[k] = val
	}
	root, nodes, err = srcTrie.Commit(false)
	if err != nil {
		panic(err)
	}
	if err := srcDb.Update(root, preRoot, NewWithNodeSet(nodes)); err != nil {
		panic(err)
	}
	if err := srcDb.Commit(root); err != nil {
		panic(err)
	}
	preRoot = root
	syncWith(t, srcTrie.Hash(), diskdb, srcDb)
	checkTrieContents(t, diskdb, srcDb.Scheme(), srcTrie.Hash().Bytes(), reverted)
}
