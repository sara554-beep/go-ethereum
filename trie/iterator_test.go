// Copyright 2014 The go-ethereum Authors
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
	"fmt"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestEmptyIterator(t *testing.T) {
	trie := newEmpty()
	iter := trie.NodeIterator(nil)

	seen := make(map[string]struct{})
	for iter.Next(true) {
		seen[string(iter.Path())] = struct{}{}
	}
	if len(seen) != 0 {
		t.Fatal("Unexpected trie node iterated")
	}
}

func TestIterator(t *testing.T) {
	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	all := make(map[string]string)
	for _, val := range vals {
		all[val.k] = val.v
		trie.Update([]byte(val.k), []byte(val.v))
	}
	trie.Commit(false)

	found := make(map[string]string)
	it := NewIterator(trie.NodeIterator(nil))
	for it.Next() {
		found[string(it.Key)] = string(it.Value)
	}

	for k, v := range all {
		if found[k] != v {
			t.Errorf("iterator value mismatch for %s: got %q want %q", k, found[k], v)
		}
	}
}

type kv struct {
	k, v []byte
	t    bool
}

func TestIteratorLargeData(t *testing.T) {
	trie := newEmpty()
	vals := make(map[string]*kv)

	for i := byte(0); i < 255; i++ {
		value := &kv{common.LeftPadBytes([]byte{i}, 32), []byte{i}, false}
		value2 := &kv{common.LeftPadBytes([]byte{10, i}, 32), []byte{i}, false}
		trie.Update(value.k, value.v)
		trie.Update(value2.k, value2.v)
		vals[string(value.k)] = value
		vals[string(value2.k)] = value2
	}

	it := NewIterator(trie.NodeIterator(nil))
	for it.Next() {
		vals[string(it.Key)].t = true
	}

	var untouched []*kv
	for _, value := range vals {
		if !value.t {
			untouched = append(untouched, value)
		}
	}

	if len(untouched) > 0 {
		t.Errorf("Missed %d nodes", len(untouched))
		for _, value := range untouched {
			t.Error(value)
		}
	}
}

type iterationElement struct {
	hash common.Hash
	path []byte
	blob []byte
}

// Tests that the node iterator indeed walks over the entire database contents.
func TestNodeIteratorCoverageHashBased(t *testing.T) { testNodeIteratorCoverage(t, HashScheme) }
func TestNodeIteratorCoveragePathBased(t *testing.T) { testNodeIteratorCoverage(t, PathScheme) }

func testNodeIteratorCoverage(t *testing.T, scheme string) {
	// Create some arbitrary test trie to iterate
	db, trie, _ := makeTestTrie(scheme)

	// Gather all the node hashes found by the iterator
	var elements = make(map[common.Hash]iterationElement)
	for it := trie.NodeIterator(nil); it.Next(true); {
		if it.Hash() != (common.Hash{}) {
			elements[it.Hash()] = iterationElement{
				hash: it.Hash(),
				path: common.CopyBytes(it.Path()),
				blob: common.CopyBytes(it.NodeBlob()),
			}
		}
	}
	// Cross check the hashes and the database itself
	for _, element := range elements {
		if blob, err := db.GetReader(trie.Hash()).NodeBlob(common.Hash{}, element.path, element.hash); err != nil {
			t.Errorf("failed to retrieve reported node %x: %v", element.hash, err)
		} else if !bytes.Equal(blob, element.blob) {
			t.Errorf("node blob is different, want %v got %v", element.blob, blob)
		}
	}
	var (
		count int
		it    = db.DiskDB().NewIterator(nil, nil)
	)
	for it.Next() {
		res, _ := db.Scheme().IsTrieNode(it.Key())
		if !res {
			continue
		}
		count += 1
		if elem, ok := elements[crypto.Keccak256Hash(it.Value())]; !ok {
			t.Error("state entry not reported")
		} else if !bytes.Equal(it.Value(), elem.blob) {
			t.Errorf("node blob is different, want %v got %v", elem.blob, it.Value())
		}
	}
	it.Release()
	if count != len(elements) {
		t.Errorf("state entry is mismatched %d %d", count, len(elements))
	}
}

type kvs struct{ k, v string }

var testdata1 = []kvs{
	{"barb", "ba"},
	{"bard", "bc"},
	{"bars", "bb"},
	{"bar", "b"},
	{"fab", "z"},
	{"food", "ab"},
	{"foos", "aa"},
	{"foo", "a"},
}

var testdata2 = []kvs{
	{"aardvark", "c"},
	{"bar", "b"},
	{"barb", "bd"},
	{"bars", "be"},
	{"fab", "z"},
	{"foo", "a"},
	{"foos", "aa"},
	{"food", "ab"},
	{"jars", "d"},
}

func TestIteratorSeek(t *testing.T) {
	trie := newEmpty()
	for _, val := range testdata1 {
		trie.Update([]byte(val.k), []byte(val.v))
	}

	// Seek to the middle.
	it := NewIterator(trie.NodeIterator([]byte("fab")))
	if err := checkIteratorOrder(testdata1[4:], it); err != nil {
		t.Fatal(err)
	}

	// Seek to a non-existent key.
	it = NewIterator(trie.NodeIterator([]byte("barc")))
	if err := checkIteratorOrder(testdata1[1:], it); err != nil {
		t.Fatal(err)
	}

	// Seek beyond the end.
	it = NewIterator(trie.NodeIterator([]byte("z")))
	if err := checkIteratorOrder(nil, it); err != nil {
		t.Fatal(err)
	}
}

func checkIteratorOrder(want []kvs, it *Iterator) error {
	for it.Next() {
		if len(want) == 0 {
			return fmt.Errorf("didn't expect any more values, got key %q", it.Key)
		}
		if !bytes.Equal(it.Key, []byte(want[0].k)) {
			return fmt.Errorf("wrong key: got %q, want %q", it.Key, want[0].k)
		}
		want = want[1:]
	}
	if len(want) > 0 {
		return fmt.Errorf("iterator ended early, want key %q", want[0])
	}
	return nil
}

func TestDifferenceIterator(t *testing.T) {
	triea := newEmpty()
	for _, val := range testdata1 {
		triea.Update([]byte(val.k), []byte(val.v))
	}
	triea.Commit(false)

	trieb := newEmpty()
	for _, val := range testdata2 {
		trieb.Update([]byte(val.k), []byte(val.v))
	}
	trieb.Commit(false)

	found := make(map[string]string)
	di, _ := NewDifferenceIterator(triea.NodeIterator(nil), trieb.NodeIterator(nil))
	it := NewIterator(di)
	for it.Next() {
		found[string(it.Key)] = string(it.Value)
	}

	all := []struct{ k, v string }{
		{"aardvark", "c"},
		{"barb", "bd"},
		{"bars", "be"},
		{"jars", "d"},
	}
	for _, item := range all {
		if found[item.k] != item.v {
			t.Errorf("iterator value mismatch for %s: got %v want %v", item.k, found[item.k], item.v)
		}
	}
	if len(found) != len(all) {
		t.Errorf("iterator count mismatch: got %d values, want %d", len(found), len(all))
	}
}

func TestUnionIterator(t *testing.T) {
	triea := newEmpty()
	for _, val := range testdata1 {
		triea.Update([]byte(val.k), []byte(val.v))
	}
	triea.Commit(false)

	trieb := newEmpty()
	for _, val := range testdata2 {
		trieb.Update([]byte(val.k), []byte(val.v))
	}
	trieb.Commit(false)

	di, _ := NewUnionIterator([]NodeIterator{triea.NodeIterator(nil), trieb.NodeIterator(nil)})
	it := NewIterator(di)

	all := []struct{ k, v string }{
		{"aardvark", "c"},
		{"barb", "ba"},
		{"barb", "bd"},
		{"bard", "bc"},
		{"bars", "bb"},
		{"bars", "be"},
		{"bar", "b"},
		{"fab", "z"},
		{"food", "ab"},
		{"foos", "aa"},
		{"foo", "a"},
		{"jars", "d"},
	}

	for i, kv := range all {
		if !it.Next() {
			t.Errorf("Iterator ends prematurely at element %d", i)
		}
		if kv.k != string(it.Key) {
			t.Errorf("iterator value mismatch for element %d: got key %s want %s", i, it.Key, kv.k)
		}
		if kv.v != string(it.Value) {
			t.Errorf("iterator value mismatch for element %d: got value %s want %s", i, it.Value, kv.v)
		}
	}
	if it.Next() {
		t.Errorf("Iterator returned extra values.")
	}
}

func TestIteratorNoDups(t *testing.T) {
	tr := NewEmpty(NewDatabase(rawdb.NewMemoryDatabase(), nil))
	for _, val := range testdata1 {
		tr.Update([]byte(val.k), []byte(val.v))
	}
	checkIteratorNoDups(t, tr.NodeIterator(nil), nil)
}

// This test checks that nodeIterator.Next can be retried after inserting missing trie nodes.
func TestIteratorContinueAfterErrorDiskHashBased(t *testing.T) {
	testIteratorContinueAfterError(t, false, HashScheme)
}
func TestIteratorContinueAfterErrorMemonlyHashBased(t *testing.T) {
	testIteratorContinueAfterError(t, true, HashScheme)
}
func TestIteratorContinueAfterErrorDiskPathBased(t *testing.T) {
	testIteratorContinueAfterError(t, false, PathScheme)
}
func TestIteratorContinueAfterErrorMemonlyPathBased(t *testing.T) {
	testIteratorContinueAfterError(t, true, PathScheme)
}

func testIteratorContinueAfterError(t *testing.T, memonly bool, scheme string) {
	diskdb := rawdb.NewMemoryDatabase()
	tdb := NewDatabase(diskdb, &Config{Scheme: scheme})

	tr := NewEmpty(tdb)
	for _, val := range testdata1 {
		tr.Update([]byte(val.k), []byte(val.v))
	}
	root, nodes, _ := tr.Commit(false)
	tdb.Update(root, common.Hash{}, NewWithNodeSet(nodes))
	if !memonly {
		tdb.Commit(root)
	}
	wantNodeCount := checkIteratorNoDups(t, tr.NodeIterator(nil), nil)

	var (
		paths  [][]byte
		hashes []common.Hash
	)
	if memonly {
		for path, n := range nodes.updates.nodes {
			paths = append(paths, []byte(path))
			hashes = append(hashes, n.hash)
		}
	} else {
		it := diskdb.NewIterator(nil, nil)
		for it.Next() {
			ok, nodeKey := tdb.Scheme().IsTrieNode(it.Key())
			if !ok {
				continue
			}
			if tdb.Scheme().Name() == PathScheme {
				_, path := decodeStorageKey(nodeKey)
				paths = append(paths, path)
			} else {
				paths = append(paths, nil) // useless for hash-scheme
			}
			hashes = append(hashes, crypto.Keccak256Hash(it.Value()))
		}
		it.Release()
	}
	for i := 0; i < 20; i++ {
		// Create trie that will load all nodes from DB.
		tr, _ := New(tr.Hash(), common.Hash{}, tr.Hash(), tdb)

		// Remove a random node from the database. It can't be the root node
		// because that one is already loaded.
		var (
			rval  []byte
			rpath []byte
			rhash common.Hash
		)
		for {
			if memonly {
				rpath = paths[rand.Intn(len(paths))]
				node := nodes.updates.nodes[string(rpath)]
				if node == nil {
					continue
				}
				rhash = node.hash
			} else {
				index := rand.Intn(len(paths))
				rpath = paths[index]
				rhash = hashes[index]
			}
			if rhash != tr.Hash() {
				break
			}
		}
		if memonly {
			tr.nodes.banned = map[string]struct{}{string(rpath): {}}
		} else {
			rval = tdb.Scheme().ReadTrieNode(diskdb, common.Hash{}, rpath, rhash)
			tdb.Scheme().DeleteTrieNode(diskdb, common.Hash{}, rpath, rhash)
		}
		// Iterate until the error is hit.
		seen := make(map[string]bool)
		it := tr.NodeIterator(nil)
		checkIteratorNoDups(t, it, seen)
		missing, ok := it.Error().(*MissingNodeError)
		if !ok || missing.NodeHash != rhash {
			t.Fatal("didn't hit missing node, got", it.Error())
		}

		// Add the node back and continue iteration.
		if memonly {
			delete(tr.nodes.banned, string(rpath))
		} else {
			tdb.Scheme().WriteTrieNode(diskdb, common.Hash{}, rpath, rhash, rval)
		}
		checkIteratorNoDups(t, it, seen)
		if it.Error() != nil {
			t.Fatal("unexpected error", it.Error())
		}
		if len(seen) != wantNodeCount {
			t.Fatal("wrong node iteration count, got", len(seen), "want", wantNodeCount)
		}
	}
}

// Similar to the test above, this one checks that failure to create nodeIterator at a
// certain key prefix behaves correctly when Next is called. The expectation is that Next
// should retry seeking before returning true for the first time.
func TestIteratorContinueAfterSeekErrorDiskHashBased(t *testing.T) {
	testIteratorContinueAfterSeekError(t, false, HashScheme)
}
func TestIteratorContinueAfterSeekErrorMemonlyHashBased(t *testing.T) {
	testIteratorContinueAfterSeekError(t, true, HashScheme)
}
func TestIteratorContinueAfterSeekErrorDiskPathBased(t *testing.T) {
	testIteratorContinueAfterSeekError(t, false, PathScheme)
}
func TestIteratorContinueAfterSeekErrorMemonlyPathBased(t *testing.T) {
	testIteratorContinueAfterSeekError(t, true, PathScheme)
}

func testIteratorContinueAfterSeekError(t *testing.T, memonly bool, scheme string) {
	// Commit test trie to db, then remove the node containing "bars".
	var (
		barNodePath []byte
		barNodeHash = common.HexToHash("05041990364eb72fcb1127652ce40d8bab765f2bfe53225b1170d276cc101c2e")
	)
	diskdb := rawdb.NewMemoryDatabase()
	triedb := NewDatabase(diskdb, &Config{Scheme: scheme})
	ctr := NewEmpty(triedb)
	for _, val := range testdata1 {
		ctr.Update([]byte(val.k), []byte(val.v))
	}
	root, nodes, _ := ctr.Commit(false)

	for path, node := range nodes.updates.nodes {
		if node.hash == barNodeHash {
			barNodePath = []byte(path)
			break
		}
	}
	triedb.Update(root, common.Hash{}, NewWithNodeSet(nodes))
	if !memonly {
		triedb.Commit(root)
	}
	var (
		barNodeBlob []byte
	)
	tr, _ := New(root, common.Hash{}, root, triedb)
	if memonly {
		tr.nodes.banned = map[string]struct{}{string(barNodePath): {}}
	} else {
		barNodeBlob = triedb.Scheme().ReadTrieNode(diskdb, common.Hash{}, barNodePath, barNodeHash)
		triedb.Scheme().DeleteTrieNode(diskdb, common.Hash{}, barNodePath, barNodeHash)
	}
	// Create a new iterator that seeks to "bars". Seeking can't proceed because
	// the node is missing.
	it := tr.NodeIterator([]byte("bars"))
	missing, ok := it.Error().(*MissingNodeError)
	if !ok {
		t.Fatal("want MissingNodeError, got", it.Error())
	} else if missing.NodeHash != barNodeHash {
		t.Fatal("wrong node missing")
	}
	// Reinsert the missing node.
	if memonly {
		delete(tr.nodes.banned, string(barNodePath))
	} else {
		triedb.Scheme().WriteTrieNode(diskdb, common.Hash{}, barNodePath, barNodeHash, barNodeBlob)
	}
	// Check that iteration produces the right set of values.
	if err := checkIteratorOrder(testdata1[2:], NewIterator(it)); err != nil {
		t.Fatal(err)
	}
}

func checkIteratorNoDups(t *testing.T, it NodeIterator, seen map[string]bool) int {
	if seen == nil {
		seen = make(map[string]bool)
	}
	for it.Next(true) {
		if seen[string(it.Path())] {
			t.Fatalf("iterator visited node path %x twice", it.Path())
		}
		seen[string(it.Path())] = true
	}
	return len(seen)
}

func TestIteratorNodeBlobHashBased(t *testing.T) { testIteratorNodeBlob(t, HashScheme) }
func TestIteratorNodeBlobPathBased(t *testing.T) { testIteratorNodeBlob(t, PathScheme) }

func testIteratorNodeBlob(t *testing.T, scheme string) {
	var (
		db     = rawdb.NewMemoryDatabase()
		triedb = NewDatabase(db, &Config{Scheme: scheme})
		trie   = NewEmpty(triedb)
	)
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	all := make(map[string]string)
	for _, val := range vals {
		all[val.k] = val.v
		trie.Update([]byte(val.k), []byte(val.v))
	}
	root, nodes, _ := trie.Commit(false)
	triedb.Update(root, common.Hash{}, NewWithNodeSet(nodes))
	triedb.Commit(root)

	var found = make(map[common.Hash][]byte)
	trie, _ = New(root, common.Hash{}, root, triedb)
	it := trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Hash() == (common.Hash{}) {
			continue
		}
		found[it.Hash()] = it.NodeBlob()
	}

	dbIter := db.NewIterator(nil, nil)
	defer dbIter.Release()

	var count int
	for dbIter.Next() {
		ok, _ := triedb.Scheme().IsTrieNode(dbIter.Key())
		if !ok {
			continue
		}
		got, present := found[crypto.Keccak256Hash(dbIter.Value())]
		if !present {
			t.Fatal("Miss trie node")
		}
		if !bytes.Equal(got, dbIter.Value()) {
			t.Fatalf("Unexpected trie node want %v got %v", dbIter.Value(), got)
		}
		count += 1
	}
	if count != len(found) {
		t.Fatal("Find extra trie node via iterator")
	}
}
