package trie

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestStateDiff(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	trie, _ := NewStateTrie(TrieID(common.Hash{}), db)

	// Insert a batch of entries, all the nodes should be marked as inserted
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	hash, set, _ := trie.Commit(false)
	db.Update(hash, common.Hash{}, NewWithNodeSet(set))

	fmt.Println("***************** ROUND 1 *******************")
	trie, _ = NewStateTrie(TrieID(hash), db)
	it := trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path())
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	// Delete all the elements, check deletion set
	trie, _ = NewStateTrie(TrieID(hash), db)
	for i, val := range vals {
		if i == len(vals)-1 {
			break
		}
		trie.Delete([]byte(val.k))
	}
	hash2, set2, _ := trie.Commit(false)
	db.Update(hash2, hash, NewWithNodeSet(set2))

	fmt.Println("***************** ROUND 2 *******************")
	trie, _ = NewStateTrie(TrieID(hash2), db)
	it = trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path())
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	resolvePrevLeaves(set2.nodes, func(path []byte, blob []byte) {
		fmt.Println("path", path, "blob", blob)
	})

}

func TestStateDiff2(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	trie, _ := NewStateTrie(TrieID(common.Hash{}), db)

	// Insert a batch of entries, all the nodes should be marked as inserted
	vals := []struct{ k, v string }{
		{"do", "v"},
		{"ether", "w"},
		{"horse", "s"},
		{"shaman", "e"},
		{"doge", "c"},
		{"dog", "y"},
		{"somethingveryoddindeedthis is", "m"},
	}
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	hash, set, _ := trie.Commit(false)
	db.Update(hash, common.Hash{}, NewWithNodeSet(set))

	fmt.Println("***************** ROUND 1 *******************")
	trie, _ = NewStateTrie(TrieID(hash), db)
	it := trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path(), "isEmbedded", it.Hash() == (common.Hash{}))
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	set.forEachWithOrder(true, func(path string, n *nodeWithPrev) {
		fmt.Println("Dirty", []byte(path))
	})
	fmt.Println("=================================")

	// Delete all the elements, check deletion set
	trie, _ = NewStateTrie(TrieID(hash), db)
	for i, val := range vals {
		if i == len(vals)-1 {
			break
		}
		trie.Delete([]byte(val.k))
	}
	hash2, set2, _ := trie.Commit(false)
	db.Update(hash2, hash, NewWithNodeSet(set2))

	fmt.Println("***************** ROUND 2 *******************")
	trie, _ = NewStateTrie(TrieID(hash2), db)
	it = trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path(), it.Hash() == (common.Hash{}))
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	set2.forEachWithOrder(true, func(path string, n *nodeWithPrev) {
		fmt.Println("Dirty", []byte(path), "isDeleted", n.isDeleted())
	})
	fmt.Println("=================================")
}

func TestStateDiff3(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	trie, _ := New(TrieID(common.Hash{}), db)

	// Insert a batch of entries, all the nodes should be marked as inserted
	vals := []struct{ k, v string }{
		{"do", "v"},
		{"ether", "w"},
		{"horse", "s"},
		{"shaman", "e"},
		{"doge", "c"},
		{"dog", "y"},
		{"somethingveryoddindeedthis is", "m"},
	}
	for _, val := range vals {
		trie.Update([]byte(val.k), []byte(val.v))
	}
	hash, set, _ := trie.Commit(false)
	db.Update(hash, common.Hash{}, NewWithNodeSet(set))

	fmt.Println("***************** ROUND 1 *******************")
	trie, _ = New(TrieID(hash), db)
	it := trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path(), "isEmbedded", it.Hash() == (common.Hash{}))
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	set.forEachWithOrder(true, func(path string, n *nodeWithPrev) {
		fmt.Println("Dirty", []byte(path))
	})
	fmt.Println("=================================")

	// Delete all the elements, check deletion set
	trie, _ = New(TrieID(hash), db)
	for i, val := range vals {
		if i == len(vals)-1 {
			break
		}
		trie.Delete([]byte(val.k))
	}
	hash2, set2, _ := trie.Commit(false)
	db.Update(hash2, hash, NewWithNodeSet(set2))

	fmt.Println("***************** ROUND 2 *******************")
	trie, _ = New(TrieID(hash2), db)
	it = trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Println("NODE", it.Path(), it.Hash() == (common.Hash{}))
	}
	fmt.Println("======================================")

	it = trie.NodeIterator(nil)
	for it.Next(true) {
		if it.Leaf() {
			fmt.Println("LEAF", it.LeafKey())
		}
	}
	fmt.Println("=================================")

	set2.forEachWithOrder(true, func(path string, n *nodeWithPrev) {
		fmt.Println("Dirty", []byte(path), "isDeleted", n.isDeleted())
	})
	fmt.Println("=================================")

	resolvePrevLeaves(set2.nodes, func(path []byte, blob []byte) {
		fmt.Println("path", path, "blob", blob)
	})
}
