package triedb

import "github.com/ethereum/go-ethereum/common"

// Reader wraps the Node and NodeBlob method of a backing trie store.
type Reader interface {
	// NodeBlob retrieves the RLP-encoded trie node blob with the provided trie
	// identifier, hexary node path and the corresponding node hash.
	// No error will be returned if the node is not found.
	NodeBlob(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}
