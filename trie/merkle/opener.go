// Copyright 2022 The go-ethereum Authors
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

package merkle

import (
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// Opener implements state.TrieOpener for constructing tries.
type Opener struct {
	db database.NodeDatabase
}

// NewOpener creates the merkle trie opener.
func NewOpener(db database.NodeDatabase) trie.Opener {
	return &Opener{db: db}
}

// Open opens the trie specified by the id.
func (o *Opener) Open(id *trie.ID) (trie.Trie, error) {
	return New(id, o.db)
}
