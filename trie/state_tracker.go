// Copyright 2021 The go-ethereum Authors
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
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// stateTracker keeps track of the newly modified trie nodes.
type stateTracker struct {
	lock     sync.RWMutex
	inserted map[string]struct{}
	deleted  map[string]struct{}
	updated  map[string][]byte
}

func newTracker() *stateTracker {
	return &stateTracker{
		inserted: make(map[string]struct{}),
		deleted:  make(map[string]struct{}),
		updated:  make(map[string][]byte),
	}
}

// onInsert tracks the newly inserted trie node. If it's already
// in the deletion set(resurrected node), then just wipe it from
// the deletion set as the "untouched".
func (tracker *stateTracker) onInsert(key []byte) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	if _, present := tracker.deleted[string(key)]; present {
		delete(tracker.deleted, string(key))
		return
	}
	tracker.inserted[string(key)] = struct{}{}
}

// onDelete tracks the newly deleted trie node. If it's already
// in the addition set, then just wipe it from the addition set
// as the "untouched".
func (tracker *stateTracker) onDelete(key []byte) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	if _, present := tracker.inserted[string(key)]; present {
		delete(tracker.inserted, string(key))
		return
	}
	tracker.deleted[string(key)] = struct{}{}
}

// onUpdate tracks the committed trie node during the commit
// operation. The committed nodes include the newly inserted
// and updated nodes. The deleted nodes are not included here.
func (tracker *stateTracker) onUpdate(key, val []byte) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	tracker.updated[string(key)] = common.CopyBytes(val)
}

// keylist returns the tracked inserted/deleted node keys in list.
func (tracker *stateTracker) keylist() ([][]byte, [][]byte) {
	tracker.lock.RLock()
	defer tracker.lock.RUnlock()

	var inserted, deleted [][]byte
	for key := range tracker.inserted {
		inserted = append(inserted, []byte(key))
	}
	for key := range tracker.deleted {
		deleted = append(deleted, []byte(key))
	}
	return inserted, deleted
}

// merge merges the state diff from the other state tracker.
func (tracker *stateTracker) merge(other *stateTracker) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()

	other.lock.RLock()
	defer other.lock.RUnlock()

	for key := range other.inserted {
		tracker.inserted[key] = struct{}{}
	}
	for key := range other.deleted {
		tracker.deleted[key] = struct{}{}
	}
	for key, val := range other.updated {
		tracker.updated[key] = val
	}
}

// get retrieves the trie state in the state tracker. Note the
// returned value shouldn't be changed by callers.
func (tracker *stateTracker) get(key string) ([]byte, bool) {
	tracker.lock.RLock()
	defer tracker.lock.RUnlock()

	if _, ok := tracker.deleted[key]; ok {
		return nil, true
	}
	if blob, ok := tracker.updated[key]; ok {
		return blob, true
	}
	return nil, false
}
