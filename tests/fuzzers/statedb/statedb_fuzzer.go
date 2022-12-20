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

package statedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"io"
	"math/big"
	"os"
)

const (
	opSetBalance = iota
	opSetNonce
	opSetState
	opSetCode
	opSuicide
	opCreateAccount
	opHash
	opFinalize
	opCommit
)

func opToString(op int) string {
	switch op {
	case opSetBalance:
		return "SetBalance"
	case opSetNonce:
		return "SetNonce"
	case opSetState:
		return "SetState"
	case opSetCode:
		return "SetCode"
	case opSuicide:
		return "Suicide"
	case opCreateAccount:
		return "CreateAccount"
	case opHash:
		return "Hash"
	case opFinalize:
		return "Finalize"
	case opCommit:
		return "Commit"
	default:
		return "Unknown"
	}
}

type fuzzer struct {
	input     io.Reader
	exhausted bool
	debugging bool
}

func (f *fuzzer) read(size int) []byte {
	out := make([]byte, size)
	if _, err := f.input.Read(out); err != nil {
		f.exhausted = true
	}
	return out
}

func (f *fuzzer) readBool() bool {
	b := f.read(1)
	return b[0]%1 == 0
}

func (f *fuzzer) readUint32() uint32 {
	return binary.BigEndian.Uint32(f.read(4))
}

func (f *fuzzer) readUint64() uint64 {
	return binary.BigEndian.Uint64(f.read(8))
}

func (f *fuzzer) intN(n int) int {
	b := f.read(1)
	return int(b[0]) % n
}

type object struct {
	storage  state.Storage
	balance  *big.Int
	code     []byte
	nonce    uint64
	suicided bool
	deleted  bool
}

type env struct {
	objects   map[common.Address]*object
	finalized bool
	root      common.Hash

	// Statistics
	finalizes int
	commits   int
}

func newEnv() *env {
	return &env{
		objects: make(map[common.Address]*object),
	}
}

func (e *env) setBalance(addr common.Address, balance *big.Int) {
	obj := e.objects[addr]
	if obj == nil || obj.deleted {
		panic("missing object")
	}
	obj.balance = new(big.Int).Set(balance)
}

func (e *env) setNonce(addr common.Address, nonce uint64) {
	obj := e.objects[addr]
	if obj == nil || obj.deleted {
		panic("missing object")
	}
	obj.nonce = nonce
}

func (e *env) setState(addr common.Address, key, val common.Hash) {
	obj := e.objects[addr]
	if obj == nil || obj.deleted {
		panic("missing object")
	}
	if val == (common.Hash{}) {
		delete(obj.storage, key)
	} else {
		obj.storage[key] = val
	}
}

func (e *env) setCode(addr common.Address, code []byte) {
	obj := e.objects[addr]
	if obj == nil || obj.deleted {
		panic("missing object")
	}
	obj.code = common.CopyBytes(code)
}

func (e *env) suicide(addr common.Address) {
	obj := e.objects[addr]
	if obj == nil || obj.deleted {
		panic("missing object")
	}
	obj.suicided = true
}

func (e *env) create(addr common.Address) {
	obj := e.objects[addr]
	if obj != nil && !obj.deleted {
		panic("unexpected object")
	}
	e.objects[addr] = &object{
		storage: make(state.Storage),
	}
}

func (e *env) finalize() {
	for _, obj := range e.objects {
		if obj.suicided {
			obj.deleted = true
		}
	}
	e.finalized = true
	e.finalizes += 1
}

func (e *env) commit(root common.Hash) {
	e.root = root
	e.commits += 1
}

func (e *env) prepare() {
	e.finalized = false
}

func (e *env) nonDeleted() []common.Address {
	var ret []common.Address
	for addr, obj := range e.objects {
		if obj.deleted {
			continue
		}
		ret = append(ret, addr)
	}
	return ret
}

func (e *env) deleted() []common.Address {
	var ret []common.Address
	for addr, obj := range e.objects {
		if !obj.deleted {
			continue
		}
		ret = append(ret, addr)
	}
	return ret
}

type testAction struct {
	op   int
	fn   func(testAction, *state.StateDB, *env)
	args []uint64
	addr common.Address
}

// newTestAction creates a random action that changes state.
func newTestAction(e *env, f *fuzzer) testAction {
	actions := []testAction{
		{
			op: opSetBalance,
			fn: func(a testAction, s *state.StateDB, e *env) {
				s.SetBalance(a.addr, big.NewInt(int64(a.args[0])))
				e.setBalance(a.addr, big.NewInt(int64(a.args[0])))
			},
			args: make([]uint64, 1),
		},
		{
			op: opSetNonce,
			fn: func(a testAction, s *state.StateDB, e *env) {
				nonce := s.GetNonce(a.addr)
				s.SetNonce(a.addr, nonce+1)
				e.setNonce(a.addr, nonce+1)
			},
		},
		{
			op: opSetState,
			fn: func(a testAction, s *state.StateDB, e *env) {
				var key, val common.Hash
				binary.BigEndian.PutUint64(key[:], a.args[0])
				binary.BigEndian.PutUint64(val[:], a.args[1])
				s.SetState(a.addr, key, val)
				e.setState(a.addr, key, val)
			},
			args: make([]uint64, 2),
		},
		{
			op: opSetCode,
			fn: func(a testAction, s *state.StateDB, e *env) {
				code := make([]byte, 16)
				binary.BigEndian.PutUint64(code, a.args[0])
				binary.BigEndian.PutUint64(code[8:], a.args[1])
				s.SetCode(a.addr, code)
				e.setCode(a.addr, code)
			},
			args: make([]uint64, 2),
		},
		{
			op: opSuicide,
			fn: func(a testAction, s *state.StateDB, e *env) {
				s.Suicide(a.addr)
				e.suicide(a.addr)
			},
		},
		{
			op: opCreateAccount,
			fn: func(a testAction, s *state.StateDB, e *env) {
				s.CreateAccount(a.addr)
				e.create(a.addr)
			},
		},
		{
			op: opHash,
			fn: func(a testAction, s *state.StateDB, e *env) {
				s.IntermediateRoot(false)
			},
		},
		{
			op: opFinalize,
			fn: func(a testAction, s *state.StateDB, e *env) {
				s.Finalise(false)
				e.finalize()
			},
		},
		{
			op: opCommit,
			fn: func(a testAction, s *state.StateDB, e *env) {
				root, _ := s.Commit(false)
				e.commit(root)
			},
		},
	}
	if e.finalized && f.intN(3) == 0 {
		return generate(actions[opCommit], f, e)
	}
	e.prepare()
	if len(e.nonDeleted()) == 0 {
		return generate(actions[opCreateAccount], f, e)
	}
	index := f.intN(len(actions))
	return generate(actions[index], f, e)
}

func generate(action testAction, f *fuzzer, e *env) testAction {
	switch action.op {
	case opSetBalance:
		nonDeleted := e.nonDeleted()
		action.addr = nonDeleted[f.intN(len(nonDeleted))]
		if f.readBool() {
			action.args[0] = uint64(f.readUint32())
		} else {
			// Set balance to zero
		}
	case opSetNonce:
		nonDeleted := e.nonDeleted()
		action.addr = nonDeleted[f.intN(len(nonDeleted))]
	case opSetState:
		nonDeleted := e.nonDeleted()
		action.addr = nonDeleted[f.intN(len(nonDeleted))]
		obj := e.objects[action.addr]
		if f.intN(3) == 0 && len(obj.storage) > 0 {
			// Reset state
			for key := range obj.storage {
				action.args[0] = binary.BigEndian.Uint64(key[:8])
				break
			}
			action.args[1] = 0
		} else {
			// Write new state
			action.args[0] = f.readUint64()
			action.args[1] = f.readUint64()
		}
	case opSetCode:
		nonDeleted := e.nonDeleted()
		action.addr = nonDeleted[f.intN(len(nonDeleted))]
		action.args[0] = f.readUint64()
		action.args[1] = f.readUint64()
	case opSuicide:
		nonDeleted := e.nonDeleted()
		action.addr = nonDeleted[f.intN(len(nonDeleted))]
	case opCreateAccount:
		deleted := e.deleted()
		if len(deleted) > 0 && f.readBool() {
			action.addr = deleted[f.intN(len(deleted))]
		} else {
			action.addr = common.BytesToAddress(f.read(common.AddressLength))
		}
	case opHash, opFinalize, opCommit:
		// no arg required
	default:
		panic("unknown action")
	}
	return action
}

// Fuzz is the fuzzing entry-point.
// The function must return
//
//   - 1 if the fuzzer should increase priority of the
//     given input during subsequent fuzzing (for example, the input is lexically
//     correct and was parsed successfully);
//   - -1 if the input must not be added to corpus even if gives new coverage; and
//   - 0 otherwise
//
// other values are reserved for future use.
func Fuzz(data []byte) int {
	if len(data) < 1024 {
		return -1
	}
	f := fuzzer{
		input:     bytes.NewReader(data),
		exhausted: false,
	}
	return f.fuzz()
}

func Debug(data []byte) int {
	f := fuzzer{
		input:     bytes.NewReader(data),
		exhausted: false,
		debugging: true,
	}
	return f.fuzz()
}

func openDatabase(kv ethdb.KeyValueStore, ancient string) *trie.Database {
	disk, _ := rawdb.NewDatabaseWithFreezer(kv, ancient, "", false)
	return trie.NewDatabase(disk, &trie.Config{
		Scheme:    rawdb.PathScheme,
		DirtySize: 1,
	})
}

func (f *fuzzer) fuzz() int {
	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	var (
		kv     = rawdb.NewMemoryDatabase()
		tdb    = openDatabase(kv, tempDir)
		snap   = rawdb.NewMemoryDatabase()
		s, _   = state.New(common.Hash{}, state.NewDatabaseWithNodeDB(kv, tdb), nil)
		e      = newEnv()
		parent = types.EmptyRootHash
	)
	for !f.exhausted {
		action := newTestAction(e, f)
		action.fn(action, s, e)

		// Verify at the end of block execution
		if action.op == opCommit && e.root != parent {
			// Write all in-memory nodes into disk
			err := tdb.Commit(e.root, false)
			if err != nil {
				panic(fmt.Sprintf("Failed to commit triedb %v", err))
			}
			backup := rawdb.NewMemoryDatabase()
			copyDB(kv, backup)

			// Verify the state after recover is still correct
			err = tdb.Recover(e.root)
			if err != nil {
				panic(fmt.Sprintf("Failed to recover triedb %v", err))
			}
			verifyState(kv, snap)

			// Verification passed, construct for next round
			tdb.Close()
			tdb = openDatabase(backup, tempDir)

			s, err = state.New(e.root, state.NewDatabaseWithNodeDB(backup, tdb), nil)
			if err != nil {
				panic(fmt.Sprintf("Failed to create statedb %v", err))
			}
			snap = rawdb.NewMemoryDatabase()
			if err := copyDB(backup, snap); err != nil {
				panic("Failed to copy disk snapshot")
			}
		}
	}
	return 1
}

func copyDB(src ethdb.Database, dest ethdb.Database) error {
	var (
		it    = src.NewIterator(nil, nil)
		batch = dest.NewBatch()
	)
	defer it.Release()

	for it.Next() {
		batch.Put(it.Key(), it.Value())
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	return batch.Write()
}

func verifyIt(dbA, dbB ethdb.Database, prefix []byte) {
	itA := dbA.NewIterator(prefix, nil)
	itB := dbB.NewIterator(prefix, nil)
	defer itA.Release()
	defer itB.Release()

	for {
		if itA.Next() && !itB.Next() {
			panic("dbB has more data")
		}
		if !itA.Next() && itB.Next() {
			panic("dbA has more data")
		}
		if !itA.Next() && !itB.Next() {
			break
		}
		keyA, valA := itA.Key(), itA.Value()
		keyB, valB := itB.Key(), itB.Value()
		if !bytes.Equal(keyA, keyB) {
			panic("different key")
		}
		if !bytes.Equal(valA, valB) {
			panic("different value")
		}
	}
}

func verifyState(dbA, dbB ethdb.Database) {
	verifyIt(dbA, dbB, rawdb.TrieNodeAccountPrefix)
	verifyIt(dbA, dbB, rawdb.TrieNodeStoragePrefix)
}
