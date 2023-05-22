// Copyright 2023 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// A stateTest checks that the state changes are correctly captured. Instances
// of this test with pseudorandom content are created by Generate.
//
// The test works as follows:
//
// A list of states are created by applying actions. The state changes between
// each state instance are tracked and be verified.
type stateTest struct {
	addrs   []common.Address               // all account addresses
	hashes  map[common.Hash]common.Address // preimages of account hash
	actions [][]testAction                 // modifications to the state
	chunk   int                            // The number of actions of each chunk
	err     error                          // failure details are reported through this field
}

// newStateTestAction creates a random action that changes state.
func newStateTestAction(addr common.Address, r *rand.Rand, index int) testAction {
	actions := []testAction{
		{
			name: "SetBalance",
			fn: func(a testAction, s *StateDB) {
				s.SetBalance(addr, big.NewInt(a.args[0]))
			},
			args: make([]int64, 1),
		},
		{
			name: "SetNonce",
			fn: func(a testAction, s *StateDB) {
				s.SetNonce(addr, uint64(a.args[0]))
			},
			args: make([]int64, 1),
		},
		{
			name: "SetState",
			fn: func(a testAction, s *StateDB) {
				var key, val common.Hash
				binary.BigEndian.PutUint16(key[:], uint16(a.args[0]))
				binary.BigEndian.PutUint16(val[:], uint16(a.args[1]))
				s.SetState(addr, key, val)
			},
			args: make([]int64, 2),
		},
		{
			name: "SetCode",
			fn: func(a testAction, s *StateDB) {
				code := make([]byte, 16)
				binary.BigEndian.PutUint64(code, uint64(a.args[0]))
				binary.BigEndian.PutUint64(code[8:], uint64(a.args[1]))
				s.SetCode(addr, code)
			},
			args: make([]int64, 2),
		},
		{
			name: "CreateAccount",
			fn: func(a testAction, s *StateDB) {
				s.CreateAccount(addr)
			},
		},
		{
			name: "Suicide",
			fn: func(a testAction, s *StateDB) {
				s.Suicide(addr)
			},
		},
	}
	var pre = index != -1
	if index == -1 {
		index = r.Intn(len(actions))
	}
	action := actions[index]
	var names []string
	if !action.noAddr {
		names = append(names, addr.Hex())
	}
	for i := range action.args {
		if pre {
			action.args[i] = rand.Int63n(10000) + 1 // set balance to non-zero
		} else {
			action.args[i] = rand.Int63n(10000)
		}
		names = append(names, fmt.Sprint(action.args[i]))
	}
	action.name += " " + strings.Join(names, ", ")
	return action
}

// Generate returns a new snapshot test of the given size. All randomness is
// derived from r.
func (*stateTest) Generate(r *rand.Rand, size int) reflect.Value {
	addrs := make([]common.Address, 5)
	hashes := make(map[common.Hash]common.Address)
	for i := range addrs {
		addrs[i][0] = byte(i)
		hashes[crypto.Keccak256Hash(addrs[i].Bytes())] = addrs[i]
	}
	addr := common.HexToAddress("0xdeadbeef")

	actions := make([][]testAction, 2)
	for i := 0; i < len(actions); i++ {
		actions[i] = make([]testAction, size)
		for j := range actions[i] {
			// Always include a set balance action to make sure
			// the state changes are not empty.
			if j == 0 {
				actions[i][j] = newStateTestAction(addr, r, 0)
				continue
			}
			actions[i][j] = newStateTestAction(addrs[r.Intn(len(addrs))], r, -1)
		}
	}
	// Generate snapshot indexes.
	chunk := int(math.Sqrt(float64(size)))
	if size > 0 && chunk == 0 {
		chunk = 1
	}
	return reflect.ValueOf(&stateTest{
		addrs:   addrs,
		hashes:  hashes,
		actions: actions,
		chunk:   chunk,
	})
}

func (test *stateTest) String() string {
	out := new(bytes.Buffer)
	for i, actions := range test.actions {
		fmt.Fprintf(out, "---- block %d ----\n", i)
		for j, action := range actions {
			if j%test.chunk == 0 {
				fmt.Fprintf(out, "---- transaction %d ----\n", j/test.chunk)
			}
			fmt.Fprintf(out, "%4d: %s\n", j%test.chunk, action.name)
		}
	}
	return out.String()
}

func (test *stateTest) run() bool {
	var (
		roots       []common.Hash
		accountList []map[common.Hash][]byte
		storageList []map[common.Hash]map[common.Hash][]byte
		onCommit    = func(accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) {
			accountList = append(accountList, copyAccounts(accounts))
			storageList = append(storageList, copyStorages(storages))
		}
		disk      = rawdb.NewMemoryDatabase()
		tdb       = trie.NewDatabaseWithConfig(disk, &trie.Config{OnCommit: onCommit})
		sdb       = NewDatabaseWithNodeDB(disk, tdb)
		byzantium = rand.Intn(2) == 0
	)
	for i, actions := range test.actions {
		root := types.EmptyRootHash
		if i != 0 {
			root = roots[len(roots)-1]
		}
		state, err := New(root, sdb, nil)
		if err != nil {
			panic(err)
		}
		for i, action := range actions {
			if i%test.chunk == 0 && i != 0 {
				if byzantium {
					state.Finalise(true) // call finalise at the transaction boundary
				} else {
					state.IntermediateRoot(true) // call intermediateRoot at the transaction boundary
				}
			}
			action.fn(action, state)
		}
		if byzantium {
			state.Finalise(true) // call finalise at the transaction boundary
		} else {
			state.IntermediateRoot(true) // call intermediateRoot at the transaction boundary
		}
		nroot, err := state.Commit(true) // call commit at the block boundary
		if err != nil {
			panic(err)
		}
		// Filter out non-change state transition
		if nroot == root {
			return true
		}
		roots = append(roots, nroot)
	}
	for i := 0; i < len(test.actions); i++ {
		root := types.EmptyRootHash
		if i != 0 {
			root = roots[i-1]
		}
		err := test.checkState(root, roots[i], tdb, accountList[i], storageList[i])
		if err != nil {
			test.err = err
			return false
		}
	}
	return true
}

func (test *stateTest) checkState(root common.Hash, next common.Hash, db *trie.Database, accountsOrigin map[common.Hash][]byte, storagesOrigin map[common.Hash]map[common.Hash][]byte) error {
	tr, err := trie.New(trie.StateTrieID(root), db)
	if err != nil {
		return err
	}
	ntr, err := trie.New(trie.StateTrieID(next), db)
	if err != nil {
		return err
	}
	for addrHash, account := range accountsOrigin {
		blob, err := tr.Get(addrHash.Bytes())
		if err != nil {
			return err
		}
		nBlob, err := ntr.Get(addrHash.Bytes())
		if err != nil {
			return err
		}
		// State diff says the account was not present previously,
		// ensure it was indeed not existing in the trie. Besides,
		// the nil->nil state change is regarded as invalid.
		if len(account) == 0 {
			if len(blob) != 0 {
				return fmt.Errorf("unexpected account %v", blob)
			}
			if len(nBlob) == 0 {
				return fmt.Errorf("invalid state change %s", test.hashes[addrHash])
			}
			continue
		}
		// Ensure account data from two sources are matched.
		var acct types.StateAccount
		if err := rlp.DecodeBytes(blob, &acct); err != nil {
			return err
		}
		slim := types.SlimAccountRLP(acct)
		if !bytes.Equal(slim, account) {
			return fmt.Errorf("account %s, got %v, want %v", addrHash.Hex(), slim, account)
		}
		slots, ok := storagesOrigin[addrHash]
		if !ok {
			// State diff says there is no storage change, ensure the storage
			// root it not changed.
			if len(nBlob) == 0 {
				if acct.Root != types.EmptyRootHash {
					return errors.New("expect storage deletion")
				}
			} else {
				var nAcct types.StateAccount
				if err := rlp.DecodeBytes(nBlob, &nAcct); err != nil {
					return err
				}
				if nAcct.Root != acct.Root {
					return errors.New("storage root is expected to be same")
				}
			}
			continue
		}
		for shash, slot := range slots {
			st, err := trie.New(trie.StorageTrieID(root, addrHash, acct.Root), db)
			if err != nil {
				return err
			}
			blob, err := st.Get(shash.Bytes())
			if err != nil {
				return err
			}
			if !bytes.Equal(blob, slot) {
				return fmt.Errorf("account %s slot %s, got %v, want %v", addrHash.Hex(), shash.Hex(), blob, slot)
			}
			// Slot was not present, ensure it's present in next block,
			// otherwise nil->nil is invalid.
			if len(slot) == 0 {
				var nAcct types.StateAccount
				if err := rlp.DecodeBytes(nBlob, &nAcct); err != nil {
					return err
				}
				nst, err := trie.New(trie.StorageTrieID(next, addrHash, nAcct.Root), db)
				if err != nil {
					return err
				}
				nslot, _ := nst.Get(shash.Bytes())
				if len(nslot) == 0 {
					return errors.New("invalid storage change")
				}
			}
		}
	}
	return nil
}

func TestStateChanges(t *testing.T) {
	config := &quick.Config{MaxCount: 1000}
	err := quick.Check((*stateTest).run, config)
	if cerr, ok := err.(*quick.CheckError); ok {
		test := cerr.In[0].(*stateTest)
		t.Errorf("%v:\n%s", test.err, test)
	} else if err != nil {
		t.Error(err)
	}
}
