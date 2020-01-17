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

package lotterybook

func TestClaimLottery(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	// Start the automatic blockchain.
	var exit = make(chan struct{})
	defer close(exit)
	go func() {
		ticker := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case <-ticker.C:
				env.backend.Commit()
			case <-exit:
				return
			}
		}
	}()
	drawer, err := NewChequeDrawer(env.drawerAddr, env.contractAddr, bind.NewKeyedTransactor(env.drawerKey), nil, env.backend.Blockchain(), env.backend, env.backend, env.drawerDb)
	if err != nil {
		t.Fatalf("Faield to create drawer, err: %v", err)
	}
	defer drawer.Close()
	drawer.keySigner = func(data []byte) ([]byte, error) {
		sig, _ := crypto.Sign(data, env.drawerKey)
		return sig, nil
	}
	drawee, err := NewChequeDrawee(bind.NewKeyedTransactor(env.draweeKey), env.draweeAddr, drawer.ContractAddr(), env.backend.Blockchain(), env.backend, env.backend, env.draweeDb)
	if err != nil {
		t.Fatalf("Faield to create drawee, err: %v", err)
	}
	defer drawee.Close()

	current := env.backend.Blockchain().CurrentHeader().Number.Uint64()
	id, err := drawer.createLottery(context.Background(), []common.Address{env.draweeAddr}, []uint64{128}, current+30)
	if err != nil {
		t.Fatalf("Faield to create lottery, err: %v", err)
	}
	cheque, err := drawer.issueCheque(env.draweeAddr, id, 128)
	if err != nil {
		t.Fatalf("Faield to create cheque, err: %v", err)
	}
	drawee.AddCheque(env.drawerAddr, cheque)
	done := make(chan struct{}, 1)
	drawee.onClaimedHook = func(id common.Hash) {
		if id == cheque.LotteryId {
			done <- struct{}{}
		}
	}
	select {
	case <-done:
	case <-time.NewTimer(10 * time.Second).C:
		t.Fatalf("timeout")
	}
}
