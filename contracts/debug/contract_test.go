package contract

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
)

func TestBlah(t *testing.T) {
	var (
		key, _  = crypto.GenerateKey()
		user, _ = bind.NewKeyedTransactorWithChainID(key, big.NewInt(1337))
		sim     = backends.NewSimulatedBackend(core.GenesisAlloc{user.From: {Balance: big.NewInt(1000000000000000000)}}, ethconfig.Defaults.Miner.GasCeil)
	)
	defer sim.Close()

	_, _, d, err := DeployContract(user, sim)
	if err != nil {
		t.Fatalf("Failed to deploy contract %v", err)
	}
	sim.Commit()

	_, err = d.TestEvent(user)
	if err != nil {
		t.Fatalf("Failed to call contract %v", err)
	}
	sim.Commit()

	it, err := d.FilterStructEvent(nil)
	if err != nil {
		t.Fatalf("Failed to filter contract event %v", err)
	}
	var count int
	for it.Next() {
		if it.Event.S.A.Cmp(big.NewInt(1)) != 0 {
			t.Fatal("Unexpected contract event")
		}
		count += 1
	}
	if count != 1 {
		t.Fatal("Unexpected contract event number")
	}
}
