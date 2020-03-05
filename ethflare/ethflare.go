package main

import (
	"os"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	client, err := rpc.Dial("/home/karalabe/.ethereum/goerli/geth.ipc")
	if err != nil {
		log.Crit("Failed to dial remote node", "err", err)
	}
	backend, err := NewBackend("goerli", client)
	if err != nil {
		log.Crit("Failed to create backend", "err", err)
	}

	sched := NewScheduler()
	sched.RegisterBackend(backend)

	time.Sleep(time.Hour)
}
