// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/ethflare"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"gopkg.in/urfave/cli.v1"
)

var commandServer = cli.Command{
	Name:  "server",
	Usage: "Start ethflare server for data indexing and serving",
	Flags: []cli.Flag{
		serverHostFlag,
		dataDirFlag,
		backendRateLimitFlag,
		backendFlag,
	},
	Action: utils.MigrateFlags(startServer),
}

// startServer creates a server instance and start serving.
func startServer(ctx *cli.Context) error {
	// Gather all the endpoints that server should connect
	var (
		backends   []*rpc.Client
		identities []string
	)
	for _, endpoint := range strings.Split(ctx.String(backendFlag.Name), ",") {
		endpoint := strings.TrimSpace(endpoint)
		client, err := rpc.Dial(endpoint)
		if err != nil {
			log.Warn("Failed to dial remote backend", "endpoint", endpoint, "err", err)
			continue
		}
		backends = append(backends, client)
		identities = append(identities, fmt.Sprintf("backend-%d", len(identities)+1))
	}
	// Initialize the ethflare server
	server, err := ethflare.NewServer(&ethflare.ServerConfig{
		Path:      ctx.String(dataDirFlag.Name),
		RateLimit: uint64(ctx.Int(backendRateLimitFlag.Name)),
	})
	if err != nil {
		log.Crit("Failed to start server", "err", err)
	}
	server.Start(ctx.String(serverHostFlag.Name))

	// Register all backends to connect
	server.RegisterBackends(identities, backends)

	// todo run as the backend daemon
	time.Sleep(time.Hour)
	return nil
}
