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

// ethflare offers a bunch of functionalities of serving and retrieval
// ethereum data(chain and state)
package main

import (
	"fmt"
	"os"
	"path"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"gopkg.in/urfave/cli.v1"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
)

var app *cli.App

func init() {
	app = utils.NewApp(gitCommit, gitDate, "ethflare(ethereum data cdn)")
	app.Commands = []cli.Command{
		commandServer,
		commandClient,
	}
	app.Flags = []cli.Flag{
		serverHostFlag,
		dataDirFlag,
		backendRateLimitFlag,
		backendFlag,
	}
	cli.CommandHelpTemplate = utils.OriginCommandHelpTemplate
}

// Commonly used command line flags.
var (
	serverHostFlag = cli.StringFlag{
		Name:  "host",
		Value: "localhost:8545",
		Usage: "The http endpoint server listens at",
	}
	dataDirFlag = utils.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the ethflare indexes and metadata",
		Value: utils.DirectoryString(path.Join(node.DefaultDataDir(), "ethflare")),
	}
	backendRateLimitFlag = cli.IntFlag{
		Name:  "ratelimit",
		Value: 10000, // The default limit for each backend is 10,000 requests per second
		Usage: "The rate limit for each coonnected backend",
	}
	backendFlag = cli.StringFlag{
		Name:  "backends",
		Usage: "Comma separated websocket endpoint of ethflare backends",
	}
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	fdlimit.Raise(2048)

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
