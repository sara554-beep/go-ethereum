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
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethflare"
	"gopkg.in/urfave/cli.v1"
)

var commandClient = cli.Command{
	Name:  "client",
	Usage: "Ethflare client for command line usage",
	Flags: []cli.Flag{
		serverHostFlag,
	},
	Action: utils.MigrateFlags(runClient),
}

func runClient(ctx *cli.Context) error {
	client := ethflare.NewClient(ctx.String(serverHostFlag.Name))
	header, err := client.GetBlockHeader(context.Background(), common.HexToHash("0x983542993423d435c0873af540ab08b02ea611c01f3a9e78c7c84e48a6608a4e"))
	if err != nil {
		fmt.Println("Failed to retrieve header", err)
		return nil
	}
	fmt.Println("Header get", header.Number)
	return nil
}
