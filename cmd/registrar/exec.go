// Copyright 2018 The go-ethereum Authors
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
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/registrar/contract"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/urfave/cli.v1"
)

var commandDeployContract = cli.Command{
	Name:        "deploy",
	Usage:       "deploy a registrar contract with specified trusted signers.",
	Description: "",
	Flags: []cli.Flag{
		trustedSignerFlag,
		clientURLFlag,
		keyFileFlag,
		utils.PasswordFileFlag,
	},
	Action: utils.MigrateFlags(deployContract),
}

var commandRegisterCheckpoint = cli.Command{
	Name:  "register",
	Usage: "register specified local checkpoint into contract",
	Description: `
Register the specified checkpoint which generated by connected node with a 
authorised private key. It can help to fasten the light client checkpoint 
syncing.
`,
	Flags: []cli.Flag{
		contractAddrFlag,
		clientURLFlag,
		checkpointIndexFlag,
		keyFileFlag,
		utils.TestnetFlag,
		utils.RinkebyFlag,
		utils.PasswordFileFlag,
	},
	Action: utils.MigrateFlags(registerCheckpoint),
}

// deployContract deploys the checkpoint registrar contract.
//
// Note the network where the contract is deployed depends on
// the network where the connected node is located.
func deployContract(ctx *cli.Context) error {
	var signerAddress []common.Address
	signers := strings.Split(ctx.GlobalString(trustedSignerFlag.Name), ",")
	for _, account := range signers {
		if trimmed := strings.TrimSpace(account); !common.IsHexAddress(trimmed) {
			utils.Fatalf("Invalid account in --signer: %s", trimmed)
		} else {
			signerAddress = append(signerAddress, common.HexToAddress(account))
		}
	}
	addr, tx, _, err := contract.DeployContract(bind.NewKeyedTransactor(getPrivateKey(ctx).PrivateKey), setupClient(ctx), signerAddress, big.NewInt(int64(params.CheckpointFrequency)),
		big.NewInt(int64(params.CheckpointProcessConfirmations)), big.NewInt(int64(params.FreezeThreshold)))
	if err != nil {
		utils.Fatalf("Failed to deploy registrar contract %v", err)
	}
	log.Info("Deploy registrar contract successfully", "address", addr, "tx", tx.Hash())
	return nil
}

// registerCheckpoint registers the specified checkpoint which generated by connected
// node with a authorised private key.
func registerCheckpoint(ctx *cli.Context) error {
	var (
		checkpoint *light.TrustedCheckpoint
		rpcClient  = setupDialContext(ctx)
	)
	// Fetch specified checkpoint from connected node.
	if ctx.GlobalIsSet(checkpointIndexFlag.Name) {
		var result [3]string
		index := ctx.GlobalInt64(checkpointIndexFlag.Name)
		if err := rpcClient.Call(&result, "les_getCheckpoint", index); err != nil {
			utils.Fatalf("Failed to get local checkpoint %v", err)
		}
		checkpoint = &light.TrustedCheckpoint{
			SectionIdx:  uint64(index),
			SectionHead: common.HexToHash(result[0]),
			CHTRoot:     common.HexToHash(result[1]),
			BloomRoot:   common.HexToHash(result[2]),
		}
		log.Info("Retrieve local checkpoint", "index", checkpoint.SectionIdx, "sectionhead", checkpoint.SectionHead,
			"chtroot", checkpoint.CHTRoot, "bloomroot", checkpoint.BloomRoot, "hash", checkpoint.Hash())
	} else {
		var result [4]string
		err := rpcClient.Call(&result, "les_latestCheckpoint")
		if err != nil {
			utils.Fatalf("Failed to get local checkpoint %v", err)
		}
		index, err := strconv.ParseUint(result[0], 0, 64)
		if err != nil {
			utils.Fatalf("Failed to parse checkpoint index %v", err)
		}
		checkpoint = &light.TrustedCheckpoint{
			SectionIdx:  index,
			SectionHead: common.HexToHash(result[1]),
			CHTRoot:     common.HexToHash(result[2]),
			BloomRoot:   common.HexToHash(result[3]),
		}
		log.Info("Retrieve local checkpoint", "index", checkpoint.SectionIdx, "sectionhead", checkpoint.SectionHead,
			"chtroot", checkpoint.CHTRoot, "bloomroot", checkpoint.BloomRoot, "hash", checkpoint.Hash())
	}
	// Ensure the invoker is a trusted signer.
	contract := setupContract(ctx, ethclient.NewClient(rpcClient))
	signers, err := contract.Contract().GetAllAdmin(nil)
	if err != nil {
		return err
	}
	key := getPrivateKey(ctx)
	isTrusted := false
	for _, signer := range signers {
		if signer == key.Address {
			isTrusted = true
		}
	}
	if !isTrusted {
		utils.Fatalf("Address %s is not a trusted signer", key.Address)
	}
	// Register the checkpoint
	tx, err := contract.SetCheckpoint(key.PrivateKey, big.NewInt(int64(checkpoint.SectionIdx)), checkpoint.Hash().Bytes())
	if err != nil {
		return err
	}
	log.Info("Set checkpoint successfully", "tx", tx.Hash())
	return nil
}
