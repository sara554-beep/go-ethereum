// Copyright 2016 The go-ethereum Authors
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

// Package les implements the Light Ethereum Subprotocol.
package les

import (
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/registrar"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// RegistrarConfig is the configuration parameters of registrar.
type RegistrarConfig struct {
	// ContractConfig is a set of checkpoint contract setting.
	ContractConfig *params.CheckpointContractConfig

	// CheckpointSize is the block frequency for creating a checkpoint.
	//
	// Notably, this is only available for testing
	CheckpointSize uint64

	// ProcessConfirms is the number of confirmations before a checkpoint is generated.
	//
	// Notably, this is only available for testing
	ProcessConfirms uint64
}

// newEmptyConfig returns a default registrar config.
func newCheckpointContractConfig() *RegistrarConfig {
	return &RegistrarConfig{
		CheckpointSize:  params.CheckpointFrequency,
		ProcessConfirms: params.CheckpointProcessConfirmations,
	}
}

const signatureLen = 65

// checkpointRegistrar is responsible for offering the latest stable checkpoint
// which generated by local and announced by contract admins in the server
// side and verifying advertised checkpoint during the checkpoint syncing
// in the client side.
type checkpointRegistrar struct {
	config        *RegistrarConfig     // configuration for registrar.
	indexerConfig *light.IndexerConfig // configuration for indexer parameters.

	chaindb  ethdb.Database
	contract *registrar.Registrar

	// Indexers
	bloomTrieIndexer *core.ChainIndexer
	chtIndexer       *core.ChainIndexer

	// Whether the contract backend is set.
	running int32

	// Test Hooks
	SyncDoneHook func() // Function used to notify that light syncing has completed.
}

// newCheckpointRegistrar returns a checkpoint registrar handler.
func newCheckpointRegistrar(chaindb ethdb.Database, config *RegistrarConfig, indexerConfig *light.IndexerConfig, chtIndexer *core.ChainIndexer, bloomTrieIndexer *core.ChainIndexer) *checkpointRegistrar {
	if config.ContractConfig == nil {
		log.Info("Checkpoint registrar is not enabled")
		return nil
	}
	if config.ContractConfig.ContractAddr == (common.Address{}) || uint64(len(config.ContractConfig.Signers)) < config.ContractConfig.Threshold {
		log.Warn("Invalid checkpoint contract config")
		return nil
	}
	log.Info("Setup checkpoint registrar", "contract", config.ContractConfig.ContractAddr, "numsigner", len(config.ContractConfig.Signers),
		"threshold", config.ContractConfig.Threshold)
	reg := &checkpointRegistrar{
		config:           config,
		indexerConfig:    indexerConfig,
		bloomTrieIndexer: bloomTrieIndexer,
		chtIndexer:       chtIndexer,
		chaindb:          chaindb,
	}
	return reg
}

// start binds the registrar contract and start listening to the
// newCheckpointEvent for the server side.
func (reg *checkpointRegistrar) start(backend bind.ContractBackend) {
	contract, err := registrar.NewRegistrar(reg.config.ContractConfig.ContractAddr, backend)
	if err != nil {
		log.Info("Bind registrar contract failed", "err", err)
		return
	}
	if !atomic.CompareAndSwapInt32(&reg.running, 0, 1) {
		log.Info("Already bound and listening to registrar contract")
		return
	}
	reg.contract = contract
}

// isRunning returns an indicator whether the registrar is running.
func (reg *checkpointRegistrar) isRunning() bool {
	return atomic.LoadInt32(&reg.running) == 1
}

// stableCheckpoint returns the stable checkpoint which generated by local indexers
// and announced by trusted signers.
func (reg *checkpointRegistrar) stableCheckpoint() (*light.TrustedCheckpoint, uint64) {
	latest, hash, _, err := reg.contract.Contract().GetLatestCheckpoint(nil)
	if err != nil || latest.Uint64() == 0 && hash == [32]byte{} {
		return nil, 0
	}

	// getLocal returns a local persisted checkpoint with specified index.
	getLocal := func(index uint64) light.TrustedCheckpoint {
		latest := (index+1)*(reg.indexerConfig.PairChtSize/reg.indexerConfig.ChtSize) - 1
		sectionHead := reg.chtIndexer.SectionHead(latest)
		return light.TrustedCheckpoint{
			SectionIndex: index,
			SectionHead:  sectionHead,
			CHTRoot:      light.GetChtRoot(reg.chaindb, latest, sectionHead),
			BloomRoot:    light.GetBloomTrieRoot(reg.chaindb, index, sectionHead),
		}
	}

	index := latest.Uint64()
	for {
		local := getLocal(index)
		hash, height, err := reg.contract.Contract().GetCheckpoint(nil, big.NewInt(int64(index)))
		if err == nil && local.HashEqual(common.Hash(hash)) {
			return &local, height.Uint64()
		}
		if index == 0 {
			break
		}
		index -= 1
	}
	return nil, 0
}

// VerifySigner recovers the signer address according to the signature and
// checks whether there are enough approves to finalize the checkpoint.
func (reg *checkpointRegistrar) verifySigner(checkpointHash [32]byte, signature []byte) (bool, []common.Address) {
	if len(signature)%signatureLen != 0 {
		log.Warn("Invalid aggregation signature", "length", len(signature))
		return false, nil
	}
	var (
		signers   []common.Address
		number    = len(signature) / signatureLen
		signerMap = make(map[common.Address]struct{})
	)
	for i := 0; i < number; i += 1 {
		pubkey, err := crypto.Ecrecover(checkpointHash[:], signature[i*signatureLen:(i+1)*signatureLen])
		if err != nil {
			return false, nil
		}
		var signer common.Address
		copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])
		if _, exist := signerMap[signer]; exist {
			continue
		}
		for _, s := range reg.config.ContractConfig.Signers {
			if s == signer {
				signers = append(signers, signer)
				signerMap[signer] = struct{}{}
			}
		}
	}
	threshold := reg.config.ContractConfig.Threshold
	if uint64(len(signers)) < threshold {
		log.Warn("Checkpoint approval is not enough", "given", len(signers), "want", threshold)
		return false, nil
	}
	return true, signers
}
