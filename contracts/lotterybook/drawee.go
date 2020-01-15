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

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// WrappedCheque wraps the cheque and an additional reveal number
type WrappedCheque struct {
	Cheque       *Cheque
	RevealNumber uint64
}

// ChequeDrawee represents the payment drawee in a off-chain payment channel.
// In chequeDrawee the most basic functions related to payment are defined
// here like: AddCheque.
//
// ChequeDrawee is self-contained and stateful. It will track all received
// cheques and claim the lottery if it's the lucky winner. Only AddCheque
// is exposed and needed by external caller.
//
// In addition, the structure depends on the blockchain state of the local node.
// In order to avoid inconsistency, you need to ensure that the local node has
// completed synchronization before using drawee.
type ChequeDrawee struct {
	selfAddr   common.Address
	drawerAddr common.Address
	cdb        *chequeDB
	book       *LotteryBook
	opts       *bind.TransactOpts
	cBackend   bind.ContractBackend
	dBackend   bind.DeployBackend
	chain      Blockchain
	queryCh    chan chan []*WrappedCheque
	chequeCh   chan *WrappedCheque
	closeCh    chan struct{}
	wg         sync.WaitGroup

	// Testing hooks
	onClaimedHook func(common.Hash) // onClaimedHook is called if a lottery is successfully claimed
}

// NewChequeDrawee creates a payment drawee instance which handles all payments.
func NewChequeDrawee(opts *bind.TransactOpts, selfAddr, drawerAddr, contractAddr common.Address, chain Blockchain, cBackend bind.ContractBackend, dBackend bind.DeployBackend, db ethdb.Database) (*ChequeDrawee, error) {
	if contractAddr == (common.Address{}) {
		return nil, errors.New("empty contract address")
	}
	book, err := newLotteryBook(contractAddr, cBackend)
	if err != nil {
		return nil, err
	}
	drawee := &ChequeDrawee{
		cdb:        newChequeDB(db),
		cBackend:   cBackend,
		dBackend:   dBackend,
		chain:      chain,
		selfAddr:   selfAddr,
		drawerAddr: drawerAddr,
		book:       book,
		opts:       opts,
		queryCh:    make(chan chan []*WrappedCheque),
		chequeCh:   make(chan *WrappedCheque),
		closeCh:    make(chan struct{}),
	}
	drawee.wg.Add(1)
	go drawee.waitingReveal()
	return drawee, nil
}

// ContractAddr returns the address of lottery contract which is used now.
func (drawee *ChequeDrawee) ContractAddr() common.Address {
	return drawee.book.address
}

// CodeHash returns the code hash of connected lottery contract.
func (drawee *ChequeDrawee) CodeHash(context context.Context) (common.Hash, error) {
	code, err := drawee.dBackend.CodeAt(context, drawee.book.address, nil)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(code), nil
}

func (drawee *ChequeDrawee) Close() {
	close(drawee.closeCh)
	drawee.wg.Wait()
}

// AddCheque receives a cheque from drawer, checks the validity and stores
// it locally.
//
// In the mean time, this function will return the net amount of this cheque.
func (drawee *ChequeDrawee) AddCheque(c *Cheque) (uint64, error) {
	if err := validateCheque(c, drawee.drawerAddr, drawee.selfAddr, drawee.ContractAddr()); err != nil {
		return 0, err
	}
	// Short circuit if specified lottery doesn't exist.
	lottery, err := drawee.book.contract.Lotteries(nil, c.LotteryId)
	if err != nil {
		return 0, err
	}
	// TODO what if the sender is deliberately attacking us
	// via sending cheques without deposit? Read status from
	// contract is not trivial.
	if lottery.Amount == 0 {
		invalidChequeMeter.Mark(1)
		return 0, errors.New("no lottery created")
	}
	// Short circuit if the lottery is already revealed.
	current := drawee.chain.CurrentHeader().Number.Uint64()
	if current >= lottery.RevealNumber {
		invalidChequeMeter.Mark(1)
		return 0, errors.New("lottery expired")
	}
	var diff uint64
	stored := drawee.cdb.readCheque(drawee.selfAddr, c.Signer(), c.LotteryId, false)
	if stored != nil {
		if stored.SignedRange >= c.SignedRange {
			// There are many cases can lead to this situation:
			// * Drawer sends a stale cheque deliberately
			// * Drawer's chequedb is broken, it loses all payment history
			// No matter which reason, reject the cheque here.
			staleChequeMeter.Mark(1)
			return 0, errors.New("stale cheque")
		}
		// Figure out the net newly signed reveal range
		diff = c.SignedRange - stored.SignedRange
	} else {
		// Reject cheque if the paid amount is zero.
		if c.SignedRange == c.LowerLimit || c.SignedRange == 0 {
			invalidChequeMeter.Mark(1)
			return 0, errors.New("invalid payment amount")
		}
		diff = c.SignedRange - c.LowerLimit + 1
	}
	// It may lose precision but it's ok.
	assigned := lottery.Amount >> (len(c.Witness) - 1)

	// Note the following calculation may lose precision, but it's okish.
	//
	// In theory diff/interval WON't be very small. So it's the best choice
	// to calculate percentage first. Otherwise the calculation may overflow.
	diffAmount := uint64((float64(diff) / float64(c.UpperLimit-c.LowerLimit+1)) * float64(assigned))
	if diffAmount == 0 {
		invalidChequeMeter.Mark(1)
		return 0, errors.New("invalid payment amount")
	}
	drawee.cdb.writeCheque(drawee.selfAddr, c.Signer(), c, false)
	select {
	case drawee.chequeCh <- &WrappedCheque{c, lottery.RevealNumber}:
	case <-drawee.closeCh:
	}
	return diffAmount, nil
}

// ActiveCheques returns all active cheques received which is
// waiting for reveal.
func (drawee *ChequeDrawee) ActiveCheques() []*WrappedCheque {
	reqCh := make(chan []*WrappedCheque, 1)
	select {
	case drawee.queryCh <- reqCh:
		return <-reqCh
	case <-drawee.closeCh:
		return nil
	}
}

// claim sends a on-chain transaction to claim the specified lottery.
func (drawee *ChequeDrawee) claim(context context.Context, cheque *Cheque) error {
	var proofslice [][32]byte
	for i := 1; i < len(cheque.Witness); i++ {
		var p [32]byte
		copy(p[:], cheque.Witness[i].Bytes())
		proofslice = append(proofslice, p)
	}
	start := time.Now()
	tx, err := drawee.book.contract.Claim(drawee.opts, cheque.LotteryId, cheque.RevealRange, cheque.Sig[64], common.BytesToHash(cheque.Sig[:common.HashLength]), common.BytesToHash(cheque.Sig[common.HashLength:2*common.HashLength]), proofslice)
	if err != nil {
		return err
	}
	receipt, err := bind.WaitMined(context, drawee.dBackend, tx)
	if err != nil {
		return err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return ErrTransactionFailed
	}
	if drawee.onClaimedHook != nil {
		drawee.onClaimedHook(cheque.LotteryId)
	}
	claimDurationTimer.UpdateSince(start)
	winLotteryGauge.Inc(1)
	log.Debug("Claimed lottery", "id", cheque.LotteryId)
	return nil
}

// waitingReveal starts a background routine to wait all received cheques to be revealed.
func (drawee *ChequeDrawee) waitingReveal() {
	defer drawee.wg.Done()

	// Establish subscriptions
	newHeadCh := make(chan core.ChainHeadEvent, 1024)
	sub := drawee.chain.SubscribeChainHeadEvent(newHeadCh)
	if sub == nil {
		return
	}
	defer sub.Unsubscribe()

	var (
		current uint64
		active  = make(map[uint64][]*Cheque)
	)
	// checkAndClaim checks whether the cheque is the winner or not.
	// If so, claim the corresponding lottery via sending on-chain
	// transaction.
	checkAndClaim := func(cheque *Cheque, hash common.Hash) (err error) {
		defer func() {
			// No matter we aren't the lucky winner or we already claim
			// the lottery, delete the record anyway. Keep it if any error
			// occurs.
			if err == nil {
				drawee.cdb.deleteCheque(drawee.selfAddr, drawee.drawerAddr, cheque.LotteryId, false)
			}
		}()
		if !cheque.reveal(hash) {
			return nil
		}
		ctx, cancelFn := context.WithTimeout(context.Background(), txTimeout)
		defer cancelFn()
		return drawee.claim(ctx, cheque)
	}
	// Initialize local blockchain height
	current = drawee.chain.CurrentHeader().Number.Uint64()

	// Read all stored cheques issued by specified sender.
	cheques, _ := drawee.cdb.listCheques(
		drawee.selfAddr,
		func(addr common.Address, id common.Hash) bool { return addr == drawee.drawerAddr },
	)
	for _, cheque := range cheques {
		ret, err := drawee.book.contract.Lotteries(nil, cheque.LotteryId)
		if err != nil {
			log.Error("Failed to retrieve corresponding lottery", "error", err)
			continue
		}
		// If the amount of corresponding lottery is 0, it means the lottery
		// is claimed or reset by someone, just delete it.
		if ret.Amount == 0 {
			log.Debug("Lottery is claimed")
			drawee.cdb.deleteCheque(drawee.selfAddr, drawee.drawerAddr, cheque.LotteryId, false)
			continue
		}
		// The valid claim block range is [revealNumber+1, revealNumber+256].
		// However the head block can be reorged with very high chance. So
		// a small processing confirms is applied to ensure the reveal hash
		// is stable enough.
		//
		// For receiver, the reasonable claim range (revealNumber+6, revealNumber+256].
		if current < ret.RevealNumber+lotteryProcessConfirms {
			active[ret.RevealNumber] = append(active[ret.RevealNumber], cheque)
		} else if current < ret.RevealNumber+lotteryClaimPeriod {
			// Lottery can still be claimed, try it!
			revealHash := drawee.chain.GetHeaderByNumber(ret.RevealNumber)

			// Create an independent routine to claim the lottery.
			// This function may takes very long time, don't block
			// the entire thread here. It's ok to spin up routines
			// blindly here, there won't have too many cheques to claim.
			go checkAndClaim(cheque, revealHash.Hash())
		} else {
			// Lottery is already out of claim window, delete it.
			log.Debug("Cheque expired", "lotteryid", cheque.LotteryId)
			drawee.cdb.deleteCheque(drawee.selfAddr, drawee.drawerAddr, cheque.LotteryId, false)
		}
	}
	for {
		select {
		case ev := <-newHeadCh:
			current = ev.Block.NumberU64()
			for revealAt, cheques := range active {
				// Short circuit if they are still active lotteries.
				if current < revealAt+lotteryProcessConfirms {
					continue
				}
				// Wipe all cheques if they are already stale.
				if current >= revealAt+lotteryClaimPeriod {
					for _, cheque := range cheques {
						drawee.cdb.deleteCheque(drawee.selfAddr, drawee.drawerAddr, cheque.LotteryId, false)
					}
					delete(active, revealAt)
					continue
				}
				revealHash := drawee.chain.GetHeaderByNumber(revealAt).Hash()
				for _, cheque := range cheques {
					// Create an independent routine to claim the lottery.
					// This function may takes very long time, don't block
					// the entire thread here. It's ok to spin up routines
					// blindly here, there won't have too many cheques to claim.
					go checkAndClaim(cheque, revealHash)
				}
				delete(active, revealAt)
			}

		case req := <-drawee.chequeCh:
			var replaced bool
			cheques := active[req.RevealNumber]
			for index, cheque := range cheques {
				if cheque.LotteryId == req.Cheque.LotteryId {
					cheques[index] = req.Cheque // Replace the original one
					replaced = true
					break
				}
			}
			if !replaced {
				active[req.RevealNumber] = append(active[req.RevealNumber], req.Cheque)
			}

		case retCh := <-drawee.queryCh:
			var ret []*WrappedCheque
			for revealAt, cheques := range active {
				for _, cheque := range cheques {
					ret = append(ret, &WrappedCheque{
						RevealNumber: revealAt,
						Cheque:       cheque,
					})
				}
			}
			retCh <- ret

		case <-drawee.closeCh:
			log.Debug("Stopping cheque drawee instance...")
			return
		}
	}
}
