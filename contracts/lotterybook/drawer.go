// Copyright 2019 The go-ethereum Authors
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
	"encoding/binary"
	"errors"
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/lotterybook/contract"
	"github.com/ethereum/go-ethereum/contracts/lotterybook/merkletree"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
)

// ChequeDrawer represents the payment drawer in a off-chain payment channel.
// Usually in LES protocol the drawer refers to a light client.
// In chequeDrawer the most basic functions related to payment are defined
// here like: (1) issue cheque (2) reset lottery or (3) destory lottery.
// Besides it can offer basic query and lottery status subscription functionality.
// More complex logic should be defined in the wrapper layer based on this.
type ChequeDrawer struct {
	address     common.Address
	cdb         *chequeDB
	book        *LotteryBook
	chain       Blockchain
	cBackend    bind.ContractBackend
	dBackend    bind.DeployBackend
	rand        *rand.Rand
	scope       event.SubscriptionScope
	lotteryFeed event.Feed
	queryCh     chan chan []*Lottery
	lotteryCh   chan *Lottery
	closeCh     chan struct{}
	wg          sync.WaitGroup

	txSigner     *bind.TransactOpts                // Used for production environment, transaction signer
	keySigner    func(data []byte) ([]byte, error) // Used for testing, cheque signer
	chequeSigner func(data []byte) ([]byte, error) // Used for production environment, cheque signer
}

// NewChequeDrawer creates a payment drawer and deploys the contract if necessary.
func NewChequeDrawer(selfAddr common.Address, txSigner *bind.TransactOpts, chequeSigner func(data []byte) ([]byte, error), chain Blockchain, cBackend bind.ContractBackend, dBackend bind.DeployBackend, db ethdb.Database) (*ChequeDrawer, error) {
	// Deploy the contract if we don't have one yet.
	var chanAddr common.Address
	cdb := newChequeDB(db)
	if stored := cdb.readContractAddr(selfAddr); stored != nil {
		chanAddr = *stored
	} else {
		addr, err := deployLotteryBook(txSigner, cBackend, dBackend)
		if err != nil {
			return nil, err
		}
		cdb.writeContractAddr(selfAddr, addr)
		chanAddr = addr
	}
	book, err := newLotteryBook(chanAddr, cBackend)
	if err != nil {
		return nil, err
	}
	drawer := &ChequeDrawer{
		address:      selfAddr,
		cdb:          cdb,
		book:         book,
		txSigner:     txSigner,
		chequeSigner: chequeSigner,
		chain:        chain,
		cBackend:     cBackend,
		dBackend:     dBackend,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		lotteryCh:    make(chan *Lottery),
		queryCh:      make(chan chan []*Lottery),
		closeCh:      make(chan struct{}),
	}
	drawer.wg.Add(1)
	go drawer.manageLotteries()
	return drawer, nil
}

// ContractAddr returns the address of deployed accountbook contract.
func (drawer *ChequeDrawer) ContractAddr() common.Address {
	return drawer.book.address
}

// Close exits all background threads and closes all event subscribers.
func (drawer *ChequeDrawer) Close() {
	drawer.scope.Close()
	close(drawer.closeCh)
	drawer.wg.Wait()
}

// newProbabilityTree constructs a probability tree(merkle tree) based on the
// given list of receivers and corresponding amounts. The payment amount can
// used as the initial weight for each payer. Since the underlying merkle tree
// is binary tree, so finally all weights will be adjusted to 1/2^N form.
func (drawer *ChequeDrawer) newProbabilityTree(payers []common.Address, amounts []uint64) (*merkletree.MerkleTree, []*merkletree.Entry, uint64, error) {
	if len(payers) != len(amounts) {
		return nil, nil, 0, errors.New("inconsistent payment receivers and amounts")
	}
	var totalAmount uint64
	for _, amount := range amounts {
		if amount == 0 {
			return nil, nil, 0, errors.New("invalid payment amount")
		}
		totalAmount += amount
	}
	entries := make([]*merkletree.Entry, len(payers))
	for index, amount := range amounts {
		entries[index] = &merkletree.Entry{
			Value:  payers[index].Bytes(),
			Weight: amount,
		}
	}
	tree, err := merkletree.NewMerkleTree(entries)
	if err != nil {
		return nil, nil, 0, err
	}
	return tree, entries, totalAmount, nil
}

// submitLottery creates the lottery based on the specified batch of payers and
// corresponding payment amount. Return the newly created cheque list.
func (drawer *ChequeDrawer) submitLottery(context context.Context, payers []common.Address, amounts []uint64, revealNumber uint64, onchainFn func(amount uint64, id [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error)) (*Lottery, error) {
	// Construct merkle probability tree with given payer list and corresponding
	// payer weight. Return error if the given weight is invalid.
	tree, entries, amount, err := drawer.newProbabilityTree(payers, amounts)
	if err != nil {
		return nil, err
	}
	// New random lottery salt to ensure the id is unique.
	salt := drawer.rand.Uint64()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, salt)

	// Generate and backup the lottery before sending the submission transaction.
	//
	// If any crash happen then we can still claim back the deposit inside.
	// If we record the lottery but we fail to send out the creation transaction,
	// we can wipe the record during recovery.
	lotteryId := crypto.Keccak256Hash(append(tree.Hash().Bytes(), buf...))
	lottery := &Lottery{Id: lotteryId, RevealNumber: revealNumber, Amount: amount}
	drawer.cdb.writeLottery(drawer.address, lotteryId, lottery)

	// Submit the new created lottery to contract by specified on-chain function.
	tx, err := onchainFn(amount, lotteryId, revealNumber, salt)
	if err != nil {
		return nil, err
	}
	receipt, err := bind.WaitMined(context, drawer.dBackend, tx)
	if err != nil {
		return nil, err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, ErrTransactionFailed
	}
	// Generate empty unused cheques based on the newly created lottery.
	for _, entry := range entries {
		witness, err := tree.Prove(entry)
		if err != nil {
			return nil, err
		}
		cheque, err := newCheque(witness, drawer.ContractAddr(), salt)
		if err != nil {
			return nil, err
		}
		if drawer.keySigner != nil {
			// If it's testing, use provided key signer.
			if err := cheque.signWithKey(drawer.keySigner); err != nil {
				return nil, err
			}
		} else {
			// Otherwise, use provided clef as the production-environment signer.
			if err := cheque.sign(drawer.chequeSigner); err != nil {
				return nil, err
			}
		}
		drawer.cdb.writeCheque(common.BytesToAddress(entry.Value), drawer.address, cheque, true)
	}
	return lottery, nil
}

// CreateLottery creates the lottery based on the specified batch of payers and
// corresponding payment amount, returns a batch of empty cheques which can used
// for payment later.
func (drawer *ChequeDrawer) CreateLottery(context context.Context, payers []common.Address, amounts []uint64, revealNumber uint64) (common.Hash, error) {
	onchainFn := func(amount uint64, id [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error) {
		// Create an independent auth opt to submit the lottery
		opt := &bind.TransactOpts{
			From:   drawer.address,
			Signer: drawer.txSigner.Signer,
			Value:  big.NewInt(int64(amount)),
		}
		return drawer.book.contract.NewLottery(opt, id, blockNumber, salt)
	}
	lottery, err := drawer.submitLottery(context, payers, amounts, revealNumber, onchainFn)
	if err != nil {
		return common.Hash{}, err
	}
	drawer.fireLottery(lottery)
	return lottery.Id, err
}

// ResetLottery resets a existed stale lottery with new batch of payment receivers
// and corresponding amount. Add more funds into lottery ff the deposit of stale
// lottery is not enough to cover the new amount. Otherwise the deposit given by
// lottery for each receiver may be higher than the specified value.
func (drawer *ChequeDrawer) ResetLottery(context context.Context, id common.Hash, payers []common.Address, amounts []uint64, revealNumber uint64) (common.Hash, error) {
	// Short circuit if the specified stale lottery doesn't exist.
	lottery := drawer.cdb.readLottery(drawer.address, id)
	if lottery == nil {
		return common.Hash{}, errors.New("the lottery specified is not-existent")
	}
	onchainFn := func(amount uint64, newid [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error) {
		// Create an independent auth opt to submit the lottery
		var netAmount uint64
		if lottery.Amount < amount {
			netAmount = amount - lottery.Amount
		}
		opt := &bind.TransactOpts{
			From:   drawer.address,
			Signer: drawer.txSigner.Signer,
			Value:  big.NewInt(int64(netAmount)),
		}
		// The on-chain transaction may fail dues to lots of reasons like
		// the lottery doesn't exist or lottery hasn't been expired yet.
		return drawer.book.contract.ResetLottery(opt, id, newid, blockNumber, salt)
	}
	newLottery, err := drawer.submitLottery(context, payers, amounts, revealNumber, onchainFn)
	if err != nil {
		return common.Hash{}, err
	}
	// Wipe useless old lottery and associated cheques
	drawer.cdb.deleteLottery(drawer.address, id)
	_, addresses := drawer.cdb.listCheques(
		drawer.address,
		func(addr common.Address, lid common.Hash) bool { return lid == id },
	)
	for _, addr := range addresses {
		drawer.cdb.deleteCheque(drawer.address, addr, id, true)
	}
	// Emit a event about new created lottery
	drawer.fireLottery(newLottery)
	return newLottery.Id, nil
}

// DestoryLottery destorys a stale lottery, claims all deposid inside back to
// our own pocket.
func (drawer *ChequeDrawer) DestoryLottery(context context.Context, id common.Hash) error {
	// Short circuit if the specified stale lottery doesn't exist.
	lottery := drawer.cdb.readLottery(drawer.address, id)
	if lottery == nil {
		return errors.New("the lottery specified is not-existent")
	}
	// The on-chain transaction may fail dues to lots of reasons like
	// the lottery doesn't exist or lottery hasn't been expired yet.
	tx, err := drawer.book.contract.DestroyLottery(drawer.txSigner, id)
	if err != nil {
		return err
	}
	receipt, err := bind.WaitMined(context, drawer.dBackend, tx)
	if err != nil {
		return err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return ErrTransactionFailed
	}
	// Wipe useless lottery and associated cheques
	drawer.cdb.deleteLottery(drawer.address, id)
	_, addresses := drawer.cdb.listCheques(
		drawer.address,
		func(addr common.Address, lid common.Hash) bool { return lid == id },
	)
	for _, addr := range addresses {
		drawer.cdb.deleteCheque(drawer.address, addr, id, true)
	}
	return nil
}

// IssueCheque creates a cheque for issuing specified amount for payee.
//
// The drawer to issue a new cheque must have a corresponding lottery as
// a deposit. This lottery contains several potential redeemers of this
// lottery. The probability that each redeemer can redeem is different, so
// the expected amount of money received by each redeemer is the redemption
// probability multiplied by the lottery amount.
//
// A lottery ticket can be divided into n cheques for payment. Therefore, the
// cheque is paid in a cumulative amount. There is a probability of redemption
// in every cheque issued. The probability of redemption of a later-issued cheque
// needs to be strictly greater than that of the first-issued cheque.
//
// Because of the possible data loss, we can issue some double-spend cheques,
// they will be rejected by drawee. Finally we will amend local broken db
// by the evidence provided by drawee.
func (drawer *ChequeDrawer) IssueCheque(payer common.Address, lotteryId common.Hash, amount uint64) (*Cheque, error) {
	cheque := drawer.cdb.readCheque(payer, drawer.address, lotteryId, true)
	if cheque == nil {
		return nil, errors.New("no cheque found")
	}
	lottery := drawer.cdb.readLottery(drawer.address, cheque.LotteryId)
	if lottery == nil {
		return nil, errors.New("broken db, has cheque but no lottery found")
	}
	// Short circuit if lottery is already expired.
	current := drawer.chain.CurrentHeader().Number.Uint64()
	if lottery.RevealNumber <= current {
		return nil, errors.New("expired lottery")
	}
	// Calculate the total assigned deposit in lottery for the specified payer.
	assigned := lottery.Amount >> (len(cheque.Witness) - 1)

	// Calculate new signed probability range according to new cumulative paid amount
	//
	// Note in the following calculation, it may lose precision.
	// In theory amount/assigned won't be very small. So it's safer to calculate
	// percentage first.
	diff := uint64(float64(amount) / float64(assigned) * float64(cheque.UpperLimit-cheque.LowerLimit+1))
	if diff == 0 {
		return nil, errors.New("invalid payment amount")
	}
	if cheque.SignedRange == 0 {
		cheque.SignedRange = cheque.LowerLimit + diff - 1
	} else {
		cheque.SignedRange = cheque.SignedRange + diff
	}
	// Ensure we still have enough deposit to cover payment.
	if cheque.SignedRange > cheque.UpperLimit {
		return nil, ErrNotEnoughDeposit
	}
	// Make the signature for cheque.
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(cheque.SignedRange))
	cheque.RevealRange = buf
	if drawer.keySigner != nil {
		// If it's testing, use provided key signer.
		if err := cheque.signWithKey(drawer.keySigner); err != nil {
			return nil, err
		}
	} else {
		// Otherwise, use provided clef as the production-environment signer.
		if err := cheque.sign(drawer.chequeSigner); err != nil {
			return nil, err
		}
	}
	drawer.cdb.writeCheque(payer, drawer.address, cheque, true)
	return cheque, nil
}

// Amend amends the local cheque db with externally provided cheque which
// is issued and signed by ourselves.
//
// todo the problem is this function will NEVER be triggered if chequedb of
// drawer is broken.
func (drawer *ChequeDrawer) Amend(receiver common.Address, cheque *Cheque) error {
	// Ensure the external provided cheque is signed by ourselves.
	if err := validateCheque(cheque, drawer.address, receiver, drawer.ContractAddr()); err != nil {
		return err
	}
	// Amend local broken database if the evidence makes sense.
	stored := drawer.cdb.readCheque(receiver, drawer.address, cheque.LotteryId, true)
	if stored == nil || stored.SignedRange < cheque.SignedRange {
		// The evidence is verified, now ensure whether the associated
		// lottery is active or not.
		//
		// Refill associated lottery if it's missing
		lottery := drawer.cdb.readLottery(drawer.address, cheque.LotteryId)
		if lottery == nil {
			ret, err := drawer.book.contract.Lotteries(nil, cheque.LotteryId)
			if err != nil {
				return err
			}
			if ret.Amount == 0 {
				return errors.New("no lottery created")
			}
			lottery = &Lottery{
				Id:           cheque.LotteryId,
				Amount:       ret.Amount,
				RevealNumber: ret.RevealNumber,
			}
			drawer.cdb.writeLottery(drawer.address, cheque.LotteryId, lottery)
			drawer.fireLottery(lottery)
		}
		current := drawer.chain.CurrentHeader().Number.Uint64()
		if current < lottery.RevealNumber {
			drawer.cdb.writeCheque(receiver, drawer.address, cheque, true)
		}
	}
	return nil
}

// Allowance returns the allowance remaining in the specified lottery that can
// be used to make payments to all included receivers.
func (drawer *ChequeDrawer) Allowance(id common.Hash) map[common.Address]uint64 {
	// Filter all cheques associated with given lottery.
	cheques, addresses := drawer.cdb.listCheques(
		drawer.address,
		func(addr common.Address, lid common.Hash) bool { return lid == id },
	)
	// Short circuit if no cheque found.
	if cheques == nil {
		return nil
	}
	// Short circuit of no corresponding lottery found, it should never happen.
	lottery := drawer.cdb.readLottery(drawer.address, id)
	if lottery == nil {
		return nil
	}
	allowance := make(map[common.Address]uint64)
	for index, cheque := range cheques {
		assigned := lottery.Amount >> (len(cheque.Witness) - 1)
		if cheque.SignedRange == 0 {
			allowance[addresses[index]] = assigned // We haven't used this check yet
		} else {
			remaining := cheque.UpperLimit - cheque.SignedRange
			allowance[addresses[index]] = uint64(float64(remaining) / float64(cheque.UpperLimit-cheque.LowerLimit+1) * float64(assigned))
		}
	}
	return allowance
}

// EstimatedExpiry returns the estimated remaining time(block number) while
// cheques can still be written using this deposit.
func (drawer *ChequeDrawer) EstimatedExpiry(lotteryId common.Hash) uint64 {
	lottery := drawer.cdb.readLottery(drawer.address, lotteryId)
	if lottery == nil {
		return 0
	}
	current := drawer.chain.CurrentHeader()
	if current == nil {
		return 0
	}
	height := current.Number.Uint64()
	if lottery.RevealNumber > height {
		return lottery.RevealNumber - height
	}
	return 0 // The lottery is already expired.
}

func (drawer *ChequeDrawer) fireLottery(lottery *Lottery) {
	select {
	case drawer.lotteryCh <- lottery:
	case <-drawer.closeCh:
	}
}

// manageLotteries is responsible for managing the entire life cycle
// of the lottery created locally.
//
// The status of lottery can be classified as four types:
// * pending: lottery is just created, have to wait a few block
//    confirms upon it.
// * active: lottery can be used to make payment, the lottery
//    reveal time has not yet arrived.
// * revealed: lottery has been revealed, can't be used to make
//    payment anymore.
// * expired: no one picks up lottery ticket within required claim
//    time, owner can reown it via resetting or destruct.
// External modules can monitor lottery status via subscription or
// direct query.
func (drawer *ChequeDrawer) manageLotteries() {
	defer drawer.wg.Done()

	// Establish subscriptions
	newHeadCh := make(chan core.ChainHeadEvent, 1024)
	sub := drawer.chain.SubscribeChainHeadEvent(newHeadCh)
	if sub == nil {
		return
	}
	defer sub.Unsubscribe()

	lotteryClaimedCh := make(chan *contract.LotteryBookLotteryClaimed)
	eventSub, err := drawer.book.contract.WatchLotteryClaimed(nil, lotteryClaimedCh, nil)
	if err != nil {
		return
	}
	defer eventSub.Unsubscribe()

	var (
		current  uint64                           // Current height of local blockchain
		events   []LotteryEvent                   // A batch of aumulative lottery events
		pending  = make(map[uint64][]*Lottery)    // A set of pending lotteries, the key is block height of creation
		active   = make(map[uint64][]*Lottery)    // A set of active lotteries, the key is block height of reveal
		revealed = make(map[common.Hash]*Lottery) // A set of revealed lotteries
		expired  = make(map[common.Hash]*Lottery) // A set of expired lotteries
	)
	// Initialize local blockchain height
	current = drawer.chain.CurrentHeader().Number.Uint64()

	// Read all stored lotteries in disk during setup
	lotteries := drawer.cdb.listLotteries(drawer.address)
	for _, lottery := range lotteries {
		if current < lottery.RevealNumber {
			active[current] = append(active[current], lottery)
			events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryActive})
		} else if current < lottery.RevealNumber+lotteryClaimPeriod {
			revealed[lottery.Id] = lottery
			events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryRevealed})
		} else {
			expired[lottery.Id] = lottery
			events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryExpired})
		}
	}
	for {
		if len(events) > 0 {
			drawer.lotteryFeed.Send(events)
			events = events[:0]
		}
		select {
		case ev := <-newHeadCh:
			current = ev.Block.NumberU64()
			// If newly created lotteries have enough confirms, move them to active set.
			for createdAt, lotteries := range pending {
				if createdAt+lotteryProcessConfirms < current {
					continue
				}
				for _, lottery := range lotteries {
					active[lottery.RevealNumber] = append(active[lottery.RevealNumber], lottery)
					events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryActive})
					log.Debug("Lottery activated", "id", lottery.Id)
				}
				delete(pending, createdAt)
			}
			// Clean stale lottery which is already revealed
			for revealAt, lotteries := range active {
				if current < revealAt {
					continue
				}
				// Move all revealed lotteries into `revealed` set.
				for _, lottery := range lotteries {
					revealed[lottery.Id] = lottery
					events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryRevealed})
					log.Debug("Lottery revealed", "id", lottery.Id)
				}
				delete(active, revealAt)
			}
			// Clean stale lottery which is already expired.
			for id, lottery := range revealed {
				// Move all expired lotteries into `expired` set.
				if lottery.RevealNumber+lotteryClaimPeriod <= current {
					delete(revealed, id)
					expired[id] = lottery
					log.Debug("Lottery expired", "id", lottery.Id)
				}
			}
			// Check all expired lotteries, if any of them can be reset,
			// fire an event for notification.
			for id, lottery := range expired {
				if lottery.RevealNumber+lotteryClaimPeriod+lotteryProcessConfirms <= current {
					// Note it might be expensive for light client to retrieve
					// information from contract.
					ret, err := drawer.book.contract.Lotteries(nil, id)
					if err != nil {
						continue
					}
					if ret.Amount > 0 {
						events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryExpired})
					}
					delete(expired, id)
					log.Debug("Lottery removed", "id", id)
				}
			}

		case lottery := <-drawer.lotteryCh:
			pending[current] = append(pending[current], lottery)
			events = append(events, LotteryEvent{Id: lottery.Id, Status: LotteryPending})
			log.Debug("Lottery created", "id", lottery.Id, "amout", lottery.Amount, "revealnumber", lottery.RevealNumber)

		case claimedEvent := <-lotteryClaimedCh:
			id := common.Hash(claimedEvent.Id)
			if _, exist := revealed[id]; exist {
				delete(revealed, id)
			}
			if _, exist := expired[id]; exist {
				delete(expired, id)
			}
			log.Debug("Lottery claimed", "id", id)

		case reqCh := <-drawer.queryCh:
			var ret []*Lottery
			for _, lotteries := range active {
				ret = append(ret, lotteries...)
			}
			reqCh <- ret

		case <-drawer.closeCh:
			log.Debug("Stopping cheque drawer instance...")
			return
		}
	}
}

// ActiveLotteris returns all active lotteris which can be used
// to make payment.
func (drawer *ChequeDrawer) ActiveLotteris() []*Lottery {
	reqCh := make(chan []*Lottery, 1)
	select {
	case drawer.queryCh <- reqCh:
		return <-reqCh
	case <-drawer.closeCh:
		return nil
	}
}

// SubscribeLotteryEvent registers a subscription of LotteryEvent.
func (drawer *ChequeDrawer) SubscribeLotteryEvent(ch chan<- []LotteryEvent) event.Subscription {
	return drawer.scope.Track(drawer.lotteryFeed.Subscribe(ch))
}
