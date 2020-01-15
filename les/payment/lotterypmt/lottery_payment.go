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

package lotterypmt

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/contracts/lotterybook/contract"
	"reflect"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/lotterybook"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/les/payment"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// Identity is the unique string identity of lottery payment.
const Identity = "Lottery"

var errInvalidOpt = errors.New("invalid operation")

// Role is the role of user in payment route.
type Role int

const (
	Sender Role = iota
	Receiver
)

type Route struct {
	role         Role
	trusted      []common.Hash
	chainReader  payment.ChainReader
	localAddr    common.Address
	contractAddr common.Address
	remoteAddr   common.Address
	peer         payment.Peer              // The peer handler of counterparty
	drawer       *lotterybook.ChequeDrawer // Nil if route is opened by receiver
	drawee       *lotterybook.ChequeDrawee // Nil if route is opened by sender
}

func newRoute(role Role, trusted []common.Hash, chainReader payment.ChainReader, localAddr, remoteAddr, chanAddr common.Address, drawer *lotterybook.ChequeDrawer, drawee *lotterybook.ChequeDrawee, peer payment.Peer) *Route {
	route := &Route{
		role:         role,
		trusted:      trusted,
		chainReader:  chainReader,
		localAddr:    localAddr,
		remoteAddr:   remoteAddr,
		contractAddr: chanAddr,
		peer:         peer,
		drawer:       drawer,
		drawee:       drawee,
	}
	return route
}

// Pay initiates a payment to the designated payee with specified
// payemnt amount and also trigger a deposit operation if auto deposit
// is set and threshold is met.
func (r *Route) Pay(amount uint64) error {
	if r.role != Sender {
		return errInvalidOpt
	}
	cheque, err := r.drawer.IssueCheque(r.remoteAddr, amount)
	if err != nil {
		return err
	}
	proofOfPayment, err := rlp.EncodeToBytes(cheque)
	if err != nil {
		return err
	}
	log.Debug("Issued payment", "amount", amount, "route", r.contractAddr)
	return r.peer.SendPayment(proofOfPayment, Identity)
}

// Receive receives a payment from the payer and returns any error
// for payment processing and proving.
func (r *Route) Receive(proofOfPayment []byte) error {
	if r.role != Receiver {
		return errInvalidOpt
	}
	var cheque lotterybook.Cheque
	if err := rlp.DecodeBytes(proofOfPayment, &cheque); err != nil {
		return err
	}
	amount, err := r.drawee.AddCheque(&cheque)
	if err != nil {
		return err
	}
	log.Debug("Received payment", "amount", amount, "route", r.contractAddr)
	return r.peer.AddBalance(amount)
}

// Close exits the payment and withdraw all expired lotteries
func (r *Route) Close() error {
	if r.role != Sender {
		return nil
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancelFn()
	return r.drawer.Destroy(ctx)
}

// Verify ensures the contract code of lottery contract is trusted.
func (r *Route) Verify() error {
	if r.role != Receiver {
		return nil
	}
	if len(r.trusted) == 0 {
		return nil
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancelFn()
	hash, err := r.drawee.CodeHash(ctx)
	if err != nil {
		return err
	}
	for _, h := range r.trusted {
		if h == hash {
			return nil
		}
	}
	return errors.New("untrusted contract")
}

// Info returns the infomation union of payment route.
func (r *Route) Info() (common.Address, common.Address, common.Address) {
	if r.role == Sender {
		return r.localAddr, r.remoteAddr, r.drawer.ContractAddr()
	} else {
		return r.remoteAddr, r.localAddr, r.contractAddr
	}
}

// Config defines all user-selectable options for both
// sender and receiver.
type Config struct {
	// Role is the role of the user in the payment channel, either the
	// payer or the payee.
	Role Role

	// TrustedContract is a list of contract code hash which payment receivers
	// can trust for usage. Les servers can configure or extend it by themselves,
	// but by default the in-built contract code hash is included.
	TrustedContracts []common.Hash
}

// DefaultSenderConfig is the default manager config for sender.
var DefaultSenderConfig = &Config{
	Role: Sender,
}

// DefaultReceiverConfig is the default manager config for receiver.
var DefaultReceiverConfig = &Config{
	Role:             Receiver,
	TrustedContracts: []common.Hash{contract.RuntimeCodehash},
}

// Manager is responsible for payment routes management.
type Manager struct {
	config      *Config
	chainReader payment.ChainReader
	localAddr   common.Address
	db          ethdb.Database

	txSigner     *bind.TransactOpts                // Signer used to sign transaction
	chequeSigner func(data []byte) ([]byte, error) // Signer used to sign cheque

	// routes are all established channels. For payment receiver,
	// the key of routes map is sender's address; otherwise the
	// key refers to receiver's address.
	routes map[common.Address]*Route
	lock   sync.RWMutex              // The lock used to protect routes
	sender *lotterybook.ChequeDrawer // Nil if manager is opened by receiver

	// Backends used to interact with the underlying contract
	cBackend bind.ContractBackend
	dBackend bind.DeployBackend
}

// newRoute initializes a one-to-one payment channel for both sender and receiver.
func NewManager(config *Config, chainReader payment.ChainReader, txSigner *bind.TransactOpts, chequeSigner func(digestHash []byte) ([]byte, error), localAddr common.Address, cBackend bind.ContractBackend, dBackend bind.DeployBackend, db ethdb.Database) (*Manager, error) {
	c := &Manager{
		config:       config,
		chainReader:  chainReader,
		localAddr:    localAddr,
		txSigner:     txSigner,
		chequeSigner: chequeSigner,
		db:           db,
		cBackend:     cBackend,
		dBackend:     dBackend,
		routes:       make(map[common.Address]*Route),
	}
	if c.config.Role == Sender {
		sender, err := lotterybook.NewChequeDrawer(c.localAddr, txSigner, chequeSigner, chainReader, cBackend, dBackend, db)
		if err != nil {
			return nil, err
		}
		c.sender = sender
	}
	return c, nil
}

// OpenRoute establishes a new payment route for new customer or new service
// provider. If we are payment receiver, the addr refers to the lottery contract
// addr, otherwise the addr refers to receiver's address.
func (c *Manager) OpenRoute(schema payment.Schema, peer payment.Peer) (payment.PaymentRoute, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.config.Role == Receiver {
		chanAddr := schema.Load("Contract").(common.Address)
		remoteAddr := schema.Load("Sender").(common.Address)
		// Filter all duplicated channels.
		if _, exist := c.routes[remoteAddr]; exist {
			return nil, errors.New("duplicated payment route")
		}
		// We are payment receiver, establish an incoming route with
		// specified contract address and counterparty peer.
		receiver, err := lotterybook.NewChequeDrawee(c.txSigner, c.localAddr, remoteAddr, chanAddr, c.chainReader, c.cBackend, c.dBackend, c.db)
		if err != nil {
			return nil, err
		}
		c.routes[remoteAddr] = newRoute(Receiver, c.config.TrustedContracts, c.chainReader, c.localAddr, remoteAddr, chanAddr, nil, receiver, peer)
		log.Debug("Opened route", "contract", chanAddr, "localAddr", c.localAddr, "remoteAddr", remoteAddr)
		return c.routes[remoteAddr], nil
	} else {
		remoteAddr := schema.Load("Receiver").(common.Address)
		// Filter all duplicated channels.
		if _, exist := c.routes[remoteAddr]; exist {
			return nil, errors.New("duplicated payment route")
		}
		// We are payment sender, establish a outgoing route with
		// specified counterparty address and peer.
		c.routes[remoteAddr] = newRoute(Sender, c.config.TrustedContracts, c.chainReader, c.localAddr, remoteAddr, c.sender.ContractAddr(), c.sender, nil, peer)
		log.Debug("Opened route", "contract", c.sender.ContractAddr(), "localAddr", c.localAddr, "remoteAddr", remoteAddr)
		return c.routes[remoteAddr], nil
	}
}

// CloseRoute closes a route with given address. If we are payment receiver,
// the addr refers to the sender's address, otherwise, the address refers to
// receiver's address.
func (c *Manager) CloseRoute(addr common.Address) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.routes, addr)
	log.Debug("Closed route", "address", addr)
	return nil
}

func (c *Manager) deposit(receivers []common.Address, amounts []uint64) (common.Hash, error) {
	if c.config.Role != Sender {
		return common.Hash{}, errInvalidOpt
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancelFn()

	// TODO(rjl493456442) implement reveal range oracle for better number recommendation.
	current := c.chainReader.CurrentHeader().Number.Uint64()
	id, err := c.sender.Deposit(ctx, receivers, amounts, current+5760)
	return id, err
}

// Deposit creates deposit for the given batch of receivers and corresponding
// deposit amount.
func (c *Manager) Deposit(receivers []common.Address, amounts []uint64) error {
	_, err := c.deposit(receivers, amounts)
	return err
}

// DepositAndWait creates deposit for the given batch of receivers and corresponding
// deposit amount. Wait until the deposit is available for payment and emit a signal
// for it.
func (c *Manager) DepositAndWait(receivers []common.Address, amounts []uint64) (chan bool, error) {
	id, err := c.deposit(receivers, amounts)
	if err != nil {
		return nil, err
	}
	done := make(chan bool, 1)
	go func() {
		sink := make(chan []lotterybook.LotteryEvent, 64)
		sub := c.sender.SubscribeLotteryEvent(sink)
		defer sub.Unsubscribe()

		for {
			select {
			case events := <-sink:
				for _, event := range events {
					if event.Id == id && event.Status == lotterybook.LotteryActive {
						done <- true
						return
					}
				}
			case <-sub.Err():
				done <- false
				return
			}
		}
	}()
	return done, nil
}

// CounterParities returns the address of counterparty peer for
// all established payment routes.
func (c *Manager) CounterParities() []common.Address {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var addresses []common.Address
	for addr := range c.routes {
		addresses = append(addresses, addr)
	}
	return addresses
}

// ContractAddr returns the address of lottery contract used. Only valid
// if caller is sender.
func (c *Manager) ContractAddr() common.Address {
	if c.config.Role == Receiver {
		return common.Address{}
	}
	return c.sender.ContractAddr()
}

// LotteryPaymentSchema defines the schema of payment.
type LotteryPaymentSchema struct {
	Sender   common.Address
	Receiver common.Address
	Contract common.Address
}

// Identity implements payment.Schema, returns the identity of payment.
func (schema *LotteryPaymentSchema) Identity() string {
	return Identity
}

// Load implements payment.Schema, returns the specified field with given
// entry key.
func (schema *LotteryPaymentSchema) Load(key string) interface{} {
	typ := reflect.TypeOf(schema).Elem()
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == key {
			val := reflect.ValueOf(schema).Elem()
			return val.Field(i).Interface()
		}
	}
	return nil
}

// LocalSchema returns the payment schema of lottery payment.
func (c *Manager) LocalSchema() (payment.SchemaRLP, error) {
	schema := &LotteryPaymentSchema{Receiver: c.localAddr}
	if c.config.Role == Sender {
		schema = &LotteryPaymentSchema{
			Sender:   c.localAddr,
			Contract: c.sender.ContractAddr(),
		}
	}
	encoded, err := rlp.EncodeToBytes(schema)
	if err != nil {
		return payment.SchemaRLP{}, err
	}
	return payment.SchemaRLP{
		Key:   schema.Identity(),
		Value: encoded,
	}, nil
}

// ResolveSchema resolves the remote schema of lottery payment,
// ensure the schema is compatible with us.
func (c *Manager) ResolveSchema(blob []byte) (payment.Schema, error) {
	var schema LotteryPaymentSchema
	if c.config.Role == Sender {
		if err := rlp.DecodeBytes(blob, &schema); err != nil {
			return nil, err
		}
		if schema.Receiver == (common.Address{}) {
			return nil, errors.New("invald schema")
		}
		return &schema, nil
	} else {
		if err := rlp.DecodeBytes(blob, &schema); err != nil {
			return nil, err
		}
		if schema.Sender == (common.Address{}) || schema.Contract == (common.Address{}) {
			return nil, errors.New("invald schema")
		}
		return &schema, nil
	}
}
