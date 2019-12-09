// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contract

import (
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = abi.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// LotteryBookABI is the input ABI used to generate the binding from.
const LotteryBookABI = "[{\"inputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"bytes32\",\"name\":\"id\",\"type\":\"bytes32\"}],\"name\":\"lotteryClaimed\",\"type\":\"event\"},{\"constant\":false,\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"id\",\"type\":\"bytes32\"},{\"internalType\":\"bytes4\",\"name\":\"revealRange\",\"type\":\"bytes4\"},{\"internalType\":\"uint8\",\"name\":\"sig_v\",\"type\":\"uint8\"},{\"internalType\":\"bytes32\",\"name\":\"sig_r\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32\",\"name\":\"sig_s\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32[]\",\"name\":\"proof\",\"type\":\"bytes32[]\"}],\"name\":\"claim\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"id\",\"type\":\"bytes32\"}],\"name\":\"destroyLottery\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"name\":\"lotteries\",\"outputs\":[{\"internalType\":\"uint64\",\"name\":\"amount\",\"type\":\"uint64\"},{\"internalType\":\"uint64\",\"name\":\"revealNumber\",\"type\":\"uint64\"},{\"internalType\":\"uint64\",\"name\":\"salt\",\"type\":\"uint64\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"id\",\"type\":\"bytes32\"},{\"internalType\":\"uint64\",\"name\":\"blockNumber\",\"type\":\"uint64\"},{\"internalType\":\"uint64\",\"name\":\"salt\",\"type\":\"uint64\"}],\"name\":\"newLottery\",\"outputs\":[],\"payable\":true,\"stateMutability\":\"payable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"addresspayable\",\"name\":\"\",\"type\":\"address\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"id\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32\",\"name\":\"newid\",\"type\":\"bytes32\"},{\"internalType\":\"uint64\",\"name\":\"newRevealNumber\",\"type\":\"uint64\"},{\"internalType\":\"uint64\",\"name\":\"newSalt\",\"type\":\"uint64\"}],\"name\":\"resetLottery\",\"outputs\":[],\"payable\":true,\"stateMutability\":\"payable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"version\",\"outputs\":[{\"internalType\":\"uint64\",\"name\":\"\",\"type\":\"uint64\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// LotteryBookBin is the compiled bytecode used for deploying new contracts.
var LotteryBookBin = "0x608060405234801561001057600080fd5b50600180546001600160a01b03191633179055610e4b806100326000396000f3fe6080604052600436106100705760003560e01c8063915c72c71161004e578063915c72c714610113578063ac209f2114610168578063add6cadb1461019c578063f10618ac146101c657610070565b806338f7e2961461007557806354fd4d50146100b15780638da5cb5b146100e2575b600080fd5b6100af6004803603608081101561008b57600080fd5b508035906020810135906001600160401b03604082013581169160600135166102a2565b005b3480156100bd57600080fd5b506100c661055f565b604080516001600160401b039092168252519081900360200190f35b3480156100ee57600080fd5b506100f7610564565b604080516001600160a01b039092168252519081900360200190f35b34801561011f57600080fd5b5061013d6004803603602081101561013657600080fd5b5035610573565b604080516001600160401b039485168152928416602084015292168183015290519081900360600190f35b6100af6004803603606081101561017e57600080fd5b508035906001600160401b03602082013581169160400135166105a4565b3480156101a857600080fd5b506100af600480360360208110156101bf57600080fd5b5035610766565b3480156101d257600080fd5b506100af600480360360c08110156101e957600080fd5b8135916001600160e01b03196020820135169160ff604083013516916060810135916080820135919081019060c0810160a082013564010000000081111561023057600080fd5b82018360208201111561024257600080fd5b8035906020019184602083028401116401000000008311171561026457600080fd5b9190808060200260200160405190810160405280939291908181526020018383602002808284376000920191909152509295506108a2945050505050565b6001546001600160a01b031633146102f6576040805162461bcd60e51b81526020600482015260126024820152711bdb9b1e481bdddb995c88185b1b1bddd95960721b604482015290519081900360640190fd5b43826001600160401b031611610353576040805162461bcd60e51b815260206004820152601f60248201527f696e76616c6964206c6f7474657279207265736574206f7065726174696f6e00604482015290519081900360640190fd5b600084815260208190526040902054600160401b90046001600160401b0316801580159061038d57504381610100016001600160401b0316105b6103c85760405162461bcd60e51b8152600401808060200182810382526023815260200180610dc66023913960400191505060405180910390fd5b600084815260208190526040808220805467ffffffffffffffff60401b1916600160401b6001600160401b03888116919091029190911767ffffffffffffffff60801b1916600160801b878316021780835589855292842054938890529290921667ffffffffffffffff19909116179055341561053d576000848152602081905260409020546001600160401b03908116903482011681106104a5576040805162461bcd60e51b81526020600482015260116024820152706164646974696f6e206f766572666c6f7760781b604482015290519081900360640190fd5b670de0b6b3a76400003482016001600160401b0316111561050d576040805162461bcd60e51b815260206004820152601f60248201527f65786365656473206d6178696d756d206c6f7474657279206465706f73697400604482015290519081900360640190fd5b6000858152602081905260409020805467ffffffffffffffff191634929092016001600160401b03169190911790555b50505060009182525060208190526040902080546001600160c01b0319169055565b600081565b6001546001600160a01b031681565b6000602081905290815260409020546001600160401b0380821691600160401b8104821691600160801b9091041683565b6001546001600160a01b031633146105f8576040805162461bcd60e51b81526020600482015260126024820152711bdb9b1e481bdddb995c88185b1b1bddd95960721b604482015290519081900360640190fd5b43826001600160401b03161180156106105750600034115b80156106245750670de0b6b3a76400003411155b610675576040805162461bcd60e51b815260206004820152601860248201527f696e76616c6964206c6f74746572792073657474696e67730000000000000000604482015290519081900360640190fd5b600083815260208190526040902054600160401b90046001600160401b0316156106db576040805162461bcd60e51b81526020600482015260126024820152716475706c696361746564206c6f747465727960701b604482015290519081900360640190fd5b604080516060810182526001600160401b03348116825293841660208083019182529385168284019081526000968752938690529190942093518454915192518416600160801b0267ffffffffffffffff60801b19938516600160401b0267ffffffffffffffff60401b199290951667ffffffffffffffff1990931692909217169290921716179055565b6001546001600160a01b031633146107ba576040805162461bcd60e51b81526020600482015260126024820152711bdb9b1e481bdddb995c88185b1b1bddd95960721b604482015290519081900360640190fd5b600081815260208190526040902054600160401b90046001600160401b031680158015906107f457504381610100016001600160401b0316105b61082f5760405162461bcd60e51b8152600401808060200182810382526023815260200180610dc66023913960400191505060405180910390fd5b6001546000838152602081905260408082205490516001600160a01b03909316926001600160401b0390911680156108fc0292909190818181858888f19350505050158015610882573d6000803e3d6000fd5b5050600090815260208190526040902080546001600160c01b0319169055565b600086815260208190526040902054600160401b90046001600160401b03168061090a576040805162461bcd60e51b81526020600482015260146024820152736e6f6e2d6578697374656e74206c6f747465727960601b604482015290519081900360640190fd5b43816001600160401b031610801561092f57504381610100016001600160401b031610155b61096a5760405162461bcd60e51b815260040180806020018281038252602e815260200180610de9602e913960400191505060405180910390fd5b604080513360601b60208083019190915282518083036014018152603490920190925280519101206000805b84518160ff161015610a48576000858260ff16815181106109b357fe5b60200260200101519050808410156109fb5783816040516020018083815260200182815260200192505050604051602081830303815290604052805190602001209350610a3f565b8160ff16600160ff16901b60ff1683019250808460405160200180838152602001828152602001925050506040516020818303038152906040528051906020012093505b50600101610996565b506000898152602081815260409182902054825180830195909552600160801b900460c01b6001600160c01b03191684830152815180850360280181526048909401909152825192019190912090818914610aea576040805162461bcd60e51b815260206004820152601d60248201527f696e76616c696420706f736974696f6e206d65726b6c652070726f6f66000000604482015290519081900360640190fd5b6001600160401b0383164060e089901c63ffffffff82161115610b4b576040805162461bcd60e51b815260206004820152601460248201527334b73b30b634b2103bb4b73732b910383937b7b360611b604482015290519081900360640190fd5b845163ffffffff821664010000000090911c6001600160401b031683021115610bb2576040805162461bcd60e51b815260206004820152601460248201527334b73b30b634b2103bb4b73732b910383937b7b360611b604482015290519081900360640190fd5b8451640100000000901c600183010263ffffffff81161580610bdd575060e08a901c63ffffffff8216115b610c25576040805162461bcd60e51b815260206004820152601460248201527334b73b30b634b2103bb4b73732b910383937b7b360611b604482015290519081900360640190fd5b60408051601960f81b6020808301919091526000602183018190523060601b6022840152603683018f90526001600160e01b03198e1660568401528351603a818503018152605a8401808652815191840191909120919052607a830180855281905260ff8d16609a84015260ba83018c905260da83018b9052925160019260fa8082019392601f1981019281900390910190855afa158015610ccb573d6000803e3d6000fd5b5050604051601f1901516001546001600160a01b039081169116149050610d2d576040805162461bcd60e51b8152602060048201526011602482015270696e76616c6964207369676e617475726560781b604482015290519081900360640190fd5b60008c81526020819052604080822054905133926001600160401b0390921680156108fc0292909190818181858888f19350505050158015610d73573d6000803e3d6000fd5b5060008c81526020819052604080822080546001600160c01b0319169055518d917f4c02162f394fb7efbecba1d186e234f1fe96b1f5f5b4fe67591b4b3e87c1881f91a250505050505050505050505056fe6e6f6e2d6578697374656e74206f72206e6f6e2d65787069726564206c6f74746572796c6f74746572792069736e277420636c61696d6561626c65206f72206974277320616c7265616479207374616c65a265627a7a72315820fe79dc4990aca832a65c9cae7e9506671c6453a75a90c29d394a099d10b60c5e64736f6c634300050f0032"

// DeployLotteryBook deploys a new Ethereum contract, binding an instance of LotteryBook to it.
func DeployLotteryBook(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *LotteryBook, error) {
	parsed, err := abi.JSON(strings.NewReader(LotteryBookABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(LotteryBookBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &LotteryBook{LotteryBookCaller: LotteryBookCaller{contract: contract}, LotteryBookTransactor: LotteryBookTransactor{contract: contract}, LotteryBookFilterer: LotteryBookFilterer{contract: contract}}, nil
}

// LotteryBook is an auto generated Go binding around an Ethereum contract.
type LotteryBook struct {
	LotteryBookCaller     // Read-only binding to the contract
	LotteryBookTransactor // Write-only binding to the contract
	LotteryBookFilterer   // Log filterer for contract events
}

// LotteryBookCaller is an auto generated read-only Go binding around an Ethereum contract.
type LotteryBookCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// LotteryBookTransactor is an auto generated write-only Go binding around an Ethereum contract.
type LotteryBookTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// LotteryBookFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type LotteryBookFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// LotteryBookSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type LotteryBookSession struct {
	Contract     *LotteryBook      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// LotteryBookCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type LotteryBookCallerSession struct {
	Contract *LotteryBookCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// LotteryBookTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type LotteryBookTransactorSession struct {
	Contract     *LotteryBookTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// LotteryBookRaw is an auto generated low-level Go binding around an Ethereum contract.
type LotteryBookRaw struct {
	Contract *LotteryBook // Generic contract binding to access the raw methods on
}

// LotteryBookCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type LotteryBookCallerRaw struct {
	Contract *LotteryBookCaller // Generic read-only contract binding to access the raw methods on
}

// LotteryBookTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type LotteryBookTransactorRaw struct {
	Contract *LotteryBookTransactor // Generic write-only contract binding to access the raw methods on
}

// NewLotteryBook creates a new instance of LotteryBook, bound to a specific deployed contract.
func NewLotteryBook(address common.Address, backend bind.ContractBackend) (*LotteryBook, error) {
	contract, err := bindLotteryBook(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &LotteryBook{LotteryBookCaller: LotteryBookCaller{contract: contract}, LotteryBookTransactor: LotteryBookTransactor{contract: contract}, LotteryBookFilterer: LotteryBookFilterer{contract: contract}}, nil
}

// NewLotteryBookCaller creates a new read-only instance of LotteryBook, bound to a specific deployed contract.
func NewLotteryBookCaller(address common.Address, caller bind.ContractCaller) (*LotteryBookCaller, error) {
	contract, err := bindLotteryBook(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &LotteryBookCaller{contract: contract}, nil
}

// NewLotteryBookTransactor creates a new write-only instance of LotteryBook, bound to a specific deployed contract.
func NewLotteryBookTransactor(address common.Address, transactor bind.ContractTransactor) (*LotteryBookTransactor, error) {
	contract, err := bindLotteryBook(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &LotteryBookTransactor{contract: contract}, nil
}

// NewLotteryBookFilterer creates a new log filterer instance of LotteryBook, bound to a specific deployed contract.
func NewLotteryBookFilterer(address common.Address, filterer bind.ContractFilterer) (*LotteryBookFilterer, error) {
	contract, err := bindLotteryBook(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &LotteryBookFilterer{contract: contract}, nil
}

// bindLotteryBook binds a generic wrapper to an already deployed contract.
func bindLotteryBook(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(LotteryBookABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_LotteryBook *LotteryBookRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _LotteryBook.Contract.LotteryBookCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_LotteryBook *LotteryBookRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _LotteryBook.Contract.LotteryBookTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_LotteryBook *LotteryBookRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _LotteryBook.Contract.LotteryBookTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_LotteryBook *LotteryBookCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _LotteryBook.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_LotteryBook *LotteryBookTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _LotteryBook.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_LotteryBook *LotteryBookTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _LotteryBook.Contract.contract.Transact(opts, method, params...)
}

// Lotteries is a free data retrieval call binding the contract method 0x915c72c7.
//
// Solidity: function lotteries(bytes32 ) constant returns(uint64 amount, uint64 revealNumber, uint64 salt)
func (_LotteryBook *LotteryBookCaller) Lotteries(opts *bind.CallOpts, arg0 [32]byte) (struct {
	Amount       uint64
	RevealNumber uint64
	Salt         uint64
}, error) {
	ret := new(struct {
		Amount       uint64
		RevealNumber uint64
		Salt         uint64
	})
	out := ret
	err := _LotteryBook.contract.Call(opts, out, "lotteries", arg0)
	return *ret, err
}

// Lotteries is a free data retrieval call binding the contract method 0x915c72c7.
//
// Solidity: function lotteries(bytes32 ) constant returns(uint64 amount, uint64 revealNumber, uint64 salt)
func (_LotteryBook *LotteryBookSession) Lotteries(arg0 [32]byte) (struct {
	Amount       uint64
	RevealNumber uint64
	Salt         uint64
}, error) {
	return _LotteryBook.Contract.Lotteries(&_LotteryBook.CallOpts, arg0)
}

// Lotteries is a free data retrieval call binding the contract method 0x915c72c7.
//
// Solidity: function lotteries(bytes32 ) constant returns(uint64 amount, uint64 revealNumber, uint64 salt)
func (_LotteryBook *LotteryBookCallerSession) Lotteries(arg0 [32]byte) (struct {
	Amount       uint64
	RevealNumber uint64
	Salt         uint64
}, error) {
	return _LotteryBook.Contract.Lotteries(&_LotteryBook.CallOpts, arg0)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() constant returns(address)
func (_LotteryBook *LotteryBookCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var (
		ret0 = new(common.Address)
	)
	out := ret0
	err := _LotteryBook.contract.Call(opts, out, "owner")
	return *ret0, err
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() constant returns(address)
func (_LotteryBook *LotteryBookSession) Owner() (common.Address, error) {
	return _LotteryBook.Contract.Owner(&_LotteryBook.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() constant returns(address)
func (_LotteryBook *LotteryBookCallerSession) Owner() (common.Address, error) {
	return _LotteryBook.Contract.Owner(&_LotteryBook.CallOpts)
}

// Version is a free data retrieval call binding the contract method 0x54fd4d50.
//
// Solidity: function version() constant returns(uint64)
func (_LotteryBook *LotteryBookCaller) Version(opts *bind.CallOpts) (uint64, error) {
	var (
		ret0 = new(uint64)
	)
	out := ret0
	err := _LotteryBook.contract.Call(opts, out, "version")
	return *ret0, err
}

// Version is a free data retrieval call binding the contract method 0x54fd4d50.
//
// Solidity: function version() constant returns(uint64)
func (_LotteryBook *LotteryBookSession) Version() (uint64, error) {
	return _LotteryBook.Contract.Version(&_LotteryBook.CallOpts)
}

// Version is a free data retrieval call binding the contract method 0x54fd4d50.
//
// Solidity: function version() constant returns(uint64)
func (_LotteryBook *LotteryBookCallerSession) Version() (uint64, error) {
	return _LotteryBook.Contract.Version(&_LotteryBook.CallOpts)
}

// Claim is a paid mutator transaction binding the contract method 0xf10618ac.
//
// Solidity: function claim(bytes32 id, bytes4 revealRange, uint8 sig_v, bytes32 sig_r, bytes32 sig_s, bytes32[] proof) returns()
func (_LotteryBook *LotteryBookTransactor) Claim(opts *bind.TransactOpts, id [32]byte, revealRange [4]byte, sig_v uint8, sig_r [32]byte, sig_s [32]byte, proof [][32]byte) (*types.Transaction, error) {
	return _LotteryBook.contract.Transact(opts, "claim", id, revealRange, sig_v, sig_r, sig_s, proof)
}

// Claim is a paid mutator transaction binding the contract method 0xf10618ac.
//
// Solidity: function claim(bytes32 id, bytes4 revealRange, uint8 sig_v, bytes32 sig_r, bytes32 sig_s, bytes32[] proof) returns()
func (_LotteryBook *LotteryBookSession) Claim(id [32]byte, revealRange [4]byte, sig_v uint8, sig_r [32]byte, sig_s [32]byte, proof [][32]byte) (*types.Transaction, error) {
	return _LotteryBook.Contract.Claim(&_LotteryBook.TransactOpts, id, revealRange, sig_v, sig_r, sig_s, proof)
}

// Claim is a paid mutator transaction binding the contract method 0xf10618ac.
//
// Solidity: function claim(bytes32 id, bytes4 revealRange, uint8 sig_v, bytes32 sig_r, bytes32 sig_s, bytes32[] proof) returns()
func (_LotteryBook *LotteryBookTransactorSession) Claim(id [32]byte, revealRange [4]byte, sig_v uint8, sig_r [32]byte, sig_s [32]byte, proof [][32]byte) (*types.Transaction, error) {
	return _LotteryBook.Contract.Claim(&_LotteryBook.TransactOpts, id, revealRange, sig_v, sig_r, sig_s, proof)
}

// DestroyLottery is a paid mutator transaction binding the contract method 0xadd6cadb.
//
// Solidity: function destroyLottery(bytes32 id) returns()
func (_LotteryBook *LotteryBookTransactor) DestroyLottery(opts *bind.TransactOpts, id [32]byte) (*types.Transaction, error) {
	return _LotteryBook.contract.Transact(opts, "destroyLottery", id)
}

// DestroyLottery is a paid mutator transaction binding the contract method 0xadd6cadb.
//
// Solidity: function destroyLottery(bytes32 id) returns()
func (_LotteryBook *LotteryBookSession) DestroyLottery(id [32]byte) (*types.Transaction, error) {
	return _LotteryBook.Contract.DestroyLottery(&_LotteryBook.TransactOpts, id)
}

// DestroyLottery is a paid mutator transaction binding the contract method 0xadd6cadb.
//
// Solidity: function destroyLottery(bytes32 id) returns()
func (_LotteryBook *LotteryBookTransactorSession) DestroyLottery(id [32]byte) (*types.Transaction, error) {
	return _LotteryBook.Contract.DestroyLottery(&_LotteryBook.TransactOpts, id)
}

// NewLottery is a paid mutator transaction binding the contract method 0xac209f21.
//
// Solidity: function newLottery(bytes32 id, uint64 blockNumber, uint64 salt) returns()
func (_LotteryBook *LotteryBookTransactor) NewLottery(opts *bind.TransactOpts, id [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error) {
	return _LotteryBook.contract.Transact(opts, "newLottery", id, blockNumber, salt)
}

// NewLottery is a paid mutator transaction binding the contract method 0xac209f21.
//
// Solidity: function newLottery(bytes32 id, uint64 blockNumber, uint64 salt) returns()
func (_LotteryBook *LotteryBookSession) NewLottery(id [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error) {
	return _LotteryBook.Contract.NewLottery(&_LotteryBook.TransactOpts, id, blockNumber, salt)
}

// NewLottery is a paid mutator transaction binding the contract method 0xac209f21.
//
// Solidity: function newLottery(bytes32 id, uint64 blockNumber, uint64 salt) returns()
func (_LotteryBook *LotteryBookTransactorSession) NewLottery(id [32]byte, blockNumber uint64, salt uint64) (*types.Transaction, error) {
	return _LotteryBook.Contract.NewLottery(&_LotteryBook.TransactOpts, id, blockNumber, salt)
}

// ResetLottery is a paid mutator transaction binding the contract method 0x38f7e296.
//
// Solidity: function resetLottery(bytes32 id, bytes32 newid, uint64 newRevealNumber, uint64 newSalt) returns()
func (_LotteryBook *LotteryBookTransactor) ResetLottery(opts *bind.TransactOpts, id [32]byte, newid [32]byte, newRevealNumber uint64, newSalt uint64) (*types.Transaction, error) {
	return _LotteryBook.contract.Transact(opts, "resetLottery", id, newid, newRevealNumber, newSalt)
}

// ResetLottery is a paid mutator transaction binding the contract method 0x38f7e296.
//
// Solidity: function resetLottery(bytes32 id, bytes32 newid, uint64 newRevealNumber, uint64 newSalt) returns()
func (_LotteryBook *LotteryBookSession) ResetLottery(id [32]byte, newid [32]byte, newRevealNumber uint64, newSalt uint64) (*types.Transaction, error) {
	return _LotteryBook.Contract.ResetLottery(&_LotteryBook.TransactOpts, id, newid, newRevealNumber, newSalt)
}

// ResetLottery is a paid mutator transaction binding the contract method 0x38f7e296.
//
// Solidity: function resetLottery(bytes32 id, bytes32 newid, uint64 newRevealNumber, uint64 newSalt) returns()
func (_LotteryBook *LotteryBookTransactorSession) ResetLottery(id [32]byte, newid [32]byte, newRevealNumber uint64, newSalt uint64) (*types.Transaction, error) {
	return _LotteryBook.Contract.ResetLottery(&_LotteryBook.TransactOpts, id, newid, newRevealNumber, newSalt)
}

// LotteryBookLotteryClaimedIterator is returned from FilterLotteryClaimed and is used to iterate over the raw logs and unpacked data for LotteryClaimed events raised by the LotteryBook contract.
type LotteryBookLotteryClaimedIterator struct {
	Event *LotteryBookLotteryClaimed // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *LotteryBookLotteryClaimedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(LotteryBookLotteryClaimed)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(LotteryBookLotteryClaimed)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *LotteryBookLotteryClaimedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *LotteryBookLotteryClaimedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// LotteryBookLotteryClaimed represents a LotteryClaimed event raised by the LotteryBook contract.
type LotteryBookLotteryClaimed struct {
	Id  [32]byte
	Raw types.Log // Blockchain specific contextual infos
}

// FilterLotteryClaimed is a free log retrieval operation binding the contract event 0x4c02162f394fb7efbecba1d186e234f1fe96b1f5f5b4fe67591b4b3e87c1881f.
//
// Solidity: event lotteryClaimed(bytes32 indexed id)
func (_LotteryBook *LotteryBookFilterer) FilterLotteryClaimed(opts *bind.FilterOpts, id [][32]byte) (*LotteryBookLotteryClaimedIterator, error) {

	var idRule []interface{}
	for _, idItem := range id {
		idRule = append(idRule, idItem)
	}

	logs, sub, err := _LotteryBook.contract.FilterLogs(opts, "lotteryClaimed", idRule)
	if err != nil {
		return nil, err
	}
	return &LotteryBookLotteryClaimedIterator{contract: _LotteryBook.contract, event: "lotteryClaimed", logs: logs, sub: sub}, nil
}

// WatchLotteryClaimed is a free log subscription operation binding the contract event 0x4c02162f394fb7efbecba1d186e234f1fe96b1f5f5b4fe67591b4b3e87c1881f.
//
// Solidity: event lotteryClaimed(bytes32 indexed id)
func (_LotteryBook *LotteryBookFilterer) WatchLotteryClaimed(opts *bind.WatchOpts, sink chan<- *LotteryBookLotteryClaimed, id [][32]byte) (event.Subscription, error) {

	var idRule []interface{}
	for _, idItem := range id {
		idRule = append(idRule, idItem)
	}

	logs, sub, err := _LotteryBook.contract.WatchLogs(opts, "lotteryClaimed", idRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(LotteryBookLotteryClaimed)
				if err := _LotteryBook.contract.UnpackLog(event, "lotteryClaimed", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseLotteryClaimed is a log parse operation binding the contract event 0x4c02162f394fb7efbecba1d186e234f1fe96b1f5f5b4fe67591b4b3e87c1881f.
//
// Solidity: event lotteryClaimed(bytes32 indexed id)
func (_LotteryBook *LotteryBookFilterer) ParseLotteryClaimed(log types.Log) (*LotteryBookLotteryClaimed, error) {
	event := new(LotteryBookLotteryClaimed)
	if err := _LotteryBook.contract.UnpackLog(event, "lotteryClaimed", log); err != nil {
		return nil, err
	}
	return event, nil
}
