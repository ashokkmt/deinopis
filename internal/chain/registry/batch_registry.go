// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package registry

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
	_ = abi.ConvertType
	_ = time.Tick
	_ = context.Background
)

// BatchRegistryBatch is an auto generated low-level Go binding around an user-defined struct.
type BatchRegistryBatch struct {
	Commitment    [32]byte
	FirstSequence uint64
	LastSequence  uint64
	RecordCount   uint64
}

// BatchRegistryMetaData contains all meta data concerning the BatchRegistry contract.
var BatchRegistryMetaData = &bind.MetaData{
	ABI: "[{\"type\":\"constructor\",\"inputs\":[],\"stateMutability\":\"nonpayable\"},{\"type\":\"function\",\"name\":\"MAX_BATCH_RECORDS\",\"inputs\":[],\"outputs\":[{\"name\":\"\",\"type\":\"uint64\",\"internalType\":\"uint64\"}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"appendBatch\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"batchNo\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"previousCommitment\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"commitment\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"firstSequence\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"lastSequence\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"recordCount\",\"type\":\"uint64\",\"internalType\":\"uint64\"}],\"outputs\":[],\"stateMutability\":\"nonpayable\"},{\"type\":\"function\",\"name\":\"batchCount\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"}],\"outputs\":[{\"name\":\"count\",\"type\":\"uint64\",\"internalType\":\"uint64\"}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"getBatch\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"batchNo\",\"type\":\"uint64\",\"internalType\":\"uint64\"}],\"outputs\":[{\"name\":\"\",\"type\":\"tuple\",\"internalType\":\"structBatchRegistry.Batch\",\"components\":[{\"name\":\"commitment\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"firstSequence\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"lastSequence\",\"type\":\"uint64\",\"internalType\":\"uint64\"},{\"name\":\"recordCount\",\"type\":\"uint64\",\"internalType\":\"uint64\"}]}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"headCommitment\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"}],\"outputs\":[{\"name\":\"head\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"owner\",\"inputs\":[],\"outputs\":[{\"name\":\"\",\"type\":\"address\",\"internalType\":\"address\"}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"publisherOf\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"}],\"outputs\":[{\"name\":\"publisher\",\"type\":\"address\",\"internalType\":\"address\"}],\"stateMutability\":\"view\"},{\"type\":\"function\",\"name\":\"registerStream\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"publisher\",\"type\":\"address\",\"internalType\":\"address\"}],\"outputs\":[],\"stateMutability\":\"nonpayable\"},{\"type\":\"function\",\"name\":\"setPublisher\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"internalType\":\"bytes32\"},{\"name\":\"publisher\",\"type\":\"address\",\"internalType\":\"address\"}],\"outputs\":[],\"stateMutability\":\"nonpayable\"},{\"type\":\"event\",\"name\":\"BatchAppended\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"indexed\":true,\"internalType\":\"bytes32\"},{\"name\":\"batchNo\",\"type\":\"uint64\",\"indexed\":true,\"internalType\":\"uint64\"},{\"name\":\"commitment\",\"type\":\"bytes32\",\"indexed\":false,\"internalType\":\"bytes32\"},{\"name\":\"firstSequence\",\"type\":\"uint64\",\"indexed\":false,\"internalType\":\"uint64\"},{\"name\":\"lastSequence\",\"type\":\"uint64\",\"indexed\":false,\"internalType\":\"uint64\"},{\"name\":\"recordCount\",\"type\":\"uint64\",\"indexed\":false,\"internalType\":\"uint64\"}],\"anonymous\":false},{\"type\":\"event\",\"name\":\"PublisherChanged\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"indexed\":true,\"internalType\":\"bytes32\"},{\"name\":\"oldPublisher\",\"type\":\"address\",\"indexed\":true,\"internalType\":\"address\"},{\"name\":\"newPublisher\",\"type\":\"address\",\"indexed\":true,\"internalType\":\"address\"}],\"anonymous\":false},{\"type\":\"event\",\"name\":\"StreamRegistered\",\"inputs\":[{\"name\":\"streamId\",\"type\":\"bytes32\",\"indexed\":true,\"internalType\":\"bytes32\"},{\"name\":\"publisher\",\"type\":\"address\",\"indexed\":true,\"internalType\":\"address\"}],\"anonymous\":false},{\"type\":\"error\",\"name\":\"InvalidBatchNumber\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"InvalidCommitment\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"InvalidPredecessor\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"InvalidPublisher\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"InvalidSequenceRange\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"InvalidStream\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"StreamAlreadyRegistered\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"StreamNotRegistered\",\"inputs\":[]},{\"type\":\"error\",\"name\":\"Unauthorized\",\"inputs\":[]}]",
	Bin: "0x60a0604052348015600e575f5ffd5b5033608052608051610a306100395f395f81816101ce0152818161022101526107720152610a305ff3fe608060405234801561000f575f5ffd5b5060043610610090575f3560e01c80638981500b116100635780638981500b146101375780638d30776c146101645780638da5cb5b146101c9578063d599ee21146101f0578063ea899b2414610203575f5ffd5b80630581e370146100945780634cd50d88146100ba578063764f73f9146100cf57806388d3e3271461010f575b5f5ffd5b61009d61c35081565b6040516001600160401b0390911681526020015b60405180910390f35b6100cd6100c8366004610881565b610216565b005b6100f76100dd3660046108ba565b5f602081905290815260409020546001600160a01b031681565b6040516001600160a01b0390911681526020016100b1565b61009d61011d3660046108ba565b60016020525f90815260409020546001600160401b031681565b6101566101453660046108ba565b60026020525f908152604090205481565b6040519081526020016100b1565b6101776101723660046108ec565b610317565b6040516100b191905f608082019050825182526001600160401b0360208401511660208301526001600160401b0360408401511660408301526001600160401b03606084015116606083015292915050565b6100f77f000000000000000000000000000000000000000000000000000000000000000081565b6100cd6101fe366004610916565b6103dc565b6100cd610211366004610881565b610767565b336001600160a01b037f0000000000000000000000000000000000000000000000000000000000000000161461025e576040516282b42960e81b815260040160405180910390fd5b5f828152602081905260409020546001600160a01b031680610293576040516327f50d9b60e21b815260040160405180910390fd5b6001600160a01b0382166102ba576040516314757fcb60e11b815260040160405180910390fd5b5f8381526020819052604080822080546001600160a01b0319166001600160a01b03868116918217909255915191929084169186917fca9dd10051facb87697c20b213fddc1424581d0699217a5a2f48f8ad55fc27bd91a4505050565b604080516080810182525f8082526020820181905291810182905260608101919091525f838152600160205260409020546001600160401b03908116908316106103745760405163c3d445a560e01b815260040160405180910390fd5b505f8281526003602090815260408083206001600160401b0380861685529083529281902081516080810183528154815260019091015480851693820193909352600160401b8304841691810191909152600160801b90910490911660608201525b92915050565b5f878152602081905260409020546001600160a01b0316331415806103ff575033155b1561041c576040516282b42960e81b815260040160405180910390fd5b5f878152600160205260409020546001600160401b03878116911614158061044c57506001600160401b03868116145b1561046a5760405163c3d445a560e01b815260040160405180910390fd5b5f878152600260205260409020548514610497576040516307768bf960e01b815260040160405180910390fd5b836104b557604051636033c4fd60e11b815260040160405180910390fd5b6001600160401b03811615806104d5575061c3506001600160401b038216115b806104e757506001600160401b038316155b806105035750826001600160401b0316826001600160401b0316105b8061053f5750806001600160401b0316836001600160401b0316836001600160401b03166105319190610996565b61053c9060016109a9565b14155b1561055d57604051633855112f60e01b815260040160405180910390fd5b856001600160401b03165f0361059c57826001600160401b031660011461059757604051633855112f60e01b815260040160405180910390fd5b610627565b5f878152600360205260408120816105b560018a6109bc565b6001600160401b03908116825260208201929092526040015f2060010154600160401b90048116915081148061060757506105f18160016109db565b6001600160401b0316846001600160401b031614155b1561062557604051633855112f60e01b815260040160405180910390fd5b505b604080516080810182528581526001600160401b038086166020808401918252868316848601908152868416606086019081525f8e8152600384528781208e871682529093529590912093518455905160019384018054925195518416600160801b0267ffffffffffffffff60801b19968516600160401b026fffffffffffffffffffffffffffffffff1990941692909416919091179190911793909316179091556106d49087906109db565b5f888152600160209081526040808320805467ffffffffffffffff19166001600160401b0395861617905560028252918290208790558151878152868416918101919091528483169181019190915282821660608201529087169088907f09c34e221d98baf118b29dd617ee2ec908628e96d1eb789bf5eeb370c19eb2699060800160405180910390a350505050505050565b336001600160a01b037f000000000000000000000000000000000000000000000000000000000000000016146107af576040516282b42960e81b815260040160405180910390fd5b816107cd5760405163a0f87d3360e01b815260040160405180910390fd5b5f828152602081905260409020546001600160a01b03161561080257604051631010f3af60e21b815260040160405180910390fd5b6001600160a01b038116610829576040516314757fcb60e11b815260040160405180910390fd5b5f8281526020819052604080822080546001600160a01b0319166001600160a01b0385169081179091559051909184917f51786738f3c4e8efc08b082181c54fba7240d7873db9177731b5b559cce7e1e09190a35050565b5f5f60408385031215610892575f5ffd5b8235915060208301356001600160a01b03811681146108af575f5ffd5b809150509250929050565b5f602082840312156108ca575f5ffd5b5035919050565b80356001600160401b03811681146108e7575f5ffd5b919050565b5f5f604083850312156108fd575f5ffd5b8235915061090d602084016108d1565b90509250929050565b5f5f5f5f5f5f5f60e0888a03121561092c575f5ffd5b8735965061093c602089016108d1565b95506040880135945060608801359350610958608089016108d1565b925061096660a089016108d1565b915061097460c089016108d1565b905092959891949750929550565b634e487b7160e01b5f52601160045260245ffd5b818103818111156103d6576103d6610982565b808201808211156103d6576103d6610982565b6001600160401b0382811682821603908111156103d6576103d6610982565b6001600160401b0381811683821601908111156103d6576103d661098256fea2646970667358221220563c83257307e9cc4b4a128d0c6b89c956d538297319e08268b05cfdcd1153d264736f6c634300081e0033",
}

// BatchRegistryABI is the input ABI used to generate the binding from.
// Deprecated: Use BatchRegistryMetaData.ABI instead.
var BatchRegistryABI = BatchRegistryMetaData.ABI

// BatchRegistryBin is the compiled bytecode used for deploying new contracts.
// Deprecated: Use BatchRegistryMetaData.Bin instead.
var BatchRegistryBin = BatchRegistryMetaData.Bin

// DeployBatchRegistry deploys a new Ethereum contract, binding an instance of BatchRegistry to it.
func DeployBatchRegistry(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *BatchRegistry, error) {
	parsed, err := BatchRegistryMetaData.GetAbi()
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	if parsed == nil {
		return common.Address{}, nil, nil, errors.New("GetABI returned nil")
	}

	address, tx, contract, err := bind.DeployContract(auth, *parsed, common.FromHex(BatchRegistryBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &BatchRegistry{BatchRegistryCaller: BatchRegistryCaller{contract: contract}, BatchRegistryTransactor: BatchRegistryTransactor{contract: contract}, BatchRegistryFilterer: BatchRegistryFilterer{contract: contract}}, nil
}

// BatchRegistry is an auto generated Go binding around an Ethereum contract.
type BatchRegistry struct {
	BatchRegistryCaller     // Read-only binding to the contract
	BatchRegistryTransactor // Write-only binding to the contract
	BatchRegistryFilterer   // Log filterer for contract events
}

// BatchRegistryCaller is an auto generated read-only Go binding around an Ethereum contract.
type BatchRegistryCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BatchRegistryTransactor is an auto generated write-only Go binding around an Ethereum contract.
type BatchRegistryTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BatchRegistryFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type BatchRegistryFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BatchRegistrySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type BatchRegistrySession struct {
	Contract     *BatchRegistry    // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// BatchRegistryCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type BatchRegistryCallerSession struct {
	Contract *BatchRegistryCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts        // Call options to use throughout this session
}

// BatchRegistryTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type BatchRegistryTransactorSession struct {
	Contract     *BatchRegistryTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts        // Transaction auth options to use throughout this session
}

// BatchRegistryRaw is an auto generated low-level Go binding around an Ethereum contract.
type BatchRegistryRaw struct {
	Contract *BatchRegistry // Generic contract binding to access the raw methods on
}

// BatchRegistryCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type BatchRegistryCallerRaw struct {
	Contract *BatchRegistryCaller // Generic read-only contract binding to access the raw methods on
}

// BatchRegistryTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type BatchRegistryTransactorRaw struct {
	Contract *BatchRegistryTransactor // Generic write-only contract binding to access the raw methods on
}

// NewBatchRegistry creates a new instance of BatchRegistry, bound to a specific deployed contract.
func NewBatchRegistry(address common.Address, backend bind.ContractBackend) (*BatchRegistry, error) {
	contract, err := bindBatchRegistry(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &BatchRegistry{BatchRegistryCaller: BatchRegistryCaller{contract: contract}, BatchRegistryTransactor: BatchRegistryTransactor{contract: contract}, BatchRegistryFilterer: BatchRegistryFilterer{contract: contract}}, nil
}

// NewBatchRegistryCaller creates a new read-only instance of BatchRegistry, bound to a specific deployed contract.
func NewBatchRegistryCaller(address common.Address, caller bind.ContractCaller) (*BatchRegistryCaller, error) {
	contract, err := bindBatchRegistry(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryCaller{contract: contract}, nil
}

// NewBatchRegistryTransactor creates a new write-only instance of BatchRegistry, bound to a specific deployed contract.
func NewBatchRegistryTransactor(address common.Address, transactor bind.ContractTransactor) (*BatchRegistryTransactor, error) {
	contract, err := bindBatchRegistry(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryTransactor{contract: contract}, nil
}

// NewBatchRegistryFilterer creates a new log filterer instance of BatchRegistry, bound to a specific deployed contract.
func NewBatchRegistryFilterer(address common.Address, filterer bind.ContractFilterer) (*BatchRegistryFilterer, error) {
	contract, err := bindBatchRegistry(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryFilterer{contract: contract}, nil
}

// bindBatchRegistry binds a generic wrapper to an already deployed contract.
func bindBatchRegistry(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := BatchRegistryMetaData.GetAbi()
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, *parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_BatchRegistry *BatchRegistryRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _BatchRegistry.Contract.BatchRegistryCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_BatchRegistry *BatchRegistryRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _BatchRegistry.Contract.BatchRegistryTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_BatchRegistry *BatchRegistryRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _BatchRegistry.Contract.BatchRegistryTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_BatchRegistry *BatchRegistryCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _BatchRegistry.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_BatchRegistry *BatchRegistryTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _BatchRegistry.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_BatchRegistry *BatchRegistryTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _BatchRegistry.Contract.contract.Transact(opts, method, params...)
}

// MAXBATCHRECORDS is a free data retrieval call binding the contract method 0x0581e370.
//
// Solidity: function MAX_BATCH_RECORDS() view returns(uint64)
func (_BatchRegistry *BatchRegistryCaller) MAXBATCHRECORDS(opts *bind.CallOpts) (uint64, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "MAX_BATCH_RECORDS")

	if err != nil {
		return *new(uint64), err
	}

	out0 := *abi.ConvertType(out[0], new(uint64)).(*uint64)

	return out0, err

}

// MAXBATCHRECORDS is a free data retrieval call binding the contract method 0x0581e370.
//
// Solidity: function MAX_BATCH_RECORDS() view returns(uint64)
func (_BatchRegistry *BatchRegistrySession) MAXBATCHRECORDS() (uint64, error) {
	return _BatchRegistry.Contract.MAXBATCHRECORDS(&_BatchRegistry.CallOpts)
}

// MAXBATCHRECORDS is a free data retrieval call binding the contract method 0x0581e370.
//
// Solidity: function MAX_BATCH_RECORDS() view returns(uint64)
func (_BatchRegistry *BatchRegistryCallerSession) MAXBATCHRECORDS() (uint64, error) {
	return _BatchRegistry.Contract.MAXBATCHRECORDS(&_BatchRegistry.CallOpts)
}

// BatchCount is a free data retrieval call binding the contract method 0x88d3e327.
//
// Solidity: function batchCount(bytes32 streamId) view returns(uint64 count)
func (_BatchRegistry *BatchRegistryCaller) BatchCount(opts *bind.CallOpts, streamId [32]byte) (uint64, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "batchCount", streamId)

	if err != nil {
		return *new(uint64), err
	}

	out0 := *abi.ConvertType(out[0], new(uint64)).(*uint64)

	return out0, err

}

// BatchCount is a free data retrieval call binding the contract method 0x88d3e327.
//
// Solidity: function batchCount(bytes32 streamId) view returns(uint64 count)
func (_BatchRegistry *BatchRegistrySession) BatchCount(streamId [32]byte) (uint64, error) {
	return _BatchRegistry.Contract.BatchCount(&_BatchRegistry.CallOpts, streamId)
}

// BatchCount is a free data retrieval call binding the contract method 0x88d3e327.
//
// Solidity: function batchCount(bytes32 streamId) view returns(uint64 count)
func (_BatchRegistry *BatchRegistryCallerSession) BatchCount(streamId [32]byte) (uint64, error) {
	return _BatchRegistry.Contract.BatchCount(&_BatchRegistry.CallOpts, streamId)
}

// GetBatch is a free data retrieval call binding the contract method 0x8d30776c.
//
// Solidity: function getBatch(bytes32 streamId, uint64 batchNo) view returns((bytes32,uint64,uint64,uint64))
func (_BatchRegistry *BatchRegistryCaller) GetBatch(opts *bind.CallOpts, streamId [32]byte, batchNo uint64) (BatchRegistryBatch, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "getBatch", streamId, batchNo)

	if err != nil {
		return *new(BatchRegistryBatch), err
	}

	out0 := *abi.ConvertType(out[0], new(BatchRegistryBatch)).(*BatchRegistryBatch)

	return out0, err

}

// GetBatch is a free data retrieval call binding the contract method 0x8d30776c.
//
// Solidity: function getBatch(bytes32 streamId, uint64 batchNo) view returns((bytes32,uint64,uint64,uint64))
func (_BatchRegistry *BatchRegistrySession) GetBatch(streamId [32]byte, batchNo uint64) (BatchRegistryBatch, error) {
	return _BatchRegistry.Contract.GetBatch(&_BatchRegistry.CallOpts, streamId, batchNo)
}

// GetBatch is a free data retrieval call binding the contract method 0x8d30776c.
//
// Solidity: function getBatch(bytes32 streamId, uint64 batchNo) view returns((bytes32,uint64,uint64,uint64))
func (_BatchRegistry *BatchRegistryCallerSession) GetBatch(streamId [32]byte, batchNo uint64) (BatchRegistryBatch, error) {
	return _BatchRegistry.Contract.GetBatch(&_BatchRegistry.CallOpts, streamId, batchNo)
}

// HeadCommitment is a free data retrieval call binding the contract method 0x8981500b.
//
// Solidity: function headCommitment(bytes32 streamId) view returns(bytes32 head)
func (_BatchRegistry *BatchRegistryCaller) HeadCommitment(opts *bind.CallOpts, streamId [32]byte) ([32]byte, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "headCommitment", streamId)

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// HeadCommitment is a free data retrieval call binding the contract method 0x8981500b.
//
// Solidity: function headCommitment(bytes32 streamId) view returns(bytes32 head)
func (_BatchRegistry *BatchRegistrySession) HeadCommitment(streamId [32]byte) ([32]byte, error) {
	return _BatchRegistry.Contract.HeadCommitment(&_BatchRegistry.CallOpts, streamId)
}

// HeadCommitment is a free data retrieval call binding the contract method 0x8981500b.
//
// Solidity: function headCommitment(bytes32 streamId) view returns(bytes32 head)
func (_BatchRegistry *BatchRegistryCallerSession) HeadCommitment(streamId [32]byte) ([32]byte, error) {
	return _BatchRegistry.Contract.HeadCommitment(&_BatchRegistry.CallOpts, streamId)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_BatchRegistry *BatchRegistryCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "owner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_BatchRegistry *BatchRegistrySession) Owner() (common.Address, error) {
	return _BatchRegistry.Contract.Owner(&_BatchRegistry.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_BatchRegistry *BatchRegistryCallerSession) Owner() (common.Address, error) {
	return _BatchRegistry.Contract.Owner(&_BatchRegistry.CallOpts)
}

// PublisherOf is a free data retrieval call binding the contract method 0x764f73f9.
//
// Solidity: function publisherOf(bytes32 streamId) view returns(address publisher)
func (_BatchRegistry *BatchRegistryCaller) PublisherOf(opts *bind.CallOpts, streamId [32]byte) (common.Address, error) {
	var out []interface{}
	err := _BatchRegistry.contract.Call(opts, &out, "publisherOf", streamId)

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// PublisherOf is a free data retrieval call binding the contract method 0x764f73f9.
//
// Solidity: function publisherOf(bytes32 streamId) view returns(address publisher)
func (_BatchRegistry *BatchRegistrySession) PublisherOf(streamId [32]byte) (common.Address, error) {
	return _BatchRegistry.Contract.PublisherOf(&_BatchRegistry.CallOpts, streamId)
}

// PublisherOf is a free data retrieval call binding the contract method 0x764f73f9.
//
// Solidity: function publisherOf(bytes32 streamId) view returns(address publisher)
func (_BatchRegistry *BatchRegistryCallerSession) PublisherOf(streamId [32]byte) (common.Address, error) {
	return _BatchRegistry.Contract.PublisherOf(&_BatchRegistry.CallOpts, streamId)
}

// AppendBatch is a paid mutator transaction binding the contract method 0xd599ee21.
//
// Solidity: function appendBatch(bytes32 streamId, uint64 batchNo, bytes32 previousCommitment, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount) returns()
func (_BatchRegistry *BatchRegistryTransactor) AppendBatch(opts *bind.TransactOpts, streamId [32]byte, batchNo uint64, previousCommitment [32]byte, commitment [32]byte, firstSequence uint64, lastSequence uint64, recordCount uint64) (*types.Transaction, error) {
	return _BatchRegistry.contract.Transact(opts, "appendBatch", streamId, batchNo, previousCommitment, commitment, firstSequence, lastSequence, recordCount)
}

// AppendBatch is a paid mutator transaction binding the contract method 0xd599ee21.
//
// Solidity: function appendBatch(bytes32 streamId, uint64 batchNo, bytes32 previousCommitment, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount) returns()
func (_BatchRegistry *BatchRegistrySession) AppendBatch(streamId [32]byte, batchNo uint64, previousCommitment [32]byte, commitment [32]byte, firstSequence uint64, lastSequence uint64, recordCount uint64) (*types.Transaction, error) {
	return _BatchRegistry.Contract.AppendBatch(&_BatchRegistry.TransactOpts, streamId, batchNo, previousCommitment, commitment, firstSequence, lastSequence, recordCount)
}

// AppendBatch is a paid mutator transaction binding the contract method 0xd599ee21.
//
// Solidity: function appendBatch(bytes32 streamId, uint64 batchNo, bytes32 previousCommitment, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount) returns()
func (_BatchRegistry *BatchRegistryTransactorSession) AppendBatch(streamId [32]byte, batchNo uint64, previousCommitment [32]byte, commitment [32]byte, firstSequence uint64, lastSequence uint64, recordCount uint64) (*types.Transaction, error) {
	return _BatchRegistry.Contract.AppendBatch(&_BatchRegistry.TransactOpts, streamId, batchNo, previousCommitment, commitment, firstSequence, lastSequence, recordCount)
}

// RegisterStream is a paid mutator transaction binding the contract method 0xea899b24.
//
// Solidity: function registerStream(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistryTransactor) RegisterStream(opts *bind.TransactOpts, streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.contract.Transact(opts, "registerStream", streamId, publisher)
}

// RegisterStream is a paid mutator transaction binding the contract method 0xea899b24.
//
// Solidity: function registerStream(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistrySession) RegisterStream(streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.Contract.RegisterStream(&_BatchRegistry.TransactOpts, streamId, publisher)
}

// RegisterStream is a paid mutator transaction binding the contract method 0xea899b24.
//
// Solidity: function registerStream(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistryTransactorSession) RegisterStream(streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.Contract.RegisterStream(&_BatchRegistry.TransactOpts, streamId, publisher)
}

// SetPublisher is a paid mutator transaction binding the contract method 0x4cd50d88.
//
// Solidity: function setPublisher(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistryTransactor) SetPublisher(opts *bind.TransactOpts, streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.contract.Transact(opts, "setPublisher", streamId, publisher)
}

// SetPublisher is a paid mutator transaction binding the contract method 0x4cd50d88.
//
// Solidity: function setPublisher(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistrySession) SetPublisher(streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.Contract.SetPublisher(&_BatchRegistry.TransactOpts, streamId, publisher)
}

// SetPublisher is a paid mutator transaction binding the contract method 0x4cd50d88.
//
// Solidity: function setPublisher(bytes32 streamId, address publisher) returns()
func (_BatchRegistry *BatchRegistryTransactorSession) SetPublisher(streamId [32]byte, publisher common.Address) (*types.Transaction, error) {
	return _BatchRegistry.Contract.SetPublisher(&_BatchRegistry.TransactOpts, streamId, publisher)
}

// BatchRegistryBatchAppendedIterator is returned from FilterBatchAppended and is used to iterate over the raw logs and unpacked data for BatchAppended events raised by the BatchRegistry contract.
type BatchRegistryBatchAppendedIterator struct {
	Event *BatchRegistryBatchAppended // Event containing the contract specifics and raw log

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
func (it *BatchRegistryBatchAppendedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(BatchRegistryBatchAppended)
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
		it.Event = new(BatchRegistryBatchAppended)
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
func (it *BatchRegistryBatchAppendedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *BatchRegistryBatchAppendedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// BatchRegistryBatchAppended represents a BatchAppended event raised by the BatchRegistry contract.
type BatchRegistryBatchAppended struct {
	StreamId      [32]byte
	BatchNo       uint64
	Commitment    [32]byte
	FirstSequence uint64
	LastSequence  uint64
	RecordCount   uint64
	Raw           types.Log // Blockchain specific contextual infos
}

// FilterBatchAppended is a free log retrieval operation binding the contract event 0x09c34e221d98baf118b29dd617ee2ec908628e96d1eb789bf5eeb370c19eb269.
//
// Solidity: event BatchAppended(bytes32 indexed streamId, uint64 indexed batchNo, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount)
func (_BatchRegistry *BatchRegistryFilterer) FilterBatchAppended(opts *bind.FilterOpts, streamId [][32]byte, batchNo []uint64) (*BatchRegistryBatchAppendedIterator, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var batchNoRule []interface{}
	for _, batchNoItem := range batchNo {
		batchNoRule = append(batchNoRule, batchNoItem)
	}

	logs, sub, err := _BatchRegistry.contract.FilterLogs(opts, "BatchAppended", streamIdRule, batchNoRule)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryBatchAppendedIterator{contract: _BatchRegistry.contract, event: "BatchAppended", logs: logs, sub: sub}, nil
}

// WatchBatchAppended is a free log subscription operation binding the contract event 0x09c34e221d98baf118b29dd617ee2ec908628e96d1eb789bf5eeb370c19eb269.
//
// Solidity: event BatchAppended(bytes32 indexed streamId, uint64 indexed batchNo, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount)
func (_BatchRegistry *BatchRegistryFilterer) WatchBatchAppended(opts *bind.WatchOpts, sink chan<- *BatchRegistryBatchAppended, streamId [][32]byte, batchNo []uint64) (event.Subscription, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var batchNoRule []interface{}
	for _, batchNoItem := range batchNo {
		batchNoRule = append(batchNoRule, batchNoItem)
	}

	logs, sub, err := _BatchRegistry.contract.WatchLogs(opts, "BatchAppended", streamIdRule, batchNoRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(BatchRegistryBatchAppended)
				if err := _BatchRegistry.contract.UnpackLog(event, "BatchAppended", log); err != nil {
					// If the signature doesn't match, skip this log.
					if errors.Is(err, bind.ErrEventSignatureMismatch) {
						continue
					}
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

// ParseBatchAppended is a log parse operation binding the contract event 0x09c34e221d98baf118b29dd617ee2ec908628e96d1eb789bf5eeb370c19eb269.
//
// Solidity: event BatchAppended(bytes32 indexed streamId, uint64 indexed batchNo, bytes32 commitment, uint64 firstSequence, uint64 lastSequence, uint64 recordCount)
func (_BatchRegistry *BatchRegistryFilterer) ParseBatchAppended(log types.Log) (*BatchRegistryBatchAppended, error) {
	event := new(BatchRegistryBatchAppended)
	if err := _BatchRegistry.contract.UnpackLog(event, "BatchAppended", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// BatchRegistryPublisherChangedIterator is returned from FilterPublisherChanged and is used to iterate over the raw logs and unpacked data for PublisherChanged events raised by the BatchRegistry contract.
type BatchRegistryPublisherChangedIterator struct {
	Event *BatchRegistryPublisherChanged // Event containing the contract specifics and raw log

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
func (it *BatchRegistryPublisherChangedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(BatchRegistryPublisherChanged)
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
		it.Event = new(BatchRegistryPublisherChanged)
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
func (it *BatchRegistryPublisherChangedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *BatchRegistryPublisherChangedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// BatchRegistryPublisherChanged represents a PublisherChanged event raised by the BatchRegistry contract.
type BatchRegistryPublisherChanged struct {
	StreamId     [32]byte
	OldPublisher common.Address
	NewPublisher common.Address
	Raw          types.Log // Blockchain specific contextual infos
}

// FilterPublisherChanged is a free log retrieval operation binding the contract event 0xca9dd10051facb87697c20b213fddc1424581d0699217a5a2f48f8ad55fc27bd.
//
// Solidity: event PublisherChanged(bytes32 indexed streamId, address indexed oldPublisher, address indexed newPublisher)
func (_BatchRegistry *BatchRegistryFilterer) FilterPublisherChanged(opts *bind.FilterOpts, streamId [][32]byte, oldPublisher []common.Address, newPublisher []common.Address) (*BatchRegistryPublisherChangedIterator, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var oldPublisherRule []interface{}
	for _, oldPublisherItem := range oldPublisher {
		oldPublisherRule = append(oldPublisherRule, oldPublisherItem)
	}
	var newPublisherRule []interface{}
	for _, newPublisherItem := range newPublisher {
		newPublisherRule = append(newPublisherRule, newPublisherItem)
	}

	logs, sub, err := _BatchRegistry.contract.FilterLogs(opts, "PublisherChanged", streamIdRule, oldPublisherRule, newPublisherRule)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryPublisherChangedIterator{contract: _BatchRegistry.contract, event: "PublisherChanged", logs: logs, sub: sub}, nil
}

// WatchPublisherChanged is a free log subscription operation binding the contract event 0xca9dd10051facb87697c20b213fddc1424581d0699217a5a2f48f8ad55fc27bd.
//
// Solidity: event PublisherChanged(bytes32 indexed streamId, address indexed oldPublisher, address indexed newPublisher)
func (_BatchRegistry *BatchRegistryFilterer) WatchPublisherChanged(opts *bind.WatchOpts, sink chan<- *BatchRegistryPublisherChanged, streamId [][32]byte, oldPublisher []common.Address, newPublisher []common.Address) (event.Subscription, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var oldPublisherRule []interface{}
	for _, oldPublisherItem := range oldPublisher {
		oldPublisherRule = append(oldPublisherRule, oldPublisherItem)
	}
	var newPublisherRule []interface{}
	for _, newPublisherItem := range newPublisher {
		newPublisherRule = append(newPublisherRule, newPublisherItem)
	}

	logs, sub, err := _BatchRegistry.contract.WatchLogs(opts, "PublisherChanged", streamIdRule, oldPublisherRule, newPublisherRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(BatchRegistryPublisherChanged)
				if err := _BatchRegistry.contract.UnpackLog(event, "PublisherChanged", log); err != nil {
					// If the signature doesn't match, skip this log.
					if errors.Is(err, bind.ErrEventSignatureMismatch) {
						continue
					}
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

// ParsePublisherChanged is a log parse operation binding the contract event 0xca9dd10051facb87697c20b213fddc1424581d0699217a5a2f48f8ad55fc27bd.
//
// Solidity: event PublisherChanged(bytes32 indexed streamId, address indexed oldPublisher, address indexed newPublisher)
func (_BatchRegistry *BatchRegistryFilterer) ParsePublisherChanged(log types.Log) (*BatchRegistryPublisherChanged, error) {
	event := new(BatchRegistryPublisherChanged)
	if err := _BatchRegistry.contract.UnpackLog(event, "PublisherChanged", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// BatchRegistryStreamRegisteredIterator is returned from FilterStreamRegistered and is used to iterate over the raw logs and unpacked data for StreamRegistered events raised by the BatchRegistry contract.
type BatchRegistryStreamRegisteredIterator struct {
	Event *BatchRegistryStreamRegistered // Event containing the contract specifics and raw log

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
func (it *BatchRegistryStreamRegisteredIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(BatchRegistryStreamRegistered)
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
		it.Event = new(BatchRegistryStreamRegistered)
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
func (it *BatchRegistryStreamRegisteredIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *BatchRegistryStreamRegisteredIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// BatchRegistryStreamRegistered represents a StreamRegistered event raised by the BatchRegistry contract.
type BatchRegistryStreamRegistered struct {
	StreamId  [32]byte
	Publisher common.Address
	Raw       types.Log // Blockchain specific contextual infos
}

// FilterStreamRegistered is a free log retrieval operation binding the contract event 0x51786738f3c4e8efc08b082181c54fba7240d7873db9177731b5b559cce7e1e0.
//
// Solidity: event StreamRegistered(bytes32 indexed streamId, address indexed publisher)
func (_BatchRegistry *BatchRegistryFilterer) FilterStreamRegistered(opts *bind.FilterOpts, streamId [][32]byte, publisher []common.Address) (*BatchRegistryStreamRegisteredIterator, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var publisherRule []interface{}
	for _, publisherItem := range publisher {
		publisherRule = append(publisherRule, publisherItem)
	}

	logs, sub, err := _BatchRegistry.contract.FilterLogs(opts, "StreamRegistered", streamIdRule, publisherRule)
	if err != nil {
		return nil, err
	}
	return &BatchRegistryStreamRegisteredIterator{contract: _BatchRegistry.contract, event: "StreamRegistered", logs: logs, sub: sub}, nil
}

// WatchStreamRegistered is a free log subscription operation binding the contract event 0x51786738f3c4e8efc08b082181c54fba7240d7873db9177731b5b559cce7e1e0.
//
// Solidity: event StreamRegistered(bytes32 indexed streamId, address indexed publisher)
func (_BatchRegistry *BatchRegistryFilterer) WatchStreamRegistered(opts *bind.WatchOpts, sink chan<- *BatchRegistryStreamRegistered, streamId [][32]byte, publisher []common.Address) (event.Subscription, error) {

	var streamIdRule []interface{}
	for _, streamIdItem := range streamId {
		streamIdRule = append(streamIdRule, streamIdItem)
	}
	var publisherRule []interface{}
	for _, publisherItem := range publisher {
		publisherRule = append(publisherRule, publisherItem)
	}

	logs, sub, err := _BatchRegistry.contract.WatchLogs(opts, "StreamRegistered", streamIdRule, publisherRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(BatchRegistryStreamRegistered)
				if err := _BatchRegistry.contract.UnpackLog(event, "StreamRegistered", log); err != nil {
					// If the signature doesn't match, skip this log.
					if errors.Is(err, bind.ErrEventSignatureMismatch) {
						continue
					}
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

// ParseStreamRegistered is a log parse operation binding the contract event 0x51786738f3c4e8efc08b082181c54fba7240d7873db9177731b5b559cce7e1e0.
//
// Solidity: event StreamRegistered(bytes32 indexed streamId, address indexed publisher)
func (_BatchRegistry *BatchRegistryFilterer) ParseStreamRegistered(log types.Log) (*BatchRegistryStreamRegistered, error) {
	event := new(BatchRegistryStreamRegistered)
	if err := _BatchRegistry.contract.UnpackLog(event, "StreamRegistered", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
