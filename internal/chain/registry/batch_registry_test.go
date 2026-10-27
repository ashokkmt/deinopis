package registry

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
)

func TestGeneratedBindingDeployAppendAndReadBack(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	chainID := big.NewInt(1337)
	owner, err := bind.NewKeyedTransactorWithChainID(key, chainID)
	if err != nil {
		t.Fatal(err)
	}
	backend := simulated.NewBackend(types.GenesisAlloc{
		crypto.PubkeyToAddress(key.PublicKey): {Balance: new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))},
	})
	defer backend.Close()

	client := backend.Client()
	address, tx, contract, err := DeployBatchRegistry(owner, client)
	if err != nil {
		t.Fatal(err)
	}
	backend.Commit()
	requireSuccessfulReceipt(t, client, tx)
	if address == (common.Address{}) {
		t.Fatal("deployment returned zero address")
	}

	stream := common.Hash{31: 1}
	publisher := crypto.PubkeyToAddress(key.PublicKey)
	tx, err = contract.RegisterStream(owner, stream, publisher)
	if err != nil {
		t.Fatal(err)
	}
	backend.Commit()
	requireSuccessfulReceipt(t, client, tx)

	commitment := common.Hash{31: 2}
	tx, err = contract.AppendBatch(owner, stream, 0, [32]byte{}, commitment, 1, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	backend.Commit()
	requireSuccessfulReceipt(t, client, tx)

	stored, err := contract.GetBatch(&bind.CallOpts{Context: context.Background()}, stream, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Commitment != commitment || stored.FirstSequence != 1 || stored.LastSequence != 2 || stored.RecordCount != 2 {
		t.Fatalf("stored batch = %+v", stored)
	}
	if count, err := contract.BatchCount(nil, stream); err != nil || count != 1 {
		t.Fatalf("batch count = %d, err=%v", count, err)
	}
	if head, err := contract.HeadCommitment(nil, stream); err != nil || head != commitment {
		t.Fatalf("head = %x, err=%v", head, err)
	}
}

func requireSuccessfulReceipt(t *testing.T, backend simulated.Client, tx *types.Transaction) {
	t.Helper()
	receipt, err := backend.TransactionReceipt(context.Background(), tx.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("transaction %s status = %d", tx.Hash(), receipt.Status)
	}
}
