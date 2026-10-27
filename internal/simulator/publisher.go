package simulator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"deinopis/internal/integrity"
	"deinopis/internal/rawstore"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type LocalAnchor struct {
	BatchNo     uint64 `json:"batch_no"`
	Transaction string `json:"transaction_hash"`
	Block       uint64 `json:"local_block"`
	Commitment  string `json:"commitment"`
}

// PublishBatch submits only a validated, sealed synthetic batch to this Runner's Anvil registry.
func (r *Runner) PublishBatch(ctx context.Context, batch rawstore.SealedBatch) (LocalAnchor, error) {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	if r.client == nil || r.key == nil || r.contract == nil {
		return LocalAnchor{}, errors.New("simulator registry is not running")
	}
	parsed, err := integrity.ParseBatchManifest(batch.Manifest)
	if err != nil || hex.EncodeToString(parsed.StreamID[:]) != batch.StreamID || parsed.BatchNo != batch.BatchNo || parsed.FirstSequence != batch.FirstSequence || parsed.LastSequence != batch.LastSequence || parsed.Count != batch.EntryCount || parsed.Commitment != batch.Commitment {
		return LocalAnchor{}, errors.New("sealed batch manifest does not match its registry publication fields")
	}
	stream := common.HexToHash("0x" + batch.StreamID)
	if !r.streams[parsed.StreamID] {
		auth, err := bind.NewKeyedTransactorWithChainID(r.key, r.ChainID)
		if err != nil {
			return LocalAnchor{}, err
		}
		auth.Context = ctx
		tx, err := r.contract.RegisterStream(auth, stream, auth.From)
		if err != nil {
			return LocalAnchor{}, fmt.Errorf("register local demo stream: %w", err)
		}
		if _, err = successfulReceipt(ctx, r.client, tx); err != nil {
			return LocalAnchor{}, err
		}
		r.streams[parsed.StreamID] = true
	}
	auth, err := bind.NewKeyedTransactorWithChainID(r.key, r.ChainID)
	if err != nil {
		return LocalAnchor{}, err
	}
	auth.Context = ctx
	tx, err := r.contract.AppendBatch(auth, stream, batch.BatchNo, parsed.Previous, parsed.Commitment, batch.FirstSequence, batch.LastSequence, batch.EntryCount)
	if err != nil {
		return LocalAnchor{}, fmt.Errorf("append batch to local registry: %w", err)
	}
	receipt, err := successfulReceipt(ctx, r.client, tx)
	if err != nil {
		return LocalAnchor{}, err
	}
	return LocalAnchor{BatchNo: batch.BatchNo, Transaction: tx.Hash().Hex(), Block: receipt.BlockNumber.Uint64(), Commitment: hex.EncodeToString(parsed.Commitment[:])}, nil
}

func (r *Runner) ReadHistory(ctx context.Context, streamID string) ([]registryEntry, error) {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	streamBytes, err := hex.DecodeString(streamID)
	if err != nil || len(streamBytes) != 32 {
		return nil, errors.New("invalid simulator stream ID")
	}
	var parsedStream [32]byte
	copy(parsedStream[:], streamBytes)
	count, err := r.contract.BatchCount(&bind.CallOpts{Context: ctx}, parsedStream)
	if err != nil {
		return nil, fmt.Errorf("read local registry batch count: %w", err)
	}
	entries := make([]registryEntry, 0, count)
	for n := uint64(0); n < count; n++ {
		batch, err := r.contract.GetBatch(&bind.CallOpts{Context: ctx}, parsedStream, n)
		if err != nil {
			return nil, fmt.Errorf("read local registry batch %d: %w", n, err)
		}
		entries = append(entries, registryEntry{BatchNo: n, Commitment: batch.Commitment, First: batch.FirstSequence, Last: batch.LastSequence, Count: batch.RecordCount})
	}
	head, err := r.contract.HeadCommitment(&bind.CallOpts{Context: ctx}, parsedStream)
	if err != nil {
		return nil, fmt.Errorf("read local registry head: %w", err)
	}
	if count == 0 && head != ([32]byte{}) || count > 0 && head != entries[len(entries)-1].Commitment {
		return nil, errors.New("local registry head does not match its enumerated history")
	}
	return entries, nil
}

type registryEntry struct {
	BatchNo            uint64
	Commitment         [32]byte
	First, Last, Count uint64
}

func successfulReceipt(ctx context.Context, client *ethclient.Client, tx *types.Transaction) (*types.Receipt, error) {
	receipt, err := bind.WaitMined(ctx, client, tx)
	if err != nil {
		return nil, fmt.Errorf("wait for local registry receipt: %w", err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, errors.New("local registry transaction reverted")
	}
	return receipt, nil
}
