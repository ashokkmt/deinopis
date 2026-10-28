package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"deinopis/internal/model"
	"deinopis/internal/rawstore"
	"deinopis/internal/witness"
	"github.com/ethereum/go-ethereum/ethclient"
)

func TestValidateLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{"https://127.0.0.1:8545", "http://192.0.2.1:8545", "http://user@127.0.0.1:8545", "http://127.0.0.1:8545/fork"} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("accepted unsafe simulator endpoint %q", endpoint)
		}
	}
	if err := validateLoopbackEndpoint("http://127.0.0.1:8545"); err != nil {
		t.Fatalf("rejected loopback endpoint: %v", err)
	}
}

func TestLiveIngestionSimulationAnchorsAndVerifiesBorrowedStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dbPath := filepath.Join(t.TempDir(), "collector.db")
	store, err := rawstore.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	demo, err := NewLiveDemo(ctx, store, dbPath, 10)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer demo.Close()
	run, err := demo.Start(ctx)
	if err != nil || run.InputMode != "LIVE_INGESTION" || run.Records != 0 {
		t.Fatalf("live run summary=%+v err=%v", run, err)
	}
	if _, err = demo.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Store(ctx, model.IngestedRecord{Transport: "http", SourceIP: "127.0.0.1", RawBytes: []byte("synthetic-live-00"), ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	pending, err := demo.Verify(ctx, run.RunID)
	if err != nil || pending.Content != "INCOMPLETE" || pending.PendingRecords != 1 {
		t.Fatalf("open batch classification=%+v err=%v", pending, err)
	}
	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()
	uploadDone := make(chan error, 1)
	go func() { uploadDone <- store.RunVaultBatchUploader(uploadCtx, demo, 10*time.Millisecond) }()
	for i := 1; i < 20; i++ {
		if _, err = store.Store(ctx, model.IngestedRecord{Transport: "http", SourceIP: "127.0.0.1", RawBytes: []byte(fmt.Sprintf("synthetic-live-%02d", i)), ReceivedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		run, err = demo.Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(run.Batches) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(run.Batches) != 2 || run.Records != 20 {
		t.Fatalf("uploaded live summary=%+v", run)
	}
	result, err := demo.Verify(ctx, run.RunID)
	if err != nil || result.Content != "INTACT" || result.RecordsChecked != 20 || result.PendingRecords != 0 {
		t.Fatalf("intact live verify=%+v err=%v", result, err)
	}
	cancelUpload()
	if err = <-uploadDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = demo.TamperDelete(ctx, run.RunID); err == nil {
		t.Fatal("live demo exposed its destructive synthetic delete control")
	}
	if _, err = store.DB().ExecContext(ctx, "UPDATE raw_events SET payload=? WHERE raw_id=(SELECT raw_id FROM vault_batch_members ORDER BY accept_seq LIMIT 1)", []byte("attacker changed sealed record")); err != nil {
		t.Fatal(err)
	}
	result, err = demo.Verify(ctx, run.RunID)
	if err != nil || result.Content != "TAMPERED" || len(result.Issues) == 0 {
		t.Fatalf("tampered live verify=%+v err=%v", result, err)
	}
	demo.Close()
	if _, err = store.VaultBatchState(ctx); err != nil {
		t.Fatalf("live demo closed its borrowed collector store: %v", err)
	}
	_ = store.Close()
}

func TestStartDeploysRegistryAndStopsOwnedAnvil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	runner, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, chainID, contract, codeHash, pid, runDir := runner.RPCURL, new(big.Int).Set(runner.ChainID), runner.Registry, runner.RegistryHash, runner.PID, runner.RunDir
	if pid <= 0 || contract == ([20]byte{}) || codeHash == ([32]byte{}) {
		t.Fatalf("invalid local deployment metadata: pid=%d address=%s codeHash=%s", pid, contract, codeHash)
	}
	info, err := os.Stat(runDir)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("run directory mode: info=%v err=%v", info, err)
	}
	var record struct {
		Mode         string `json:"mode"`
		ChainID      string `json:"chain_id"`
		Registry     string `json:"registry_address"`
		RegistryHash string `json:"registry_code_hash"`
		ProcessID    int    `json:"anvil_process_id"`
	}
	body, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		t.Fatalf("read simulator run record: %v", err)
	}
	if err = json.Unmarshal(body, &record); err != nil {
		t.Fatalf("decode simulator run record: %v", err)
	}
	if record.Mode != "SIMULATED" || record.ChainID != chainID.String() || record.Registry != contract.Hex() || record.RegistryHash != codeHash.Hex() || record.ProcessID != pid {
		t.Fatalf("run record does not match deployment: %+v", record)
	}
	client, err := ethclient.Dial(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	gotChain, err := client.ChainID(ctx)
	client.Close()
	if err != nil || gotChain.Cmp(chainID) != 0 {
		t.Fatalf("local RPC chain ID = %v, err=%v, want %s", gotChain, err, chainID)
	}
	runner.Stop()
	if _, err = os.Stat(filepath.Join(runDir, "run.json")); err != nil {
		t.Fatalf("Stop removed retained run artifacts: %v", err)
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer checkCancel()
	if client, err = ethclient.DialContext(checkCtx, endpoint); err == nil {
		_, err = client.ChainID(checkCtx)
		client.Close()
		if err == nil {
			t.Fatal("owned Anvil process remained available after Stop")
		}
	}
	// This run directory was created by the simulator test and contains synthetic metadata only.
	if err = os.RemoveAll(runDir); err != nil {
		t.Fatalf("remove test-owned run directory: %v", err)
	}
	second, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondDir, secondChainID := second.RunDir, new(big.Int).Set(second.ChainID)
	second.Stop()
	if secondDir == runDir || secondChainID.Cmp(chainID) == 0 {
		t.Fatalf("repeat run reused state identity: dir=%q chain_id=%s", secondDir, secondChainID)
	}
	if err = os.RemoveAll(secondDir); err != nil {
		t.Fatalf("remove second test-owned run directory: %v", err)
	}
}

func TestSyntheticDemoVerifiesAndLocalizesDeletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	demo := NewDemo()
	defer demo.Close()
	run, err := demo.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if run.Mode != "SIMULATED" || run.Records != 13 || len(run.Batches) != 4 {
		t.Fatalf("unexpected demo summary: %+v", run)
	}
	countBatches, ageBatches := 0, 0
	for _, batch := range run.Batches {
		if batch.CloseReason == "count" {
			countBatches++
		}
		if batch.CloseReason == "age" {
			ageBatches++
		}
	}
	if countBatches != 3 || ageBatches != 1 {
		t.Fatalf("count/age batches=%d/%d", countBatches, ageBatches)
	}
	clean, err := demo.Verify(ctx, run.RunID)
	if err != nil || clean.Content != "INTACT" || clean.RecordsChecked != 13 || clean.Coverage != "complete" {
		t.Fatalf("clean verify=%+v err=%v", clean, err)
	}
	if err = demo.TamperDelete(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	changed, err := demo.Verify(ctx, run.RunID)
	if err != nil || changed.Content != "TAMPERED" {
		t.Fatalf("tampered verify=%+v err=%v", changed, err)
	}
	if changed.FirstAdverseAt == nil || changed.Observations != 2 {
		t.Fatalf("first adverse observation was not persisted: %+v", changed)
	}
	found := false
	for _, issue := range changed.Issues {
		if issue.Sequence == 6 && issue.Reason == "accepted raw parent or membership is missing" {
			found = true
		}
	}
	if !found {
		t.Fatalf("deletion was not localized to sequence 6: %+v", changed.Issues)
	}
	if err = demo.TamperDeleteTail(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	tail, err := demo.Verify(ctx, run.RunID)
	if err != nil || tail.Content != "TAMPERED" {
		t.Fatalf("tail deletion verify=%+v err=%v", tail, err)
	}
	if tail.FirstAdverseAt == nil || !tail.FirstAdverseAt.Equal(*changed.FirstAdverseAt) {
		t.Fatalf("later failure moved first adverse observation: first=%v tail=%v", changed.FirstAdverseAt, tail.FirstAdverseAt)
	}
	demoRun, err := demo.getRun(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = demoRun.witness.Close(); err != nil {
		t.Fatal(err)
	}
	demoRun.witness, err = witness.NewStore(demoRun.summary.WitnessPath, filepath.Join(demoRun.root, "witness-manifests"))
	if err != nil {
		t.Fatalf("reopen run-scoped witness: %v", err)
	}
	reopened, err := demo.Verify(ctx, run.RunID)
	if err != nil || reopened.FirstAdverseAt == nil || !reopened.FirstAdverseAt.Equal(*changed.FirstAdverseAt) {
		t.Fatalf("witness restart moved first adverse observation: result=%+v err=%v", reopened, err)
	}
	found = false
	for _, issue := range tail.Issues {
		if issue.Sequence == 13 && issue.Reason == "accepted raw parent or membership is missing" {
			found = true
		}
	}
	if !found {
		t.Fatalf("registry-backed tail deletion was not localized to sequence 13: %+v", tail.Issues)
	}
	evidence, err := demo.Evidence(run.RunID)
	if err != nil || !json.Valid(evidence) || !bytes.Contains(evidence, []byte(`"first_adverse_at_utc"`)) {
		t.Fatalf("completed synthetic evidence export unavailable: err=%v", err)
	}
}

func TestSyntheticDemoSurvivesStartRequestContextCancellation(t *testing.T) {
	startCtx, cancelStart := context.WithTimeout(context.Background(), 45*time.Second)
	demo := NewDemo()
	defer demo.Close()
	run, err := demo.Start(startCtx)
	cancelStart()
	if err != nil {
		t.Fatal(err)
	}

	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelVerify()
	result, err := demo.Verify(verifyCtx, run.RunID)
	if err != nil || result.Content != "INTACT" || result.Coverage != "complete" {
		t.Fatalf("demo did not survive start request cancellation: result=%+v err=%v", result, err)
	}
}
