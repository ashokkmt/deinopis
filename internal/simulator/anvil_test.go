package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

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
