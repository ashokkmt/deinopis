package simulator

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/witness"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

func newQualificationDemo(t *testing.T) (context.Context, context.CancelFunc, *Demo, DemoSummary) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	demo := NewDemo()
	run, err := demo.Start(ctx)
	if err != nil {
		cancel()
		demo.Close()
		t.Fatalf("start isolated demo: %v", err)
	}
	t.Cleanup(func() { demo.Close(); cancel() })
	return ctx, cancel, demo, run
}

func verifyHasIssue(t *testing.T, demo *Demo, ctx context.Context, run DemoSummary, sequence uint64, reason string) DemoVerification {
	t.Helper()
	got, err := demo.Verify(ctx, run.RunID)
	if err != nil {
		t.Fatalf("verify run: %v", err)
	}
	for _, issue := range got.Issues {
		if issue.Sequence == sequence && issue.Reason == reason {
			return got
		}
	}
	t.Fatalf("verification did not report sequence %d / %q: %+v", sequence, reason, got.Issues)
	return DemoVerification{}
}

func TestPhase4TamperMatrixAndAppendOnlyRecovery(t *testing.T) {
	ctx, _, demo, summary := newQualificationDemo(t)
	run, err := demo.getRun(summary.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var rawID, originalHash, originalMetadata string
	var originalPayload []byte
	if err = run.store.DB().QueryRowContext(ctx, `SELECT m.raw_id,r.raw_sha256,r.metadata_json,r.payload FROM vault_batch_members m JOIN raw_events r USING(raw_id) WHERE m.stream_id=? AND m.accept_seq=6`, run.streamID).Scan(&rawID, &originalHash, &originalMetadata, &originalPayload); err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name, query, changed string
		restore              string
		reason               string
	}{
		{"payload", `UPDATE raw_events SET payload=? WHERE raw_id=?`, "payload", `UPDATE raw_events SET payload=? WHERE raw_id=?`, "payload or protected metadata differs from the immutable witness entry"},
		{"metadata", `UPDATE raw_events SET metadata_json=? WHERE raw_id=?`, `{"listener":"tampered"}`, `UPDATE raw_events SET metadata_json=? WHERE raw_id=?`, "payload or protected metadata differs from the immutable witness entry"},
		{"stored hash", `UPDATE raw_events SET raw_sha256=? WHERE raw_id=?`, "00", `UPDATE raw_events SET raw_sha256=? WHERE raw_id=?`, "payload or protected metadata differs from the immutable witness entry"},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			value := any(mutation.changed)
			if mutation.name == "payload" {
				value = append(append([]byte(nil), originalPayload...), 0)
			}
			if _, err := run.store.DB().ExecContext(ctx, mutation.query, value, rawID); err != nil {
				t.Fatal(err)
			}
			verifyHasIssue(t, demo, ctx, summary, 6, mutation.reason)
			var restore any = originalHash
			switch mutation.name {
			case "payload":
				restore = originalPayload
			case "metadata":
				restore = originalMetadata
			}
			if _, err := run.store.DB().ExecContext(ctx, mutation.restore, restore, rawID); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err = run.store.DB().ExecContext(ctx, `UPDATE vault_batch_members SET batch_no=2 WHERE stream_id=? AND accept_seq=6`, run.streamID); err != nil {
		t.Fatal(err)
	}
	verifyHasIssue(t, demo, ctx, summary, 6, "record moved to another batch")
	if _, err = run.store.DB().ExecContext(ctx, `UPDATE vault_batch_members SET batch_no=1 WHERE stream_id=? AND accept_seq=6`, run.streamID); err != nil {
		t.Fatal(err)
	}
	if err = demo.TamperDelete(ctx, summary.RunID); err != nil {
		t.Fatal(err)
	}
	if err = run.appendRecovery(ctx, 6); err != nil {
		t.Fatalf("append recovery history: %v", err)
	}
	result := verifyHasIssue(t, demo, ctx, summary, 6, "accepted raw parent or membership is missing")
	if result.Content != "TAMPERED" || result.RecordsChecked != 13 || len(run.summary.Batches) != 5 {
		t.Fatalf("recovery rewrote first observation or failed to append a new batch: %+v", result)
	}
	for _, issue := range result.Issues {
		if issue.Sequence == 14 {
			t.Fatalf("new recovery sequence unexpectedly has an integrity issue: %+v", issue)
		}
	}
	if _, err = run.store.DB().ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	tx, err := run.store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT raw_id FROM vault_batch_members WHERE stream_id=? AND batch_no=2`, run.streamID)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	var batchIDs []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			tx.Rollback()
			t.Fatal(err)
		}
		batchIDs = append(batchIDs, id)
	}
	if err = rows.Close(); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if len(batchIDs) != 4 {
		tx.Rollback()
		t.Fatalf("batch 2 contained %d members, want 4", len(batchIDs))
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM vault_batch_members WHERE stream_id=? AND batch_no=2`, run.streamID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	for _, id := range batchIDs {
		if _, err = tx.ExecContext(ctx, `DELETE FROM raw_events WHERE raw_id=?`, id); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = run.store.DB().ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	verifyHasIssue(t, demo, ctx, summary, 9, "accepted raw parent or membership is missing")
	if err = demo.TamperDeleteTail(ctx, summary.RunID); err != nil {
		t.Fatal(err)
	}
	verifyHasIssue(t, demo, ctx, summary, 14, "accepted raw parent or membership is missing")
	evidence, err := os.ReadFile(summary.EvidencePath)
	if err != nil || !strings.Contains(string(evidence), `"mode": "SIMULATED"`) || !strings.Contains(string(evidence), `"artifact_sha256"`) {
		t.Fatalf("reproducible simulator evidence report unavailable or mislabeled: err=%v", err)
	}
}

func TestPhase4WitnessRestartConflictAndOutage(t *testing.T) {
	ctx, _, demo, summary := newQualificationDemo(t)
	run, err := demo.getRun(summary.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = run.witness.Close(); err != nil {
		t.Fatal(err)
	}
	run.witness, err = witness.NewStore(summary.WitnessPath, run.root+"/witness-manifests")
	if err != nil {
		t.Fatal(err)
	}
	clean, err := demo.Verify(ctx, summary.RunID)
	if err != nil || clean.Content != "INTACT" {
		t.Fatalf("witness restart lost first-write originals: result=%+v err=%v", clean, err)
	}

	stored, err := run.witness.ReadBatchManifests(ctx, run.streamID)
	if err != nil {
		t.Fatal(err)
	}
	conflict := append([]byte(nil), stored[0].Manifest...)
	parsed, err := integrity.ParseBatchManifest(conflict)
	if err != nil {
		t.Fatal(err)
	}
	rootOffset := len("DEINOPIS-BATCH-MANIFEST-V2") + 2 + 32 + 6*8 + 32
	entriesOffset := rootOffset + 32
	acc := &integrity.MerkleAccumulator{}
	for i := uint64(0); i < parsed.Count; i++ {
		size := binary.BigEndian.Uint32(conflict[entriesOffset : entriesOffset+4])
		entriesOffset += 4
		entry := conflict[entriesOffset : entriesOffset+int(size)]
		if i == 0 {
			entry[len(entry)-1] ^= 1
		}
		if err = acc.AddEntry(entry); err != nil {
			t.Fatal(err)
		}
		entriesOffset += int(size)
	}
	root := acc.Root()
	copy(conflict[rootOffset:rootOffset+32], root[:])
	digest := integrity.ManifestDigest(conflict)
	_, err = run.witness.AcceptBatch(ctx, run.streamID, stored[0].BatchNo, fmt.Sprintf("%x", digest), run.fingerprint, run.token, conflict, time.Now().UTC().Add(time.Second))
	if !errors.Is(err, witness.ErrConflict) {
		t.Fatalf("conflicting immutable-manifest retry was not refused as conflict: %v", err)
	}
	unchanged, err := run.witness.ReadBatchManifests(ctx, run.streamID)
	if err != nil || unchanged[0].Digest != stored[0].Digest {
		t.Fatalf("conflict changed the first-write original: err=%v", err)
	}

	if err = run.witness.Close(); err != nil {
		t.Fatal(err)
	}
	outage, err := demo.Verify(ctx, summary.RunID)
	if err != nil || outage.Content != "UNKNOWN" || outage.Witness != "unavailable" {
		t.Fatalf("witness outage was not classified as unknown: result=%+v err=%v", outage, err)
	}
}

func TestPhase5SimulatedAnchorIdentityAndRollback(t *testing.T) {
	ctx, _, demo, summary := newQualificationDemo(t)
	run, err := demo.getRun(summary.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(summary.AnvilRunDir, filepath.Dir(summary.EvidencePath)+string(os.PathSeparator)) {
		t.Fatalf("Anvil state escaped its disposable demo root: run=%q evidence=%q", summary.AnvilRunDir, summary.EvidencePath)
	}

	originalHash := run.summary.RegistryCodeHash
	run.summary.RegistryCodeHash = "0xdeadbeef"
	wrongIdentity, err := demo.Verify(ctx, summary.RunID)
	if err != nil || wrongIdentity.Content != "UNKNOWN" || wrongIdentity.Anchor != "identity mismatch" {
		t.Fatalf("wrong local registry code hash was not rejected: result=%+v err=%v", wrongIdentity, err)
	}
	run.summary.RegistryCodeHash = originalHash
	originalChainID := run.summary.ChainID
	run.summary.ChainID = "1"
	wrongChain, err := demo.Verify(ctx, summary.RunID)
	if err != nil || wrongChain.Content != "UNKNOWN" || wrongChain.Anchor != "identity mismatch" {
		t.Fatalf("wrong local chain ID was not rejected: result=%+v err=%v", wrongChain, err)
	}
	run.summary.ChainID = originalChainID

	reverted, err := run.runner.Revert(ctx, run.chainSnapshot)
	if err != nil || !reverted {
		t.Fatalf("revert local chain snapshot: reverted=%v err=%v", reverted, err)
	}
	afterRollback, err := demo.Verify(ctx, summary.RunID)
	if err != nil || afterRollback.Content != "UNKNOWN" || afterRollback.Anchor != "local registry readback" {
		t.Fatalf("simulated rollback did not surface missing anchor history: result=%+v err=%v", afterRollback, err)
	}
	if len(afterRollback.Issues) == 0 {
		t.Fatal("simulated rollback produced no reconciliation evidence")
	}
	run.runner.Stop()
	afterDisconnect, err := demo.Verify(ctx, summary.RunID)
	if err != nil || afterDisconnect.Content != "UNKNOWN" || afterDisconnect.Anchor != "identity mismatch" {
		t.Fatalf("local RPC disconnect was not classified as unknown: result=%+v err=%v", afterDisconnect, err)
	}
}

func TestPhase4PublisherTimeoutAfterSendAndInterruption(t *testing.T) {
	ctx, _, demo, summary := newQualificationDemo(t)
	run, err := demo.getRun(summary.RunID)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := run.store.SealedBatch(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runner.Stop() })
	client, err := rpc.DialContext(ctx, runner.RPCURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := integrity.ParseBatchManifest(batch.Manifest)
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	stream := common.BytesToHash(parsed.StreamID[:])
	auth, err := bind.NewKeyedTransactorWithChainID(runner.key, runner.ChainID)
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	auth.Context = ctx
	registration, err := runner.contract.RegisterStream(auth, stream, auth.From)
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	if _, err = successfulReceipt(ctx, runner.client, registration); err != nil {
		client.Close()
		t.Fatal(err)
	}
	runner.streams[parsed.StreamID] = true
	var ignored any
	if err = client.CallContext(ctx, &ignored, "anvil_setAutomine", false); err != nil {
		client.Close()
		t.Fatalf("disable local automining: %v", err)
	}
	client.Close()

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second)
	_, sendErr := runner.PublishBatch(timeoutCtx, batch)
	timeoutCancel()
	if sendErr == nil {
		t.Fatal("publisher returned before the deliberately withheld local receipt")
	}
	client, err = rpc.DialContext(ctx, runner.RPCURL)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.CallContext(ctx, &ignored, "anvil_setAutomine", true); err != nil {
		client.Close()
		t.Fatalf("enable local automining: %v", err)
	}
	var mined any
	if err = client.CallContext(ctx, &mined, "anvil_mine", "0x1"); err != nil {
		client.Close()
		t.Fatalf("mine accepted local registration: %v", err)
	}
	client.Close()

	anchor, err := runner.PublishBatch(ctx, batch)
	if err != nil {
		t.Fatalf("retry after local timeout did not reconcile prior append: %v", err)
	}
	history, err := runner.ReadHistory(ctx, batch.StreamID)
	if err != nil || len(history) != 1 || history[0].Commitment != parsed.Commitment || anchor.BatchNo != 0 {
		t.Fatalf("reconciled local publication history: len=%d err=%v", len(history), err)
	}
	revertAuth, err := bind.NewKeyedTransactorWithChainID(runner.key, runner.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	revertAuth.Context = ctx
	revertAuth.GasLimit = 500_000
	streamID, _ := hex.DecodeString(batch.StreamID)
	var streamKey [32]byte
	copy(streamKey[:], streamID)
	revertedTx, err := runner.contract.AppendBatch(revertAuth, streamKey, 0, parsed.Previous, parsed.Commitment, batch.FirstSequence, batch.LastSequence, batch.EntryCount)
	if err != nil {
		t.Fatalf("submit deliberate local duplicate for receipt-revert drill: %v", err)
	}
	if _, err = successfulReceipt(ctx, runner.client, revertedTx); err == nil || !strings.Contains(err.Error(), "reverted") {
		t.Fatalf("reverted local receipt was not surfaced: %v", err)
	}
	afterRevert, err := runner.ReadHistory(ctx, batch.StreamID)
	if err != nil || len(afterRevert) != 1 || afterRevert[0].Commitment != parsed.Commitment {
		t.Fatalf("reverted duplicate changed local history: history=%+v err=%v", afterRevert, err)
	}
	if _, err = runner.PublishBatch(ctx, batch); err == nil {
		t.Fatal("duplicate local batch publication did not fail")
	}
	runner.Stop()
	if _, err = runner.PublishBatch(ctx, batch); err == nil {
		t.Fatal("interrupted local publisher accepted a batch")
	}
}
