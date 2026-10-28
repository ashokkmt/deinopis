package rawstore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/model"
)

type retryBatchUploader struct {
	calls []uint64
	first error
}

func (u *retryBatchUploader) UploadBatch(_ context.Context, _ string, batchNo uint64, _ integrity.Digest, _ []byte) (string, error) {
	u.calls = append(u.calls, batchNo)
	if len(u.calls) == 1 && u.first != nil {
		return "", u.first
	}
	return fmt.Sprintf("receipt-%d", batchNo), nil
}

type permanentBatchUploader struct{ calls int }
type permanentBatchFailure struct{}

func (permanentBatchFailure) Error() string   { return "rejected" }
func (permanentBatchFailure) Permanent() bool { return true }
func (u *permanentBatchUploader) UploadBatch(context.Context, string, uint64, integrity.Digest, []byte) (string, error) {
	u.calls++
	return "", permanentBatchFailure{}
}

func setBatchLimits(t *testing.T, store *RawStore, target, soft, hardCount, hardBytes int64) {
	t.Helper()
	if _, err := store.DB().Exec(`UPDATE vault_batch_streams SET target_count=?,soft_manifest_bytes=?,hard_count=?,hard_manifest_bytes=? WHERE id=1`, target, soft, hardCount, hardBytes); err != nil {
		t.Fatal(err)
	}
}

func acceptTestRecord(t *testing.T, store *RawStore, payload string) *model.RawEvent {
	t.Helper()
	event, err := store.Store(context.Background(), model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte(payload), ReceivedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestRawAcceptanceAndV2MembershipAreAtomicAndOrdered(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	first := acceptTestRecord(t, store, "first")
	second := acceptTestRecord(t, store, "second")
	var firstSeq, secondSeq uint64
	if err := store.DB().QueryRowContext(ctx, "SELECT accept_seq FROM vault_batch_members WHERE raw_id=?", first.RawID).Scan(&firstSeq); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRowContext(ctx, "SELECT accept_seq FROM vault_batch_members WHERE raw_id=?", second.RawID).Scan(&secondSeq); err != nil {
		t.Fatal(err)
	}
	if firstSeq != 1 || secondSeq != 2 {
		t.Fatalf("sequences = %d,%d", firstSeq, secondSeq)
	}
	var memberCount int
	var encodedBytes int64
	if err := store.DB().QueryRowContext(ctx, "SELECT member_count,encoded_bytes FROM vault_batch_state WHERE batch_no=0").Scan(&memberCount, &encodedBytes); err != nil {
		t.Fatal(err)
	}
	if memberCount != 2 || encodedBytes <= 0 {
		t.Fatalf("batch counters = %d members, %d encoded bytes", memberCount, encodedBytes)
	}
	state, err := store.VaultBatchState(ctx)
	if err != nil || state.NextSequence != 3 {
		t.Fatalf("batch state = %+v, err=%v", state, err)
	}
	var orphaned int
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM raw_events r LEFT JOIN vault_batch_members m USING(raw_id) WHERE m.raw_id IS NULL`).Scan(&orphaned); err != nil {
		t.Fatal(err)
	}
	if orphaned != 0 {
		t.Fatalf("accepted raw parents without v2 membership: %d", orphaned)
	}
}

func TestConcurrentRawAcceptanceAndSealingPreserveSequence(t *testing.T) {
	store := setupTestStore(t)
	setBatchLimits(t, store, 16, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	const count = 64
	errs := make(chan error, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			_, err := store.Store(context.Background(), model.IngestedRecord{
				Transport: "test", SourceIP: "local", RawBytes: []byte(fmt.Sprintf("concurrent-%d", i)), ReceivedAt: time.Now().UTC(),
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent raw acceptance and sealing: %v", err)
	}
	var members, distinct, minSeq, maxSeq int
	if err := store.DB().QueryRow(`SELECT COUNT(*),COUNT(DISTINCT accept_seq),MIN(accept_seq),MAX(accept_seq) FROM vault_batch_members`).Scan(&members, &distinct, &minSeq, &maxSeq); err != nil {
		t.Fatal(err)
	}
	if members != count || distinct != count || minSeq != 1 || maxSeq != count {
		t.Fatalf("concurrent acceptance membership: count=%d distinct=%d range=%d..%d", members, distinct, minSeq, maxSeq)
	}
	for batchNo := uint64(0); batchNo < count/16; batchNo++ {
		batch, err := store.SealedBatch(context.Background(), batchNo)
		if err != nil {
			t.Fatal(err)
		}
		first := batchNo*16 + 1
		if batch.EntryCount != 16 || batch.FirstSequence != first || batch.LastSequence != first+15 {
			t.Fatalf("batch %d sequence range/count: %d..%d/%d", batchNo, batch.FirstSequence, batch.LastSequence, batch.EntryCount)
		}
	}
}

func TestVaultBatchSealTaskResumesAfterFailureAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seal-restart.db")
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	setBatchLimits(t, store, 1, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	if _, err = store.DB().Exec(`CREATE TRIGGER fail_vault_manifest BEFORE INSERT ON vault_batch_manifests BEGIN SELECT RAISE(ABORT,'injected seal failure'); END`); err != nil {
		t.Fatal(err)
	}
	accepted, err := store.Store(context.Background(), model.IngestedRecord{Transport: "test", SourceIP: "local", RawBytes: []byte("durably accepted"), ReceivedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("seal failure must not roll back already accepted raw bytes: %v", err)
	}
	var rawCount, membershipCount, taskCount int
	if err = store.DB().QueryRow(`SELECT COUNT(*) FROM raw_events WHERE raw_id=?`, accepted.RawID).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRow(`SELECT COUNT(*) FROM vault_batch_members WHERE raw_id=?`, accepted.RawID).Scan(&membershipCount); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRow(`SELECT COUNT(*) FROM vault_batch_seal_tasks`).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != 1 || membershipCount != 1 || taskCount != 1 {
		t.Fatalf("after injected seal failure: raw=%d membership=%d durable seal task=%d", rawCount, membershipCount, taskCount)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.DB().Exec(`DROP TRIGGER fail_vault_manifest`); err != nil {
		t.Fatal(err)
	}
	if err = store.SealDueVaultBatches(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	sealed, err := store.SealedBatch(context.Background(), 0)
	if err != nil || sealed.EntryCount != 1 || sealed.FirstSequence != 1 || sealed.LastSequence != 1 {
		t.Fatalf("recovered sealed batch=%+v err=%v", sealed, err)
	}
}

func TestCountBatchClosesNonemptyAndChainsBySequence(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	setBatchLimits(t, store, 2, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	acceptTestRecord(t, store, "one")
	acceptTestRecord(t, store, "two")
	first, err := store.SealedBatch(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := integrity.ParseBatchManifest(first.Manifest)
	if err != nil || parsed.FirstSequence != 1 || parsed.LastSequence != 2 || parsed.Count != 2 || first.CloseReason != "count" {
		t.Fatalf("count batch=%+v parsed=%+v err=%v", first, parsed, err)
	}
	acceptTestRecord(t, store, "three")
	if err = store.SealDueVaultBatches(ctx, time.Now().Add(vaultBatchMaxAge+time.Second)); err != nil {
		t.Fatal(err)
	}
	second, err := store.SealedBatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second.EntryCount != 1 || second.FirstSequence != 3 || second.Previous != first.Commitment || second.CloseReason != "age" {
		t.Fatalf("second batch=%+v", second)
	}
	if err = store.SealDueVaultBatches(ctx, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_batch_manifests").Scan(&count); err != nil || count != 2 {
		t.Fatalf("idle time created batch: count=%d err=%v", count, err)
	}
}

func TestByteLimitClosesBatchAndOversizedParentIsRefusedAtomically(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	setBatchLimits(t, store, vaultBatchTargetCount, vaultBatchHeaderSize+1, vaultBatchHardCount, vaultBatchHardBytes)
	acceptTestRecord(t, store, "small")
	batch, err := store.SealedBatch(ctx, 0)
	if err != nil || batch.CloseReason != "bytes" {
		t.Fatalf("byte-triggered batch=%+v err=%v", batch, err)
	}
	setBatchLimits(t, store, vaultBatchTargetCount, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHeaderSize+1)
	if _, err = store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("too large")}); err == nil {
		t.Fatal("accepted record larger than the hard manifest limit")
	}
	var rawCount int
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_events").Scan(&rawCount); err != nil || rawCount != 1 {
		t.Fatalf("oversized refusal changed raw rows: %d, err=%v", rawCount, err)
	}
}

func TestAgeCloseCreatesNoEmptyBatches(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	if err := store.SealDueVaultBatches(ctx, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	acceptTestRecord(t, store, "age-flush")
	if err := store.SealDueVaultBatches(ctx, time.Now().Add(vaultBatchMaxAge+time.Second)); err != nil {
		t.Fatal(err)
	}
	batch, err := store.SealedBatch(ctx, 0)
	if err != nil || batch.CloseReason != "age" || batch.EntryCount != 1 {
		t.Fatalf("age batch=%+v err=%v", batch, err)
	}
	if err = store.SealDueVaultBatches(ctx, time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_batch_manifests").Scan(&count); err != nil || count != 1 {
		t.Fatalf("empty batch created: count=%d err=%v", count, err)
	}
}

func TestBatchUploadBackoffCannotSkipOldest(t *testing.T) {
	store := setupTestStore(t)
	setBatchLimits(t, store, 1, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	acceptTestRecord(t, store, "one")
	acceptTestRecord(t, store, "two")
	uploader := &retryBatchUploader{first: fmt.Errorf("temporary outage")}
	now := time.Now().UTC()
	worked, err := store.ProcessNextVaultBatchUpload(context.Background(), uploader, now)
	if !worked || err == nil || len(uploader.calls) != 1 || uploader.calls[0] != 0 {
		t.Fatalf("first attempt worked=%v calls=%v err=%v", worked, uploader.calls, err)
	}
	worked, err = store.ProcessNextVaultBatchUpload(context.Background(), uploader, now.Add(500*time.Millisecond))
	if err != nil || worked || len(uploader.calls) != 1 {
		t.Fatalf("backoff skipped oldest batch: worked=%v calls=%v err=%v", worked, uploader.calls, err)
	}
	worked, err = store.ProcessNextVaultBatchUpload(context.Background(), uploader, now.Add(3*time.Second))
	if !worked || err != nil || len(uploader.calls) != 2 || uploader.calls[1] != 0 {
		t.Fatalf("oldest retry worked=%v calls=%v err=%v", worked, uploader.calls, err)
	}
	worked, err = store.ProcessNextVaultBatchUpload(context.Background(), uploader, now.Add(3*time.Second))
	if !worked || err != nil || len(uploader.calls) != 3 || uploader.calls[2] != 1 {
		t.Fatalf("next batch worked=%v calls=%v err=%v", worked, uploader.calls, err)
	}
	state, err := store.VaultBatchState(context.Background())
	if err != nil || state.PendingCount != 0 || state.PendingBytes != 0 {
		t.Fatalf("pending state=%+v err=%v", state, err)
	}
}

func TestPermanentBatchRejectionBlocksLaterBatches(t *testing.T) {
	store := setupTestStore(t)
	setBatchLimits(t, store, 1, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	acceptTestRecord(t, store, "one")
	acceptTestRecord(t, store, "two")
	uploader := &permanentBatchUploader{}
	worked, err := store.ProcessNextVaultBatchUpload(context.Background(), uploader, time.Now().UTC())
	if !worked || err == nil || uploader.calls != 1 {
		t.Fatalf("permanent attempt worked=%v calls=%d err=%v", worked, uploader.calls, err)
	}
	worked, err = store.ProcessNextVaultBatchUpload(context.Background(), uploader, time.Now().Add(time.Hour))
	if err != nil || worked || uploader.calls != 1 {
		t.Fatalf("blocked predecessor was bypassed: worked=%v calls=%d err=%v", worked, uploader.calls, err)
	}
	var state string
	if err = store.DB().QueryRow("SELECT state FROM vault_batch_uploads WHERE batch_no=0").Scan(&state); err != nil || state != "blocked" {
		t.Fatalf("batch upload state=%q err=%v", state, err)
	}
	queue, err := store.VaultBatchState(context.Background())
	if err != nil || queue.BlockedReason == "" {
		t.Fatalf("blocked queue status=%+v err=%v", queue, err)
	}
}

func TestBacklogBudgetRefusesBeforeRawAcceptance(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `UPDATE vault_batch_streams SET pending_upload_count=?,pending_upload_bytes=? WHERE id=1`, maxPendingUploads, maxPendingBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("must-not-be-accepted")}); err == nil {
		t.Fatal("accepted raw input after backlog budget was exhausted")
	}
	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM raw_events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refused input left %d raw rows (err=%v)", count, err)
	}
}

func TestV1EvidenceRemainsUnchangedAcrossV2StreamCutover(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	epoch := time.Now().UTC().Truncate(300 * time.Second)
	stream := integrity.Digest{1, 9}
	streamText := hex.EncodeToString(stream[:])
	if err := store.ConfigureVaultStream(ctx, streamText, epoch); err != nil {
		t.Fatal(err)
	}
	entry := integrity.Entry{StreamID: stream, Slot: 0, AcceptSeq: 1, AcceptedAt: epoch.UnixNano(), RawID: "deinopis-raw-legacy", ReceivedAt: epoch.UnixNano(), Transport: "stdin", SourceIP: "local", Payload: []byte("legacy")}
	manifest := integrity.Manifest{StreamID: stream, Slot: 0, Start: uint64(epoch.Unix()), End: uint64(epoch.Unix() + 300), Coverage: integrity.CoverageInterrupted, Entries: []integrity.Entry{entry}}
	body, err := integrity.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	commitment, err := integrity.WindowCommitment(manifest, body)
	if err != nil {
		t.Fatal(err)
	}
	encodedEntry, err := integrity.EncodeEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	root, err := integrity.MerkleRootEntries([]integrity.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO raw_events(raw_id,raw_sha256,byte_count,transport,source_ip,source_port,metadata_json,received_at,payload,status) VALUES(?,?,?,?,?,?,?,?,?,?)`, entry.RawID, "legacy-hash", len(entry.Payload), entry.Transport, entry.SourceIP, 0, "{}", epoch.Format(time.RFC3339Nano), entry.Payload, string(model.StatusAccepted)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO vault_members(stream_id,accept_seq,raw_id,slot,accepted_at_ns,entry_bytes) VALUES(?,?,?,?,?,?)`, streamText, 1, entry.RawID, 0, epoch.UnixNano(), len(encodedEntry)); err != nil {
		t.Fatal(err)
	}
	manifestDigest := integrity.ManifestDigest(body)
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO vault_windows(stream_id,slot,coverage,entry_count,high_water_sequence,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns) VALUES(?,0,1,1,1,?,?,?,?,?,?)`, streamText, body, manifestDigest[:], root[:], make([]byte, 32), commitment[:], epoch.UnixNano()); err != nil {
		t.Fatal(err)
	}
	newStream := integrity.Digest{2, 8}
	if err = store.ConfigureVaultBatchStream(ctx, hex.EncodeToString(newStream[:])); err != nil {
		t.Fatal(err)
	}
	newEvent := acceptTestRecord(t, store, "new-generation")
	var unchanged []byte
	if err = store.DB().QueryRowContext(ctx, "SELECT manifest FROM vault_windows WHERE slot=0").Scan(&unchanged); err != nil || string(unchanged) != string(body) {
		t.Fatalf("v1 manifest changed during v2 cutover: err=%v", err)
	}
	var legacyMembership, v2Membership int
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_members WHERE raw_id=?", entry.RawID).Scan(&legacyMembership); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_batch_members WHERE raw_id=?", newEvent.RawID).Scan(&v2Membership); err != nil {
		t.Fatal(err)
	}
	if legacyMembership != 1 || v2Membership != 1 {
		t.Fatalf("generation memberships legacy=%d v2=%d", legacyMembership, v2Membership)
	}
}

func TestConfigureVaultBatchTargetCountRequiresEmptyStore(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	if err := store.ConfigureVaultBatchTargetCount(ctx, 10); err != nil {
		t.Fatalf("configure empty store batch target: %v", err)
	}
	var got uint64
	if err := store.DB().QueryRowContext(ctx, "SELECT target_count FROM vault_batch_streams WHERE id=1").Scan(&got); err != nil || got != 10 {
		t.Fatalf("target count=%d err=%v", got, err)
	}
	acceptTestRecord(t, store, "accepted")
	if err := store.ConfigureVaultBatchTargetCount(ctx, 20); err == nil {
		t.Fatal("allowed target count change after acceptance")
	}
	if err := store.ConfigureVaultBatchTargetCount(ctx, 0); err == nil {
		t.Fatal("allowed zero target count")
	}
}

func TestFutureSchemaVersionRefusedWithoutDowngrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA user_version=99"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	store, err := New(path)
	if err == nil {
		store.Close()
		t.Fatal("future schema was accepted")
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 99 {
		t.Fatalf("future schema version = %d, err=%v", version, err)
	}
}

func TestVaultBatchUploadQueueRetriesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upload.db")
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	setBatchLimits(t, store, 1, vaultBatchSoftBytes, vaultBatchHardCount, vaultBatchHardBytes)
	acceptTestRecord(t, store, "queued")
	uploader := &retryBatchUploader{first: fmt.Errorf("temporary outage")}
	attemptAt := time.Now().UTC().Add(time.Second)
	worked, err := store.ProcessNextVaultBatchUpload(context.Background(), uploader, attemptAt)
	if !worked || err == nil {
		t.Fatalf("first upload worked=%v err=%v", worked, err)
	}
	var attempts int
	var state string
	if err = store.DB().QueryRow("SELECT attempts,state FROM vault_batch_uploads WHERE batch_no=0").Scan(&attempts, &state); err != nil || attempts != 1 || state != "pending" {
		t.Fatalf("retry state attempts=%d state=%s err=%v", attempts, state, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	worked, err = store.ProcessNextVaultBatchUpload(context.Background(), uploader, attemptAt.Add(2*time.Second))
	if !worked || err != nil {
		t.Fatalf("retry worked=%v err=%v", worked, err)
	}
	if err = store.DB().QueryRow("SELECT attempts,state FROM vault_batch_uploads WHERE batch_no=0").Scan(&attempts, &state); err != nil || attempts != 2 || state != "uploaded" {
		t.Fatalf("final state attempts=%d state=%s err=%v", attempts, state, err)
	}
}
