package rawstore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/model"
)

type retryUploader struct{ calls int }

func (u *retryUploader) UploadManifest(context.Context, string, uint64, integrity.Digest, []byte) (string, error) {
	u.calls++
	if u.calls == 1 {
		return "", fmt.Errorf("temporary outage")
	}
	return "witness-receipt", nil
}

func TestRawAcceptanceAndMembershipAreAtomicAndOrdered(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	first, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("first"), ReceivedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("second"), ReceivedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	var firstSeq, secondSeq uint64
	if err = store.DB().QueryRowContext(ctx, "SELECT accept_seq FROM vault_members WHERE raw_id=?", first.RawID).Scan(&firstSeq); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, "SELECT accept_seq FROM vault_members WHERE raw_id=?", second.RawID).Scan(&secondSeq); err != nil {
		t.Fatal(err)
	}
	if firstSeq != 1 || secondSeq != 2 {
		t.Fatalf("sequences = %d,%d", firstSeq, secondSeq)
	}
	state, err := store.VaultState(ctx)
	if err != nil || state.NextSequence != 3 {
		t.Fatalf("vault state = %+v, err=%v", state, err)
	}
	var count int
	if err = store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_events r LEFT JOIN vault_members m USING(raw_id) WHERE m.raw_id IS NULL").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("accepted raw parents without membership: %d", count)
	}
}

func TestStoreSealsMissedSlotsAsGapsAndChainsCommitments(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-600 * time.Second)
	var stream integrity.Digest
	stream[0], stream[31] = 1, 2
	if err := store.ConfigureVaultStream(ctx, hex.EncodeToString(stream[:]), epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("current"), ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	for slot := uint64(0); slot < 2; slot++ {
		window, err := store.SealedWindow(ctx, slot)
		if err != nil {
			t.Fatal(err)
		}
		if window.Coverage != integrity.CoverageGap || window.EntryCount != 0 {
			t.Fatalf("slot %d = %+v", slot, window)
		}
		if slot == 0 && window.Previous != (integrity.Digest{}) {
			t.Fatal("first predecessor is not zero")
		}
		if slot == 1 {
			first, err := store.SealedWindow(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if window.Previous != first.Commitment {
				t.Fatal("gap commitment chain is broken")
			}
		}
	}
}

func TestNonemptyWindowSealsInterruptedAndLegacyRowsStayUnanchored(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	epoch := time.Now().UTC().Truncate(300 * time.Second)
	var stream integrity.Digest
	stream[0] = 9
	if err := store.ConfigureVaultStream(ctx, hex.EncodeToString(stream[:]), epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("member"), ReceivedAt: epoch}); err != nil {
		t.Fatal(err)
	}
	if err := store.SealDueWindows(ctx, epoch.Add(300*time.Second)); err != nil {
		t.Fatal(err)
	}
	window, err := store.SealedWindow(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if window.Coverage != integrity.CoverageInterrupted || window.EntryCount != 1 {
		t.Fatalf("unexpected sealed window: %+v", window)
	}

	legacyDB := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := New(legacyDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.DB().Exec(`INSERT INTO raw_events(raw_id,raw_sha256,byte_count,transport,source_ip,source_port,metadata_json,received_at,payload,status) VALUES('legacy','00',0,'stdin','local',0,'{}',?,X'', 'accepted')`, epoch.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"DELETE FROM vault_members", "DROP TABLE vault_windows", "DROP TABLE vault_members", "DROP TABLE vault_streams", "PRAGMA user_version=4"} {
		if _, err = legacy.DB().Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	_ = legacy.Close()
	reopened, err := New(legacyDB)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var n int
	if err = reopened.DB().QueryRow("SELECT COUNT(*) FROM vault_members").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("legacy record received invented membership: %d", n)
	}
}

func TestContinuouslyObservedWindowCanBeComplete(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()
	ctx := context.Background()
	epoch := time.Now().UTC().Truncate(300 * time.Second)
	var stream integrity.Digest
	stream[0] = 10
	if err := store.ConfigureVaultStream(ctx, hex.EncodeToString(stream[:]), epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("observed"), ReceivedAt: epoch}); err != nil {
		t.Fatal(err)
	}
	tx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.sealDueWindowsObservedTx(ctx, tx, epoch.Add(300*time.Second), epoch.Add(-time.Second), true); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	window, err := store.SealedWindow(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if window.Coverage != integrity.CoverageComplete {
		t.Fatalf("continuously observed window coverage = %v", window.Coverage)
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
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 99 {
		t.Fatalf("future schema version was overwritten: %d", version)
	}
}

func TestVaultUploadQueueRetriesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upload.db")
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-300 * time.Second)
	var stream integrity.Digest
	stream[0] = 4
	if err = store.ConfigureVaultStream(ctx, hex.EncodeToString(stream[:]), epoch); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Store(ctx, model.IngestedRecord{Transport: "stdin", SourceIP: "local", RawBytes: []byte("queued"), ReceivedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	uploader := &retryUploader{}
	attemptAt := time.Now().UTC().Add(time.Second)
	worked, err := store.ProcessNextVaultUpload(ctx, uploader, attemptAt)
	if !worked || err == nil {
		t.Fatalf("first upload worked=%v err=%v", worked, err)
	}
	var attempts int
	var state string
	if err = store.DB().QueryRow("SELECT attempts,state FROM vault_uploads WHERE slot=0").Scan(&attempts, &state); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || state != "pending" {
		t.Fatalf("retry state attempts=%d state=%s", attempts, state)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	worked, err = store.ProcessNextVaultUpload(ctx, uploader, attemptAt.Add(2*time.Second))
	if !worked || err != nil {
		t.Fatalf("retry worked=%v err=%v", worked, err)
	}
	if err = store.DB().QueryRow("SELECT attempts,state FROM vault_uploads WHERE slot=0").Scan(&attempts, &state); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || state != "uploaded" {
		t.Fatalf("final state attempts=%d state=%s", attempts, state)
	}
}
