package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"deinopis/internal/api"
	"deinopis/internal/ingest"
	"deinopis/internal/output"
	"deinopis/internal/packs"
	"deinopis/internal/pipeline"
	"deinopis/internal/rawstore"
)

func TestEndToEnd_ReplayAndPacks(t *testing.T) {
	t.Setenv("DEINOPIS_ADMIN_TOKEN", "integration-token")
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "deinopis.db")
	outputPath := filepath.Join(tempDir, "normalized.ndjson")
	inboxDir := filepath.Join(tempDir, "inbox")
	_ = os.MkdirAll(inboxDir, 0755)

	store, err := rawstore.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create raw store: %v", err)
	}
	defer store.Close()

	root := findProjectRoot(t)
	snap, err := packs.LoadDir(filepath.Join(root, "packs"))
	if err != nil {
		t.Fatalf("failed to load packs: %v", err)
	}
	packMgr := packs.NewSnapshotManager(snap)

	sink, err := output.NewNDJSONSink(outputPath)
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	defer sink.Close()

	pipe := pipeline.New(pipeline.Config{
		Workers:     4,
		BufferSize:  1000,
		PackManager: packMgr,
	}, store, sink)
	pipe.Start()
	defer pipe.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileWatcher := ingest.NewFileWatcher(inboxDir, 20*time.Millisecond)
	mgr := ingest.NewManager(100)
	mgr.Register(fileWatcher)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start ingest manager: %v", err)
	}
	pipe.ConnectIngest(ctx, mgr.Channel())

	// Copy the baseline and initially unmatched samples into the watched inbox.
	replayDir := filepath.Join(root, "testdata", "replay")
	for _, group := range []string{"initial", "unmatched"} {
		groupDir := filepath.Join(replayDir, group)
		entries, err := os.ReadDir(groupDir)
		if err != nil {
			t.Fatalf("failed to read replay fixture directory %s: %v", groupDir, err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(groupDir, e.Name()))
			if err != nil {
				t.Fatalf("failed to read fixture %s: %v", e.Name(), err)
			}
			dest := filepath.Join(inboxDir, e.Name())
			if err := os.WriteFile(dest, data, 0644); err != nil {
				t.Fatalf("failed to stage fixture %s: %v", dest, err)
			}
		}
	}

	// Wait for the watcher and workers to process the complete fixture set.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, quarantined, err := store.ListQuarantinePaged(ctx, 10, 0)
		if err != nil {
			t.Fatalf("ListQuarantinePaged failed while waiting for ingestion: %v", err)
		}
		_, normalized, err := store.ListNormalized(ctx, "", "", "", 50, 0)
		if err != nil {
			t.Fatalf("ListNormalized failed while waiting for ingestion: %v", err)
		}
		if quarantined == 5 && normalized == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fixture ingestion: got %d quarantined and %d normalized, want 5 and 16", quarantined, normalized)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Two order-service NDJSON records plus three other unmatched records quarantine.
	qEntries, qTotal, err := store.ListQuarantinePaged(ctx, 10, 0)
	if err != nil {
		t.Fatalf("ListQuarantinePaged failed: %v", err)
	}
	for _, q := range qEntries {
		t.Logf("  * Quarantine ID: %s | Stage: %s | Reason: %s | Preview: %s", q.QuarantineID, q.Stage, q.Reason, q.RawPreview)
	}
	if qTotal != 5 {
		t.Fatalf("expected 5 quarantined entries, got %d", qTotal)
	}
	t.Logf("Successfully verified 5 quarantined entries:")
	for _, q := range qEntries {
		t.Logf("  * Quarantine ID: %s | Stage: %s | Reason: %s", q.QuarantineID, q.Stage, q.Reason)
	}

	// Every one of the 16 baseline pack samples should normalize before corrections.
	_, normTotal, err := store.ListNormalized(ctx, "", "", "", 50, 0)
	if err != nil {
		t.Fatalf("ListNormalized failed: %v", err)
	}
	if normTotal != 16 {
		t.Fatalf("expected 16 normalized entries initially, got %d", normTotal)
	}
	t.Logf("Successfully verified 16 normalized entries initially")

	tempPacksDir := filepath.Join(tempDir, "packs")
	_ = os.MkdirAll(tempPacksDir, 0755)
	basePacks, _ := os.ReadDir(filepath.Join(root, "packs"))
	for _, bp := range basePacks {
		if bp.IsDir() || !strings.HasSuffix(strings.ToLower(bp.Name()), ".yaml") {
			continue
		}
		bdata, _ := os.ReadFile(filepath.Join(root, "packs", bp.Name()))
		_ = os.WriteFile(filepath.Join(tempPacksDir, bp.Name()), bdata, 0644)
	}

	// Setup API server to test dynamic pack activation and replay-all
	srv := api.NewServer(
		":0",
		store,
		pipe,
		packMgr,
		tempPacksDir,
		api.ConfigInfo{InboxDir: inboxDir},
		nil,
		"test-version",
	)
	handler := srv.Handler()

	// Activate every correction pack while leaving quarantine pending until replay-all.
	correctionDir := filepath.Join(replayDir, "packs")
	correctionPacks, err := os.ReadDir(correctionDir)
	if err != nil {
		t.Fatalf("failed to read correction packs: %v", err)
	}
	for _, packFile := range correctionPacks {
		if packFile.IsDir() || !strings.HasSuffix(strings.ToLower(packFile.Name()), ".yaml") {
			continue
		}
		packPath := filepath.Join(correctionDir, packFile.Name())
		packYAML, err := os.ReadFile(packPath)
		if err != nil {
			t.Fatalf("failed to read correction pack %s: %v", packPath, err)
		}
		body, err := json.Marshal(map[string]string{
			"yaml_content": string(packYAML),
			"filename":     packFile.Name(),
		})
		if err != nil {
			t.Fatalf("failed to encode correction pack %s: %v", packPath, err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/packs/activate", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer integration-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("activate pack %s failed with %d: %s", packFile.Name(), rec.Code, rec.Body.String())
		}
		t.Logf("Activated correction pack: %s", packFile.Name())
	}

	// Trigger global replay after all correction packs are active.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/quarantine/replay-all", nil)
	req.Header.Set("Authorization", "Bearer integration-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("replay-all failed with %d: %s", rec.Code, rec.Body.String())
	}

	var replayResp struct {
		Total    int `json:"total"`
		Replayed int `json:"replayed"`
		Failed   int `json:"failed"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&replayResp); err != nil {
		t.Fatalf("failed to decode replay-all response: %v", err)
	}
	if replayResp.Total != 5 || replayResp.Replayed != 5 || replayResp.Failed != 0 {
		t.Fatalf("expected 5/5 replayed, got %+v", replayResp)
	}
	t.Logf("Replay All executed successfully: %+v", replayResp)

	// Verify all original and replayed records are now normalized.
	normEvents, normTotal, err := store.ListNormalized(ctx, "", "", "", 50, 0)
	if err != nil {
		t.Fatalf("ListNormalized failed: %v", err)
	}
	if normTotal != 21 {
		t.Fatalf("expected 21 total normalized events after replay, got %d", normTotal)
	}

	// 5. Verify Cryptographic Integrity of every normalized event
	for _, evt := range normEvents {
		rep, err := store.VerifyNormalized(ctx, evt.Deinopis.EventID)
		if err != nil {
			t.Fatalf("VerifyNormalized error for %s: %v", evt.Deinopis.EventID, err)
		}
		if !rep.Passed {
			t.Fatalf("integrity check FAILED for %s: %s", evt.Deinopis.EventID, rep.FailureReason)
		}
	}
	t.Logf("Cryptographic SHA-256 verification PASSED for all 21 normalized events!")
}
