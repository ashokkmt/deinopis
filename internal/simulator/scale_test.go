package simulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"deinopis/internal/model"
	"deinopis/internal/rawstore"
)

type scaleProfileResult struct {
	Parents          int      `json:"parent_target"`
	Profile          string   `json:"payload_profile"`
	StorageMode      string   `json:"storage_mode"`
	SQLiteSync       string   `json:"sqlite_sync_mode"`
	PayloadBytes     int      `json:"payload_bytes"`
	MetadataBytes    int      `json:"metadata_bytes"`
	Accepted         int      `json:"accepted"`
	Batches          uint64   `json:"batches_sealed"`
	ManifestBytes    uint64   `json:"manifest_bytes"`
	BacklogCount     uint32   `json:"upload_backlog_count"`
	AcceptDurationMS int64    `json:"accept_duration_ms"`
	SealDurationMS   int64    `json:"seal_duration_ms"`
	CheckDurationMS  int64    `json:"rehash_check_duration_ms"`
	PeakRSSBytes     uint64   `json:"process_peak_rss_bytes"`
	HeapAllocBytes   uint64   `json:"heap_alloc_bytes_after_acceptance"`
	CPUSeconds       float64  `json:"process_cpu_seconds"`
	ResourceNotes    []string `json:"resource_metric_notes,omitempty"`
	DatabaseBytes    int64    `json:"database_bytes"`
	WALBytes         int64    `json:"wal_bytes"`
	RefusedInputs    int      `json:"refused_inputs"`
	InputDigest      string   `json:"input_sha256"`
	DatabaseSHA256   string   `json:"database_sha256"`
	WALSHA256        string   `json:"wal_sha256,omitempty"`
}

func TestScaleQualification(t *testing.T) {
	if os.Getenv("DEINOPIS_SIM_SCALE") != "1" {
		t.Skip("set DEINOPIS_SIM_SCALE=1 to run the local 1k/2k/10k/30k synthetic matrix")
	}
	root, err := os.MkdirTemp("", "deinopis-sim-scale-")
	if err != nil {
		t.Fatal(err)
	}
	results := struct {
		Mode        string               `json:"mode"`
		GeneratedAt string               `json:"generated_at_utc"`
		GoVersion   string               `json:"go_version"`
		GOOS        string               `json:"goos"`
		GOARCH      string               `json:"goarch"`
		Seed        int64                `json:"seed"`
		Profiles    []scaleProfileResult `json:"profiles"`
		Limits      []string             `json:"limits"`
	}{Mode: "SIMULATED", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Seed: 42, Limits: []string{"local synthetic host measurements only", "1k/2k profiles use disk SQLite with NORMAL sync override; 10k/30k profiles use in-memory SQLite and report a post-run database snapshot, so high-scale WAL and FULL-sync durability are not measured", "process peak RSS is cumulative for the test process; CPU time is measured as a per-profile delta", "these profiles are not production capacity or public-chain throughput, gas, or fee evidence"}}
	var testOutput strings.Builder
	for _, count := range []int{1000, 2000, 10000, 30000} {
		for _, profile := range []struct {
			name     string
			payload  int
			metadata string
		}{{"small", 128, "synthetic"}, {"large", 4096, "synthetic-metadata-" + strings.Repeat("m", 2048)}} {
			result, err := runScaleProfile(t, root, count, profile.name, profile.payload, profile.metadata)
			if err != nil {
				t.Fatalf("scale profile %d/%s: %v", count, profile.name, err)
			}
			results.Profiles = append(results.Profiles, result)
			fmt.Fprintf(&testOutput, "%d/%s storage=%s accepted=%d batches=%d accept_ms=%d seal_ms=%d check_ms=%d peak_rss=%d heap=%d cpu_seconds=%.3f db=%d wal=%d refused=%d input_sha256=%s\n", result.Parents, result.Profile, result.StorageMode, result.Accepted, result.Batches, result.AcceptDurationMS, result.SealDurationMS, result.CheckDurationMS, result.PeakRSSBytes, result.HeapAllocBytes, result.CPUSeconds, result.DatabaseBytes, result.WALBytes, result.RefusedInputs, result.InputDigest)
		}
	}
	body, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "scale-qualification.json")
	if err = os.WriteFile(path, append(body, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "scale-test-output.txt"), []byte(testOutput.String()), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("SIMULATED scale evidence: %s", path)
}

func runScaleProfile(t *testing.T, root string, count int, profile string, payloadSize int, metadataValue string) (scaleProfileResult, error) {
	t.Helper()
	ctx := context.Background()
	storagePath := filepath.Join(root, fmt.Sprintf("%d-%s.sqlite", count, profile))
	diskBacked := count <= 2000
	dbPath := ":memory:"
	storageMode := "in-memory; post-run SQLite snapshot"
	if diskBacked {
		dbPath = storagePath
		storageMode = "disk-backed SQLite"
	}
	store, err := rawstore.New(dbPath)
	if err != nil {
		return scaleProfileResult{}, err
	}
	defer store.Close()
	if _, err = store.DB().ExecContext(ctx, "PRAGMA synchronous=NORMAL"); err != nil {
		return scaleProfileResult{}, err
	}
	if _, err = store.DB().ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		return scaleProfileResult{}, err
	}
	streamDigest := sha256.Sum256([]byte(fmt.Sprintf("deinopis-scale:%d:%s", count, profile)))
	streamID := hex.EncodeToString(streamDigest[:])
	if err = store.ConfigureVaultBatchStream(ctx, streamID); err != nil {
		return scaleProfileResult{}, err
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE vault_batch_streams SET target_count=? WHERE id=1`, count); err != nil {
		return scaleProfileResult{}, err
	}
	cpuBefore, _, beforeErr := processMetrics()
	rng := rand.New(rand.NewSource(42 + int64(count)))
	inputHash := sha256.New()
	started := time.Now()
	for i := 0; i < count; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte(rng.Intn(256))
		}
		inputHash.Write(payload)
		inputHash.Write([]byte(metadataValue))
		_, err = store.Store(ctx, model.IngestedRecord{Transport: "simulator", SourceIP: "127.0.0.1", RawBytes: payload, ReceivedAt: time.Unix(int64(i+1), 0).UTC(), Metadata: model.ReceiveMetadata{Listener: metadataValue}})
		if err != nil {
			return scaleProfileResult{}, fmt.Errorf("accept record %d: %w", i+1, err)
		}
	}
	acceptDuration := time.Since(started)
	started = time.Now()
	if err = store.SealDueVaultBatches(ctx, time.Now().UTC().Add(6*time.Minute)); err != nil {
		return scaleProfileResult{}, err
	}
	sealDuration := time.Since(started)
	state, err := store.VaultBatchState(ctx)
	if err != nil {
		return scaleProfileResult{}, err
	}
	var manifestBytes uint64
	for batchNo := uint64(0); batchNo < state.NextBatch; batchNo++ {
		batch, batchErr := store.SealedBatch(ctx, batchNo)
		if batchErr != nil {
			return scaleProfileResult{}, batchErr
		}
		manifestBytes += uint64(len(batch.Manifest))
	}
	started = time.Now()
	rows, err := store.DB().QueryContext(ctx, `SELECT raw_sha256,payload FROM raw_events ORDER BY raw_id`)
	if err != nil {
		return scaleProfileResult{}, err
	}
	checked := 0
	for rows.Next() {
		var stored string
		var payload []byte
		if err = rows.Scan(&stored, &payload); err != nil {
			rows.Close()
			return scaleProfileResult{}, err
		}
		sum := sha256.Sum256(payload)
		if stored != hex.EncodeToString(sum[:]) {
			rows.Close()
			return scaleProfileResult{}, fmt.Errorf("stored hash mismatch at checked row %d", checked+1)
		}
		checked++
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return scaleProfileResult{}, err
	}
	if err = rows.Close(); err != nil {
		return scaleProfileResult{}, err
	}
	if checked != count {
		return scaleProfileResult{}, fmt.Errorf("rehash checked %d records, expected %d", checked, count)
	}
	checkDuration := time.Since(started)
	if !diskBacked {
		if _, err = store.DB().ExecContext(ctx, "VACUUM INTO '"+strings.ReplaceAll(storagePath, "'", "''")+"'"); err != nil {
			return scaleProfileResult{}, fmt.Errorf("write post-run SQLite snapshot: %w", err)
		}
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	cpuAfter, peakRSS, afterErr := processMetrics()
	cpuSeconds := cpuAfter - cpuBefore
	var resourceNotes []string
	if beforeErr != nil || afterErr != nil {
		resourceNotes = append(resourceNotes, "OS process CPU/RSS metrics unavailable")
	}
	dbInfo, err := os.Stat(storagePath)
	if err != nil {
		return scaleProfileResult{}, err
	}
	walBytes := int64(0)
	walHash := ""
	if diskBacked {
		if info, statErr := os.Stat(dbPath + "-wal"); statErr == nil {
			walBytes = info.Size()
			walHash, _ = fileSHA256(dbPath + "-wal")
		}
	}
	refused := 0
	if _, err = store.Store(ctx, model.IngestedRecord{Transport: "simulator", SourceIP: "127.0.0.1", SourcePort: 65536, RawBytes: []byte("invalid synthetic port")}); err != nil {
		refused++
	}
	dbHash, err := fileSHA256(storagePath)
	if err != nil {
		return scaleProfileResult{}, err
	}
	return scaleProfileResult{Parents: count, Profile: profile, StorageMode: storageMode, SQLiteSync: "NORMAL (test-only override)", PayloadBytes: payloadSize, MetadataBytes: len(metadataValue), Accepted: count, Batches: state.NextBatch, ManifestBytes: manifestBytes, BacklogCount: state.PendingCount, AcceptDurationMS: acceptDuration.Milliseconds(), SealDurationMS: sealDuration.Milliseconds(), CheckDurationMS: checkDuration.Milliseconds(), PeakRSSBytes: peakRSS, HeapAllocBytes: memory.Alloc, CPUSeconds: cpuSeconds, ResourceNotes: resourceNotes, DatabaseBytes: dbInfo.Size(), WALBytes: walBytes, RefusedInputs: refused, InputDigest: hex.EncodeToString(inputHash.Sum(nil)), DatabaseSHA256: dbHash, WALSHA256: walHash}, nil
}
