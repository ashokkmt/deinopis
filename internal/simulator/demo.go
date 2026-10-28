package simulator

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/logsim"
	"deinopis/internal/model"
	"deinopis/internal/rawstore"
	"deinopis/internal/witness"
)

const demoGeneratorVersion = "deinopis-logsim-v1"
const demoSeed int64 = 42

var ErrRunNotFound = errors.New("simulated integrity run not found")

type DemoSummary struct {
	Mode             string            `json:"mode"`
	InputMode        string            `json:"input_mode"`
	RunID            string            `json:"run_id"`
	StreamID         string            `json:"stream_id"`
	GeneratorVersion string            `json:"generator_version"`
	Seed             int64             `json:"seed"`
	Records          int               `json:"records"`
	FixtureDigests   map[string]string `json:"fixture_digests"`
	Batches          []DemoBatch       `json:"batches"`
	CollectorPath    string            `json:"collector_path"`
	WitnessPath      string            `json:"witness_path"`
	AnvilRunDir      string            `json:"anvil_run_directory"`
	ChainID          string            `json:"chain_id"`
	Registry         string            `json:"registry"`
	RegistryCodeHash string            `json:"registry_code_hash"`
	EvidencePath     string            `json:"evidence_path"`
}

type DemoBatch struct {
	BatchNo        uint64      `json:"batch_no"`
	FirstSequence  uint64      `json:"first_sequence"`
	LastSequence   uint64      `json:"last_sequence"`
	Records        uint64      `json:"records"`
	CloseReason    string      `json:"close_reason"`
	MerkleRoot     string      `json:"merkle_root"`
	WitnessReceipt string      `json:"witness_receipt"`
	Anchor         LocalAnchor `json:"anchor"`
}

type VerificationIssue struct {
	Sequence uint64 `json:"sequence,omitempty"`
	BatchNo  uint64 `json:"batch_no,omitempty"`
	RawID    string `json:"raw_id,omitempty"`
	Reason   string `json:"reason"`
}

type DemoVerification struct {
	Mode           string              `json:"mode"`
	CheckedAt      time.Time           `json:"checked_at_utc"`
	FirstAdverseAt *time.Time          `json:"first_adverse_at_utc,omitempty"`
	Observations   int                 `json:"observation_count"`
	Content        string              `json:"content"`
	Witness        string              `json:"witness"`
	Anchor         string              `json:"anchor"`
	Coverage       string              `json:"coverage"`
	RecordsChecked int                 `json:"records_checked"`
	BatchesChecked int                 `json:"batches_checked"`
	PendingRecords int                 `json:"pending_records"`
	Issues         []VerificationIssue `json:"issues"`
}

type demoRun struct {
	summary       DemoSummary
	store         *rawstore.RawStore
	witness       *witness.Store
	runner        *Runner
	root          string
	streamID      string
	fingerprint   string
	token         string
	chainSnapshot string
	ownsStore     bool
	mu            sync.Mutex
}

// Demo owns one disposable collector, witness and local-chain run; it never opens product stores.
type Demo struct {
	mu  sync.Mutex
	run *demoRun
}

func NewDemo() *Demo { return &Demo{} }

// NewLiveDemo attaches the local witness/Anvil simulation to an empty, disposable collector store.
func NewLiveDemo(ctx context.Context, store *rawstore.RawStore, dbPath string, targetCount uint64) (*Demo, error) {
	if store == nil || dbPath == "" {
		return nil, errors.New("live simulation requires an open disposable store and its database path")
	}
	var accepted int
	if err := store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_events").Scan(&accepted); err != nil {
		return nil, err
	}
	if accepted != 0 {
		return nil, errors.New("live simulation requires a fresh collector database")
	}
	root, err := os.MkdirTemp("", "deinopis-live-integrity-demo-")
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(root, 0700); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = os.RemoveAll(root)
		}
	}()
	var runBytes [16]byte
	if _, err = crand.Read(runBytes[:]); err != nil {
		return nil, err
	}
	runID := hex.EncodeToString(runBytes[:])
	stream := sha256.Sum256(append([]byte("deinopis-live-sim-stream:"), runBytes[:]...))
	streamID := hex.EncodeToString(stream[:])
	tokenBytes := make([]byte, 32)
	if _, err = crand.Read(tokenBytes); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(tokenBytes)
	fingerprint := sha256.Sum256(append([]byte("deinopis-live-sim-witness:"), runBytes[:]...))
	fingerprintText := hex.EncodeToString(fingerprint[:])
	wit, err := witness.NewStore(filepath.Join(root, "witness.sqlite"), filepath.Join(root, "witness-manifests"))
	if err != nil {
		return nil, err
	}
	closeWitness := true
	defer func() {
		if closeWitness {
			_ = wit.Close()
		}
	}()
	if err = wit.EnrollBatch(ctx, streamID, fingerprintText, token); err != nil {
		return nil, err
	}
	runner, err := start(context.WithoutCancel(ctx), root)
	if err != nil {
		return nil, err
	}
	chainSnapshot, err := runner.Snapshot(ctx)
	if err != nil {
		runner.Stop()
		return nil, err
	}
	if err = store.ConfigureVaultBatchStream(ctx, streamID); err != nil {
		runner.Stop()
		return nil, err
	}
	if err = store.ConfigureVaultBatchTargetCount(ctx, targetCount); err != nil {
		runner.Stop()
		return nil, err
	}
	summary := DemoSummary{Mode: "SIMULATED", InputMode: "LIVE_INGESTION", RunID: runID, StreamID: streamID, CollectorPath: dbPath, WitnessPath: filepath.Join(root, "witness.sqlite"), AnvilRunDir: runner.RunDir, ChainID: runner.ChainID.String(), Registry: runner.Registry.Hex(), RegistryCodeHash: runner.RegistryHash.Hex(), EvidencePath: filepath.Join(root, "qualification.json"), FixtureDigests: map[string]string{}}
	run := &demoRun{summary: summary, store: store, witness: wit, runner: runner, root: root, streamID: streamID, fingerprint: fingerprintText, token: token, chainSnapshot: chainSnapshot, ownsStore: false}
	d := &Demo{run: run}
	closeWitness = false
	failed = false
	return d, nil
}

func (d *Demo) Start(ctx context.Context) (DemoSummary, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run != nil {
		if !d.run.ownsStore {
			d.run.mu.Lock()
			summary := d.run.summary
			summary.Batches = append([]DemoBatch(nil), summary.Batches...)
			d.run.mu.Unlock()
			return summary, nil
		}
		d.run.mu.Lock()
		d.run.close()
		d.run.mu.Unlock()
		d.run = nil
	}
	root, err := os.MkdirTemp("", "deinopis-integrity-demo-")
	if err != nil {
		return DemoSummary{}, err
	}
	if err = os.Chmod(root, 0700); err != nil {
		_ = os.RemoveAll(root)
		return DemoSummary{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()
	var runBytes [16]byte
	if _, err = crand.Read(runBytes[:]); err != nil {
		return DemoSummary{}, err
	}
	runID := hex.EncodeToString(runBytes[:])
	store, err := rawstore.New(filepath.Join(root, "collector.sqlite"))
	if err != nil {
		return DemoSummary{}, err
	}
	closeStore := true
	defer func() {
		if closeStore {
			_ = store.Close()
		}
	}()
	wit, err := witness.NewStore(filepath.Join(root, "witness.sqlite"), filepath.Join(root, "witness-manifests"))
	if err != nil {
		return DemoSummary{}, err
	}
	closeWitness := true
	defer func() {
		if closeWitness {
			_ = wit.Close()
		}
	}()
	streamBytes := sha256.Sum256(append([]byte("deinopis-sim-stream:"), runBytes[:]...))
	streamID := hex.EncodeToString(streamBytes[:])
	if err = store.ConfigureVaultBatchStream(ctx, streamID); err != nil {
		return DemoSummary{}, err
	}
	// Small per-run target makes count and age closure visible without changing production defaults.
	if _, err = store.DB().ExecContext(ctx, "UPDATE vault_batch_streams SET target_count=4 WHERE id=1"); err != nil {
		return DemoSummary{}, err
	}
	tokenBytes := make([]byte, 32)
	if _, err = crand.Read(tokenBytes); err != nil {
		return DemoSummary{}, err
	}
	token := hex.EncodeToString(tokenBytes)
	fingerprint := sha256.Sum256(append([]byte("deinopis-sim-witness:"), runBytes[:]...))
	fingerprintText := hex.EncodeToString(fingerprint[:])
	if err = wit.EnrollBatch(ctx, streamID, fingerprintText, token); err != nil {
		return DemoSummary{}, err
	}
	// The HTTP request that starts a demo ends immediately after this returns;
	// keep the owned Anvil process alive until Demo.Close instead.
	runner, err := start(context.WithoutCancel(ctx), root)
	if err != nil {
		return DemoSummary{}, err
	}
	chainSnapshot, err := runner.Snapshot(ctx)
	if err != nil {
		runner.Stop()
		return DemoSummary{}, err
	}
	failed := true
	defer func() {
		if failed {
			runner.Stop()
		}
	}()
	summary := DemoSummary{Mode: "SIMULATED", InputMode: "FIXED_SYNTHETIC", RunID: runID, StreamID: streamID, GeneratorVersion: demoGeneratorVersion, Seed: demoSeed, Records: 13, FixtureDigests: map[string]string{}, CollectorPath: filepath.Join(root, "collector.sqlite"), WitnessPath: filepath.Join(root, "witness.sqlite"), AnvilRunDir: runner.RunDir, ChainID: runner.ChainID.String(), Registry: runner.Registry.Hex(), RegistryCodeHash: runner.RegistryHash.Hex(), EvidencePath: filepath.Join(root, "qualification.json")}
	rng := rand.New(rand.NewSource(demoSeed))
	fixtureHashes := map[string]hashCollector{}
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 13; i++ {
		scenario := []string{"normal", "bruteforce", "scan"}[i%3]
		sources := []logsim.SourceSimulator{&logsim.SSHDSimulator{}, &logsim.WebServerSimulator{}, &logsim.AppSimulator{}}
		source := sources[i%len(sources)]
		payload := source.Generate(scenario, base.Add(time.Duration(i)*time.Second), rng)
		key := source.Name() + "/" + scenario
		h := sha256.Sum256(payload)
		collector := fixtureHashes[key]
		collector.add(h)
		fixtureHashes[key] = collector
		if _, err = store.Store(ctx, model.IngestedRecord{Transport: "simulator", SourceIP: "127.0.0.1", RawBytes: payload, ReceivedAt: base.Add(time.Duration(i) * time.Second), Metadata: model.ReceiveMetadata{Listener: "synthetic-demo", RequestID: fmt.Sprintf("%s-%02d", runID, i+1)}}); err != nil {
			return DemoSummary{}, fmt.Errorf("accept synthetic record %d: %w", i+1, err)
		}
	}
	for key, hashes := range fixtureHashes {
		summary.FixtureDigests[key] = hashes.sum()
	}
	flushAt := time.Now().UTC().Add(6 * time.Minute)
	if err = store.SealDueVaultBatches(ctx, flushAt); err != nil {
		return DemoSummary{}, err
	}
	for batchNo := uint64(0); batchNo < 4; batchNo++ {
		batch, e := store.SealedBatch(ctx, batchNo)
		if e != nil {
			return DemoSummary{}, fmt.Errorf("read sealed demo batch %d: %w", batchNo, e)
		}
		digest := hex.EncodeToString(batch.Digest[:])
		receipt, e := wit.AcceptBatch(ctx, streamID, batchNo, digest, fingerprintText, token, batch.Manifest, flushAt)
		if e != nil {
			return DemoSummary{}, fmt.Errorf("store original demo batch %d at witness: %w", batchNo, e)
		}
		anchor, e := runner.PublishBatch(ctx, batch)
		if e != nil {
			return DemoSummary{}, e
		}
		summary.Batches = append(summary.Batches, DemoBatch{BatchNo: batchNo, FirstSequence: batch.FirstSequence, LastSequence: batch.LastSequence, Records: batch.EntryCount, CloseReason: batch.CloseReason, MerkleRoot: hex.EncodeToString(batch.Root[:]), WitnessReceipt: fmt.Sprintf("local witness receipt %s", hex.EncodeToString(receipt.Digest[:])), Anchor: anchor})
	}
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return DemoSummary{}, err
	}
	if err = os.WriteFile(filepath.Join(root, "demo.json"), append(body, '\n'), 0600); err != nil {
		return DemoSummary{}, err
	}
	cleanup = false
	closeStore = false
	closeWitness = false
	failed = false
	d.run = &demoRun{summary: summary, store: store, witness: wit, runner: runner, root: root, streamID: streamID, fingerprint: fingerprintText, token: token, chainSnapshot: chainSnapshot, ownsStore: true}
	return summary, nil
}

type hashCollector struct{ hashes [][32]byte }

func (h *hashCollector) add(v [32]byte) { h.hashes = append(h.hashes, v) }
func (h hashCollector) sum() string {
	var data []byte
	for _, v := range h.hashes {
		data = append(data, v[:]...)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (d *Demo) Verify(ctx context.Context, runID string) (DemoVerification, error) {
	run, err := d.getRun(runID)
	if err != nil {
		return DemoVerification{}, err
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	result, err := run.verify(ctx)
	if err != nil {
		return result, err
	}
	result.CheckedAt = time.Now().UTC()
	result.FirstAdverseAt, result.Observations, err = recordVerification(ctx, run.summary.WitnessPath, runID, result)
	if err != nil {
		return DemoVerification{}, fmt.Errorf("persist simulated verification observation: %w", err)
	}
	if err = run.writeEvidence(ctx, result); err != nil {
		return DemoVerification{}, err
	}
	return result, nil
}

// Evidence returns only the JSON report produced by a completed synthetic verification.
func (d *Demo) Evidence(runID string) ([]byte, error) {
	run, err := d.getRun(runID)
	if err != nil {
		return nil, err
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	return os.ReadFile(run.summary.EvidencePath)
}

// UploadBatch accepts the collector's durable queue item into the local witness and Anvil registry.
func (d *Demo) UploadBatch(ctx context.Context, streamID string, batchNo uint64, digest integrity.Digest, body []byte) (string, error) {
	d.mu.Lock()
	run := d.run
	d.mu.Unlock()
	if run == nil || run.ownsStore {
		return "", errors.New("live simulated publisher is not configured")
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if streamID != run.streamID {
		return "", errors.New("queued batch belongs to another simulated stream")
	}
	batch, err := run.store.SealedBatch(ctx, batchNo)
	if err != nil {
		return "", err
	}
	if batch.Digest != digest || !equalBytes(batch.Manifest, body) {
		return "", errors.New("queued batch differs from the sealed collector manifest")
	}
	receipt, err := run.witness.AcceptBatch(ctx, streamID, batchNo, hex.EncodeToString(digest[:]), run.fingerprint, run.token, body, time.Now().UTC())
	if err != nil {
		return "", err
	}
	anchor, err := run.runner.PublishBatch(ctx, batch)
	if err != nil {
		return "", err
	}
	run.summary.Records = int(batch.LastSequence)
	run.summary.Batches = append(run.summary.Batches, DemoBatch{BatchNo: batchNo, FirstSequence: batch.FirstSequence, LastSequence: batch.LastSequence, Records: batch.EntryCount, CloseReason: batch.CloseReason, MerkleRoot: hex.EncodeToString(batch.Root[:]), WitnessReceipt: fmt.Sprintf("local witness receipt %s", hex.EncodeToString(receipt.Digest[:])), Anchor: anchor})
	return hex.EncodeToString(receipt.Digest[:]), nil
}

func (r *demoRun) writeEvidence(ctx context.Context, result DemoVerification) error {
	history, err := r.runner.ReadHistory(ctx, r.streamID)
	evidence := struct {
		Mode              string            `json:"mode"`
		GeneratedAt       string            `json:"generated_at_utc"`
		GoVersion         string            `json:"go_version"`
		GOOS              string            `json:"goos"`
		GOARCH            string            `json:"goarch"`
		Revision          string            `json:"source_revision,omitempty"`
		Summary           DemoSummary       `json:"run"`
		RegistryBatches   int               `json:"registry_batches"`
		RegistryHead      string            `json:"registry_head"`
		Verification      DemoVerification  `json:"verification"`
		FirstAdverseAt    *time.Time        `json:"first_adverse_at_utc,omitempty"`
		ObservationCount  int               `json:"observation_count"`
		ManifestDigests   []string          `json:"witness_manifest_sha256"`
		ArtifactChecksums map[string]string `json:"artifact_sha256"`
		ToolVersions      map[string]string `json:"tool_versions"`
		EvidenceNotes     []string          `json:"evidence_notes,omitempty"`
		ClaimLimits       []string          `json:"claim_limits"`
	}{Mode: "SIMULATED", GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Summary: r.summary, RegistryBatches: len(history), Verification: result, FirstAdverseAt: result.FirstAdverseAt, ObservationCount: result.Observations, ArtifactChecksums: map[string]string{}, ToolVersions: map[string]string{}, ClaimLimits: []string{"local Anvil state is not public-chain finality", "same-host witness storage is not independent custody", "local measurements are not production capacity or gas/fee evidence", "each check uses one consistent local SQLite read transaction; no scheduled or resumable snapshot job is simulated"}}
	if err != nil {
		evidence.EvidenceNotes = append(evidence.EvidenceNotes, "local registry inventory unavailable: "+err.Error())
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			if setting.Key == "vcs.revision" {
				evidence.Revision = setting.Value
			}
		}
	}
	if evidence.Revision == "" {
		if output, revErr := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output(); revErr == nil {
			evidence.Revision = strings.TrimSpace(string(output))
		}
	}
	if len(history) > 0 {
		evidence.RegistryHead = fmt.Sprintf("0x%x", history[len(history)-1].Commitment)
	}
	manifests, manifestErr := r.witness.ReadBatchManifests(ctx, r.streamID)
	if manifestErr != nil {
		evidence.EvidenceNotes = append(evidence.EvidenceNotes, "witness manifest inventory unavailable: "+manifestErr.Error())
	}
	for _, batch := range manifests {
		evidence.ManifestDigests = append(evidence.ManifestDigests, hex.EncodeToString(batch.Digest[:]))
	}
	paths := []string{filepath.Join(r.runner.RunDir, "run.json")}
	if r.ownsStore {
		paths = append(paths, r.summary.CollectorPath, r.summary.WitnessPath, filepath.Join(r.root, "demo.json"))
	} else {
		evidence.EvidenceNotes = append(evidence.EvidenceNotes, "live collector and witness databases are open during verification and are omitted from file checksums")
	}
	for _, path := range paths {
		sum, hashErr := fileSHA256(path)
		if hashErr == nil {
			evidence.ArtifactChecksums[filepath.Base(path)] = sum
		}
	}
	if path, findErr := findAnvil(); findErr == nil {
		evidence.ToolVersions["anvil"] = commandVersion(ctx, path)
	} else {
		evidence.EvidenceNotes = append(evidence.EvidenceNotes, "Anvil version unavailable: "+findErr.Error())
	}
	if path, findErr := exec.LookPath("forge"); findErr == nil {
		evidence.ToolVersions["forge"] = commandVersion(ctx, path)
	} else {
		evidence.EvidenceNotes = append(evidence.EvidenceNotes, "Foundry forge version unavailable: "+findErr.Error())
	}
	body, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.summary.EvidencePath, append(body, '\n'), 0600)
}

func commandVersion(ctx context.Context, path string) string {
	versionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	body, err := exec.CommandContext(versionCtx, path, "--version").CombinedOutput()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return strings.TrimSpace(string(body))
}

func fileSHA256(path string) (string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// TamperDelete removes one accepted raw parent only from the disposable demo store.
func (d *Demo) TamperDelete(ctx context.Context, runID string) error {
	run, err := d.getRun(runID)
	if err != nil {
		return err
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if !run.ownsStore {
		return errors.New("tamper controls are unavailable in live-ingestion mode; edit only its disposable demo database manually")
	}
	if _, err = run.store.DB().ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return err
	}
	defer func() { _, _ = run.store.DB().Exec("PRAGMA foreign_keys=ON") }()
	var rawID string
	if err = run.store.DB().QueryRowContext(ctx, "SELECT raw_id FROM vault_batch_members WHERE stream_id=? AND accept_seq=6", run.streamID).Scan(&rawID); err != nil {
		return err
	}
	tx, err := run.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	member, err := tx.ExecContext(ctx, "DELETE FROM vault_batch_members WHERE stream_id=? AND accept_seq=6", run.streamID)
	if err != nil {
		return err
	}
	if n, _ := member.RowsAffected(); n != 1 {
		return errors.New("synthetic target membership was not found")
	}
	parent, err := tx.ExecContext(ctx, "DELETE FROM raw_events WHERE raw_id=?", rawID)
	if err != nil {
		return err
	}
	if n, _ := parent.RowsAffected(); n != 1 {
		return errors.New("synthetic target parent was not found")
	}
	return tx.Commit()
}

// TamperDeleteTail removes the final accepted batch only from its disposable collector snapshot.
func (d *Demo) TamperDeleteTail(ctx context.Context, runID string) error {
	run, err := d.getRun(runID)
	if err != nil {
		return err
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if !run.ownsStore {
		return errors.New("tamper controls are unavailable in live-ingestion mode; edit only its disposable demo database manually")
	}
	if len(run.summary.Batches) == 0 {
		return errors.New("synthetic tail batch is unavailable")
	}
	batchNo := run.summary.Batches[len(run.summary.Batches)-1].BatchNo
	if _, err = run.store.DB().ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return err
	}
	defer func() { _, _ = run.store.DB().Exec("PRAGMA foreign_keys=ON") }()
	tx, err := run.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT accept_seq,raw_id FROM vault_batch_members WHERE stream_id=? AND batch_no=? ORDER BY accept_seq", run.streamID, batchNo)
	if err != nil {
		return err
	}
	type member struct {
		seq uint64
		id  string
	}
	var members []member
	for rows.Next() {
		var item member
		if err = rows.Scan(&item.seq, &item.id); err != nil {
			_ = rows.Close()
			return err
		}
		members = append(members, item)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if len(members) == 0 {
		return errors.New("synthetic tail batch has already been deleted")
	}
	for _, item := range members {
		if _, err = tx.ExecContext(ctx, "DELETE FROM vault_batch_members WHERE stream_id=? AND accept_seq=?", run.streamID, item.seq); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM raw_events WHERE raw_id=?", item.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// appendRecovery adds a new, separately sequenced synthetic record after a simulated loss.
func (r *demoRun) appendRecovery(ctx context.Context, lostSequence uint64) error {
	payload := []byte(fmt.Sprintf("SIMULATED recovery record for missing sequence %d", lostSequence))
	_, err := r.store.Store(ctx, model.IngestedRecord{Transport: "simulator", SourceIP: "127.0.0.1", RawBytes: payload, ReceivedAt: time.Now().UTC(), Metadata: model.ReceiveMetadata{Listener: "synthetic-recovery", RequestID: fmt.Sprintf("recovery-%d", lostSequence)}})
	if err != nil {
		return err
	}
	flushAt := time.Now().UTC().Add(6 * time.Minute)
	if err = r.store.SealDueVaultBatches(ctx, flushAt); err != nil {
		return err
	}
	batchNo := r.summary.Batches[len(r.summary.Batches)-1].BatchNo + 1
	batch, err := r.store.SealedBatch(ctx, batchNo)
	if err != nil {
		return err
	}
	receipt, err := r.witness.AcceptBatch(ctx, r.streamID, batchNo, hex.EncodeToString(batch.Digest[:]), r.fingerprint, r.token, batch.Manifest, flushAt)
	if err != nil {
		return err
	}
	anchor, err := r.runner.PublishBatch(ctx, batch)
	if err != nil {
		return err
	}
	r.summary.Batches = append(r.summary.Batches, DemoBatch{BatchNo: batchNo, FirstSequence: batch.FirstSequence, LastSequence: batch.LastSequence, Records: batch.EntryCount, CloseReason: batch.CloseReason, MerkleRoot: hex.EncodeToString(batch.Root[:]), WitnessReceipt: fmt.Sprintf("local witness receipt %s", hex.EncodeToString(receipt.Digest[:])), Anchor: anchor})
	r.summary.Records++
	body, err := json.MarshalIndent(r.summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.root, "demo.json"), append(body, '\n'), 0600)
}

func (d *Demo) getRun(id string) (*demoRun, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run == nil || d.run.summary.RunID != id {
		return nil, ErrRunNotFound
	}
	return d.run, nil
}

func (d *Demo) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run != nil {
		d.run.mu.Lock()
		d.run.close()
		d.run.mu.Unlock()
		d.run = nil
	}
}
func (r *demoRun) close() {
	if r.ownsStore {
		_ = r.store.Close()
	}
	_ = r.witness.Close()
	r.runner.Stop()
}

func (r *demoRun) verify(ctx context.Context) (DemoVerification, error) {
	result := DemoVerification{Mode: "SIMULATED", Content: "UNKNOWN", Witness: "first-write originals", Anchor: "local registry readback", Coverage: "incomplete", Issues: []VerificationIssue{}}
	if err := r.runner.VerifyIdentity(ctx, r.summary.ChainID, r.summary.Registry, r.summary.RegistryCodeHash); err != nil {
		result.Anchor = "identity mismatch"
		result.Issues = append(result.Issues, VerificationIssue{Reason: "simulated chain or registry identity differs from the run record: " + err.Error()})
		return result, nil
	}
	manifests, err := r.witness.ReadBatchManifests(ctx, r.streamID)
	if err != nil {
		result.Witness = "unavailable"
		result.Issues = append(result.Issues, VerificationIssue{Reason: "witness originals unavailable: " + err.Error()})
		return result, nil
	}
	history, err := r.runner.ReadHistory(ctx, r.streamID)
	if err != nil {
		result.Anchor = "unavailable"
		result.Issues = append(result.Issues, VerificationIssue{Reason: "local registry unavailable: " + err.Error()})
		return result, nil
	}
	tx, err := r.store.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "collector snapshot unavailable: " + err.Error()})
		return result, nil
	}
	defer tx.Rollback()
	result.BatchesChecked = len(manifests)
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM vault_batch_members WHERE stream_id=? AND batch_no>=?`, r.streamID, len(manifests)).Scan(&result.PendingRecords); err != nil {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "pending accepted-record inventory unavailable: " + err.Error()})
	}
	inventoryComplete := len(manifests) == len(history)
	if !inventoryComplete {
		result.Issues = append(result.Issues, VerificationIssue{Reason: fmt.Sprintf("registry/witness batch inventory mismatch (%d registry, %d witness)", len(history), len(manifests))})
	}
	expected := map[uint64][]byte{}
	expectedBatch := map[uint64]uint64{}
	for i, m := range manifests {
		parsed, e := integrity.ParseBatchManifest(m.Manifest)
		if e != nil {
			result.Issues = append(result.Issues, VerificationIssue{BatchNo: m.BatchNo, Reason: "witness manifest is invalid"})
			continue
		}
		if m.BatchNo != uint64(i) {
			inventoryComplete = false
			result.Issues = append(result.Issues, VerificationIssue{BatchNo: m.BatchNo, Reason: "witness batch sequence has a gap or reorder"})
		}
		for j, entry := range parsed.Entries {
			seq := parsed.FirstSequence + uint64(j)
			expected[seq] = entry
			expectedBatch[seq] = parsed.BatchNo
		}
		if m.BatchNo < uint64(len(history)) {
			a := history[m.BatchNo]
			if a.BatchNo != m.BatchNo || a.Commitment != m.Commitment || a.First != m.FirstSequence || a.Last != m.LastSequence || a.Count != m.Count {
				result.Issues = append(result.Issues, VerificationIssue{BatchNo: m.BatchNo, Reason: "local registry record differs from witness original"})
			}
		}
	}
	if len(manifests) == 0 || len(history) == 0 {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "witness or registry inventory is empty"})
		if result.PendingRecords > 0 {
			result.Content = "INCOMPLETE"
		}
		return result, nil
	}
	streamBytes, e := hex.DecodeString(r.streamID)
	if e != nil || len(streamBytes) != 32 {
		return DemoVerification{}, errors.New("invalid simulator stream ID")
	}
	var stream integrity.Digest
	copy(stream[:], streamBytes)
	rows, err := tx.QueryContext(ctx, `SELECT m.accept_seq,m.accepted_at_ns,m.batch_no,m.raw_id,r.raw_id,r.raw_sha256,r.received_at,r.transport,r.source_ip,r.source_port,r.metadata_json,r.payload FROM vault_batch_members m LEFT JOIN raw_events r ON r.raw_id=m.raw_id WHERE m.stream_id=? AND m.batch_no<? ORDER BY m.accept_seq`, r.streamID, len(manifests))
	if err != nil {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "collector snapshot enumeration failed: " + err.Error()})
		return result, nil
	}
	seen := map[uint64]bool{}
	for rows.Next() {
		var seq, batch uint64
		var accepted int64
		var rawID string
		var parentID, storedHash, received, transport, sourceIP, metadata sql.NullString
		var sourcePort sql.NullInt64
		var payload []byte
		if err = rows.Scan(&seq, &accepted, &batch, &rawID, &parentID, &storedHash, &received, &transport, &sourceIP, &sourcePort, &metadata, &payload); err != nil {
			_ = rows.Close()
			return DemoVerification{}, err
		}
		seen[seq] = true
		if _, ok := expected[seq]; !ok {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "extra or orphan membership"})
			continue
		}
		if expectedBatch[seq] != batch {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "record moved to another batch"})
			continue
		}
		if !parentID.Valid {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "accepted raw parent is missing"})
			continue
		}
		receivedAt, parseErr := time.Parse(time.RFC3339Nano, received.String)
		if parseErr != nil || !sourcePort.Valid || sourcePort.Int64 < 0 || sourcePort.Int64 > 65535 {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "protected raw metadata is invalid"})
			continue
		}
		var meta model.ReceiveMetadata
		if !metadata.Valid || json.Unmarshal([]byte(metadata.String), &meta) != nil {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "protected receive metadata is invalid"})
			continue
		}
		entry := integrity.BatchEntry{BatchNo: batch, Entry: integrity.Entry{StreamID: stream, AcceptSeq: seq, AcceptedAt: accepted, RawID: rawID, ReceivedAt: receivedAt.UnixNano(), Transport: transport.String, SourceIP: sourceIP.String, SourcePort: uint16(sourcePort.Int64), Metadata: meta, Payload: payload}}
		encoded, encodeErr := integrity.EncodeBatchEntry(entry)
		payloadHash := sha256.Sum256(payload)
		if encodeErr != nil || !storedHash.Valid || storedHash.String != hex.EncodeToString(payloadHash[:]) || !equalBytes(encoded, expected[seq]) {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, RawID: rawID, Reason: "payload or protected metadata differs from the immutable witness entry"})
		}
		result.RecordsChecked++
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		result.Issues = append(result.Issues, VerificationIssue{Reason: "collector snapshot read failed: " + err.Error()})
		return result, nil
	}
	if err = rows.Close(); err != nil {
		return DemoVerification{}, err
	}
	for seq, batch := range expectedBatch {
		if !seen[seq] {
			result.Issues = append(result.Issues, VerificationIssue{Sequence: seq, BatchNo: batch, Reason: "accepted raw parent or membership is missing"})
		}
	}
	orphans, err := tx.QueryContext(ctx, `SELECT r.raw_id FROM raw_events r LEFT JOIN vault_batch_members m ON m.raw_id=r.raw_id AND m.stream_id=? WHERE m.raw_id IS NULL`, r.streamID)
	if err != nil {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "orphan check unavailable: " + err.Error()})
		return result, nil
	}
	for orphans.Next() {
		var id string
		if err = orphans.Scan(&id); err != nil {
			_ = orphans.Close()
			return DemoVerification{}, err
		}
		result.Issues = append(result.Issues, VerificationIssue{RawID: id, Reason: "orphan raw parent is outside the anchored sequence"})
	}
	if err = orphans.Err(); err != nil {
		_ = orphans.Close()
		return DemoVerification{}, err
	}
	_ = orphans.Close()
	result.Coverage = "complete"
	if !inventoryComplete || result.PendingRecords > 0 {
		result.Coverage = "incomplete"
	}
	if hasContentTamper(result.Issues) {
		result.Content = "TAMPERED"
	} else if result.Coverage == "incomplete" || len(result.Issues) > 0 {
		result.Content = "INCOMPLETE"
	} else {
		result.Content = "INTACT"
	}
	return result, nil
}

func hasContentTamper(issues []VerificationIssue) bool {
	for _, issue := range issues {
		switch issue.Reason {
		case "extra or orphan membership", "record moved to another batch", "accepted raw parent is missing", "protected raw metadata is invalid", "protected receive metadata is invalid", "payload or protected metadata differs from the immutable witness entry", "accepted raw parent or membership is missing", "orphan raw parent is outside the anchored sequence":
			return true
		}
	}
	return false
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
