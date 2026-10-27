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
	"path/filepath"
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
	RunID            string            `json:"run_id"`
	StreamID         string            `json:"stream_id"`
	GeneratorVersion string            `json:"generator_version"`
	Seed             int64             `json:"seed"`
	Records          int               `json:"records"`
	FixtureDigests   map[string]string `json:"fixture_digests"`
	Batches          []DemoBatch       `json:"batches"`
	CollectorPath    string            `json:"collector_path"`
	WitnessPath      string            `json:"witness_path"`
	ChainID          string            `json:"chain_id"`
	Registry         string            `json:"registry"`
	RegistryCodeHash string            `json:"registry_code_hash"`
}

type DemoBatch struct {
	BatchNo        uint64      `json:"batch_no"`
	FirstSequence  uint64      `json:"first_sequence"`
	LastSequence   uint64      `json:"last_sequence"`
	Records        uint64      `json:"records"`
	CloseReason    string      `json:"close_reason"`
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
	Content        string              `json:"content"`
	Witness        string              `json:"witness"`
	Anchor         string              `json:"anchor"`
	Coverage       string              `json:"coverage"`
	RecordsChecked int                 `json:"records_checked"`
	BatchesChecked int                 `json:"batches_checked"`
	Issues         []VerificationIssue `json:"issues"`
}

type demoRun struct {
	summary  DemoSummary
	store    *rawstore.RawStore
	witness  *witness.Store
	runner   *Runner
	root     string
	streamID string
	mu       sync.Mutex
}

// Demo owns one disposable collector, witness and local-chain run; it never opens product stores.
type Demo struct {
	mu  sync.Mutex
	run *demoRun
}

func NewDemo() *Demo { return &Demo{} }

func (d *Demo) Start(ctx context.Context) (DemoSummary, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.run != nil {
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
	runner, err := Start(ctx)
	if err != nil {
		return DemoSummary{}, err
	}
	failed := true
	defer func() {
		if failed {
			runner.Stop()
		}
	}()
	summary := DemoSummary{Mode: "SIMULATED", RunID: runID, StreamID: streamID, GeneratorVersion: demoGeneratorVersion, Seed: demoSeed, Records: 13, FixtureDigests: map[string]string{}, CollectorPath: filepath.Join(root, "collector.sqlite"), WitnessPath: filepath.Join(root, "witness.sqlite"), ChainID: runner.ChainID.String(), Registry: runner.Registry.Hex(), RegistryCodeHash: runner.RegistryHash.Hex()}
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
		summary.Batches = append(summary.Batches, DemoBatch{BatchNo: batchNo, FirstSequence: batch.FirstSequence, LastSequence: batch.LastSequence, Records: batch.EntryCount, CloseReason: batch.CloseReason, WitnessReceipt: fmt.Sprintf("local witness receipt %s", hex.EncodeToString(receipt.Digest[:])), Anchor: anchor})
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
	d.run = &demoRun{summary: summary, store: store, witness: wit, runner: runner, root: root, streamID: streamID}
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
	return run.verify(ctx)
}

// TamperDelete removes one accepted raw parent only from the disposable demo store.
func (d *Demo) TamperDelete(ctx context.Context, runID string) error {
	run, err := d.getRun(runID)
	if err != nil {
		return err
	}
	run.mu.Lock()
	defer run.mu.Unlock()
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
func (r *demoRun) close() { _ = r.store.Close(); _ = r.witness.Close(); r.runner.Stop() }

func (r *demoRun) verify(ctx context.Context) (DemoVerification, error) {
	result := DemoVerification{Mode: "SIMULATED", Content: "UNKNOWN", Witness: "first-write originals", Anchor: "local registry readback", Coverage: "incomplete", Issues: []VerificationIssue{}}
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
	result.BatchesChecked = len(manifests)
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
		return result, nil
	}
	streamBytes, e := hex.DecodeString(r.streamID)
	if e != nil || len(streamBytes) != 32 {
		return DemoVerification{}, errors.New("invalid simulator stream ID")
	}
	var stream integrity.Digest
	copy(stream[:], streamBytes)
	tx, err := r.store.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		result.Issues = append(result.Issues, VerificationIssue{Reason: "collector snapshot unavailable: " + err.Error()})
		return result, nil
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT m.accept_seq,m.accepted_at_ns,m.batch_no,m.raw_id,r.raw_id,r.raw_sha256,r.received_at,r.transport,r.source_ip,r.source_port,r.metadata_json,r.payload FROM vault_batch_members m LEFT JOIN raw_events r ON r.raw_id=m.raw_id WHERE m.stream_id=? ORDER BY m.accept_seq`, r.streamID)
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
	result.Content = "INTACT"
	if !inventoryComplete {
		result.Coverage = "incomplete"
		result.Content = "INCOMPLETE"
	} else if len(result.Issues) > 0 {
		result.Content = "TAMPERED"
	}
	return result, nil
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
