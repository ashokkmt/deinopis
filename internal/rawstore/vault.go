package rawstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/model"
)

const (
	vaultWindowSeconds = int64(300)
	maxWindowMembers   = 100_000
	maxWindowBytes     = 64 * 1024 * 1024
	maxSealCatchup     = 10_000
)

type VaultState struct {
	StreamID       string
	EpochStart     time.Time
	NextSequence   uint64
	LastSealedSlot int64
}

type SealedWindow struct {
	StreamID       string
	Slot           uint64
	Coverage       integrity.Coverage
	EntryCount     uint64
	HighWaterSeq   uint64
	Manifest       []byte
	ManifestDigest integrity.Digest
	MerkleRoot     integrity.Digest
	Previous       integrity.Digest
	Commitment     integrity.Digest
	SealedAt       time.Time
}

func ensureVaultStream(db *sql.DB) error {
	var exists int
	if err := db.QueryRow("SELECT COUNT(*) FROM vault_streams").Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	var id [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("create stream identity: %w", err)
	}
	now := time.Now().UTC().Unix()
	epoch := now - now%vaultWindowSeconds
	zero := make([]byte, 32)
	_, err := db.Exec(`INSERT OR IGNORE INTO vault_streams(id,stream_id,epoch_start,window_seconds,next_sequence,last_sealed_slot,last_commitment)
		VALUES(1,?,?,?,?,?,?)`, hex.EncodeToString(id[:]), epoch, vaultWindowSeconds, 1, -1, zero)
	return err
}

func (s *RawStore) VaultState(ctx context.Context) (VaultState, error) {
	var state VaultState
	var epoch int64
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT stream_id,epoch_start,next_sequence,last_sealed_slot FROM vault_streams WHERE id=1`).Scan(&state.StreamID, &epoch, &seq, &state.LastSealedSlot)
	state.EpochStart, state.NextSequence = time.Unix(epoch, 0).UTC(), seq
	return state, err
}

func (s *RawStore) PendingVaultUploads(ctx context.Context) (uint32, error) {
	var pending uint64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM vault_uploads WHERE state='pending'`).Scan(&pending)
	if err != nil {
		return 0, err
	}
	if pending > uint64(^uint32(0)) {
		return ^uint32(0), nil
	}
	return uint32(pending), nil
}

// ConfigureVaultStream replaces the automatically created stream before any raw evidence is accepted.
func (s *RawStore) ConfigureVaultStream(ctx context.Context, streamID string, epochStart time.Time) error {
	if len(streamID) != 64 || strings.ToLower(streamID) != streamID {
		return errors.New("stream ID must be 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(streamID)
	if err != nil || len(decoded) != 32 {
		return errors.New("invalid stream ID")
	}
	if epochStart.Nanosecond() != 0 || epochStart.Unix()%vaultWindowSeconds != 0 {
		return errors.New("stream epoch must align to a 300-second UTC boundary")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var members, windows int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_members").Scan(&members); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_windows").Scan(&windows); err != nil {
		return err
	}
	if members != 0 || windows != 0 {
		return errors.New("vault stream is immutable after acceptance begins")
	}
	zero := make([]byte, 32)
	if _, err = tx.ExecContext(ctx, `UPDATE vault_streams SET stream_id=?,epoch_start=?,window_seconds=300,next_sequence=1,last_sealed_slot=-1,last_commitment=? WHERE id=1`, streamID, epochStart.Unix(), zero); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RawStore) sealDueWindowsTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	return s.sealDueWindowsObservedTx(ctx, tx, now, time.Time{}, false)
}

func (s *RawStore) sealDueWindowsObservedTx(ctx context.Context, tx *sql.Tx, now, observedFrom time.Time, clockHealthy bool) error {
	var streamID string
	var epoch, windowSeconds, lastSealed int64
	var nextSeq uint64
	var previousBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT stream_id,epoch_start,window_seconds,last_sealed_slot,next_sequence,last_commitment FROM vault_streams WHERE id=1`).Scan(&streamID, &epoch, &windowSeconds, &lastSealed, &nextSeq, &previousBytes); err != nil {
		return err
	}
	if windowSeconds != vaultWindowSeconds {
		return errors.New("unsupported vault window duration")
	}
	if now.Unix() < epoch {
		return nil
	}
	current := (now.Unix() - epoch) / windowSeconds
	closedThrough := current - 1
	if closedThrough <= lastSealed {
		return nil
	}
	if closedThrough-lastSealed > maxSealCatchup {
		return fmt.Errorf("vault has %d overdue windows; operator recovery required", closedThrough-lastSealed)
	}
	var stream integrity.Digest
	decoded, err := hex.DecodeString(streamID)
	if err != nil || len(decoded) != len(stream) {
		return errors.New("stored vault stream ID is invalid")
	}
	copy(stream[:], decoded)
	var previous integrity.Digest
	if len(previousBytes) != len(previous) {
		return errors.New("stored predecessor commitment is invalid")
	}
	copy(previous[:], previousBytes)
	for slot := lastSealed + 1; slot <= closedThrough; slot++ {
		entries, err := readWindowEntries(ctx, tx, streamID, uint64(slot))
		if err != nil {
			return err
		}
		coverage := integrity.CoverageInterrupted
		if len(entries) == 0 {
			coverage = integrity.CoverageGap
		}
		start := uint64(epoch + slot*windowSeconds)
		if clockHealthy && !observedFrom.IsZero() && observedFrom.Unix() <= int64(start) {
			coverage = integrity.CoverageComplete
		} else if !observedFrom.IsZero() && observedFrom.Unix() < int64(start+uint64(windowSeconds)) {
			coverage = integrity.CoverageInterrupted
		}
		manifest := integrity.Manifest{StreamID: stream, Slot: uint64(slot), Start: start, End: start + uint64(windowSeconds), Coverage: coverage, Previous: previous, Entries: entries}
		manifestBytes, err := integrity.EncodeManifest(manifest)
		if err != nil {
			return fmt.Errorf("encode vault window %d: %w", slot, err)
		}
		commitment, err := integrity.WindowCommitment(manifest, manifestBytes)
		if err != nil {
			return err
		}
		root, err := integrity.MerkleRootEntries(entries)
		if err != nil {
			return err
		}
		digest := integrity.ManifestDigest(manifestBytes)
		highWater := uint64(0)
		if len(entries) != 0 {
			highWater = entries[len(entries)-1].AcceptSeq
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO vault_windows(stream_id,slot,coverage,entry_count,high_water_sequence,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, streamID, slot, int(coverage), len(entries), highWater, manifestBytes, digest[:], root[:], previous[:], commitment[:], now.UTC().UnixNano()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO vault_uploads(stream_id,slot,state,retry_at_ns) VALUES(?,?,'pending',?)`, streamID, slot, now.UTC().UnixNano()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE vault_streams SET last_sealed_slot=?,last_commitment=? WHERE id=1`, slot, commitment[:]); err != nil {
			return err
		}
		previous = commitment
	}
	return nil
}

func readWindowEntries(ctx context.Context, tx *sql.Tx, streamID string, slot uint64) ([]integrity.Entry, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.accept_seq,m.accepted_at_ns,r.raw_id,r.received_at,r.transport,r.source_ip,r.source_port,r.metadata_json,r.payload
		FROM vault_members m JOIN raw_events r USING(raw_id) WHERE m.stream_id=? AND m.slot=? ORDER BY m.accept_seq`, streamID, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	streamBytes, _ := hex.DecodeString(streamID)
	var stream integrity.Digest
	copy(stream[:], streamBytes)
	entries := make([]integrity.Entry, 0)
	for rows.Next() {
		var seq uint64
		var accepted int64
		var rawID, received, transport, sourceIP, metadataJSON string
		var sourcePort int
		var payload []byte
		if err := rows.Scan(&seq, &accepted, &rawID, &received, &transport, &sourceIP, &sourcePort, &metadataJSON, &payload); err != nil {
			return nil, err
		}
		receivedAt, err := time.Parse(time.RFC3339Nano, received)
		if err != nil {
			return nil, fmt.Errorf("invalid raw receive time for %s: %w", rawID, err)
		}
		if sourcePort < 0 || sourcePort > math.MaxUint16 {
			return nil, fmt.Errorf("invalid source port for %s", rawID)
		}
		var metadata model.ReceiveMetadata
		if err = json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return nil, err
		}
		entries = append(entries, integrity.Entry{StreamID: stream, Slot: slot, AcceptSeq: seq, AcceptedAt: accepted, RawID: rawID, ReceivedAt: receivedAt.UnixNano(), Transport: transport, SourceIP: sourceIP, SourcePort: uint16(sourcePort), Metadata: metadata, Payload: payload})
	}
	return entries, rows.Err()
}

// SealDueWindows closes elapsed slots. Empty slots are recorded as gaps; nonempty
// slots stay interrupted until continuity monitoring can prove full coverage.
func (s *RawStore) SealDueWindows(ctx context.Context, now time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.sealDueWindowsTx(ctx, tx, now.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RawStore) RunVaultSealer(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("sealer poll interval must be positive")
	}
	started := time.Now()
	previous := started
	clockHealthy := true
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		now := time.Now()
		wallDelta := now.UnixNano() - previous.UnixNano()
		monoDelta := now.Sub(previous).Nanoseconds()
		if delta := wallDelta - monoDelta; delta > int64(5*time.Second) || delta < -int64(5*time.Second) || monoDelta > int64(time.Duration(vaultWindowSeconds)*time.Second+5*time.Second) {
			clockHealthy = false
		}
		previous = now
		tx, err := s.db.BeginTx(ctx, nil)
		if err == nil {
			err = s.sealDueWindowsObservedTx(ctx, tx, now.UTC(), started, clockHealthy)
		}
		if err == nil {
			err = tx.Commit()
		} else if tx != nil {
			_ = tx.Rollback()
		}
		if err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *RawStore) SealedWindow(ctx context.Context, slot uint64) (SealedWindow, error) {
	state, err := s.VaultState(ctx)
	if err != nil {
		return SealedWindow{}, err
	}
	var out SealedWindow
	var coverage int
	var count, high uint64
	var digest, root, previous, commitment []byte
	var sealed int64
	err = s.db.QueryRowContext(ctx, `SELECT stream_id,slot,coverage,entry_count,high_water_sequence,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns FROM vault_windows WHERE stream_id=? AND slot=?`, state.StreamID, slot).Scan(&out.StreamID, &out.Slot, &coverage, &count, &high, &out.Manifest, &digest, &root, &previous, &commitment, &sealed)
	if err != nil {
		return SealedWindow{}, err
	}
	out.Coverage, out.EntryCount, out.HighWaterSeq, out.SealedAt = integrity.Coverage(coverage), count, high, time.Unix(0, sealed).UTC()
	for src, dst := range map[*[]byte]*integrity.Digest{&digest: &out.ManifestDigest, &root: &out.MerkleRoot, &previous: &out.Previous, &commitment: &out.Commitment} {
		if len(*src) != len(dst) {
			return SealedWindow{}, errors.New("corrupt sealed window digest")
		}
		copy(dst[:], *src)
	}
	return out, nil
}

type ManifestUploader interface {
	UploadManifest(context.Context, string, uint64, integrity.Digest, []byte) (string, error)
}

// ProcessNextVaultUpload performs one durable queue attempt; repeated calls resume after restart.
func (s *RawStore) ProcessNextVaultUpload(ctx context.Context, uploader ManifestUploader, now time.Time) (bool, error) {
	if uploader == nil {
		return false, errors.New("manifest uploader is required")
	}
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	var streamID string
	var slot uint64
	var attempts uint
	err := s.db.QueryRowContext(ctx, `SELECT u.stream_id,u.slot,u.attempts FROM vault_uploads u WHERE u.state='pending' AND u.retry_at_ns<=? ORDER BY u.slot LIMIT 1`, now.UTC().UnixNano()).Scan(&streamID, &slot, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var manifest []byte
	var digestBytes []byte
	if err = s.db.QueryRowContext(ctx, `SELECT manifest,manifest_digest FROM vault_windows WHERE stream_id=? AND slot=?`, streamID, slot).Scan(&manifest, &digestBytes); err != nil {
		return false, err
	}
	if len(digestBytes) != 32 {
		return false, errors.New("corrupt queued manifest digest")
	}
	var digest integrity.Digest
	copy(digest[:], digestBytes)
	receipt, uploadErr := uploader.UploadManifest(ctx, streamID, slot, digest, manifest)
	attempts++
	if uploadErr == nil && receipt == "" {
		uploadErr = errors.New("witness returned an empty receipt")
	}
	if uploadErr == nil {
		_, err = s.db.ExecContext(ctx, `UPDATE vault_uploads SET state='uploaded',attempts=?,last_error=NULL,witness_receipt=? WHERE stream_id=? AND slot=? AND state='pending'`, attempts, receipt, streamID, slot)
		return true, err
	}
	delay := time.Second
	shift := attempts - 1
	if shift > 6 {
		shift = 6
	}
	if shift > 0 {
		delay *= time.Duration(uint64(1) << shift)
	}
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	_, err = s.db.ExecContext(ctx, `UPDATE vault_uploads SET attempts=?,retry_at_ns=?,last_error='witness upload failed' WHERE stream_id=? AND slot=? AND state='pending'`, attempts, now.Add(delay).UTC().UnixNano(), streamID, slot)
	if err != nil {
		return true, err
	}
	return true, uploadErr
}

func (s *RawStore) RunVaultUploader(ctx context.Context, uploader ManifestUploader, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("upload poll interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		worked, err := s.ProcessNextVaultUpload(ctx, uploader, time.Now().UTC())
		if err == nil && worked {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err != nil && ctx.Err() == nil {
			log.Print("witness manifest upload failed; durable retry remains queued")
		}
	}
}
