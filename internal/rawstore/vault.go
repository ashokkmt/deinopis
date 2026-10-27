package rawstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
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
	vaultWindowSeconds       = int64(300)
	maxWindowMembers         = 100_000
	maxWindowBytes           = 64 * 1024 * 1024
	maxSealCatchup           = 10_000
	maxPendingUploads        = 1_000
	maxPendingBytes          = 1 << 30
	maxPendingAge            = 24 * time.Hour
	vaultManifestHeaderBytes = int64(len("DEINOPIS-MANIFEST-V1") + 2 + 32 + 8 + 8 + 8 + 1 + 8 + 32 + 32)
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
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM vault_uploads WHERE state IN ('pending','blocked')) +
		(SELECT pending_upload_count FROM vault_batch_streams WHERE id=1)`).Scan(&pending)
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
	if err := tx.QueryRowContext(ctx, `SELECT stream_id,epoch_start,window_seconds,last_sealed_slot FROM vault_streams WHERE id=1`).Scan(&streamID, &epoch, &windowSeconds, &lastSealed); err != nil {
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
	for slot := lastSealed + 1; slot <= closedThrough; slot++ {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM vault_seal_tasks WHERE stream_id=? AND slot=?`, streamID, slot).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			continue
		}
		var count int64
		var bytes int64
		var highWater int64
		if err := tx.QueryRowContext(ctx, `SELECT member_count,encoded_bytes,high_water_sequence FROM vault_slot_state WHERE stream_id=? AND slot=?`, streamID, slot).Scan(&count, &bytes, &highWater); errors.Is(err, sql.ErrNoRows) {
			count, bytes, highWater = 0, 0, 0
		} else if err != nil {
			return err
		}
		coverage := integrity.CoverageInterrupted
		if count == 0 {
			coverage = integrity.CoverageGap
		}
		start := uint64(epoch + slot*windowSeconds)
		if clockHealthy && !observedFrom.IsZero() && observedFrom.Unix() <= int64(start) {
			coverage = integrity.CoverageComplete
		} else if !observedFrom.IsZero() && observedFrom.Unix() < int64(start+uint64(windowSeconds)) {
			coverage = integrity.CoverageInterrupted
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO vault_slot_state(stream_id,slot,member_count,encoded_bytes,high_water_sequence,closed,closed_at_ns)
			VALUES(?,?,?,?,?,1,?) ON CONFLICT(stream_id,slot) DO UPDATE SET closed=1,closed_at_ns=excluded.closed_at_ns`, streamID, slot, count, bytes, highWater, now.UTC().UnixNano()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO vault_seal_tasks(stream_id,slot,coverage,member_count,encoded_bytes,high_water_sequence,closed_at_ns) VALUES(?,?,?,?,?,?,?)`, streamID, slot, int(coverage), count, bytes, highWater, now.UTC().UnixNano()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE vault_streams SET pending_upload_count=pending_upload_count+1,pending_upload_bytes=pending_upload_bytes+? WHERE id=1`, bytes+vaultManifestHeaderBytes); err != nil {
			return err
		}
	}
	return nil
}

func streamWindowEntries(ctx context.Context, tx *sql.Tx, streamID string, slot uint64, consume func(integrity.Entry, []byte) error) (uint64, uint64, uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.accept_seq,m.accepted_at_ns,r.raw_id,r.received_at,r.transport,r.source_ip,r.source_port,r.metadata_json,r.payload
		FROM vault_members m JOIN raw_events r USING(raw_id) WHERE m.stream_id=? AND m.slot=? ORDER BY m.accept_seq`, streamID, slot)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	streamBytes, _ := hex.DecodeString(streamID)
	var stream integrity.Digest
	copy(stream[:], streamBytes)
	var count, encodedBytes, highWater uint64
	for rows.Next() {
		var seq uint64
		var accepted int64
		var rawID, received, transport, sourceIP, metadataJSON string
		var sourcePort int
		var payload []byte
		if err := rows.Scan(&seq, &accepted, &rawID, &received, &transport, &sourceIP, &sourcePort, &metadataJSON, &payload); err != nil {
			return 0, 0, 0, err
		}
		if seq == 0 || (count > 0 && (highWater == math.MaxUint64 || seq != highWater+1)) {
			return 0, 0, 0, errors.New("vault membership sequence is not contiguous")
		}
		receivedAt, err := time.Parse(time.RFC3339Nano, received)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid raw receive time for %s: %w", rawID, err)
		}
		if sourcePort < 0 || sourcePort > math.MaxUint16 {
			return 0, 0, 0, fmt.Errorf("invalid source port for %s", rawID)
		}
		var metadata model.ReceiveMetadata
		if err = json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return 0, 0, 0, err
		}
		entry := integrity.Entry{StreamID: stream, Slot: slot, AcceptSeq: seq, AcceptedAt: accepted, RawID: rawID, ReceivedAt: receivedAt.UnixNano(), Transport: transport, SourceIP: sourceIP, SourcePort: uint16(sourcePort), Metadata: metadata, Payload: payload}
		encoded, err := integrity.EncodeEntry(entry)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("encode raw parent %s: %w", rawID, err)
		}
		if err = consume(entry, encoded); err != nil {
			return 0, 0, 0, err
		}
		count++
		encodedBytes += uint64(len(encoded) + 4)
		highWater = seq
	}
	if err = rows.Err(); err != nil {
		return 0, 0, 0, err
	}
	return count, encodedBytes, highWater, nil
}

// processNextVaultSeal builds one manifest outside the writer transaction from
// a durably closed slot, then atomically publishes the window and upload intent.
func (s *RawStore) processNextVaultSeal(ctx context.Context, now time.Time) (bool, error) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	var streamID string
	var slot uint64
	var coverage int
	var expectedCount, expectedBytes, highWater uint64
	err := s.db.QueryRowContext(ctx, `SELECT stream_id,slot,coverage,member_count,encoded_bytes,high_water_sequence
		FROM vault_seal_tasks ORDER BY slot LIMIT 1`).Scan(&streamID, &slot, &coverage, &expectedCount, &expectedBytes, &highWater)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var lastSealed int64
	var previousBytes []byte
	if err = s.db.QueryRowContext(ctx, `SELECT last_sealed_slot,last_commitment FROM vault_streams WHERE id=1`).Scan(&lastSealed, &previousBytes); err != nil {
		return false, err
	}
	if int64(slot) != lastSealed+1 || len(previousBytes) != 32 {
		return false, fmt.Errorf("vault seal task %d is out of predecessor order", slot)
	}
	var previous integrity.Digest
	copy(previous[:], previousBytes)
	var stream integrity.Digest
	streamBytes, err := hex.DecodeString(streamID)
	if err != nil || len(streamBytes) != 32 {
		return false, errors.New("stored vault stream ID is invalid")
	}
	copy(stream[:], streamBytes)
	var accumulator integrity.MerkleAccumulator
	var body bytes.Buffer
	if expectedBytes > maxWindowBytes {
		return false, errors.New("vault encoded-byte counter exceeds the manifest limit")
	}
	body.Grow(int(expectedBytes))
	readTx, err := s.readerDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	count, actualBytes, actualHigh, scanErr := streamWindowEntries(ctx, readTx, streamID, slot, func(entry integrity.Entry, encoded []byte) error {
		if body.Len()+len(encoded)+4+len("DEINOPIS-MANIFEST-V1")+2+32+8*3+1+8+64 > maxWindowBytes {
			return errors.New("vault manifest exceeds 64 MiB")
		}
		if encodeErr := accumulator.AddEntry(encoded); encodeErr != nil {
			return encodeErr
		}
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(encoded)))
		body.Write(size[:])
		body.Write(encoded)
		return nil
	})
	if scanErr != nil {
		_ = readTx.Rollback()
		return false, scanErr
	}
	if err = readTx.Commit(); err != nil {
		return false, err
	}
	if count != expectedCount || actualBytes != expectedBytes || actualHigh != highWater {
		return false, fmt.Errorf("closed vault slot %d encoded-byte counter mismatch", slot)
	}
	root := accumulator.Root()
	var epoch int64
	if err = s.db.QueryRowContext(ctx, `SELECT epoch_start FROM vault_streams WHERE id=1`).Scan(&epoch); err != nil {
		return false, err
	}
	start := uint64(epoch) + slot*uint64(vaultWindowSeconds)
	prefix, err := integrity.ManifestPrefix(stream, slot, start, start+uint64(vaultWindowSeconds), integrity.Coverage(coverage), expectedCount, previous, root)
	if err != nil {
		return false, err
	}
	manifest := append(prefix, body.Bytes()...)
	if len(manifest) > maxWindowBytes {
		return false, errors.New("vault manifest exceeds 64 MiB")
	}
	digest := integrity.ManifestDigest(manifest)
	commitment, err := integrity.WindowCommitmentFields(stream, slot, start, start+uint64(vaultWindowSeconds), integrity.Coverage(coverage), expectedCount, previous, root, digest)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var currentCount, currentBytes, currentHigh uint64
	if err = tx.QueryRowContext(ctx, `SELECT member_count,encoded_bytes,high_water_sequence FROM vault_slot_state WHERE stream_id=? AND slot=? AND closed=1`, streamID, slot).Scan(&currentCount, &currentBytes, &currentHigh); err != nil {
		return false, err
	}
	if currentCount != expectedCount || currentBytes != expectedBytes || currentHigh != highWater {
		return false, fmt.Errorf("closed vault slot %d counter mismatch", slot)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_windows(stream_id,slot,coverage,entry_count,high_water_sequence,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, streamID, slot, coverage, expectedCount, highWater, manifest, digest[:], root[:], previous[:], commitment[:], now.UTC().UnixNano()); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_uploads(stream_id,slot,state,retry_at_ns) VALUES(?,?,'pending',?)`, streamID, slot, now.UTC().UnixNano()); err != nil {
		return false, err
	}
	updated, updateErr := tx.ExecContext(ctx, `UPDATE vault_streams SET last_sealed_slot=?,last_commitment=? WHERE id=1 AND last_sealed_slot=?`, slot, commitment[:], lastSealed)
	if updateErr != nil {
		return false, updateErr
	}
	changed, updateErr := updated.RowsAffected()
	if updateErr != nil || changed != 1 {
		return false, errors.New("vault predecessor changed before seal finalization")
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM vault_seal_tasks WHERE stream_id=? AND slot=?`, streamID, slot); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *RawStore) processVaultSealTasks(ctx context.Context, now time.Time) error {
	for {
		worked, err := s.processNextVaultSeal(ctx, now)
		if err != nil || !worked {
			return err
		}
	}
}

// SealDueWindows closes elapsed slots. Empty slots are recorded as gaps; nonempty
// slots stay interrupted until continuity monitoring can prove full coverage.
func (s *RawStore) SealDueWindows(ctx context.Context, now time.Time) error {
	s.writeMu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	if err = s.sealDueWindowsTx(ctx, tx, now.UTC()); err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	if err = tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return err
	}
	s.writeMu.Unlock()
	return s.processVaultSealTasks(ctx, now.UTC())
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
		if err == nil {
			if err = s.processVaultSealTasks(ctx, now.UTC()); err != nil && ctx.Err() == nil {
				return err
			}
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
	var retryAt int64
	var state string
	err := s.db.QueryRowContext(ctx, `SELECT u.stream_id,u.slot,u.attempts,u.retry_at_ns,u.state FROM vault_uploads u WHERE u.state IN ('pending','blocked') ORDER BY u.slot LIMIT 1`).Scan(&streamID, &slot, &attempts, &retryAt, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state == "blocked" || retryAt > now.UTC().UnixNano() {
		return false, nil
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
		tx, beginErr := s.db.BeginTx(ctx, nil)
		if beginErr != nil {
			return true, beginErr
		}
		defer tx.Rollback()
		updated, updateErr := tx.ExecContext(ctx, `UPDATE vault_uploads SET state='uploaded',attempts=?,last_error=NULL,witness_receipt=? WHERE stream_id=? AND slot=? AND state='pending'`, attempts, receipt, streamID, slot)
		if updateErr != nil {
			return true, updateErr
		}
		changed, updateErr := updated.RowsAffected()
		if updateErr != nil {
			return true, updateErr
		}
		if changed == 1 {
			budget, budgetErr := tx.ExecContext(ctx, `UPDATE vault_streams SET pending_upload_count=pending_upload_count-1,
				pending_upload_bytes=pending_upload_bytes-(SELECT length(manifest) FROM vault_windows WHERE stream_id=? AND slot=?) WHERE id=1 AND pending_upload_count>0
				AND pending_upload_bytes >= (SELECT length(manifest) FROM vault_windows WHERE stream_id=? AND slot=?)`, streamID, slot, streamID, slot)
			if budgetErr != nil {
				return true, budgetErr
			}
			budgetRows, budgetErr := budget.RowsAffected()
			if budgetErr != nil || budgetRows != 1 {
				return true, errors.New("vault upload backlog counters are inconsistent")
			}
		}
		return true, tx.Commit()
	}
	var permanent interface{ Permanent() bool }
	if errors.As(uploadErr, &permanent) && permanent.Permanent() {
		_, err = s.db.ExecContext(ctx, `UPDATE vault_uploads SET state='blocked',attempts=?,last_error='permanent witness rejection' WHERE stream_id=? AND slot=? AND state='pending'`, attempts, streamID, slot)
		return true, errOr(err, uploadErr)
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

func errOr(updateErr, cause error) error {
	if updateErr != nil {
		return updateErr
	}
	return cause
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
