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
	"log"
	"math"
	"strings"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/model"
)

const (
	vaultBatchTargetCount = 30_000
	vaultBatchSoftBytes   = 16 * 1024 * 1024
	vaultBatchHardCount   = 50_000
	vaultBatchHardBytes   = 64 * 1024 * 1024
	vaultBatchMaxAge      = 5 * time.Minute
	vaultBatchHeaderSize  = int64(len("DEINOPIS-BATCH-MANIFEST-V2") + 2 + 32 + 6*8 + 64)
)

type VaultBatchState struct {
	StreamID        string
	NextSequence    uint64
	NextBatch       uint64
	LastSealedBatch int64
	PendingCount    uint32
	PendingBytes    uint64
	OldestPendingAt time.Time
	BlockedReason   string
}

type SealedBatch struct {
	StreamID      string
	BatchNo       uint64
	FirstSequence uint64
	LastSequence  uint64
	EntryCount    uint64
	StartedAt     time.Time
	ClosedAt      time.Time
	CloseReason   string
	Manifest      []byte
	Digest        integrity.Digest
	Root          integrity.Digest
	Previous      integrity.Digest
	Commitment    integrity.Digest
	SealedAt      time.Time
}

func ensureVaultBatchStream(db *sql.DB) error {
	var exists int
	if err := db.QueryRow("SELECT COUNT(*) FROM vault_batch_streams").Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	var id [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	_, err := db.Exec(`INSERT INTO vault_batch_streams(id,stream_id,last_commitment) VALUES(1,? ,?)`, hex.EncodeToString(id[:]), make([]byte, 32))
	return err
}

func (s *RawStore) VaultBatchState(ctx context.Context) (VaultBatchState, error) {
	var state VaultBatchState
	var oldest sql.NullInt64
	var pending uint64
	err := s.db.QueryRowContext(ctx, `SELECT stream_id,next_sequence,next_batch_no,last_sealed_batch_no,pending_upload_count,pending_upload_bytes,COALESCE(blocked_reason,''),
		(SELECT MIN(created_ns) FROM (SELECT closed_at_ns AS created_ns FROM vault_batch_seal_tasks UNION ALL
		 SELECT m.sealed_at_ns FROM vault_batch_uploads u JOIN vault_batch_manifests m USING(stream_id,batch_no) WHERE u.state IN ('pending','blocked')))
		FROM vault_batch_streams WHERE id=1`).Scan(&state.StreamID, &state.NextSequence, &state.NextBatch, &state.LastSealedBatch, &pending, &state.PendingBytes, &state.BlockedReason, &oldest)
	if err != nil {
		return VaultBatchState{}, err
	}
	if pending > math.MaxUint32 {
		state.PendingCount = math.MaxUint32
	} else {
		state.PendingCount = uint32(pending)
	}
	if oldest.Valid {
		state.OldestPendingAt = time.Unix(0, oldest.Int64).UTC()
	}
	return state, nil
}

func (s *RawStore) ConfigureVaultBatchStream(ctx context.Context, streamID string) error {
	if len(streamID) != 64 || strings.ToLower(streamID) != streamID {
		return errors.New("stream ID must be 64 lowercase hexadecimal characters")
	}
	if decoded, err := hex.DecodeString(streamID); err != nil || len(decoded) != 32 {
		return errors.New("invalid stream ID")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var members, batches int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_batch_members").Scan(&members); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM vault_batch_manifests").Scan(&batches); err != nil {
		return err
	}
	if members != 0 || batches != 0 {
		return errors.New("version-2 stream identity is immutable after acceptance begins")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE vault_batch_streams SET stream_id=? WHERE id=1`, streamID); err != nil {
		return err
	}
	return tx.Commit()
}

func closeVaultBatchTx(ctx context.Context, tx *sql.Tx, streamID string, batchNo uint64, at time.Time, reason string) error {
	res, err := tx.ExecContext(ctx, `UPDATE vault_batch_state SET closed=1,closed_at_ns=?,close_reason=? WHERE stream_id=? AND batch_no=? AND closed=0`, at.UTC().UnixNano(), reason, streamID, batchNo)
	if err != nil {
		return err
	}
	changed, err := res.RowsAffected()
	if err != nil || changed == 0 {
		return err
	}
	var first, high, count, size uint64
	var started int64
	if err = tx.QueryRowContext(ctx, `SELECT first_sequence,high_water_sequence,member_count,encoded_bytes,started_at_ns FROM vault_batch_state WHERE stream_id=? AND batch_no=?`, streamID, batchNo).Scan(&first, &high, &count, &size, &started); err != nil {
		return err
	}
	if count == 0 || high-first+1 != count {
		return errors.New("refusing to close an empty or noncontiguous evidence batch")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_batch_seal_tasks(stream_id,batch_no,first_sequence,high_water_sequence,member_count,encoded_bytes,started_at_ns,closed_at_ns,close_reason) VALUES(?,?,?,?,?,?,?,?,?)`, streamID, batchNo, first, high, count, size, started, at.UTC().UnixNano(), reason); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE vault_batch_streams SET next_batch_no=next_batch_no+1,pending_upload_count=pending_upload_count+1,pending_upload_bytes=pending_upload_bytes+? WHERE id=1 AND stream_id=?`, int64(size)+vaultBatchHeaderSize, streamID); err != nil {
		return err
	}
	return nil
}

func sealDueVaultBatchesTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	var streamID string
	var batchNo uint64
	err := tx.QueryRowContext(ctx, `SELECT stream_id,batch_no FROM vault_batch_state WHERE closed=0 AND started_at_ns<=? ORDER BY batch_no LIMIT 1`, now.UTC().Add(-vaultBatchMaxAge).UnixNano()).Scan(&streamID, &batchNo)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return closeVaultBatchTx(ctx, tx, streamID, batchNo, now, "age")
}

type BatchUploader interface {
	UploadBatch(context.Context, string, uint64, integrity.Digest, []byte) (string, error)
}

func (s *RawStore) SealedBatch(ctx context.Context, batchNo uint64) (SealedBatch, error) {
	state, err := s.VaultBatchState(ctx)
	if err != nil {
		return SealedBatch{}, err
	}
	var out SealedBatch
	var started, closed, sealed int64
	var digest, root, previous, commitment []byte
	err = s.db.QueryRowContext(ctx, `SELECT stream_id,batch_no,first_sequence,high_water_sequence,member_count,started_at_ns,closed_at_ns,close_reason,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns FROM vault_batch_manifests WHERE stream_id=? AND batch_no=?`, state.StreamID, batchNo).Scan(&out.StreamID, &out.BatchNo, &out.FirstSequence, &out.LastSequence, &out.EntryCount, &started, &closed, &out.CloseReason, &out.Manifest, &digest, &root, &previous, &commitment, &sealed)
	if err != nil {
		return SealedBatch{}, err
	}
	out.StartedAt, out.ClosedAt, out.SealedAt = time.Unix(0, started).UTC(), time.Unix(0, closed).UTC(), time.Unix(0, sealed).UTC()
	for src, dst := range map[*[]byte]*integrity.Digest{&digest: &out.Digest, &root: &out.Root, &previous: &out.Previous, &commitment: &out.Commitment} {
		if len(*src) != len(dst) {
			return SealedBatch{}, errors.New("corrupt sealed batch digest")
		}
		copy(dst[:], *src)
	}
	return out, nil
}

func streamVaultBatch(ctx context.Context, tx *sql.Tx, streamID string, batchNo uint64, consume func([]byte) error) (uint64, uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.accept_seq,m.accepted_at_ns,r.raw_id,r.received_at,r.transport,r.source_ip,r.source_port,r.metadata_json,r.payload
		FROM vault_batch_members m JOIN raw_events r USING(raw_id) WHERE m.stream_id=? AND m.batch_no=? ORDER BY m.accept_seq`, streamID, batchNo)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	streamBytes, err := hex.DecodeString(streamID)
	if err != nil || len(streamBytes) != 32 {
		return 0, 0, errors.New("stored v2 stream ID is invalid")
	}
	var stream integrity.Digest
	copy(stream[:], streamBytes)
	var count, size, high uint64
	for rows.Next() {
		var seq uint64
		var accepted int64
		var rawID, received, transport, sourceIP, metadataJSON string
		var sourcePort int
		var payload []byte
		if err := rows.Scan(&seq, &accepted, &rawID, &received, &transport, &sourceIP, &sourcePort, &metadataJSON, &payload); err != nil {
			return 0, 0, err
		}
		if (count == 0 && seq == 0) || (count > 0 && (high == math.MaxUint64 || seq != high+1)) {
			return 0, 0, errors.New("v2 batch sequence is not contiguous")
		}
		receivedAt, err := time.Parse(time.RFC3339Nano, received)
		if err != nil || sourcePort < 0 || sourcePort > math.MaxUint16 {
			return 0, 0, errors.New("invalid accepted raw metadata in v2 batch")
		}
		var metadata model.ReceiveMetadata
		if err = json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return 0, 0, err
		}
		entry, err := integrity.EncodeBatchEntry(integrity.BatchEntry{Entry: integrity.Entry{
			StreamID: stream, AcceptSeq: seq, AcceptedAt: accepted, RawID: rawID, ReceivedAt: receivedAt.UnixNano(),
			Transport: transport, SourceIP: sourceIP, SourcePort: uint16(sourcePort), Metadata: metadata, Payload: payload,
		}, BatchNo: batchNo})
		if err != nil {
			return 0, 0, err
		}
		if err = consume(entry); err != nil {
			return 0, 0, err
		}
		count++
		size += uint64(len(entry) + 4)
		high = seq
	}
	if err = rows.Err(); err != nil {
		return 0, 0, err
	}
	return count, size, nil
}

func (s *RawStore) processNextVaultBatchSeal(ctx context.Context, now time.Time) (bool, error) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	var streamID, reason string
	var batchNo, first, high, expectedCount, expectedBytes uint64
	var started, closed int64
	err := s.db.QueryRowContext(ctx, `SELECT stream_id,batch_no,first_sequence,high_water_sequence,member_count,encoded_bytes,started_at_ns,closed_at_ns,close_reason FROM vault_batch_seal_tasks ORDER BY batch_no LIMIT 1`).Scan(&streamID, &batchNo, &first, &high, &expectedCount, &expectedBytes, &started, &closed, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var lastSealed int64
	var previousBytes []byte
	if err = s.db.QueryRowContext(ctx, `SELECT last_sealed_batch_no,last_commitment FROM vault_batch_streams WHERE id=1 AND stream_id=?`, streamID).Scan(&lastSealed, &previousBytes); err != nil {
		return false, err
	}
	if int64(batchNo) != lastSealed+1 || len(previousBytes) != 32 {
		return false, errors.New("v2 seal task is not in predecessor order")
	}
	var previous integrity.Digest
	copy(previous[:], previousBytes)
	var stream integrity.Digest
	streamBytes, _ := hex.DecodeString(streamID)
	copy(stream[:], streamBytes)
	var accumulator integrity.MerkleAccumulator
	var body bytes.Buffer
	body.Grow(int(expectedBytes) + int(vaultBatchHeaderSize))
	readTx, err := s.readerDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	count, size, scanErr := streamVaultBatch(ctx, readTx, streamID, batchNo, func(entry []byte) error {
		if int64(body.Len()+len(entry)+4)+vaultBatchHeaderSize > vaultBatchHardBytes {
			return errors.New("v2 manifest exceeds hard byte limit")
		}
		if err := accumulator.AddEntry(entry); err != nil {
			return err
		}
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(entry)))
		body.Write(n[:])
		body.Write(entry)
		return nil
	})
	if scanErr != nil {
		_ = readTx.Rollback()
		return false, scanErr
	}
	if err = readTx.Commit(); err != nil {
		return false, err
	}
	if count != expectedCount || size != expectedBytes || count == 0 || high-first+1 != count {
		return false, errors.New("v2 closed-batch counters or sequence range mismatch")
	}
	root := accumulator.Root()
	prefix, err := integrity.BatchManifestPrefix(stream, batchNo, first, high, count, started, closed, previous, root)
	if err != nil {
		return false, err
	}
	manifest := append(prefix, body.Bytes()...)
	if len(manifest) > vaultBatchHardBytes {
		return false, errors.New("v2 manifest exceeds hard byte limit")
	}
	digest := integrity.ManifestDigest(manifest)
	commitment, err := integrity.BatchCommitmentFields(stream, batchNo, first, high, count, started, closed, previous, root, digest)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var currentCount, currentBytes, currentHigh uint64
	var currentClosed int
	if err = tx.QueryRowContext(ctx, `SELECT member_count,encoded_bytes,high_water_sequence,closed FROM vault_batch_state WHERE stream_id=? AND batch_no=?`, streamID, batchNo).Scan(&currentCount, &currentBytes, &currentHigh, &currentClosed); err != nil {
		return false, err
	}
	if currentCount != expectedCount || currentBytes != expectedBytes || currentHigh != high || currentClosed != 1 {
		return false, errors.New("v2 batch counters changed while sealing")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_batch_manifests(stream_id,batch_no,first_sequence,high_water_sequence,member_count,started_at_ns,closed_at_ns,close_reason,manifest,manifest_digest,merkle_root,previous_commitment,commitment,sealed_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, streamID, batchNo, first, high, count, started, closed, reason, manifest, digest[:], root[:], previous[:], commitment[:], now.UTC().UnixNano()); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_batch_uploads(stream_id,batch_no,state,retry_at_ns) VALUES(?,?,'pending',?)`, streamID, batchNo, now.UTC().UnixNano()); err != nil {
		return false, err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE vault_batch_streams SET last_sealed_batch_no=?,last_commitment=?,pending_upload_bytes=pending_upload_bytes+? WHERE id=1 AND last_sealed_batch_no=? AND stream_id=?`, batchNo, commitment[:], int64(len(manifest))-int64(expectedBytes)-vaultBatchHeaderSize, lastSealed, streamID)
	if err != nil {
		return false, err
	}
	changed, err := updated.RowsAffected()
	if err != nil || changed != 1 {
		return false, errors.New("v2 stream head changed while sealing")
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM vault_batch_seal_tasks WHERE stream_id=? AND batch_no=?`, streamID, batchNo); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *RawStore) processVaultBatchSealTasks(ctx context.Context, now time.Time) error {
	for {
		worked, err := s.processNextVaultBatchSeal(ctx, now)
		if err != nil || !worked {
			return err
		}
	}
}

func (s *RawStore) SealDueVaultBatches(ctx context.Context, now time.Time) error {
	s.writeMu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	if err = sealDueVaultBatchesTx(ctx, tx, now.UTC()); err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	if err = tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return err
	}
	s.writeMu.Unlock()
	return s.processVaultBatchSealTasks(ctx, now.UTC())
}

func (s *RawStore) RunVaultBatchSealer(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("batch sealer poll interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.SealDueVaultBatches(ctx, time.Now().UTC()); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *RawStore) ProcessNextVaultBatchUpload(ctx context.Context, uploader BatchUploader, now time.Time) (bool, error) {
	if uploader == nil {
		return false, errors.New("batch uploader is required")
	}
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()
	var streamID string
	var batchNo uint64
	var attempts uint
	var retryAt int64
	var state string
	err := s.db.QueryRowContext(ctx, `SELECT stream_id,batch_no,attempts,retry_at_ns,state FROM vault_batch_uploads WHERE state IN ('pending','blocked') ORDER BY batch_no LIMIT 1`).Scan(&streamID, &batchNo, &attempts, &retryAt, &state)
	if errors.Is(err, sql.ErrNoRows) || state == "blocked" {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if retryAt > now.UTC().UnixNano() {
		return false, nil
	}
	var body, digestBytes, commitmentBytes []byte
	if err = s.db.QueryRowContext(ctx, `SELECT manifest,manifest_digest,commitment FROM vault_batch_manifests WHERE stream_id=? AND batch_no=?`, streamID, batchNo).Scan(&body, &digestBytes, &commitmentBytes); err != nil {
		return false, err
	}
	if len(digestBytes) != 32 || len(commitmentBytes) != 32 {
		return false, errors.New("stored v2 manifest digest mismatch")
	}
	var digest integrity.Digest
	copy(digest[:], digestBytes)
	if integrity.ManifestDigest(body) != digest {
		return false, errors.New("stored v2 manifest digest mismatch")
	}
	_, uploadErr := uploader.UploadBatch(ctx, streamID, batchNo, digest, body)
	attempts++
	if uploadErr == nil {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return true, err
		}
		defer tx.Rollback()
		updated, err := tx.ExecContext(ctx, `UPDATE vault_batch_uploads SET state='uploaded',attempts=?,last_error=NULL,witness_receipt='verified' WHERE stream_id=? AND batch_no=? AND state='pending'`, attempts, streamID, batchNo)
		if err != nil {
			return true, err
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return true, err
		}
		if changed == 1 {
			budget, err := tx.ExecContext(ctx, `UPDATE vault_batch_streams SET pending_upload_count=pending_upload_count-1,pending_upload_bytes=pending_upload_bytes-(SELECT length(manifest) FROM vault_batch_manifests WHERE stream_id=? AND batch_no=?) WHERE id=1 AND stream_id=? AND pending_upload_count>0 AND pending_upload_bytes >= (SELECT length(manifest) FROM vault_batch_manifests WHERE stream_id=? AND batch_no=?)`, streamID, batchNo, streamID, streamID, batchNo)
			if err != nil {
				return true, err
			}
			n, err := budget.RowsAffected()
			if err != nil || n != 1 {
				return true, errors.New("v2 upload backlog counters are inconsistent")
			}
		}
		return true, tx.Commit()
	}
	var permanent interface{ Permanent() bool }
	if errors.As(uploadErr, &permanent) && permanent.Permanent() {
		_, updateErr := s.db.ExecContext(ctx, `UPDATE vault_batch_uploads SET state='blocked',attempts=?,last_error='permanent witness rejection' WHERE stream_id=? AND batch_no=? AND state='pending'`, attempts, streamID, batchNo)
		if updateErr != nil {
			return true, updateErr
		}
		_, _ = s.db.ExecContext(ctx, `UPDATE vault_batch_streams SET blocked_reason='permanent witness rejection for batch '||? WHERE id=1 AND stream_id=?`, batchNo, streamID)
		return true, uploadErr
	}
	delay := time.Second << min(attempts-1, 6)
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	_, err = s.db.ExecContext(ctx, `UPDATE vault_batch_uploads SET attempts=?,retry_at_ns=?,last_error='witness upload failed' WHERE stream_id=? AND batch_no=? AND state='pending'`, attempts, now.Add(delay).UTC().UnixNano(), streamID, batchNo)
	if err != nil {
		return true, err
	}
	return true, uploadErr
}

func (s *RawStore) RunVaultBatchUploader(ctx context.Context, uploader BatchUploader, interval time.Duration) error {
	if uploader == nil || interval <= 0 {
		return errors.New("batch uploader and positive poll interval are required")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		worked, err := s.ProcessNextVaultBatchUpload(ctx, uploader, time.Now().UTC())
		if err == nil && worked {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err != nil && ctx.Err() == nil {
			log.Print("witness batch upload failed; durable retry remains queued")
		}
	}
}
