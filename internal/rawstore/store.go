package rawstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"deinopis/internal/integrity"
	"deinopis/internal/model"
	"github.com/shirou/gopsutil/disk"

	_ "modernc.org/sqlite"
)

// RawStore provides durable, cryptographically verified storage for raw byte streams.
type RawStore struct {
	db       *sql.DB
	readerDB *sql.DB
	dbPath   string
	writeMu  sync.Mutex
	sealMu   sync.Mutex
	uploadMu sync.Mutex
	counter  atomic.Uint64
}

// New opens an SQLite database, creates required tables and enables WAL mode.
func New(dbPath string) (*RawStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open raw store sqlite db: %w", err)
	}

	// Single connection serialization for SQLite concurrency safety
	db.SetMaxOpenConns(1)

	// Set pragmas for crash resilience and concurrency
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set pragma %q: %w", p, err)
		}
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureVaultStream(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureVaultBatchStream(db); err != nil {
		db.Close()
		return nil, err
	}
	readerDB := db
	if dbPath != ":memory:" {
		readerDB, err = sql.Open("sqlite", dbPath)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open vault snapshot reader: %w", err)
		}
		readerDB.SetMaxOpenConns(1)
		if _, err = readerDB.Exec("PRAGMA query_only=ON"); err != nil {
			readerDB.Close()
			db.Close()
			return nil, fmt.Errorf("configure vault snapshot reader: %w", err)
		}
	}

	return &RawStore{db: db, readerDB: readerDB, dbPath: dbPath}, nil
}

// CurrentSchemaVersion tracks the active database schema migration version.
const CurrentSchemaVersion = 9

func initSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin schema migration: %w", err)
	}
	defer tx.Rollback()
	if err := migrateSchema(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateSchema(db sqlExecutor) error {
	var version int
	if err := db.QueryRow("PRAGMA user_version;").Scan(&version); err != nil {
		return fmt.Errorf("failed to read user_version: %w", err)
	}
	if version > CurrentSchemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, CurrentSchemaVersion)
	}

	if version < 1 {
		schema := `
		CREATE TABLE IF NOT EXISTS raw_events (
			raw_id TEXT PRIMARY KEY,
			raw_sha256 TEXT NOT NULL,
			byte_count INTEGER NOT NULL,
			transport TEXT NOT NULL,
			source_ip TEXT NOT NULL,
			source_port INTEGER NOT NULL DEFAULT 0,
			received_at TEXT NOT NULL,
			payload BLOB NOT NULL,
			status TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_raw_events_sha ON raw_events(raw_sha256);
		CREATE INDEX IF NOT EXISTS idx_raw_events_status ON raw_events(status);

		CREATE TABLE IF NOT EXISTS quarantine (
			quarantine_id TEXT PRIMARY KEY,
			raw_id TEXT NOT NULL,
			record_ordinal INTEGER NOT NULL DEFAULT 0,
			raw_sha256 TEXT NOT NULL,
			stage TEXT NOT NULL,
			reason TEXT NOT NULL,
			error_details TEXT,
			raw_preview TEXT NOT NULL,
			candidate_packs TEXT,
			replay_eligible INTEGER NOT NULL,
			quarantined_at TEXT NOT NULL,
			replayed_at TEXT,
			FOREIGN KEY (raw_id) REFERENCES raw_events(raw_id)
		);
		CREATE INDEX IF NOT EXISTS idx_quarantine_raw ON quarantine(raw_id);
		CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON quarantine(reason);

		CREATE TABLE IF NOT EXISTS normalized_events (
			event_id TEXT PRIMARY KEY,
			raw_id TEXT NOT NULL,
			record_ordinal INTEGER NOT NULL DEFAULT 0,
			source_category TEXT NOT NULL,
			source_id TEXT NOT NULL,
			severity_id TEXT,
			activity_name TEXT,
			message TEXT,
			event_time INTEGER NOT NULL,
			received_time TEXT NOT NULL,
			event_json TEXT NOT NULL,
			event_sha256 TEXT NOT NULL DEFAULT '',
			FOREIGN KEY (raw_id) REFERENCES raw_events(raw_id)
		);
		CREATE INDEX IF NOT EXISTS idx_norm_cat ON normalized_events(source_category);
		CREATE INDEX IF NOT EXISTS idx_norm_time ON normalized_events(event_time);
		`
		if _, err := db.Exec(schema); err != nil {
			return fmt.Errorf("failed to initialize raw store schema v2: %w", err)
		}
		version = 2
	}

	if version < 2 {
		migrations := []string{
			"ALTER TABLE raw_events ADD COLUMN source_port INTEGER NOT NULL DEFAULT 0;",
			"ALTER TABLE quarantine ADD COLUMN record_ordinal INTEGER NOT NULL DEFAULT 0;",
			"ALTER TABLE normalized_events ADD COLUMN record_ordinal INTEGER NOT NULL DEFAULT 0;",
		}
		for _, m := range migrations {
			if _, err := db.Exec(m); err != nil {
				return fmt.Errorf("failed to apply schema migration %q: %w", m, err)
			}
		}
		version = 2
	}

	if version < 3 {
		if _, err := db.Exec("ALTER TABLE raw_events ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';"); err != nil {
			return fmt.Errorf("failed to add raw event metadata column: %w", err)
		}
		version = 3
	}

	if version < 4 {
		// A process can restart after a child was quarantined but before its parent
		// raw status was finalized. Keep that recovery replay idempotent.
		if _, err := db.Exec(`DELETE FROM quarantine WHERE rowid NOT IN (
			SELECT MIN(rowid) FROM quarantine GROUP BY raw_id, record_ordinal, stage, reason
		);`); err != nil {
			return fmt.Errorf("failed to deduplicate quarantine rows: %w", err)
		}
		if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_quarantine_child
			ON quarantine(raw_id, record_ordinal, stage, reason);`); err != nil {
			return fmt.Errorf("failed to create quarantine child index: %w", err)
		}
		version = 4
	}

	if version < 5 {
		vaultSchema := `
		CREATE TABLE IF NOT EXISTS vault_streams (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			stream_id TEXT NOT NULL UNIQUE,
			epoch_start INTEGER NOT NULL,
			window_seconds INTEGER NOT NULL CHECK (window_seconds = 300),
			next_sequence INTEGER NOT NULL CHECK (next_sequence > 0),
			last_sealed_slot INTEGER NOT NULL DEFAULT -1,
			last_commitment BLOB NOT NULL CHECK (length(last_commitment) = 32)
		);
		CREATE TABLE IF NOT EXISTS vault_members (
			stream_id TEXT NOT NULL REFERENCES vault_streams(stream_id),
			accept_seq INTEGER NOT NULL CHECK (accept_seq > 0),
			raw_id TEXT NOT NULL UNIQUE REFERENCES raw_events(raw_id) ON DELETE RESTRICT,
			slot INTEGER NOT NULL CHECK (slot >= 0),
			accepted_at_ns INTEGER NOT NULL CHECK (accepted_at_ns >= 0),
			entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
			PRIMARY KEY (stream_id, accept_seq)
		);
		CREATE INDEX IF NOT EXISTS idx_vault_members_window ON vault_members(stream_id, slot, accept_seq);
		CREATE TABLE IF NOT EXISTS vault_windows (
			stream_id TEXT NOT NULL REFERENCES vault_streams(stream_id),
			slot INTEGER NOT NULL CHECK (slot >= 0),
			coverage INTEGER NOT NULL CHECK (coverage BETWEEN 0 AND 2),
			entry_count INTEGER NOT NULL CHECK (entry_count >= 0),
			high_water_sequence INTEGER NOT NULL CHECK (high_water_sequence >= 0),
			manifest BLOB NOT NULL,
			manifest_digest BLOB NOT NULL CHECK (length(manifest_digest) = 32),
			merkle_root BLOB NOT NULL CHECK (length(merkle_root) = 32),
			previous_commitment BLOB NOT NULL CHECK (length(previous_commitment) = 32),
			commitment BLOB NOT NULL CHECK (length(commitment) = 32),
			sealed_at_ns INTEGER NOT NULL,
			PRIMARY KEY (stream_id, slot)
		);
		CREATE TABLE IF NOT EXISTS vault_uploads (
			stream_id TEXT NOT NULL,
			slot INTEGER NOT NULL,
			state TEXT NOT NULL CHECK(state IN ('pending','uploaded')),
			attempts INTEGER NOT NULL DEFAULT 0,
			retry_at_ns INTEGER NOT NULL,
			last_error TEXT,
			witness_receipt TEXT,
			PRIMARY KEY(stream_id,slot),
			FOREIGN KEY(stream_id,slot) REFERENCES vault_windows(stream_id,slot)
		);
		CREATE INDEX IF NOT EXISTS idx_vault_uploads_pending ON vault_uploads(state,retry_at_ns);
		`
		if _, err := db.Exec(vaultSchema); err != nil {
			return fmt.Errorf("failed to create vault membership schema: %w", err)
		}
		version = 5
	}

	if version < 6 {
		// Keep acceptance limits O(1) and persist the close boundary independently
		// from the expensive manifest build so a restart cannot reopen a slot.
		if _, err := db.Exec(`CREATE TABLE vault_slot_state (
			stream_id TEXT NOT NULL REFERENCES vault_streams(stream_id),
			slot INTEGER NOT NULL CHECK(slot >= 0),
			member_count INTEGER NOT NULL CHECK(member_count >= 0),
			encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes >= 0),
			high_water_sequence INTEGER NOT NULL CHECK(high_water_sequence >= 0),
			closed INTEGER NOT NULL DEFAULT 0 CHECK(closed IN (0,1)),
			closed_at_ns INTEGER,
			PRIMARY KEY(stream_id,slot)
		);
		INSERT INTO vault_slot_state(stream_id,slot,member_count,encoded_bytes,high_water_sequence,closed,closed_at_ns)
		SELECT m.stream_id,m.slot,COUNT(*),SUM(m.entry_bytes+4),MAX(m.accept_seq),
			CASE WHEN EXISTS(SELECT 1 FROM vault_windows w WHERE w.stream_id=m.stream_id AND w.slot=m.slot) THEN 1 ELSE 0 END,
			(SELECT w.sealed_at_ns FROM vault_windows w WHERE w.stream_id=m.stream_id AND w.slot=m.slot)
		FROM vault_members m GROUP BY m.stream_id,m.slot;
		INSERT OR IGNORE INTO vault_slot_state(stream_id,slot,member_count,encoded_bytes,high_water_sequence,closed,closed_at_ns)
		SELECT stream_id,slot,entry_count,COALESCE((SELECT SUM(entry_bytes+4) FROM vault_members m WHERE m.stream_id=w.stream_id AND m.slot=w.slot),0),high_water_sequence,1,sealed_at_ns
		FROM vault_windows w;
		CREATE TABLE vault_seal_tasks (
			stream_id TEXT NOT NULL,
			slot INTEGER NOT NULL,
			coverage INTEGER NOT NULL CHECK(coverage BETWEEN 0 AND 2),
			member_count INTEGER NOT NULL CHECK(member_count >= 0),
			encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes >= 0),
			high_water_sequence INTEGER NOT NULL CHECK(high_water_sequence >= 0),
			closed_at_ns INTEGER NOT NULL,
			PRIMARY KEY(stream_id,slot),
			FOREIGN KEY(stream_id,slot) REFERENCES vault_slot_state(stream_id,slot)
		);
		PRAGMA user_version=6;`); err != nil {
			return fmt.Errorf("migrate vault counters and durable close state: %w", err)
		}
		version = 6
	}
	if version < 7 {
		if _, err := db.Exec(`CREATE TABLE vault_uploads_v7 (
			stream_id TEXT NOT NULL,
			slot INTEGER NOT NULL,
			state TEXT NOT NULL CHECK(state IN ('pending','uploaded','blocked')),
			attempts INTEGER NOT NULL DEFAULT 0,
			retry_at_ns INTEGER NOT NULL,
			last_error TEXT,
			witness_receipt TEXT,
			PRIMARY KEY(stream_id,slot),
			FOREIGN KEY(stream_id,slot) REFERENCES vault_windows(stream_id,slot)
		);
		INSERT INTO vault_uploads_v7 SELECT * FROM vault_uploads;
		DROP TABLE vault_uploads;
		ALTER TABLE vault_uploads_v7 RENAME TO vault_uploads;
		CREATE INDEX idx_vault_uploads_pending ON vault_uploads(state,retry_at_ns);
		PRAGMA user_version=7;`); err != nil {
			return fmt.Errorf("migrate permanent witness upload state: %w", err)
		}
		version = 7
	}
	if version < 8 {
		if _, err := db.Exec(`ALTER TABLE vault_streams ADD COLUMN pending_upload_count INTEGER NOT NULL DEFAULT 0 CHECK(pending_upload_count >= 0);
		ALTER TABLE vault_streams ADD COLUMN pending_upload_bytes INTEGER NOT NULL DEFAULT 0 CHECK(pending_upload_bytes >= 0);
		UPDATE vault_streams SET
			pending_upload_count=(SELECT COUNT(*) FROM vault_uploads WHERE state IN ('pending','blocked')),
			pending_upload_bytes=COALESCE((SELECT SUM(length(w.manifest)) FROM vault_uploads u JOIN vault_windows w USING(stream_id,slot) WHERE u.state IN ('pending','blocked')),0);
		PRAGMA user_version=8;`); err != nil {
			return fmt.Errorf("migrate bounded upload backlog counters: %w", err)
		}
		version = 8
	}
	if version < 9 {
		if _, err := db.Exec(`CREATE TABLE vault_batch_streams (
			id INTEGER PRIMARY KEY CHECK(id=1), format_version INTEGER NOT NULL DEFAULT 2 CHECK(format_version=2),
			stream_id TEXT NOT NULL UNIQUE, next_sequence INTEGER NOT NULL DEFAULT 1 CHECK(next_sequence>0),
			next_batch_no INTEGER NOT NULL DEFAULT 0 CHECK(next_batch_no>=0), last_sealed_batch_no INTEGER NOT NULL DEFAULT -1,
			last_commitment BLOB NOT NULL CHECK(length(last_commitment)=32), target_count INTEGER NOT NULL DEFAULT 30000 CHECK(target_count>0),
			soft_manifest_bytes INTEGER NOT NULL DEFAULT 16777216 CHECK(soft_manifest_bytes>0), max_age_ns INTEGER NOT NULL DEFAULT 300000000000 CHECK(max_age_ns>0),
			hard_count INTEGER NOT NULL DEFAULT 50000 CHECK(hard_count>0), hard_manifest_bytes INTEGER NOT NULL DEFAULT 67108864 CHECK(hard_manifest_bytes>0),
			pending_upload_count INTEGER NOT NULL DEFAULT 0 CHECK(pending_upload_count>=0), pending_upload_bytes INTEGER NOT NULL DEFAULT 0 CHECK(pending_upload_bytes>=0),
			blocked_reason TEXT
		);
		CREATE TABLE vault_batch_state (
			stream_id TEXT NOT NULL REFERENCES vault_batch_streams(stream_id), batch_no INTEGER NOT NULL CHECK(batch_no>=0),
			first_sequence INTEGER NOT NULL CHECK(first_sequence>0), high_water_sequence INTEGER NOT NULL CHECK(high_water_sequence>=first_sequence),
			member_count INTEGER NOT NULL CHECK(member_count>0), encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes>0),
			started_at_ns INTEGER NOT NULL CHECK(started_at_ns>=0), closed INTEGER NOT NULL DEFAULT 0 CHECK(closed IN (0,1)),
			closed_at_ns INTEGER, close_reason TEXT CHECK(close_reason IN ('count','bytes','age')),
			PRIMARY KEY(stream_id,batch_no)
		);
		CREATE TABLE vault_batch_members (
			stream_id TEXT NOT NULL REFERENCES vault_batch_streams(stream_id), accept_seq INTEGER NOT NULL CHECK(accept_seq>0),
			raw_id TEXT NOT NULL UNIQUE REFERENCES raw_events(raw_id) ON DELETE RESTRICT, batch_no INTEGER NOT NULL CHECK(batch_no>=0),
			accepted_at_ns INTEGER NOT NULL CHECK(accepted_at_ns>=0), entry_bytes INTEGER NOT NULL CHECK(entry_bytes>0),
			PRIMARY KEY(stream_id,accept_seq), FOREIGN KEY(stream_id,batch_no) REFERENCES vault_batch_state(stream_id,batch_no)
		);
		CREATE INDEX idx_vault_batch_members_order ON vault_batch_members(stream_id,batch_no,accept_seq);
		CREATE TABLE vault_batch_seal_tasks (
			stream_id TEXT NOT NULL, batch_no INTEGER NOT NULL, first_sequence INTEGER NOT NULL, high_water_sequence INTEGER NOT NULL,
			member_count INTEGER NOT NULL, encoded_bytes INTEGER NOT NULL, started_at_ns INTEGER NOT NULL,
			closed_at_ns INTEGER NOT NULL, close_reason TEXT NOT NULL CHECK(close_reason IN ('count','bytes','age')),
			PRIMARY KEY(stream_id,batch_no), FOREIGN KEY(stream_id,batch_no) REFERENCES vault_batch_state(stream_id,batch_no)
		);
		CREATE TABLE vault_batch_manifests (
			stream_id TEXT NOT NULL REFERENCES vault_batch_streams(stream_id), batch_no INTEGER NOT NULL, first_sequence INTEGER NOT NULL,
			high_water_sequence INTEGER NOT NULL, member_count INTEGER NOT NULL, started_at_ns INTEGER NOT NULL, closed_at_ns INTEGER NOT NULL,
			close_reason TEXT NOT NULL CHECK(close_reason IN ('count','bytes','age')), manifest BLOB NOT NULL,
			manifest_digest BLOB NOT NULL CHECK(length(manifest_digest)=32), merkle_root BLOB NOT NULL CHECK(length(merkle_root)=32),
			previous_commitment BLOB NOT NULL CHECK(length(previous_commitment)=32), commitment BLOB NOT NULL CHECK(length(commitment)=32),
			sealed_at_ns INTEGER NOT NULL, PRIMARY KEY(stream_id,batch_no)
		);
		CREATE TABLE vault_batch_uploads (
			stream_id TEXT NOT NULL, batch_no INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','uploaded','blocked')),
			attempts INTEGER NOT NULL DEFAULT 0, retry_at_ns INTEGER NOT NULL, last_error TEXT, witness_receipt TEXT,
			PRIMARY KEY(stream_id,batch_no), FOREIGN KEY(stream_id,batch_no) REFERENCES vault_batch_manifests(stream_id,batch_no)
		);
		CREATE INDEX idx_vault_batch_uploads_pending ON vault_batch_uploads(state,retry_at_ns);
		PRAGMA user_version=9;`); err != nil {
			return fmt.Errorf("create version-2 batch integrity schema: %w", err)
		}
		version = 9
	}

	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d;", CurrentSchemaVersion)); err != nil {
		return fmt.Errorf("failed to set user_version: %w", err)
	}

	return nil
}

type sqlExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Store commits verbatim raw bytes, calculates SHA-256 digest, and assigns an immutable raw ID.
func (s *RawStore) Store(ctx context.Context, rec model.IngestedRecord) (*model.RawEvent, error) {
	if s.dbPath != ":memory:" {
		usage, err := disk.Usage(filepath.Dir(s.dbPath))
		if err != nil {
			return nil, fmt.Errorf("check raw store disk watermark: %w", err)
		}
		if usage.UsedPercent >= 85 {
			return nil, errors.New("raw store disk watermark reached; raw acceptance refused")
		}
	}
	s.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			s.writeMu.Unlock()
		}
	}()

	if rec.ReceivedAt.IsZero() {
		rec.ReceivedAt = time.Now().UTC()
	}

	hash := sha256.Sum256(rec.RawBytes)
	shaHex := hex.EncodeToString(hash[:])
	acceptedAt := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin raw acceptance transaction: %w", err)
	}
	defer tx.Rollback()
	if err = sealDueVaultBatchesTx(ctx, tx, acceptedAt); err != nil {
		return nil, fmt.Errorf("close expired evidence batches: %w", err)
	}
	if rec.Transport == "file" && rec.Metadata.FileID != "" {
		var existingID, existingHash string
		err = tx.QueryRowContext(ctx, `SELECT raw_id, raw_sha256 FROM raw_events
			WHERE json_extract(metadata_json, '$.file_id')=? AND COALESCE(json_extract(metadata_json, '$.file_offset'),0)=?
			LIMIT 1`, rec.Metadata.FileID, rec.Metadata.FileOffset).Scan(&existingID, &existingHash)
		if err == nil {
			if existingHash != shaHex {
				return nil, fmt.Errorf("file spool identity %s at offset %d was already committed with different bytes", rec.Metadata.FileID, rec.Metadata.FileOffset)
			}
			if err = tx.Rollback(); err != nil {
				return nil, err
			}
			event, retrieveErr := s.Retrieve(ctx, existingID)
			if retrieveErr != nil {
				return nil, retrieveErr
			}
			event.Duplicate = true
			return event, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("check file spool replay identity: %w", err)
		}
	}
	var streamID string
	var sequence, nextBatch uint64
	var hardCount, hardBytes, softBytes, targetCount, pendingCount, pendingBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT stream_id,next_sequence,next_batch_no,hard_count,hard_manifest_bytes,soft_manifest_bytes,target_count,pending_upload_count,pending_upload_bytes FROM vault_batch_streams WHERE id=1`).Scan(&streamID, &sequence, &nextBatch, &hardCount, &hardBytes, &softBytes, &targetCount, &pendingCount, &pendingBytes); err != nil {
		return nil, err
	}
	if sequence == 0 || sequence >= math.MaxInt64 {
		return nil, errors.New("vault acceptance sequence exhausted")
	}
	// The process-local counter is only an ID suffix; durable order is next_sequence.
	seq := s.counter.Add(1)
	var randBytes [4]byte
	if _, err = rand.Read(randBytes[:]); err != nil {
		return nil, fmt.Errorf("create raw ID entropy: %w", err)
	}
	rawID := fmt.Sprintf("deinopis-raw-%s-%06d-%x", rec.ReceivedAt.Format("20060102"), seq, randBytes)

	event := &model.RawEvent{
		RawID:      rawID,
		RawSHA256:  shaHex,
		ByteCount:  len(rec.RawBytes),
		Transport:  rec.Transport,
		SourceIP:   rec.SourceIP,
		SourcePort: rec.SourcePort,
		Metadata:   rec.Metadata,
		ReceivedAt: rec.ReceivedAt,
		Payload:    rec.RawBytes,
		Status:     model.StatusAccepted,
	}
	if rec.SourcePort < 0 || rec.SourcePort > math.MaxUint16 {
		return nil, errors.New("source port is outside the integrity encoding range")
	}
	streamBytes, err := hex.DecodeString(streamID)
	if err != nil || len(streamBytes) != 32 {
		return nil, errors.New("stored v2 stream ID is invalid")
	}
	var streamDigest integrity.Digest
	copy(streamDigest[:], streamBytes)
	var batchNo, batchCount, batchBytes, firstSequence, highWater uint64
	var startedAt int64
	activeBatch := true
	batchNo = nextBatch
	err = tx.QueryRowContext(ctx, `SELECT member_count,encoded_bytes,first_sequence,high_water_sequence,started_at_ns FROM vault_batch_state WHERE stream_id=? AND batch_no=? AND closed=0`, streamID, batchNo).Scan(&batchCount, &batchBytes, &firstSequence, &highWater, &startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		activeBatch = false
	} else if err != nil {
		return nil, err
	}
	encodeRecord := func(batch uint64) ([]byte, error) {
		return integrity.EncodeBatchEntry(integrity.BatchEntry{Entry: integrity.Entry{StreamID: streamDigest, AcceptSeq: sequence,
			AcceptedAt: acceptedAt.UnixNano(), RawID: rawID, ReceivedAt: rec.ReceivedAt.UnixNano(), Transport: rec.Transport,
			SourceIP: rec.SourceIP, SourcePort: uint16(rec.SourcePort), Metadata: rec.Metadata, Payload: rec.RawBytes}, BatchNo: batch})
	}
	encodedEntry, err := encodeRecord(batchNo)
	if err != nil {
		return nil, fmt.Errorf("raw record is outside the v2 vault contract: %w", err)
	}
	entryBytes := int64(len(encodedEntry) + 4)
	if entryBytes+vaultBatchHeaderSize > hardBytes || hardCount <= 0 || targetCount <= 0 || softBytes <= 0 {
		return nil, errors.New("raw record exceeds the v2 batch hard manifest limit")
	}
	if activeBatch && batchCount > 0 && (int64(batchBytes)+entryBytes+vaultBatchHeaderSize > softBytes || batchCount >= uint64(hardCount) || int64(batchBytes)+entryBytes+vaultBatchHeaderSize > hardBytes) {
		if err = closeVaultBatchTx(ctx, tx, streamID, batchNo, acceptedAt, "bytes"); err != nil {
			return nil, err
		}
		if err = tx.QueryRowContext(ctx, `SELECT pending_upload_count,pending_upload_bytes FROM vault_batch_streams WHERE id=1`).Scan(&pendingCount, &pendingBytes); err != nil {
			return nil, err
		}
		batchNo++
		batchCount, batchBytes, firstSequence, highWater, startedAt = 0, 0, 0, 0, 0
		activeBatch = false
		encodedEntry, err = encodeRecord(batchNo)
		if err != nil {
			return nil, err
		}
		entryBytes = int64(len(encodedEntry) + 4)
	}
	if batchCount+1 > uint64(hardCount) || entryBytes+vaultBatchHeaderSize > hardBytes {
		return nil, errors.New("v2 batch hard member or byte limit reached; raw acceptance refused")
	}
	var oldest sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT MIN(closed_ns) FROM (
		SELECT closed_at_ns AS closed_ns FROM vault_batch_seal_tasks
		UNION ALL SELECT m.sealed_at_ns FROM vault_batch_uploads u JOIN vault_batch_manifests m USING(stream_id,batch_no) WHERE u.state IN ('pending','blocked')
	)`).Scan(&oldest); err != nil {
		return nil, err
	}
	if oldest.Valid && acceptedAt.Sub(time.Unix(0, oldest.Int64)) > maxPendingAge {
		return nil, errors.New("v2 witness backlog exceeded its age budget; raw acceptance refused")
	}
	if pendingCount >= maxPendingUploads || pendingBytes+int64(batchBytes)+entryBytes+vaultBatchHeaderSize > maxPendingBytes {
		return nil, errors.New("v2 witness backlog reached its count or byte budget; raw acceptance refused")
	}

	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode receive metadata: %w", err)
	}
	query := `
	INSERT INTO raw_events (raw_id, raw_sha256, byte_count, transport, source_ip, source_port, metadata_json, received_at, payload, status)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	if _, err = tx.ExecContext(ctx, query,
		event.RawID,
		event.RawSHA256,
		event.ByteCount,
		event.Transport,
		event.SourceIP,
		event.SourcePort,
		string(metadataJSON),
		event.ReceivedAt.Format(time.RFC3339Nano),
		event.Payload,
		string(event.Status),
	); err != nil {
		return nil, fmt.Errorf("failed to persist raw record %s: %w", rawID, err)
	}
	if !activeBatch {
		firstSequence, highWater, batchCount, batchBytes, startedAt = sequence, sequence, 1, uint64(entryBytes), acceptedAt.UnixNano()
		if _, err = tx.ExecContext(ctx, `INSERT INTO vault_batch_state(stream_id,batch_no,first_sequence,high_water_sequence,member_count,encoded_bytes,started_at_ns) VALUES(?,?,?,?,1,?,?)`, streamID, batchNo, sequence, sequence, entryBytes, startedAt); err != nil {
			return nil, fmt.Errorf("failed to initialize v2 batch counters: %w", err)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO vault_batch_members(stream_id,accept_seq,raw_id,batch_no,accepted_at_ns,entry_bytes) VALUES(?,?,?,?,?,?)`, streamID, sequence, rawID, batchNo, acceptedAt.UnixNano(), len(encodedEntry)); err != nil {
		return nil, fmt.Errorf("failed to persist v2 batch membership for %s: %w", rawID, err)
	}
	if activeBatch {
		if _, err = tx.ExecContext(ctx, `UPDATE vault_batch_state SET member_count=member_count+1,encoded_bytes=encoded_bytes+?,high_water_sequence=? WHERE stream_id=? AND batch_no=? AND closed=0`, entryBytes, sequence, streamID, batchNo); err != nil {
			return nil, fmt.Errorf("failed to update v2 batch counters: %w", err)
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE vault_batch_streams SET next_sequence=? WHERE id=1", sequence+1); err != nil {
		return nil, fmt.Errorf("failed to advance vault sequence: %w", err)
	}
	newCount, newBytes := batchCount, batchBytes
	if activeBatch {
		newCount++
		newBytes += uint64(entryBytes)
	} else {
		newCount = 1
		newBytes = uint64(entryBytes)
	}
	closeReason := ""
	if newCount >= uint64(targetCount) || newCount >= uint64(hardCount) {
		closeReason = "count"
	} else if int64(newBytes)+vaultBatchHeaderSize >= softBytes || int64(newBytes)+vaultBatchHeaderSize >= hardBytes {
		closeReason = "bytes"
	}
	if closeReason != "" {
		if err = closeVaultBatchTx(ctx, tx, streamID, batchNo, acceptedAt, closeReason); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit raw acceptance and membership: %w", err)
	}
	s.writeMu.Unlock()
	writeLocked = false
	if err = s.processVaultBatchSealTasks(ctx, acceptedAt); err != nil {
		log.Printf("v2 batch seal remains pending after raw acceptance: %v", err)
	}

	return event, nil
}

// Retrieve returns the raw record and exact bytes by raw ID.
func (s *RawStore) Retrieve(ctx context.Context, rawID string) (*model.RawEvent, error) {
	query := `
	SELECT raw_id, raw_sha256, byte_count, transport, source_ip, source_port, metadata_json, received_at, payload, status
	FROM raw_events WHERE raw_id = ?;
	`
	var (
		evt          model.RawEvent
		receivedAt   string
		statusStr    string
		metadataJSON string
	)

	err := s.db.QueryRowContext(ctx, query, rawID).Scan(
		&evt.RawID,
		&evt.RawSHA256,
		&evt.ByteCount,
		&evt.Transport,
		&evt.SourceIP,
		&evt.SourcePort,
		&metadataJSON,
		&receivedAt,
		&evt.Payload,
		&statusStr,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("raw event not found: %s", rawID)
		}
		return nil, fmt.Errorf("failed to retrieve raw event %s: %w", rawID, err)
	}

	t, err := time.Parse(time.RFC3339Nano, receivedAt)
	if err != nil {
		t, _ = time.Parse(time.RFC3339, receivedAt)
	}
	evt.ReceivedAt = t
	evt.Status = model.RawStatus(statusStr)
	if err := json.Unmarshal([]byte(metadataJSON), &evt.Metadata); err != nil {
		return nil, fmt.Errorf("invalid receive metadata for %s: %w", rawID, err)
	}

	return &evt, nil
}

// Verify recalculates the SHA-256 digest of stored bytes and verifies cryptographic integrity.
func (s *RawStore) Verify(ctx context.Context, rawID string) (bool, string, error) {
	evt, err := s.Retrieve(ctx, rawID)
	if err != nil {
		return false, "", err
	}

	computed := sha256.Sum256(evt.Payload)
	computedHex := hex.EncodeToString(computed[:])

	matches := (computedHex == evt.RawSHA256)
	return matches, computedHex, nil
}

// Quarantine persists an unparseable or schema-violating event in the quarantine table
// and updates the raw event status to "quarantined".
func (s *RawStore) Quarantine(ctx context.Context, entry model.QuarantineEntry) error {
	return s.quarantine(ctx, entry, true)
}

// QuarantineChild records a batch child while keeping its parent recoverable until the batch finalizes.
func (s *RawStore) QuarantineChild(ctx context.Context, entry model.QuarantineEntry) error {
	return s.quarantine(ctx, entry, false)
}

func (s *RawStore) quarantine(ctx context.Context, entry model.QuarantineEntry, finalize bool) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if entry.QuarantineID == "" {
		var randBytes [4]byte
		_, _ = rand.Read(randBytes[:])
		entry.QuarantineID = fmt.Sprintf("deinopis-quar-%s-%x", time.Now().Format("20060102"), randBytes)
	}
	if entry.QuarantinedAt.IsZero() {
		entry.QuarantinedAt = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin quarantine tx: %w", err)
	}
	defer tx.Rollback()

	replayVal := 0
	if entry.ReplayEligible {
		replayVal = 1
	}

	candidateStr := strings.Join(entry.CandidatePacks, ",")
	qQuery := `
	INSERT INTO quarantine (quarantine_id, raw_id, record_ordinal, raw_sha256, stage, reason, error_details, raw_preview, candidate_packs, replay_eligible, quarantined_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(raw_id, record_ordinal, stage, reason) DO UPDATE SET
		error_details=excluded.error_details,
		raw_preview=excluded.raw_preview,
		candidate_packs=excluded.candidate_packs,
		replay_eligible=excluded.replay_eligible;
	`
	_, err = tx.ExecContext(ctx, qQuery,
		entry.QuarantineID,
		entry.RawID,
		entry.RecordOrdinal,
		entry.RawSHA256,
		entry.Stage,
		entry.Reason,
		entry.ErrorDetails,
		entry.RawPreview,
		candidateStr,
		replayVal,
		entry.QuarantinedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("failed to insert quarantine entry: %w", err)
	}
	if finalize {
		if _, err := tx.ExecContext(ctx, `UPDATE raw_events SET status = ? WHERE raw_id = ?`, string(model.StatusQuarantined), entry.RawID); err != nil {
			return fmt.Errorf("failed to update raw event status to quarantined: %w", err)
		}
	}

	return tx.Commit()
}

// UpdateStatus updates the lifecycle state of a committed raw record.
func (s *RawStore) UpdateStatus(ctx context.Context, rawID string, status model.RawStatus) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `UPDATE raw_events SET status = ? WHERE raw_id = ?;`
	res, err := s.db.ExecContext(ctx, query, string(status), rawID)
	if err != nil {
		return fmt.Errorf("failed to update status of %s: %w", rawID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("raw event not found for status update: %s", rawID)
	}
	return nil
}

// GetQuarantined retrieves paginated quarantined records.
func (s *RawStore) GetQuarantined(ctx context.Context, limit, offset int) ([]model.QuarantineEntry, error) {
	query := `
	SELECT quarantine_id, raw_id, record_ordinal, raw_sha256, stage, reason, error_details, raw_preview, candidate_packs, replay_eligible, quarantined_at, replayed_at
	FROM quarantine
	ORDER BY quarantined_at DESC
	LIMIT ? OFFSET ?;
	`
	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query quarantine entries: %w", err)
	}
	defer rows.Close()

	var entries []model.QuarantineEntry
	for rows.Next() {
		var (
			e            model.QuarantineEntry
			candidateStr sql.NullString
			replayInt    int
			qAtStr       string
			rAtStr       sql.NullString
		)
		err := rows.Scan(
			&e.QuarantineID,
			&e.RawID,
			&e.RecordOrdinal,
			&e.RawSHA256,
			&e.Stage,
			&e.Reason,
			&e.ErrorDetails,
			&e.RawPreview,
			&candidateStr,
			&replayInt,
			&qAtStr,
			&rAtStr,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan quarantine entry: %w", err)
		}

		if candidateStr.Valid && candidateStr.String != "" {
			e.CandidatePacks = strings.Split(candidateStr.String, ",")
		}
		e.ReplayEligible = (replayInt == 1)
		t, _ := time.Parse(time.RFC3339Nano, qAtStr)
		e.QuarantinedAt = t
		if rAtStr.Valid {
			rt, _ := time.Parse(time.RFC3339Nano, rAtStr.String)
			e.ReplayedAt = &rt
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate quarantine entries: %w", err)
	}
	return entries, nil
}

// GetQuarantineByID retrieves a single quarantined entry by its quarantine ID.
func (s *RawStore) GetQuarantineByID(ctx context.Context, qID string) (*model.QuarantineEntry, error) {
	query := `
	SELECT quarantine_id, raw_id, record_ordinal, raw_sha256, stage, reason, error_details, raw_preview, candidate_packs, replay_eligible, quarantined_at, replayed_at
	FROM quarantine
	WHERE quarantine_id = ?;
	`
	var (
		e            model.QuarantineEntry
		candidateStr sql.NullString
		replayInt    int
		qAtStr       string
		rAtStr       sql.NullString
	)
	err := s.db.QueryRowContext(ctx, query, qID).Scan(
		&e.QuarantineID,
		&e.RawID,
		&e.RecordOrdinal,
		&e.RawSHA256,
		&e.Stage,
		&e.Reason,
		&e.ErrorDetails,
		&e.RawPreview,
		&candidateStr,
		&replayInt,
		&qAtStr,
		&rAtStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("quarantine entry not found: %s", qID)
		}
		return nil, fmt.Errorf("failed to query quarantine entry %s: %w", qID, err)
	}

	if candidateStr.Valid && candidateStr.String != "" {
		e.CandidatePacks = strings.Split(candidateStr.String, ",")
	}
	e.ReplayEligible = (replayInt == 1)
	t, _ := time.Parse(time.RFC3339Nano, qAtStr)
	e.QuarantinedAt = t
	if rAtStr.Valid {
		rt, _ := time.Parse(time.RFC3339Nano, rAtStr.String)
		e.ReplayedAt = &rt
	}
	return &e, nil
}

// MarkReplayed updates the replayed_at timestamp of a quarantined record.
func (s *RawStore) MarkReplayed(ctx context.Context, qID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	query := `UPDATE quarantine SET replayed_at = ? WHERE quarantine_id = ?;`
	res, err := s.db.ExecContext(ctx, query, nowStr, qID)
	if err != nil {
		return fmt.Errorf("failed to mark quarantine %s as replayed: %w", qID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("quarantine entry not found for replay update: %s", qID)
	}
	return nil
}

// GetReplayPendingIDs returns quarantine IDs eligible for reprocessing.
// If includeAlreadyReplayed is false, returns only records where replayed_at IS NULL.
func (s *RawStore) GetReplayPendingIDs(ctx context.Context, includeAlreadyReplayed bool) ([]string, error) {
	query := `SELECT quarantine_id FROM quarantine WHERE replay_eligible = 1 AND replayed_at IS NULL ORDER BY quarantined_at ASC;`
	if includeAlreadyReplayed {
		query = `SELECT quarantine_id FROM quarantine WHERE replay_eligible = 1 ORDER BY quarantined_at ASC;`
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query replayable quarantine IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate replayable quarantine IDs: %w", err)
	}
	return ids, nil
}

// DeleteQuarantine removes a record from the quarantine table once it has been successfully replayed.
func (s *RawStore) DeleteQuarantine(ctx context.Context, qID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	query := `DELETE FROM quarantine WHERE quarantine_id = ?;`
	_, err := s.db.ExecContext(ctx, query, qID)
	return err
}

// CountRaw returns the total number of raw events in storage.
func (s *RawStore) CountRaw(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_events;").Scan(&count)
	return count, err
}

// CountNormalized returns the total number of normalized events in storage.
func (s *RawStore) CountNormalized(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM normalized_events;").Scan(&count)
	return count, err
}

// CountQuarantine returns the total number of unresolved quarantine records in storage.
func (s *RawStore) CountQuarantine(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM quarantine;").Scan(&count)
	return count, err
}

func (s *RawStore) CountUnmapped(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM normalized_events WHERE EXISTS (SELECT 1 FROM json_each(json_extract(event_json,'$.ocsf.unmapped')))`).Scan(&n)
	return n, err
}

// StoreNormalized inserts or updates a normalized event in the SQLite database.
func (s *RawStore) StoreNormalized(ctx context.Context, event *model.NormalizedEvent) error {
	return s.StoreNormalizedWithOutbox(ctx, event, nil)
}

func (s *RawStore) DeleteNormalizedChild(ctx context.Context, rawID string, ordinal int) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, "DELETE FROM normalized_events WHERE raw_id=? AND record_ordinal=?", rawID, ordinal)
	return err
}

// StoreNormalizedWithOutbox atomically stores the projection and one idempotent delivery row per destination.
func (s *RawStore) StoreNormalizedWithOutbox(ctx context.Context, event *model.NormalizedEvent, destinations []string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := storeNormalizedTx(ctx, tx, event, destinations); err != nil {
		return err
	}
	return tx.Commit()
}

// ResolveQuarantineWithOutbox atomically replaces one quarantined child and updates its parent outcome.
func (s *RawStore) ResolveQuarantineWithOutbox(ctx context.Context, qID string, event *model.NormalizedEvent, destinations []string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := storeNormalizedTx(ctx, tx, event, destinations); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM quarantine WHERE quarantine_id=? AND raw_id=? AND record_ordinal=?`, qID, event.Deinopis.RawID, event.Deinopis.RecordOrdinal)
	if err != nil {
		return fmt.Errorf("delete resolved quarantine entry: %w", err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return fmt.Errorf("quarantine entry %s was not resolved", qID)
	}
	var normalized, quarantined int64
	if err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM normalized_events WHERE raw_id=?),
		(SELECT COUNT(*) FROM quarantine WHERE raw_id=?)`, event.Deinopis.RawID, event.Deinopis.RawID).Scan(&normalized, &quarantined); err != nil {
		return err
	}
	status := model.StatusNormalized
	if normalized == 0 && quarantined > 0 {
		status = model.StatusQuarantined
	} else if quarantined > 0 {
		status = model.StatusPartial
	}
	if _, err := tx.ExecContext(ctx, `UPDATE raw_events SET status=? WHERE raw_id=?`, string(status), event.Deinopis.RawID); err != nil {
		return err
	}
	return tx.Commit()
}

func storeNormalizedTx(ctx context.Context, tx *sql.Tx, event *model.NormalizedEvent, destinations []string) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal normalized event: %w", err)
	}
	eventTime := event.Deinopis.ReceivedTime.UnixMilli()
	switch v := event.OCSF["time"].(type) {
	case int64:
		eventTime = v
	case float64:
		eventTime = int64(v)
	case int:
		eventTime = int64(v)
	}
	severity, activity, msg := "", "", ""
	if v, ok := event.OCSF["severity_id"]; ok && v != nil {
		severity = fmt.Sprint(v)
	}
	if v, ok := event.OCSF["activity_name"].(string); ok {
		activity = v
	}
	if v, ok := event.OCSF["message"].(string); ok {
		msg = v
	}
	var existing string
	if err = tx.QueryRowContext(ctx, "SELECT event_id FROM normalized_events WHERE raw_id=? AND record_ordinal=? LIMIT 1", event.Deinopis.RawID, event.Deinopis.RecordOrdinal).Scan(&existing); err == nil && existing != "" {
		event.Deinopis.EventID = existing
		data, _ = json.Marshal(event)
	}
	sum := sha256.Sum256(data)
	_, err = tx.ExecContext(ctx, `INSERT INTO normalized_events(event_id,raw_id,record_ordinal,source_category,source_id,severity_id,activity_name,message,event_time,received_time,event_json,event_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET source_category=excluded.source_category,source_id=excluded.source_id,severity_id=excluded.severity_id,activity_name=excluded.activity_name,message=excluded.message,event_time=excluded.event_time,received_time=excluded.received_time,event_json=excluded.event_json,event_sha256=excluded.event_sha256`, event.Deinopis.EventID, event.Deinopis.RawID, event.Deinopis.RecordOrdinal, event.Deinopis.SourceCategory, event.Deinopis.SourceID, severity, activity, msg, eventTime, event.Deinopis.ReceivedTime.Format(time.RFC3339Nano), string(data), hex.EncodeToString(sum[:]))
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, dest := range destinations {
		if _, err = tx.ExecContext(ctx, `INSERT INTO outbox(event_id,destination,payload,attempts,next_retry_at,status,created_at) VALUES(?,?,?,0,?,'pending',?) ON CONFLICT(event_id,destination) DO UPDATE SET payload=excluded.payload WHERE outbox.status!='delivered'`, event.Deinopis.EventID, dest, data, now, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *RawStore) DB() *sql.DB { return s.db }

// CountCompletedChildren returns child outcomes already committed for a raw input.
func (s *RawStore) CountCompletedChildren(ctx context.Context, rawID string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM normalized_events WHERE raw_id=?) +
		(SELECT COUNT(*) FROM quarantine WHERE raw_id=?)`, rawID, rawID).Scan(&count)
	return count, err
}

func (s *RawStore) ChildOutcomeCounts(ctx context.Context, rawID string) (normalized, quarantined int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM normalized_events WHERE raw_id=?),
		(SELECT COUNT(*) FROM quarantine WHERE raw_id=?)`, rawID, rawID).Scan(&normalized, &quarantined)
	return
}

// HasCompletedChild reports whether normalization or quarantine has committed a child outcome.
func (s *RawStore) HasCompletedChild(ctx context.Context, rawID string, ordinal int) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM normalized_events WHERE raw_id=? AND record_ordinal=?) +
		(SELECT COUNT(*) FROM quarantine WHERE raw_id=? AND record_ordinal=?)`, rawID, ordinal, rawID, ordinal).Scan(&count)
	return count > 0, err
}

func (s *RawStore) CountRawStatus(ctx context.Context, status model.RawStatus) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_events WHERE status=?", string(status)).Scan(&n)
	return n, err
}
func (s *RawStore) ListRawByStatus(ctx context.Context, status model.RawStatus) ([]*model.RawEvent, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT raw_id FROM raw_events WHERE status=? ORDER BY received_at", string(status))
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	// RawStore intentionally uses one SQLite connection. Close the ID cursor
	// before Retrieve opens another query or restart recovery will deadlock.
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]*model.RawEvent, 0, len(ids))
	for _, id := range ids {
		e, err := s.Retrieve(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// ListNormalized retrieves paginated normalized events with optional category and severity filtering.
func (s *RawStore) ListNormalized(ctx context.Context, category, severity, search string, limit, offset int) ([]*model.NormalizedEvent, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var conditions []string
	var args []any

	if category != "" && category != "all" {
		conditions = append(conditions, "source_category = ?")
		args = append(args, category)
	}
	if severity != "" && severity != "all" {
		conditions = append(conditions, "severity_id = ?")
		args = append(args, severity)
	}
	if search != "" {
		conditions = append(conditions, "(message LIKE ? OR source_id LIKE ? OR event_id LIKE ?)")
		pattern := "%" + search + "%"
		args = append(args, pattern, pattern, pattern)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM normalized_events %s;", whereClause)
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count normalized events: %w", err)
	}

	dataQuery := fmt.Sprintf(`
		SELECT event_json
		FROM normalized_events
		%s
		ORDER BY event_time DESC
		LIMIT ? OFFSET ?;
	`, whereClause)

	dataArgs := append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, dataQuery, dataArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list normalized events: %w", err)
	}
	defer rows.Close()

	var events []*model.NormalizedEvent
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, 0, err
		}
		var evt model.NormalizedEvent
		if err := json.Unmarshal([]byte(jsonStr), &evt); err != nil {
			continue
		}
		events = append(events, &evt)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate normalized events: %w", err)
	}

	return events, total, nil
}

// GetNormalizedByID retrieves a single normalized event by its unique event ID.
func (s *RawStore) GetNormalizedByID(ctx context.Context, eventID string) (*model.NormalizedEvent, error) {
	query := `SELECT event_json FROM normalized_events WHERE event_id = ?;`
	var jsonStr string
	err := s.db.QueryRowContext(ctx, query, eventID).Scan(&jsonStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("event not found: %s", eventID)
		}
		return nil, fmt.Errorf("failed to query event %s: %w", eventID, err)
	}

	var evt model.NormalizedEvent
	if err := json.Unmarshal([]byte(jsonStr), &evt); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event json: %w", err)
	}
	return &evt, nil
}

// NormalizedVerificationReport holds the cryptographic verification report for a normalized event.
type NormalizedVerificationReport struct {
	EventID         string `json:"event_id"`
	RawID           string `json:"raw_id"`
	StoredHash      string `json:"stored_hash"`
	ComputedHash    string `json:"computed_hash"`
	HashMatches     bool   `json:"hash_matches"`
	StoredMessage   string `json:"stored_message"`
	ExpectedMessage string `json:"expected_message"`
	MessageMatches  bool   `json:"message_matches"`
	RawValid        bool   `json:"raw_valid"`
	Passed          bool   `json:"passed"`
	Status          string `json:"status"`
	FailureReason   string `json:"failure_reason,omitempty"`
}

// VerifyNormalized verifies the cryptographic integrity of a normalized event:
// 1. Checks that sha256(event_json) matches event_sha256.
// 2. Checks that the column message matches the message declared in event_json.
// 3. Checks that the underlying raw wire datagram in raw_events is untampered.
func (s *RawStore) VerifyNormalized(ctx context.Context, eventID string) (*NormalizedVerificationReport, error) {
	query := `SELECT event_id, raw_id, message, event_json, event_sha256 FROM normalized_events WHERE event_id = ?;`
	var (
		evtID      string
		rawID      string
		colMsg     string
		eventJSON  string
		storedHash string
	)
	err := s.db.QueryRowContext(ctx, query, eventID).Scan(&evtID, &rawID, &colMsg, &eventJSON, &storedHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("normalized event not found: %s", eventID)
		}
		return nil, fmt.Errorf("query error for event %s: %w", eventID, err)
	}

	computed := sha256.Sum256([]byte(eventJSON))
	computedHex := hex.EncodeToString(computed[:])

	hashMatches := (storedHash != "" && computedHex == storedHash)

	// Extract message from event_json to check column integrity
	var parsed model.NormalizedEvent
	expectedMsg := ""
	if err := json.Unmarshal([]byte(eventJSON), &parsed); err == nil {
		if m, ok := parsed.OCSF["message"].(string); ok {
			expectedMsg = m
		}
	}
	messageMatches := (colMsg == expectedMsg)

	// Verify parent raw bytes independently; decoded messages need not be literal wire substrings.
	rawMatches, _, rawErr := s.Verify(ctx, rawID)
	rawValid := (rawErr == nil && rawMatches)
	passed := hashMatches && messageMatches && rawValid

	report := &NormalizedVerificationReport{
		EventID:         evtID,
		RawID:           rawID,
		StoredHash:      storedHash,
		ComputedHash:    computedHex,
		HashMatches:     hashMatches,
		StoredMessage:   colMsg,
		ExpectedMessage: expectedMsg,
		MessageMatches:  messageMatches,
		RawValid:        rawValid,
		Passed:          passed,
		Status:          "PASSED",
	}

	if !passed {
		report.Status = "TAMPER DETECTED"
		var reasons []string
		if !hashMatches {
			reasons = append(reasons, "event_json payload was modified (hash mismatch)")
		}
		if !messageMatches {
			reasons = append(reasons, fmt.Sprintf("message column (%q) does not match event_json payload (%q)", colMsg, expectedMsg))
		}
		if !rawValid {
			reasons = append(reasons, "parent raw event wire bytes were modified or corrupted")
		}
		report.FailureReason = strings.Join(reasons, "; ")
	}

	return report, nil
}

// ListQuarantinePaged retrieves a paginated list of dead-letter quarantine entries.
func (s *RawStore) ListQuarantinePaged(ctx context.Context, limit, offset int) ([]model.QuarantineEntry, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM quarantine;").Scan(&total); err != nil {
		return nil, 0, err
	}

	query := `
	SELECT quarantine_id, raw_id, record_ordinal, raw_sha256, stage, reason, error_details, raw_preview, candidate_packs, replay_eligible, quarantined_at, replayed_at
	FROM quarantine
	ORDER BY quarantined_at DESC
	LIMIT ? OFFSET ?;
	`
	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var entries []model.QuarantineEntry
	for rows.Next() {
		var (
			e            model.QuarantineEntry
			candidateStr sql.NullString
			replayInt    int
			qAtStr       string
			rAtStr       sql.NullString
		)
		err := rows.Scan(
			&e.QuarantineID,
			&e.RawID,
			&e.RecordOrdinal,
			&e.RawSHA256,
			&e.Stage,
			&e.Reason,
			&e.ErrorDetails,
			&e.RawPreview,
			&candidateStr,
			&replayInt,
			&qAtStr,
			&rAtStr,
		)
		if err != nil {
			return nil, 0, err
		}

		if candidateStr.Valid && candidateStr.String != "" {
			e.CandidatePacks = strings.Split(candidateStr.String, ",")
		}
		e.ReplayEligible = (replayInt == 1)
		t, _ := time.Parse(time.RFC3339Nano, qAtStr)
		e.QuarantinedAt = t
		if rAtStr.Valid {
			rt, _ := time.Parse(time.RFC3339Nano, rAtStr.String)
			e.ReplayedAt = &rt
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate quarantine entries: %w", err)
	}

	return entries, total, nil
}

// ListSources returns an aggregated inventory of active data sources.
func (s *RawStore) ListSources(ctx context.Context) ([]map[string]any, error) {
	query := `
	SELECT 
		source_category,
		source_id,
		COUNT(*) as event_count,
		MAX(received_time) as last_seen
	FROM normalized_events
	GROUP BY source_category, source_id
	ORDER BY event_count DESC;
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []map[string]any
	for rows.Next() {
		var (
			category string
			sourceID string
			count    int64
			lastSeen string
		)
		if err := rows.Scan(&category, &sourceID, &count, &lastSeen); err != nil {
			return nil, err
		}
		sources = append(sources, map[string]any{
			"category":  category,
			"source_id": sourceID,
			"events":    count,
			"last_seen": lastSeen,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate sources: %w", err)
	}
	return sources, nil
}

// Ping verifies that the underlying SQLite database handle is healthy.
func (s *RawStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the underlying SQLite database handle.
func (s *RawStore) Close() error {
	err := s.readerDB.Close()
	if s.readerDB != s.db {
		if closeErr := s.db.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
