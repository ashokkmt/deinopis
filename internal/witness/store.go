package witness

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"deinopis/internal/integrity"

	_ "modernc.org/sqlite"
)

const manifestLimit = 64 * 1024 * 1024

var (
	ErrUnauthorized = errors.New("witness authentication failed")
	ErrConflict     = errors.New("original manifest conflict")
	ErrNotEnrolled  = errors.New("stream is not enrolled")
)

type Store struct {
	db  *sql.DB
	dir string
	mu  sync.Mutex
}

type Receipt struct {
	StreamID   string
	Slot       uint64
	Digest     integrity.Digest
	Commitment integrity.Digest
	ReceivedAt time.Time
	Existing   bool
}

func NewStore(dbPath, manifestDir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(manifestDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(manifestDir, 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err = db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	if version > 2 {
		db.Close()
		return nil, fmt.Errorf("witness schema %d is newer than supported schema 2", version)
	}
	if version == 0 {
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS streams(
			stream_id TEXT PRIMARY KEY, epoch_start INTEGER NOT NULL, window_seconds INTEGER NOT NULL CHECK(window_seconds=300),
			collector_fingerprint TEXT NOT NULL, token_hash BLOB NOT NULL CHECK(length(token_hash)=32),
			next_slot INTEGER NOT NULL DEFAULT 0, previous_commitment BLOB NOT NULL CHECK(length(previous_commitment)=32),
			last_heartbeat_ns INTEGER, boot_id TEXT, boot_counter INTEGER NOT NULL DEFAULT 0, next_sequence INTEGER NOT NULL DEFAULT 1);
		CREATE TABLE IF NOT EXISTS manifests(
			stream_id TEXT NOT NULL REFERENCES streams(stream_id), slot INTEGER NOT NULL, digest BLOB NOT NULL CHECK(length(digest)=32),
			commitment BLOB NOT NULL CHECK(length(commitment)=32), path TEXT NOT NULL, received_at_ns INTEGER NOT NULL, high_water_sequence INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(stream_id,slot), UNIQUE(stream_id,digest));
		CREATE TABLE IF NOT EXISTS incidents(
			id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id TEXT NOT NULL REFERENCES streams(stream_id), slot INTEGER NOT NULL,
			kind TEXT NOT NULL, observed_at_ns INTEGER NOT NULL, detail TEXT NOT NULL, UNIQUE(stream_id,slot,kind));
		PRAGMA user_version=2;`)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate witness schema: %w", err)
		}
	}
	if version == 1 {
		if _, err = db.Exec(`ALTER TABLE streams ADD COLUMN next_sequence INTEGER NOT NULL DEFAULT 1; ALTER TABLE manifests ADD COLUMN high_water_sequence INTEGER NOT NULL DEFAULT 0; PRAGMA user_version=2;`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate witness sequence state: %w", err)
		}
	}
	if err = os.Chmod(dbPath, 0600); err != nil {
		db.Close()
		return nil, fmt.Errorf("restrict witness database permissions: %w", err)
	}
	return &Store{db: db, dir: manifestDir}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Enroll(ctx context.Context, streamID string, epoch time.Time, clientFingerprint, bearerToken string) error {
	if !validDigestText(streamID) || !validDigestText(clientFingerprint) || bearerToken == "" || epoch.Nanosecond() != 0 || epoch.Unix() < 0 || epoch.Unix()%300 != 0 {
		return errors.New("invalid stream enrollment")
	}
	clientFingerprint = strings.ToLower(clientFingerprint)
	tokenHash := sha256.Sum256([]byte(bearerToken))
	zero := make([]byte, 32)
	var oldEpoch int64
	var oldFingerprint string
	var oldToken []byte
	err := s.db.QueryRowContext(ctx, "SELECT epoch_start,collector_fingerprint,token_hash FROM streams WHERE stream_id=?", streamID).Scan(&oldEpoch, &oldFingerprint, &oldToken)
	if err == nil {
		if oldEpoch == epoch.Unix() && oldFingerprint == clientFingerprint && len(oldToken) == 32 && subtle.ConstantTimeCompare(oldToken, tokenHash[:]) == 1 {
			return nil
		}
		return errors.New("stream is already enrolled with different settings")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO streams(stream_id,epoch_start,window_seconds,collector_fingerprint,token_hash,previous_commitment) VALUES(?,?,300,?,?,?)`, streamID, epoch.Unix(), clientFingerprint, tokenHash[:], zero)
	if err != nil {
		return fmt.Errorf("enroll witness stream: %w", err)
	}
	return nil
}

func (s *Store) AcceptManifest(ctx context.Context, streamID string, slot uint64, digestText, certFingerprint, bearerToken string, body []byte, now time.Time) (Receipt, error) {
	if len(body) == 0 || len(body) > manifestLimit {
		return Receipt{}, errors.New("manifest body is empty or exceeds 64 MiB")
	}
	if !validDigestText(streamID) || !validDigestText(digestText) {
		return Receipt{}, errors.New("invalid stream or digest")
	}
	parsed, err := integrity.ParseManifest(body)
	if err != nil {
		return Receipt{}, fmt.Errorf("invalid manifest: %w", err)
	}
	digest := integrity.ManifestDigest(body)
	if hex.EncodeToString(digest[:]) != digestText || hex.EncodeToString(parsed.StreamID[:]) != streamID || parsed.Slot != slot {
		return Receipt{}, errors.New("manifest identity or digest header mismatch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var epoch int64
	var window int64
	var fingerprint string
	var tokenHash, previous []byte
	var nextSlot, nextSequence uint64
	err = s.db.QueryRowContext(ctx, `SELECT epoch_start,window_seconds,collector_fingerprint,token_hash,next_slot,previous_commitment,next_sequence FROM streams WHERE stream_id=?`, streamID).Scan(&epoch, &window, &fingerprint, &tokenHash, &nextSlot, &previous, &nextSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, ErrNotEnrolled
	}
	if err != nil {
		return Receipt{}, err
	}
	wantToken := sha256.Sum256([]byte(bearerToken))
	if len(tokenHash) != 32 || subtle.ConstantTimeCompare(tokenHash, wantToken[:]) != 1 || subtle.ConstantTimeCompare([]byte(strings.ToLower(certFingerprint)), []byte(fingerprint)) != 1 {
		return Receipt{}, ErrUnauthorized
	}
	if epoch > math.MaxInt64-window || window <= 0 || slot > uint64((math.MaxInt64-epoch-window)/window) {
		return Receipt{}, errors.New("manifest slot overflows schedule")
	}
	start := uint64(epoch) + slot*uint64(window)
	if parsed.Start != start || parsed.End != start+uint64(window) {
		return Receipt{}, errors.New("manifest schedule mismatch")
	}
	if now.Unix() < int64(parsed.End) {
		return Receipt{}, errors.New("future or open window manifest rejected")
	}
	var oldDigest, oldCommitment []byte
	var oldPath string
	var received int64
	err = s.db.QueryRowContext(ctx, `SELECT digest,commitment,path,received_at_ns FROM manifests WHERE stream_id=? AND slot=?`, streamID, slot).Scan(&oldDigest, &oldCommitment, &oldPath, &received)
	if err == nil {
		stored, readErr := os.ReadFile(filepath.Join(s.dir, oldPath))
		if readErr != nil {
			return Receipt{}, fmt.Errorf("read immutable original manifest: %w", readErr)
		}
		if bytesEqual(oldDigest, digest[:]) && bytesEqual(stored, body) {
			var c integrity.Digest
			copy(c[:], oldCommitment)
			return Receipt{StreamID: streamID, Slot: slot, Digest: digest, Commitment: c, ReceivedAt: time.Unix(0, received).UTC(), Existing: true}, nil
		}
		_, incidentErr := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO incidents(stream_id,slot,kind,observed_at_ns,detail) VALUES(?,?,'manifest_conflict',?,?)`, streamID, slot, now.UTC().UnixNano(), "different bytes submitted for an immutable original manifest")
		if incidentErr != nil {
			return Receipt{}, incidentErr
		}
		return Receipt{}, ErrConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, err
	}
	if len(previous) != 32 || !bytesEqual(parsed.Previous[:], previous) {
		return Receipt{}, errors.New("manifest predecessor mismatch")
	}
	if slot != nextSlot {
		return Receipt{}, fmt.Errorf("manifest slot %d is not the expected next slot %d", slot, nextSlot)
	}
	if parsed.Count > 0 && (parsed.FirstSequence != nextSequence || parsed.HighWaterSequence >= math.MaxInt64) {
		return Receipt{}, errors.New("manifest acceptance sequence is not contiguous with prior windows")
	}
	name := hex.EncodeToString(digest[:]) + ".bin"
	if err = durableWrite(s.dir, name, body); err != nil {
		return Receipt{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO manifests(stream_id,slot,digest,commitment,path,received_at_ns,high_water_sequence) VALUES(?,?,?,?,?,?,?)`, streamID, slot, digest[:], parsed.Commitment[:], name, now.UTC().UnixNano(), parsed.HighWaterSequence); err != nil {
		return Receipt{}, err
	}
	newSequence := nextSequence
	if parsed.Count > 0 {
		newSequence = parsed.HighWaterSequence + 1
	}
	res, err := tx.ExecContext(ctx, `UPDATE streams SET next_slot=?,previous_commitment=?,next_sequence=? WHERE stream_id=? AND next_slot=?`, slot+1, parsed.Commitment[:], newSequence, streamID, slot)
	if err != nil {
		return Receipt{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return Receipt{}, errors.New("witness stream changed during intake")
	}
	if err = tx.Commit(); err != nil {
		return Receipt{}, fmt.Errorf("commit witness manifest: %w", err)
	}
	return Receipt{StreamID: streamID, Slot: slot, Digest: digest, Commitment: parsed.Commitment, ReceivedAt: now.UTC()}, nil
}

type Heartbeat struct {
	BootID      string `json:"boot_id"`
	BootCounter uint64 `json:"boot_counter"`
	QueueDepth  uint32 `json:"queue_depth"`
}

func (s *Store) RecordHeartbeat(ctx context.Context, streamID, certFingerprint, bearerToken string, h Heartbeat, now time.Time) error {
	if !validDigestText(streamID) || h.BootID == "" || len(h.BootID) > 128 {
		return errors.New("invalid heartbeat")
	}
	var fingerprint string
	var tokenHash []byte
	var epoch int64
	var window int64
	err := s.db.QueryRowContext(ctx, `SELECT collector_fingerprint,token_hash,epoch_start,window_seconds FROM streams WHERE stream_id=?`, streamID).Scan(&fingerprint, &tokenHash, &epoch, &window)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotEnrolled
	}
	if err != nil {
		return err
	}
	want := sha256.Sum256([]byte(bearerToken))
	if len(tokenHash) != 32 || subtle.ConstantTimeCompare(tokenHash, want[:]) != 1 || subtle.ConstantTimeCompare([]byte(strings.ToLower(certFingerprint)), []byte(fingerprint)) != 1 {
		return ErrUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var oldBoot sql.NullString
	var lastHeartbeat sql.NullInt64
	var oldCounter uint64
	if err = tx.QueryRowContext(ctx, "SELECT boot_id,boot_counter,last_heartbeat_ns FROM streams WHERE stream_id=?", streamID).Scan(&oldBoot, &oldCounter, &lastHeartbeat); err != nil {
		return err
	}
	if oldBoot.Valid && oldBoot.String == h.BootID && h.BootCounter < oldCounter {
		return errors.New("heartbeat counter moved backwards")
	}
	if _, err = tx.ExecContext(ctx, "UPDATE streams SET last_heartbeat_ns=?,boot_id=?,boot_counter=? WHERE stream_id=?", now.UTC().UnixNano(), h.BootID, h.BootCounter, streamID); err != nil {
		return err
	}
	if (lastHeartbeat.Valid && now.UnixNano()-lastHeartbeat.Int64 > int64(90*time.Second)) || (!lastHeartbeat.Valid && now.Unix()-epoch > 90) {
		slot := int64(0)
		if now.Unix() > epoch && window > 0 {
			slot = (now.Unix() - epoch) / window
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO incidents(stream_id,slot,kind,observed_at_ns,detail) VALUES(?,?,'heartbeat_missed',?,'no collector heartbeat was observed for more than 90 seconds')`, streamID, slot, now.UTC().UnixNano()); err != nil {
			return err
		}
	}
	if now.Unix() > epoch {
		current := (now.Unix() - epoch) / window
		var next uint64
		if err = tx.QueryRowContext(ctx, "SELECT next_slot FROM streams WHERE stream_id=?", streamID).Scan(&next); err != nil {
			return err
		}
		if current > int64(next) {
			_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO incidents(stream_id,slot,kind,observed_at_ns,detail) VALUES(?,?,'expected_manifest_missed',?,'scheduled slot has not been received')`, streamID, next, now.UTC().UnixNano())
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func durableWrite(dir, name string, data []byte) error {
	dest := filepath.Join(dir, name)
	if existing, err := os.ReadFile(dest); err == nil {
		if bytesEqual(existing, data) {
			return os.Chmod(dest, 0600)
		}
		return errors.New("content-addressed manifest path contains conflicting bytes")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, dest); err != nil {
		if existing, readErr := os.ReadFile(dest); readErr == nil && bytesEqual(existing, data) {
			return nil
		}
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func validDigestText(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}

func readBounded(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("request body too large")
	}
	return b, nil
}
