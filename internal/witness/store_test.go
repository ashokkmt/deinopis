package witness

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"deinopis/internal/integrity"
)

func TestManifestIntakeRequiresMTLSAndIsFirstWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "witness.db"), filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert, clientKey, clientCAPool, _ := makeClientIdentity(t)
	clientFingerprint := sha256.Sum256(clientCert.Raw)
	stream := integrity.Digest{1, 2, 3}
	streamText := hex.EncodeToString(stream[:])
	token := "synthetic-test-token"
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-300 * time.Second)
	if err = store.Enroll(ctx, streamText, epoch, hex.EncodeToString(clientFingerprint[:]), token); err != nil {
		t.Fatal(err)
	}
	manifest := makeWitnessManifest(t, stream, epoch, 0, "deinopis-raw-one", []byte("original"))
	encoded, err := integrity.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(NewHandler(store))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAPool}
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	client.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{clientCert.Raw}, PrivateKey: clientKey, Leaf: clientCert}}
	post := func(body []byte) (*http.Response, error) {
		manifestDigest := integrity.ManifestDigest(body)
		req, err := http.NewRequest(http.MethodPut, server.URL+"/internal/v1/streams/"+streamText+"/windows/0", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-DEINOPIS-Manifest-SHA256", hex.EncodeToString(manifestDigest[:]))
		req.Header.Set("Authorization", "Bearer "+token)
		return client.Do(req)
	}
	resp, err := post(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first write status=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp, err = post(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status=%d", resp.StatusCode)
	}
	var receipt map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if receipt["existing"] != true {
		t.Fatal("retry did not return the stored receipt")
	}
	conflicting := makeWitnessManifest(t, stream, epoch, 0, "deinopis-raw-one", []byte("changed"))
	conflictBytes, err := integrity.EncodeManifest(conflicting)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = post(conflictBytes)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status=%d", resp.StatusCode)
	}
	resp.Body.Close()
	var incidents int
	if err = store.db.QueryRow("SELECT COUNT(*) FROM incidents WHERE kind='manifest_conflict'").Scan(&incidents); err != nil {
		t.Fatal(err)
	}
	if incidents != 1 {
		t.Fatalf("conflict incidents=%d", incidents)
	}
	files, err := os.ReadDir(filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("stored manifests=%d", len(files))
	}
	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(server.Certificate())
	collector, err := NewClient(server.URL, token, tls.Certificate{Certificate: [][]byte{clientCert.Raw}, PrivateKey: clientKey, Leaf: clientCert}, serverRoots)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.SendHeartbeat(ctx, streamText, "boot-1", 1, 0); err != nil {
		t.Fatalf("mTLS heartbeat failed: %v", err)
	}
}

func TestCountBatchIntakeIsOrderedImmutableAndQueuesPublication(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "witness.db"), filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream := integrity.Digest{11, 22}
	streamID := hex.EncodeToString(stream[:])
	fingerprintDigest := integrity.Digest{33, 44}
	fingerprint := hex.EncodeToString(fingerprintDigest[:])
	token := "synthetic-token"
	ctx := context.Background()
	if err = store.EnrollBatch(ctx, streamID, fingerprint, token); err != nil {
		t.Fatal(err)
	}
	firstBody := makeWitnessBatch(t, stream, 0, 1, 1, integrity.Digest{}, "normal")
	firstDigest := manifestDigestText(firstBody)
	first, err := store.AcceptBatch(ctx, streamID, 0, firstDigest, fingerprint, token, firstBody, time.Now().UTC())
	if err != nil || first.Existing || first.FirstSequence != 1 || first.Count != 1 {
		t.Fatalf("first receipt=%+v err=%v", first, err)
	}
	duplicate, err := store.AcceptBatch(ctx, streamID, 0, firstDigest, fingerprint, token, firstBody, time.Now().UTC())
	if err != nil || !duplicate.Existing || duplicate.Commitment != first.Commitment {
		t.Fatalf("duplicate receipt=%+v err=%v", duplicate, err)
	}
	secondBody := makeWitnessBatch(t, stream, 1, 2, 1, first.Commitment, "normal")
	second, err := store.AcceptBatch(ctx, streamID, 1, manifestDigestText(secondBody), fingerprint, token, secondBody, time.Now().UTC())
	if err != nil || second.FirstSequence != 2 {
		t.Fatalf("second receipt=%+v err=%v", second, err)
	}
	gap := makeWitnessBatch(t, stream, 2, 4, 1, second.Commitment, "normal")
	if _, err = store.AcceptBatch(ctx, streamID, 2, manifestDigestText(gap), fingerprint, token, gap, time.Now().UTC()); err == nil {
		t.Fatal("accepted a skipped acceptance sequence")
	}
	conflict := makeWitnessBatch(t, stream, 1, 2, 1, first.Commitment, "conflicting-original")
	if _, err = store.AcceptBatch(ctx, streamID, 1, manifestDigestText(conflict), fingerprint, token, conflict, time.Now().UTC()); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting original returned %v", err)
	}
	retry, err := store.AcceptBatch(ctx, streamID, 1, manifestDigestText(secondBody), fingerprint, token, secondBody, time.Now().UTC())
	if err != nil || !retry.Existing {
		t.Fatalf("exact immutable retry after incident = %+v, err=%v", retry, err)
	}
	third := makeWitnessBatch(t, stream, 2, 3, 1, second.Commitment, "normal")
	if _, err = store.AcceptBatch(ctx, streamID, 2, manifestDigestText(third), fingerprint, token, third, time.Now().UTC()); err == nil {
		t.Fatal("accepted a later batch after immutable manifest conflict")
	}
	var pending, stored, incidents int
	if err = store.db.QueryRow("SELECT COUNT(*) FROM publication_jobs WHERE state='pending'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow("SELECT COUNT(*) FROM batches").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow("SELECT COUNT(*) FROM batch_incidents WHERE kind='manifest_conflict'").Scan(&incidents); err != nil {
		t.Fatal(err)
	}
	if pending != 2 || stored != 2 || incidents != 1 {
		t.Fatalf("publication jobs=%d batches=%d conflicts=%d", pending, stored, incidents)
	}
	queue, err := store.PublicationQueueStatus(ctx)
	if err != nil || len(queue) != 1 || queue[0].Blocked == "" || queue[0].ByState["pending"] != 2 || queue[0].QueuedBytes == 0 {
		t.Fatalf("publication queue status=%+v err=%v", queue, err)
	}
}

func TestCountBatchIntakeEnforcesPerStreamQuota(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "witness.db"), filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream := integrity.Digest{77}
	streamID := hex.EncodeToString(stream[:])
	fingerprintDigest := integrity.Digest{88}
	fingerprint := hex.EncodeToString(fingerprintDigest[:])
	if err = store.EnrollBatch(context.Background(), streamID, fingerprint, "token"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(`UPDATE batch_streams SET max_manifest_count=0 WHERE stream_id=?`, streamID); err != nil {
		t.Fatal(err)
	}
	body := makeWitnessBatch(t, stream, 0, 1, 1, integrity.Digest{}, "normal")
	if _, err = store.AcceptBatch(context.Background(), streamID, 0, manifestDigestText(body), fingerprint, "token", body, time.Now().UTC()); err == nil {
		t.Fatal("accepted manifest after per-stream quota was exhausted")
	}
	var count int
	if err = store.db.QueryRow("SELECT COUNT(*) FROM batches").Scan(&count); err != nil || count != 0 {
		t.Fatalf("quota rejection stored %d batches (err=%v)", count, err)
	}
}

func TestPublicationHistorySurvivesPublisherRemoval(t *testing.T) {
	dir := t.TempDir()
	dbPath, manifestDir := filepath.Join(dir, "witness.db"), filepath.Join(dir, "manifests")
	store, err := NewStore(dbPath, manifestDir)
	if err != nil {
		t.Fatal(err)
	}
	stream := integrity.Digest{91}
	streamID := hex.EncodeToString(stream[:])
	fingerprintDigest := integrity.Digest{92}
	fingerprint := hex.EncodeToString(fingerprintDigest[:])
	if err = store.EnrollBatch(context.Background(), streamID, fingerprint, "synthetic-token"); err != nil {
		t.Fatal(err)
	}
	body := makeWitnessBatch(t, stream, 0, 1, 1, integrity.Digest{}, "history-preservation")
	if _, err = store.AcceptBatch(context.Background(), streamID, 0, manifestDigestText(body), fingerprint, "synthetic-token", body, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signed := []byte("synthetic-signed-transaction")
	txHash, blockHash := integrity.Digest{93}, integrity.Digest{94}
	_, err = store.db.Exec(`UPDATE publication_jobs SET state='included',chain_id='31337',contract_address='0x1111111111111111111111111111111111111111',nonce=7,signed_tx=?,tx_hash=?,fee_cap='1000000',submitted_at_ns=10,included_at_ns=20,block_number=3,block_hash=? WHERE stream_id=? AND batch_no=0`, signed, txHash[:], blockHash[:], streamID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.db.Exec(`INSERT INTO publication_attempts(stream_id,batch_no,attempt_no,nonce,signed_tx,tx_hash,fee_cap,created_at_ns,sent_at_ns,last_error) VALUES(?,0,1,7,?,?, '1000000',10,20,'')`, streamID, signed, txHash[:])
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.db.Exec(`INSERT INTO publication_receipt_history(stream_id,batch_no,tx_hash,block_number,block_hash,status,observed_at_ns) VALUES(?,0,?,3,?,'included',20)`, streamID, txHash[:], blockHash[:])
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewStore(dbPath, manifestDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err = store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 4 {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
	var gotSigned, gotTxHash, gotBlockHash []byte
	var state string
	if err = store.db.QueryRow(`SELECT state,signed_tx,tx_hash,block_hash FROM publication_jobs WHERE stream_id=? AND batch_no=0`, streamID).Scan(&state, &gotSigned, &gotTxHash, &gotBlockHash); err != nil {
		t.Fatal(err)
	}
	if state != "included" || !bytes.Equal(gotSigned, signed) || !bytes.Equal(gotTxHash, txHash[:]) || !bytes.Equal(gotBlockHash, blockHash[:]) {
		t.Fatalf("publication history changed after reopen: state=%q signed=%x tx=%x block=%x", state, gotSigned, gotTxHash, gotBlockHash)
	}
	var attempts, receipts int
	if err = store.db.QueryRow("SELECT COUNT(*) FROM publication_attempts WHERE stream_id=?", streamID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow("SELECT COUNT(*) FROM publication_receipt_history WHERE stream_id=?", streamID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || receipts != 1 {
		t.Fatalf("publication history counts changed: attempts=%d receipts=%d", attempts, receipts)
	}
	status, err := store.PublicationQueueStatus(context.Background())
	if err != nil || len(status) != 1 || status[0].ByState["included"] != 1 {
		t.Fatalf("preserved queue status=%+v err=%v", status, err)
	}
}

func makeWitnessBatch(t *testing.T, stream integrity.Digest, batchNo, first, count uint64, previous integrity.Digest, content string) []byte {
	t.Helper()
	now := time.Now().UTC().Add(-time.Second).UnixNano()
	entries := make([]integrity.BatchEntry, count)
	for i := range entries {
		seq := first + uint64(i)
		entries[i] = integrity.BatchEntry{BatchNo: batchNo, Entry: integrity.Entry{
			StreamID: stream, AcceptSeq: seq, AcceptedAt: now + int64(i), ReceivedAt: now,
			RawID: fmt.Sprintf("deinopis-batch-%d-record-%d", batchNo, seq), Transport: "stdin", SourceIP: "local", Payload: []byte(fmt.Sprintf("%s-record-%d", content, seq)),
		}}
	}
	m := integrity.BatchManifest{StreamID: stream, BatchNo: batchNo, FirstSequence: first, LastSequence: first + count - 1,
		StartedAt: now, ClosedAt: now + int64(count), Previous: previous, Entries: entries}
	body, err := integrity.EncodeBatchManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestWitnessRejectsUnauthorizedAndHeartbeatRollback(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "w.db"), filepath.Join(dir, "m"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream := integrity.Digest{7}
	fingerprint := integrity.Digest{8}
	epoch := time.Now().UTC().Truncate(300 * time.Second)
	if err = store.Enroll(context.Background(), hex.EncodeToString(stream[:]), epoch, hex.EncodeToString(fingerprint[:]), "token"); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordHeartbeat(context.Background(), hex.EncodeToString(stream[:]), hex.EncodeToString(fingerprint[:]), "wrong", Heartbeat{BootID: "boot", BootCounter: 1}, epoch); err != ErrUnauthorized {
		t.Fatalf("wrong token error=%v", err)
	}
	if err = store.RecordHeartbeat(context.Background(), hex.EncodeToString(stream[:]), hex.EncodeToString(fingerprint[:]), "token", Heartbeat{BootID: "boot", BootCounter: 2}, epoch); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordHeartbeat(context.Background(), hex.EncodeToString(stream[:]), hex.EncodeToString(fingerprint[:]), "token", Heartbeat{BootID: "boot", BootCounter: 1}, epoch.Add(time.Second)); err == nil {
		t.Fatal("backward heartbeat counter accepted")
	}
	if err = store.RecordHeartbeat(context.Background(), hex.EncodeToString(stream[:]), hex.EncodeToString(fingerprint[:]), "token", Heartbeat{BootID: "boot", BootCounter: 3}, epoch.Add(92*time.Second)); err != nil {
		t.Fatal(err)
	}
	var missed int
	if err = store.db.QueryRow("SELECT COUNT(*) FROM incidents WHERE kind='heartbeat_missed'").Scan(&missed); err != nil || missed != 1 {
		t.Fatalf("missed heartbeat incident count=%d err=%v", missed, err)
	}
}

func TestWitnessRequiresSequenceContinuityAcrossWindowsAndAllowsGaps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStore(filepath.Join(dir, "w.db"), filepath.Join(dir, "m"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream, cert := integrity.Digest{3}, integrity.Digest{4}
	streamID, fingerprint, token := hex.EncodeToString(stream[:]), hex.EncodeToString(cert[:]), "token"
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-600 * time.Second)
	if err = store.Enroll(ctx, streamID, epoch, fingerprint, token); err != nil {
		t.Fatal(err)
	}
	first := makeWitnessManifest(t, stream, epoch, 0, "deinopis-raw-1", []byte("one"))
	firstBytes, _ := integrity.EncodeManifest(first)
	firstReceipt, err := store.AcceptManifest(ctx, streamID, 0, manifestDigestText(firstBytes), fingerprint, token, firstBytes, epoch.Add(300*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Empty slot 1 preserves the next expected sequence; slot 2 must continue at 2.
	gap := integrity.Manifest{StreamID: stream, Slot: 1, Start: uint64(epoch.Unix() + 300), End: uint64(epoch.Unix() + 600), Coverage: integrity.CoverageGap, Previous: firstReceipt.Commitment}
	gapBytes, _ := integrity.EncodeManifest(gap)
	gapReceipt, err := store.AcceptManifest(ctx, streamID, 1, manifestDigestText(gapBytes), fingerprint, token, gapBytes, epoch.Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	second := makeWitnessManifest(t, stream, epoch, 2, "deinopis-raw-2", []byte("two"))
	second.Previous = gapReceipt.Commitment
	second.Entries[0].AcceptSeq = 2
	secondBytes, _ := integrity.EncodeManifest(second)
	if _, err = store.AcceptManifest(ctx, streamID, 2, manifestDigestText(secondBytes), fingerprint, token, secondBytes, epoch.Add(900*time.Second)); err != nil {
		t.Fatalf("contiguous sequence after gap rejected: %v", err)
	}
}

func TestImmutableManifestRetrySurvivesWitnessRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath, manifestDir := filepath.Join(dir, "w.db"), filepath.Join(dir, "manifests")
	store, err := NewStore(dbPath, manifestDir)
	if err != nil {
		t.Fatal(err)
	}
	stream, fingerprint := integrity.Digest{5}, integrity.Digest{6}
	streamID, fingerprintText, token := hex.EncodeToString(stream[:]), hex.EncodeToString(fingerprint[:]), "restart-token"
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-300 * time.Second)
	if err = store.Enroll(ctx, streamID, epoch, fingerprintText, token); err != nil {
		t.Fatal(err)
	}
	manifest := makeWitnessManifest(t, stream, epoch, 0, "deinopis-raw-restart", []byte("original"))
	body, err := integrity.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := manifestDigestText(body)
	if _, err = store.AcceptManifest(ctx, streamID, 0, digest, fingerprintText, token, body, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewStore(dbPath, manifestDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Enroll(ctx, streamID, epoch, fingerprintText, token); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.AcceptManifest(ctx, streamID, 0, digest, fingerprintText, token, body, time.Now().UTC())
	if err != nil || !receipt.Existing {
		t.Fatalf("restart retry = %+v, err=%v", receipt, err)
	}
}

func TestManifestAuthorizationPrecedesParsing(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "witness.db"), filepath.Join(t.TempDir(), "manifests"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var stream integrity.Digest
	stream[0] = 99
	fingerprint := manifestDigestText([]byte("client certificate fingerprint"))
	epoch := time.Now().UTC().Truncate(300 * time.Second).Add(-300 * time.Second)
	if err = store.Enroll(context.Background(), hex.EncodeToString(stream[:]), epoch, fingerprint, "correct-token"); err != nil {
		t.Fatal(err)
	}
	_, err = store.AcceptManifest(context.Background(), hex.EncodeToString(stream[:]), 0, manifestDigestText([]byte("malformed")), fingerprint, "wrong-token", []byte("malformed"), epoch.Add(300*time.Second))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized body reached manifest parser: %v", err)
	}
}

func manifestDigestText(body []byte) string {
	digest := integrity.ManifestDigest(body)
	return hex.EncodeToString(digest[:])
}

func makeWitnessManifest(t *testing.T, stream integrity.Digest, epoch time.Time, slot uint64, rawID string, payload []byte) integrity.Manifest {
	t.Helper()
	start := uint64(epoch.Unix()) + slot*300
	return integrity.Manifest{StreamID: stream, Slot: slot, Start: start, End: start + 300, Coverage: integrity.CoverageComplete, Entries: []integrity.Entry{{StreamID: stream, Slot: slot, AcceptSeq: slot + 1, AcceptedAt: epoch.UnixNano() + int64(slot)*300_000_000_000 + 1, RawID: rawID, ReceivedAt: epoch.UnixNano() + 1, Transport: "stdin", SourceIP: "local", Payload: payload}}}
}

func makeClientIdentity(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool, []byte) {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test client CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "collector"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return cert, key, pool, caDER
}
