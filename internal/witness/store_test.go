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
	clientCAPool.AddCert(server.Certificate())
	collector, err := NewClient(server.URL, token, tls.Certificate{Certificate: [][]byte{clientCert.Raw}, PrivateKey: clientKey, Leaf: clientCert}, clientCAPool)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.SendHeartbeat(ctx, streamText, "boot-1", 1, 0); err != nil {
		t.Fatalf("mTLS heartbeat failed: %v", err)
	}
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
