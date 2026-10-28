package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"deinopis/internal/rawstore"
)

func TestVaultWitnessConfigurationIsAllOrNone(t *testing.T) {
	keys := []string{"DEINOPIS_VAULT_STREAM_ID", "DEINOPIS_VAULT_EPOCH", "DEINOPIS_WITNESS_URL", "DEINOPIS_WITNESS_TOKEN", "DEINOPIS_WITNESS_CA", "DEINOPIS_WITNESS_CLIENT_CERT", "DEINOPIS_WITNESS_CLIENT_KEY"}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	store, err := rawstore.New(filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	before, err := store.VaultState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client, streamID, err := configureVaultWitness(store)
	if err != nil || client != nil || streamID != "" {
		t.Fatalf("unset configuration = (%v,%q,%v)", client, streamID, err)
	}
	t.Setenv(keys[0], "0000000000000000000000000000000000000000000000000000000000000001")
	if _, _, err = configureVaultWitness(store); err == nil {
		t.Fatal("partial witness configuration accepted")
	}
	after, err := store.VaultState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("partial configuration changed stream state: before=%+v after=%+v", before, after)
	}
}

func TestValidateIntegritySimulationBoundary(t *testing.T) {
	for _, key := range []string{"DEINOPIS_VAULT_STREAM_ID", "DEINOPIS_WITNESS_URL", "DEINOPIS_WITNESS_TOKEN", "DEINOPIS_WITNESS_CA", "DEINOPIS_WITNESS_CLIENT_CERT", "DEINOPIS_WITNESS_CLIENT_KEY"} {
		t.Setenv(key, "")
	}
	db := filepath.Join(t.TempDir(), "fresh", "demo.db")
	valid := func(path, http, ui string) error {
		return validateIntegritySimulation(path, http, ui, "none", "none", "none", "none", "", "none", false, false, true)
	}
	if err := valid(db, "127.0.0.1:8080", "127.0.0.1:9090"); err != nil {
		t.Fatalf("rejected isolated loopback configuration: %v", err)
	}
	if err := valid("data/deinopis.db", "127.0.0.1:8080", "127.0.0.1:9090"); err == nil {
		t.Fatal("allowed the default collector database")
	}
	defaultDB, err := filepath.Abs("data/deinopis.db")
	if err != nil {
		t.Fatal(err)
	}
	if err = valid(defaultDB, "127.0.0.1:8080", "127.0.0.1:9090"); err == nil {
		t.Fatal("allowed an absolute path to the default collector database")
	}
	if err := valid(db, "0.0.0.0:8080", "127.0.0.1:9090"); err == nil {
		t.Fatal("allowed a non-loopback ingest listener")
	}
	if err := valid(db, "127.0.0.1:8080", "127.0.0.1:9090"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(db), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(db, []byte("existing database"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := valid(db, "127.0.0.1:8080", "127.0.0.1:9090"); err == nil {
		t.Fatal("allowed a non-empty database")
	}
	if err := validateIntegritySimulation(db, "127.0.0.1:8080", "127.0.0.1:9090", "127.0.0.1:1514", "none", "none", "none", "", "none", false, false, true); err == nil {
		t.Fatal("allowed syslog ingestion")
	}
	if err := validateIntegritySimulation(filepath.Join(t.TempDir(), "fresh.db"), "127.0.0.1:8080", "127.0.0.1:9090", "none", "none", "none", "none", "", "data/output/normalized.ndjson", false, false, true); err == nil {
		t.Fatal("allowed shared default output file")
	}
	t.Setenv("DEINOPIS_WITNESS_URL", "https://witness.example")
	if err := valid(filepath.Join(t.TempDir(), "fresh.db"), "127.0.0.1:8080", "127.0.0.1:9090"); err == nil {
		t.Fatal("allowed external witness configuration")
	}
}
