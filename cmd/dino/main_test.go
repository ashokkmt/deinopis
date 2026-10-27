package main

import (
	"context"
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
