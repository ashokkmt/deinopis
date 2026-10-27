package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRejectExternalPublisherEnvironment(t *testing.T) {
	for _, entry := range []string{
		"DEINOPIS_CHAIN_RPC_URL=",
		"DEINOPIS_CHAIN_ID=31337",
		"DEINOPIS_REGISTRY_ADDRESS=0x1234",
		"DEINOPIS_PUBLISHER_KEY_FILE=/tmp/test-key",
	} {
		err := rejectExternalPublisherEnvironment([]string{"PATH=/bin", entry})
		if err == nil || !strings.Contains(err.Error(), "external chain publishing is disabled") {
			t.Errorf("retired setting %q was not rejected explicitly: %v", entry, err)
		}
	}
	if err := rejectExternalPublisherEnvironment([]string{"DEINOPIS_WITNESS_TOKEN=synthetic", "PATH=/bin"}); err != nil {
		t.Fatalf("retained witness configuration rejected: %v", err)
	}
}

func TestRetiredPublisherSettingsFailBeforeOpeningWitnessStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-be-created")
	store, err := openWitnessStore([]string{"DEINOPIS_CHAIN_RPC_URL=http://example.invalid"}, filepath.Join(dir, "witness.db"), filepath.Join(dir, "manifests"))
	if err == nil || store != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("retired setting opened witness store: store=%v err=%v", store, err)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("rejected config created witness path: stat err=%v", statErr)
	}
}
