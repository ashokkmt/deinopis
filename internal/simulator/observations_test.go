package simulator

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPhase6FirstAdverseObservationSurvivesRetryAndRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "witness.sqlite")
	firstTime := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	bad := DemoVerification{Mode: "SIMULATED", CheckedAt: firstTime, Content: "TAMPERED", Coverage: "complete", Witness: "first-write originals", Anchor: "local registry readback"}
	first, count, err := recordVerification(ctx, dbPath, "run-1", bad)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || !first.Equal(firstTime) || count != 1 {
		t.Fatalf("first=%v count=%d, want first detection at %s and one observation", first, count, firstTime)
	}

	bad.CheckedAt = firstTime.Add(time.Minute)
	firstRetry, count, err := recordVerification(ctx, dbPath, "run-1", bad)
	if err != nil {
		t.Fatal(err)
	}
	if firstRetry == nil || !firstRetry.Equal(firstTime) || count != 2 {
		t.Fatalf("retry moved first detection: first=%v count=%d", firstRetry, count)
	}

	recovered := DemoVerification{Mode: "SIMULATED", CheckedAt: firstTime.Add(2 * time.Minute), Content: "INTACT", Coverage: "complete", Witness: "first-write originals", Anchor: "local registry readback"}
	firstRecovery, count, err := recordVerification(ctx, dbPath, "run-1", recovered)
	if err != nil {
		t.Fatal(err)
	}
	if firstRecovery == nil || !firstRecovery.Equal(firstTime) || count != 3 {
		t.Fatalf("recovery cleared first detection: first=%v count=%d", firstRecovery, count)
	}
}
