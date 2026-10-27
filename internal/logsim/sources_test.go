package logsim

import (
	"math/rand"
	"testing"
	"time"
)

func TestAppSimulatorSeedProducesRepeatableFixture(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	first := (&AppSimulator{}).Generate("normal", ts, rand.New(rand.NewSource(42)))
	second := (&AppSimulator{}).Generate("normal", ts, rand.New(rand.NewSource(42)))
	if string(first) != string(second) {
		t.Fatalf("same source seed produced different fixture bytes")
	}
}
