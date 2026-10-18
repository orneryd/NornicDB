//go:build localllm

package embed

import (
	"testing"
	"time"
)

// Mirrors the constructor's WarmupInterval < 0 state: channels exist, but
// there has never been a warmup goroutine to close warmupStopped.
func TestSolReviewNativeDisabledWarmupCloseLatency(t *testing.T) {
	e := &LocalGGUFEmbedder{stopWarmup: make(chan struct{}), warmupStopped: make(chan struct{})}
	started := time.Now()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	t.Logf("native Close with no warmup worker: %s", elapsed)
	if elapsed > time.Second {
		t.Errorf("Close waited for a worker that was never started: %s", elapsed)
	}
}
