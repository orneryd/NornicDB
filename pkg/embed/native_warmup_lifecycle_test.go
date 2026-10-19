//go:build localllm

package embed

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalGGUFWarmupShutdown(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, time.Hour} {
		t.Run(interval.String(), func(t *testing.T) {
			e := &LocalGGUFEmbedder{stopWarmup: make(chan struct{}), warmupStopped: make(chan struct{})}
			e.startWarmup(interval)
			started := time.Now()
			require.NoError(t, e.Close())
			require.Less(t, time.Since(started), time.Second, "no fixed five-second shutdown wait")
			if interval > 0 {
				select {
				case <-e.warmupStopped:
				default:
					t.Error("Close did not join the real warmup loop")
				}
			}
			require.NoError(t, e.Close())
		})
	}
}

func TestLocalGGUFCloseJoinsWarmupWithoutHoldingProviderLock(t *testing.T) {
	e := &LocalGGUFEmbedder{
		stopWarmup: make(chan struct{}), warmupStopped: make(chan struct{}), warmupStarted: true,
	}
	go func() {
		<-e.stopWarmup
		// A warmup which has selected its tick may be waiting for this lock.
		e.mu.RLock()
		e.mu.RUnlock()
		close(e.warmupStopped)
	}()
	var calls sync.WaitGroup
	calls.Add(2)
	for range 2 {
		go func() { defer calls.Done(); _ = e.Close() }()
	}
	done := make(chan struct{})
	go func() { calls.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close held the lock needed by warmup or did not join concurrent Close")
	}
	select {
	case <-e.warmupStopped:
	default:
		t.Error("concurrent Close returned before warmup finished")
	}
}
