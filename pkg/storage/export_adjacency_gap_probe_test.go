package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test-only scheduling seams for an external public-Cypher reproduction.
func (b *BadgerEngine) GapProbeStopWorker() { b.writeBehind.Close() }

func (b *BadgerEngine) GapProbeAcrossReplay(t *testing.T, run func() (int64, error)) int64 {
	t.Helper()
	b.writeBehind.rotate()
	var frontier *CommitBuffer
	b.writeBehind.mu.RLock()
	if len(b.writeBehind.draining) > 0 {
		frontier = b.writeBehind.draining[len(b.writeBehind.draining)-1]
	}
	b.writeBehind.mu.RUnlock()
	require.NotNil(t, frontier)
	b.adjCacheInvalidateAll()
	// Pause the same old read immediately before its cache fill, without
	// holding the durable issuer's adjacency lock. Issuance now precedes
	// publication, so holding that lock would prevent the replay under test.
	paused, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	hook := func() { pauseOnce.Do(func() { close(paused); <-resume }) }
	previous := b.adjCacheFillHook.Swap(&hook)
	defer b.adjCacheFillHook.Store(previous)
	defer unblock()
	type result struct {
		count int64
		err   error
	}
	read := make(chan result, 1)
	go func() { count, err := run(); read <- result{count, err} }()
	deadline := time.Now().Add(3 * time.Second)
	select {
	case <-paused:
	case <-time.After(3 * time.Second):
		t.Fatal("public query did not reach adjacency cache publication")
	}
	b.writeBehind.mu.Lock()
	overlayHeld := true
	defer func() {
		if overlayHeld {
			b.writeBehind.mu.Unlock()
		}
	}()
	replayed := make(chan error, 1)
	go func() { replayed <- b.replayWriteBehind(frontier) }()
	for !frontier.countsApplied.Load() {
		if time.Now().After(deadline) {
			t.Fatal("replay did not publish committed data")
		}
		time.Sleep(time.Millisecond)
	}
	unblock()
	select {
	case err := <-replayed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("replay did not finish")
	}
	require.True(t, frontier.IsApplied())
	b.writeBehind.mu.Unlock()
	overlayHeld = false
	select {
	case got := <-read:
		require.NoError(t, got.err)
		return got.count
	case <-time.After(3 * time.Second):
		t.Fatal("public query did not finish")
		return -1
	}
}
