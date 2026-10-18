package storage

import (
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
	if len(b.writeBehind.draining) > 0 { frontier = b.writeBehind.draining[len(b.writeBehind.draining)-1] }
	b.writeBehind.mu.RUnlock()
	require.NotNil(t, frontier)
	b.adjCacheInvalidateAll()
	b.adjCacheMu.RLock()
	adjHeld := true
	defer func() { if adjHeld { b.adjCacheMu.RUnlock() } }()
	type result struct { count int64; err error }
	read := make(chan result, 1)
	go func() { count, err := run(); read <- result{count, err} }()
	deadline := time.Now().Add(3 * time.Second)
	for b.adjCacheMu.TryRLock() {
		b.adjCacheMu.RUnlock()
		if time.Now().After(deadline) { t.Fatal("public query did not reach adjacency cache publication") }
		time.Sleep(time.Millisecond)
	}
	b.writeBehind.mu.Lock()
	overlayHeld := true
	defer func() { if overlayHeld { b.writeBehind.mu.Unlock() } }()
	replayed := make(chan error, 1)
	go func() { replayed <- b.replayWriteBehind(frontier) }()
	for !frontier.countsApplied.Load() {
		if time.Now().After(deadline) { t.Fatal("replay did not publish committed data") }
		time.Sleep(time.Millisecond)
	}
	b.adjCacheMu.RUnlock()
	adjHeld = false
	select {
	case err := <-replayed: require.NoError(t, err)
	case <-time.After(3 * time.Second): t.Fatal("replay did not finish")
	}
	require.True(t, frontier.IsApplied())
	b.writeBehind.mu.Unlock()
	overlayHeld = false
	select {
	case got := <-read: require.NoError(t, got.err); return got.count
	case <-time.After(3 * time.Second): t.Fatal("public query did not finish"); return -1
	}
}
