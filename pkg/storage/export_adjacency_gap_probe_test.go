package storage

import (
	"sync"
	"sync/atomic"
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
	b.adjCacheInvalidate()
	// Park after capture, outside all product cache locks. Durable revocation
	// must finish before oracle publication; a cache-lock park would deadlock
	// the fixture against that required ordering.
	captured, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	var claimed atomic.Bool
	observe := func(engine *BadgerEngine) {
		if engine == b && claimed.CompareAndSwap(false, true) {
			close(captured)
			<-resume
		}
	}
	previous := adjacencyReadCaptureObserver.Swap(&observe)
	type result struct {
		count int64
		err   error
	}
	read := make(chan result, 1)
	readerJoined, replayJoined := make(chan struct{}), make(chan struct{})
	overlayHeld, replayStarted := false, false
	defer func() {
		adjacencyReadCaptureObserver.Store(previous)
		if overlayHeld {
			b.writeBehind.mu.Unlock()
		}
		unblock()
		select {
		case <-readerJoined:
		case <-time.After(3 * time.Second):
			t.Error("public query did not join")
		}
		if replayStarted {
			select {
			case <-replayJoined:
			case <-time.After(3 * time.Second):
				t.Error("replay did not join")
			}
		}
	}()
	go func() {
		defer close(readerJoined)
		count, err := run()
		read <- result{count, err}
	}()
	select {
	case <-captured:
	case got := <-read:
		require.NoError(t, got.err)
		t.Fatal("public query completed before its captured read was parked")
	case <-time.After(3 * time.Second):
		t.Fatal("public query did not reach its captured read")
	}
	b.writeBehind.mu.Lock()
	overlayHeld = true
	replayed := make(chan error, 1)
	beforeReplay := b.db.oracle.published.Load()
	replayStarted = true
	go func() {
		defer close(replayJoined)
		replayed <- b.replayWriteBehind(frontier)
	}()
	select {
	case err := <-replayed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("replay did not finish")
	}
	require.Greater(t, b.db.oracle.published.Load(), beforeReplay)
	require.True(t, frontier.IsApplied())
	b.writeBehind.mu.Unlock()
	overlayHeld = false
	unblock()
	select {
	case got := <-read:
		require.NoError(t, got.err)
		return got.count
	case <-time.After(3 * time.Second):
		t.Fatal("public query did not finish")
		return -1
	}
}
