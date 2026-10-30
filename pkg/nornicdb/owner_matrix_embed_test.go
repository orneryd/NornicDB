package nornicdb

import (
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Shutdown matrix S2, EmbedWorker: concurrent Close callers all wait for the
// generation's held timer callback. A racing Reset either joins the same
// generation or returns at once because the worker is closing; Close is
// permanent, so Reset never publishes a generation after it.
func TestOwnerMatrixEmbedWorkerConcurrentCloseAndReset(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	engine := storage.NewNamespacedEngine(base, "test")
	worker := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
		NumWorkers: 1, ScanInterval: time.Hour, BatchDelay: time.Millisecond,
		MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	release, accepted := worker.retainTimerWork() // a timer callback in flight
	require.True(t, accepted)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()

	const closers = 3
	closed := make(chan struct{}, closers)
	for i := 0; i < closers; i++ {
		go func() { worker.Close(); closed <- struct{}{} }()
	}
	reset := make(chan struct{})
	go func() { worker.Reset(); close(reset) }()
	select {
	case <-closed:
		t.Fatal("Close returned while the generation's callback was held")
	case <-time.After(100 * time.Millisecond):
	}
	require.True(t, worker.work.Closing(), "Close must seal the worker before joining")
	_, lateAccepted := worker.retainTimerWork()
	require.False(t, lateAccepted, "timer enrollment after Close began must be refused")

	released = true
	release()
	for i := 0; i < closers; i++ {
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not complete after the callback returned")
		}
	}
	select {
	case <-reset:
	case <-time.After(5 * time.Second):
		t.Fatal("Reset did not complete")
	}
	gen := worker.generation()
	require.True(t, gen.Closing(), "Reset must not publish a generation after Close")
	require.Zero(t, gen.Active())
	worker.Close() // a repeat Close joins the completed stop
}
