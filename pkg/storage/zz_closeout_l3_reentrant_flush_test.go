package storage

// Close-out lane L3 (v1.5.2 verification campaign), item P-N152-16 (NOR-208,
// N15-72): "A cancelled write-behind flush cannot return while called from
// inside the worker's own notification dispatch". Test-side only.
//
// The background write-behind worker replays a generation and then delivers
// that generation's commit notifications synchronously on its own goroutine
// (drainOne -> dispatch -> drainCommitNotifications). A listener that, inside
// that delivery, acknowledges another buffered write and then calls
// FlushWriteBehind with a cancellable context waits for a replay only the same
// worker goroutine performs, so that replay is blocked for as long as the
// listener waits. The item's expectation: the cancelled flush returns its
// cancellation promptly and the accepted write is kept.
//
// Messages starting with PRECONDITION mean the setup did not reach the state
// the item describes; DEFECT marks the item's disagreement.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

const (
	closeoutL3CancelAfter  = 200 * time.Millisecond
	closeoutL3ReturnBound  = 3 * time.Second
	closeoutL3HangDeadline = 15 * time.Second
)

type closeoutL3ListenerResult struct {
	goroutine      uint64
	afterSignal    bool
	writeErr       error
	writeBuffered  bool
	pendingAtFlush int
	flushErr       error
	elapsed        time.Duration
}

// closeoutL3BufferedNode acknowledges one node create through the write-behind
// buffer without the testing helpers, so it can run on a listener goroutine.
func closeoutL3BufferedNode(ctx context.Context, engine *BadgerEngine, id NodeID) (bool, error) {
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := tx.SetImplicit(true); err != nil {
		return false, err
	}
	if err := tx.SetNamespace("test"); err != nil {
		return false, err
	}
	if _, err := tx.CreateNode(ctx, &Node{ID: id, Labels: []string{"L3"}}); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return tx.AcknowledgedBuffered(), nil
}

// runCloseoutL3ReentrantFlush arranges the reentrant flush and returns the
// listener's result. flushCtx builds the listener's flush context.
func runCloseoutL3ReentrantFlush(t *testing.T, flushCtx func() (context.Context, context.CancelFunc)) (*BadgerEngine, closeoutL3ListenerResult, bool) {
	t.Helper()
	// Background worker running; its timer never fires within the test, so
	// only the explicit signal below starts the first replay.
	engine := newWriteBehindTestEngine(t, time.Hour)
	var afterSignal atomic.Bool
	results := make(chan closeoutL3ListenerResult, 1)
	var once sync.Once
	engine.OnNodeCreated(func(n *Node) {
		if n == nil || n.ID != "test:closeout-l3-first" {
			return
		}
		once.Do(func() {
			r := closeoutL3ListenerResult{goroutine: fixtureowner.CurrentGoroutineID(), afterSignal: afterSignal.Load()}
			writeCtx, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
			r.writeBuffered, r.writeErr = closeoutL3BufferedNode(writeCtx, engine, "test:closeout-l3-second")
			cancelWrite()
			if r.writeErr == nil {
				r.pendingAtFlush = engine.writeBehind.PendingOps()
				ctx, cancel := flushCtx()
				started := time.Now()
				r.flushErr = engine.FlushWriteBehind(ctx)
				r.elapsed = time.Since(started)
				cancel()
			}
			results <- r
		})
	})
	bufferedCreate(t, engine, &Node{ID: "test:closeout-l3-first", Labels: []string{"L3"}})
	select {
	case r := <-results:
		t.Fatalf("PRECONDITION: the listener ran at commit time on goroutine %d, before any replay (result %+v)", r.goroutine, r)
	default:
	}
	require.Positive(t, engine.writeBehind.PendingOps(), "PRECONDITION: the first create must be pending before the worker replays it")
	testGoroutine := fixtureowner.CurrentGoroutineID()
	afterSignal.Store(true)
	engine.writeBehind.signal()
	select {
	case r := <-results:
		t.Logf("listener goroutine=%d test goroutine=%d afterSignal=%v writeBuffered=%v writeErr=%v pendingAtFlush=%d flushErr=%v elapsed=%s",
			r.goroutine, testGoroutine, r.afterSignal, r.writeBuffered, r.writeErr, r.pendingAtFlush, r.flushErr, r.elapsed)
		require.True(t, r.afterSignal, "PRECONDITION: listener must run in the worker's replay dispatch")
		require.NotEqual(t, testGoroutine, r.goroutine, "PRECONDITION: listener must run on the worker goroutine")
		require.NoError(t, r.writeErr, "PRECONDITION: the listener's buffered write must be accepted")
		require.True(t, r.writeBuffered, "PRECONDITION: the listener's write must be ACKed through the buffer")
		return engine, r, true
	case <-time.After(closeoutL3HangDeadline):
		return engine, closeoutL3ListenerResult{}, false
	}
}

// The item's case: a cancelled flush from inside the worker's own dispatch.
func TestCloseoutL3_N152_16_ReentrantCancelledFlushReturns(t *testing.T) {
	engine, r, returned := runCloseoutL3ReentrantFlush(t, func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), closeoutL3CancelAfter)
	})
	if !returned {
		// Unstick: a non-cancellable flush from this goroutine replays the
		// pending generation inline, which lets the listener's wait finish so
		// the engine can close.
		unstuck := make(chan error, 1)
		go func() { unstuck <- engine.FlushWriteBehind(context.Background()) }()
		select {
		case err := <-unstuck:
			t.Logf("unstick flush returned %v", err)
		case <-time.After(closeoutL3HangDeadline):
			t.Logf("unstick flush did not return either")
		}
		t.Fatalf("DEFECT: the listener's flush, cancelled after %s, had not returned %s later (reentrant wait on its own worker)", closeoutL3CancelAfter, closeoutL3HangDeadline)
	}
	require.True(t, errors.Is(r.flushErr, context.DeadlineExceeded), "DEFECT: the cancelled reentrant flush must return its cancellation, got %v", r.flushErr)
	require.Less(t, r.elapsed, closeoutL3ReturnBound, "DEFECT: the cancelled reentrant flush must return promptly after cancellation at %s", closeoutL3CancelAfter)
	// Accepted data is kept: the second write replays once the worker is free.
	done := make(chan error, 1)
	go func() { done <- engine.FlushWriteBehind(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err, "DEFECT: the accepted write must replay after the listener returned")
	case <-time.After(closeoutL3HangDeadline):
		t.Fatalf("DEFECT: the follow-up flush did not return")
	}
	got, err := engine.GetNode(t.Context(), "test:closeout-l3-second")
	require.NoError(t, err, "DEFECT: the listener's accepted write must be retained")
	require.Equal(t, NodeID("test:closeout-l3-second"), got.ID)
}

// Control (item matrix row 2): the same reentrant listener with a flush that
// cannot cancel completes and replays the accepted write.
func TestCloseoutL3_N152_16_ControlReentrantUncancellableFlushCompletes(t *testing.T) {
	engine, r, returned := runCloseoutL3ReentrantFlush(t, func() (context.Context, context.CancelFunc) {
		return context.Background(), func() {}
	})
	require.True(t, returned, "control: the uncancellable reentrant flush did not return within %s", closeoutL3HangDeadline)
	require.NoError(t, r.flushErr, "control: uncancellable reentrant flush")
	got, err := engine.GetNode(t.Context(), "test:closeout-l3-second")
	require.NoError(t, err, "control: accepted write retained")
	require.Equal(t, NodeID("test:closeout-l3-second"), got.ID)
}

// Control (item matrix row 3): an ordinary (non-reentrant) flush cancelled
// while the worker's replay of the generation is blocked returns promptly.
func TestCloseoutL3_N152_16_ControlNonReentrantCancelledFlushReturns(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce, enterOnce sync.Once
	releaseApply := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseApply)
	apply := engine.writeBehind.apply
	engine.writeBehind.apply = func(buf *CommitBuffer) error {
		if _, found, _ := buf.getNode("test:closeout-l3-blocked"); found {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return apply(buf)
	}
	bufferedCreate(t, engine, &Node{ID: "test:closeout-l3-blocked", Labels: []string{"L3"}})
	engine.writeBehind.signal()
	select {
	case <-entered:
	case <-time.After(closeoutL3HangDeadline):
		t.Fatal("PRECONDITION: the worker never started the blocked replay")
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeoutL3CancelAfter)
	started := time.Now()
	flushErr := make(chan error, 1)
	go func() { flushErr <- engine.FlushWriteBehind(ctx) }()
	select {
	case err := <-flushErr:
		elapsed := time.Since(started)
		cancel()
		t.Logf("non-reentrant cancelled flush: err=%v elapsed=%s", err, elapsed)
		require.True(t, errors.Is(err, context.DeadlineExceeded), "control: expected cancellation, got %v", err)
		require.Less(t, elapsed, closeoutL3ReturnBound, "control: prompt return")
	case <-time.After(closeoutL3HangDeadline):
		cancel()
		releaseApply()
		t.Fatal("control: the non-reentrant cancelled flush did not return")
	}
	releaseApply()
	require.NoError(t, engine.FlushWriteBehind(context.Background()), "control: replay after release")
	got, err := engine.GetNode(t.Context(), "test:closeout-l3-blocked")
	require.NoError(t, err, "control: accepted write retained")
	require.Equal(t, NodeID("test:closeout-l3-blocked"), got.ID)
}
