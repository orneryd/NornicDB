package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A withUpdate blocked on the write-order admission of its entities writes
// nothing when its owning context ends: the callback never runs, the metadata
// stays absent, the order hold is released, and a later healthy write of the
// same entities succeeds. The supplied context (value and deadline) is the one
// the order admission actually waits on.
func TestWithUpdateCancellationAtHeldOrderWritesNothing(t *testing.T) {
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)

	entity := writtenEntities{nodes: []NodeID{"test:order-held"}}
	var stripes orderStripeSet
	stripes.entities(entity)
	require.NoError(t, engine.writeBehind.order.lockContext(t.Context(), &stripes, true))
	released := false
	releaseOrder := func() {
		if released {
			return
		}
		released = true
		engine.writeBehind.order.unlock(&stripes, true)
	}
	t.Cleanup(releaseOrder)

	type requestContextKey struct{}
	marker := &struct{ request string }{"update-order-admission"}
	ctx, cancel := context.WithDeadline(context.WithValue(t.Context(), requestContextKey{}, marker), time.Now().Add(time.Hour))
	defer cancel()
	type seenAdmission struct {
		value    any
		deadline time.Time
	}
	waited := make(chan seenAdmission, 1)
	t.Cleanup(setAdmissionWaitObserver(func(_ any, seen context.Context) {
		if seen == nil {
			return
		}
		deadline, _ := seen.Deadline()
		select {
		case waited <- seenAdmission{value: seen.Value(requestContextKey{}), deadline: deadline}:
		default:
		}
	}))

	markerKey := []byte("test:update-order-marker")
	callbacks := 0
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		releaseOrder()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("canceled withUpdate did not join")
		}
	})
	go func() {
		defer close(joined)
		done <- engine.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
			callbacks++
			return txn.Set(markerKey, []byte{1})
		}, entity)
	}()

	select {
	case admission := <-waited:
		require.Same(t, marker, admission.value, "order admission waits on the supplied context value")
		require.False(t, admission.deadline.IsZero(), "order admission waits on the supplied deadline")
	case <-time.After(5 * time.Second):
		t.Fatal("withUpdate did not reach the held order admission")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("withUpdate stayed blocked after its owning context ended")
	}
	require.Zero(t, callbacks, "a write canceled before admission must not run its callback")
	releaseOrder()
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(markerKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))

	// Ownership was released: the same entities admit a healthy write.
	require.NoError(t, engine.withUpdate(t.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		callbacks++
		return txn.Set(markerKey, []byte{1})
	}, entity))
	require.Equal(t, 1, callbacks, "the healthy retry runs its callback once")
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(markerKey)
		return err
	}))
}

// The deleted-entry clean-up marker write runs under the clean-up worker's own
// context. A canceled worker request writes no marker and keeps the delete
// count, so the next clean-up retries it; a live one writes and drops the
// marker. The engine's stop joins the worker on the real native store.
func TestDeleteCleanupMarkerUsesWorkerContext(t *testing.T) {
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	cleanup := engine.deleteCleanup
	require.NotNil(t, cleanup)
	markerPresent := func() bool {
		t.Helper()
		present := false
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(deleteCleanupMarkerKey)
			present = err == nil
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		}))
		return present
	}

	cleanup.deletes.Store(3)
	engine.runDeleteCleanup(t.Context(), 3)
	require.Zero(t, cleanup.deletes.Load())
	require.False(t, markerPresent(), "a completed clean-up drops its marker")

	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	cleanup.deletes.Store(4)
	engine.runDeleteCleanup(stopped, 4)
	require.Equal(t, int64(4), cleanup.deletes.Load(), "a canceled clean-up keeps its count for the next attempt")
	require.False(t, markerPresent(), "a canceled worker request must not write the clean-up marker")

	// The worker is accepted engine work: Close cancels and joins it.
	require.NoError(t, engine.Close())
	joined := make(chan struct{})
	go func() {
		cleanup.run.stopAndJoin()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join the delete-clean-up worker")
	}
}

// Accepted write-behind replay applies its derived counts on the release's
// detached completion lifetime: a caller that cancels after its commit was
// acknowledged must not stop the replay's count writes, so the persisted label
// count agrees with the physically committed node.
func TestAcceptedReplayCountsSurviveCallerCancellation(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, engine, &Node{ID: "test:accepted-counts", Labels: []string{"Accepted"}, Properties: map[string]any{"v": int64(1)}})

	engine.db.commitGate.Lock()
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(engine.db.commitGate.Unlock) }
	workerWaiting := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &engine.db.commitGate {
			select {
			case workerWaiting <- seen:
			default:
			}
		}
	}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	joined := make(chan struct{})
	var generation *CommitBuffer
	heldBuffer := false
	releaseBuffer := func() {
		if heldBuffer {
			heldBuffer = false
			engine.writeBehind.mu.Unlock()
		}
	}
	t.Cleanup(func() {
		cancel()
		releaseBuffer()
		releaseGate()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("flush waiter did not join")
		}
		if generation != nil {
			select {
			case <-generation.Settled():
			case <-time.After(5 * time.Second):
				t.Error("accepted replay did not settle")
			}
		}
		engine.writeBehind.mu.Lock()
		engine.writeBehind.mu.Unlock()
	})
	go func() {
		defer close(joined)
		done <- engine.FlushWriteBehind(ctx)
	}()
	var replayContext context.Context
	select {
	case replayContext = <-workerWaiting:
		require.NotNil(t, replayContext, "accepted replay selects an explicit completion owner")
		require.Nil(t, replayContext.Done(), "accepted completion must outlive caller and engine-stop cancellation")
		require.NotEqual(t, ctx, replayContext, "replay must not retain the flush caller as its completion owner")
	case <-time.After(5 * time.Second):
		t.Fatal("accepted replay did not reach the physical commit gate")
	}
	engine.writeBehind.mu.Lock()
	heldBuffer = true
	require.Len(t, engine.writeBehind.draining, 1)
	generation = engine.writeBehind.draining[0]

	cancel()
	require.NoError(t, replayContext.Err(), "real caller cancellation must not cancel acknowledged replay")
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("caller cancellation did not stop its buffer wait")
	}
	releaseBuffer()
	releaseGate()
	select {
	case <-generation.Settled():
	case <-time.After(10 * time.Second):
		t.Fatal("accepted replay did not finish after the physical gate released")
	}
	require.True(t, generation.IsApplied())

	node, err := engine.GetNode(t.Context(), "test:accepted-counts")
	require.NoError(t, err)
	require.Contains(t, node.Labels, "Accepted")
	count, err := engine.NodeCountByLabel(t.Context(), "Accepted")
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "the accepted replay's count writes must complete after caller cancellation")
}
