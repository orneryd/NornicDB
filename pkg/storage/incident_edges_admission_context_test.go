package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The transaction's incidence read is admitted through the transaction's own
// read lock, so a request already waiting for that lock ends when its context
// ends instead of waiting for the current holder. The bounded return below is
// measured before the holder releases, the holder keeps its hold, the canceled
// request leaves no hold behind, and the same transaction still answers a
// healthy caller afterwards.
//
// The admission observer pins the wait to the actual tx.mu and records the
// context the read passed to admission, so this covers the native public
// method's own admission rather than an earlier lock.
func TestIncidentEdgesCancelsWaitingForTransactionLock(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	hub := seedIncidentContextGraph(t, engine, 2)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waited := make(chan context.Context, 1)
	restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
		if owner != &tx.mu {
			return
		}
		select {
		case waited <- observed:
		default:
		}
	})
	t.Cleanup(restore)

	tx.mu.Lock()
	released := false
	unlock := func() {
		if released {
			return
		}
		released = true
		tx.mu.Unlock()
	}
	type outcome struct {
		edges []*Edge
		err   error
	}
	done := make(chan outcome, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unlock()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			t.Error("the incidence read goroutine did not join after its admission was released")
		}
	})
	go func() {
		defer close(joined)
		edges, err := tx.IncidentEdges(ctx, hub)
		done <- outcome{edges: edges, err: err}
	}()

	var admission context.Context
	select {
	case admission = <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("the incidence read never reached the transaction's held read lock")
	}

	cancel()
	started := time.Now()
	var out outcome
	select {
	case out = <-done:
	case <-time.After(100 * time.Millisecond):
		unlock()
		select {
		case out = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the incidence read did not return after its admission was released")
		}
		t.Fatalf("a canceled incidence read waited %v for the lock holder instead of returning within 100ms before the holder released it", time.Since(started))
	}
	elapsed := time.Since(started)
	<-joined
	require.Less(t, elapsed, 100*time.Millisecond,
		"a canceled incidence read returns without waiting for the lock holder")
	require.ErrorIs(t, out.err, context.Canceled)
	require.Nil(t, out.edges, "a canceled read publishes no partial relationship list")
	require.True(t, admission == ctx, "the incidence read dropped the caller context at admission")

	// The holder released nothing above: the canceled request returned while
	// the lock was still held, and it left no hold of its own behind.
	unlock()
	require.True(t, tx.mu.TryLock(), "the canceled request left no hold on the transaction")
	tx.mu.Unlock()

	edges, err := tx.IncidentEdges(t.Context(), hub)
	require.NoError(t, err, "the canceled request leaves the transaction healthy")
	require.Len(t, edges, 2)
}

// The transaction's own state keeps its precedence at admission: a
// transaction whose client released it reports ErrTransactionClosed even for a
// canceled request, and an admitted read on a closed transaction reports that
// closure instead of returning an empty relationship list.
func TestIncidentEdgesKeepsTransactionStateErrorPrecedence(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	hub := seedIncidentContextGraph(t, engine, 1)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	edges, err := tx.IncidentEdges(t.Context(), hub)
	require.ErrorIs(t, err, ErrTransactionClosed, "a closed transaction reports its closure, not an empty list")
	require.Nil(t, edges)

	tx.clientDetached.Store(true)
	canceled, stop := context.WithCancel(t.Context())
	stop()
	edges, err = tx.IncidentEdges(canceled, hub)
	require.ErrorIs(t, err, ErrTransactionClosed, "a released client reports its closure ahead of the request's cancellation")
	require.NotErrorIs(t, err, context.Canceled)
	require.Nil(t, edges)
}
