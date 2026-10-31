package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestResidualFlushWaitCancellation(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	workerAtDispatch, releaseDispatch := make(chan struct{}), make(chan struct{})
	var dispatchOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseDispatch) })
	dispatch := b.writeBehind.dispatch
	// Pause the existing worker after it retired generation one. Generation
	// two remains unclaimed, so a synchronous drain by the waiter is exposed.
	b.writeBehind.dispatch = func() {
		dispatch()
		dispatchOnce.Do(func() {
			close(workerAtDispatch)
			<-releaseDispatch
		})
	}
	ack := func(id NodeID) {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.SetImplicit(true))
		_, err = tx.CreateNode(&Node{ID: id})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	ack("test:flush-one")
	b.writeBehind.signal()
	select {
	case <-workerAtDispatch:
	case <-time.After(2 * time.Second):
		t.Fatal("existing replay worker did not reach its dispatch boundary")
	}
	ack("test:flush-two")
	b.writeBehind.mu.RLock()
	second := b.writeBehind.active
	b.writeBehind.mu.RUnlock()
	b.db.commitGate.Lock()
	held := true
	defer func() {
		if held {
			b.db.commitGate.Unlock()
		}
	}()
	waited := make(chan struct{}, 1)
	workerWaiting := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, waitCtx context.Context) {
		if owner == b.writeBehind || owner == &b.db.commitGate {
			select {
			case waited <- struct{}{}:
			default:
			}
		}
		if owner == &b.db.commitGate {
			select {
			case workerWaiting <- waitCtx:
			default:
			}
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- b.FlushWriteBehind(ctx)
	}()
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		b.db.commitGate.Unlock()
		held = false
		releaseOnce.Do(func() { close(releaseDispatch) })
		<-done
		t.Fatal("flush did not reach an actual replay/admission wait")
	}
	cancel()
	returned := false
	var flushErr error
	select {
	case flushErr = <-done:
		returned = true
	case <-time.After(2 * time.Second):
	}
	releaseOnce.Do(func() { close(releaseDispatch) })
	// The cancelled waiter has returned. Only the existing worker can now
	// claim generation two; it remains blocked until its physical peer leaves.
	select {
	case workerCtx := <-workerWaiting:
		require.Nil(t, workerCtx, "accepted replay retains its detached context")
	case <-time.After(2 * time.Second):
		t.Fatal("existing worker did not retain accepted replay")
	}
	b.db.commitGate.Unlock()
	held = false
	if !returned {
		flushErr = <-done
	}
	require.True(t, returned, "cancelled flush must return while accepted replay is still blocked")
	require.ErrorIs(t, flushErr, context.Canceled)
	// Cancellation ends only the wait. Both accepted generations still apply.
	select {
	case <-second.Settled():
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not settle accepted replay after the peer left")
	}
	require.True(t, second.IsApplied())
	_, err = b.GetStoredNode("test:flush-two")
	require.NoError(t, err)
}
