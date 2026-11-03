package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlushWriteBehindCancellationBeforeRotationAdmission(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{ID: "test:flush-admission", Properties: map[string]any{"payload": "accepted"}})
	ctx, cancel := context.WithCancel(t.Context())
	type admission struct {
		ctx      context.Context
		rotation bool
	}
	entered := make(chan admission, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner != &b.writeBehind.mu {
			return
		}
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		rotation := false
		for {
			frame, more := frames.Next()
			rotation = rotation || strings.HasSuffix(frame.Function, ".rotate")
			if !more {
				break
			}
		}
		select {
		case entered <- admission{seen, rotation}:
		default:
		}
	}))
	b.writeBehind.mu.Lock()
	active := b.writeBehind.active
	pending := b.writeBehind.opCount
	draining := len(b.writeBehind.draining)
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(b.writeBehind.mu.Unlock) }
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unlock()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("flush waiter did not join")
		}
	})
	go func() { defer close(joined); done <- b.FlushWriteBehind(ctx) }()
	select {
	case event := <-entered:
		require.True(t, event.rotation, "must reach actual rotation lock admission")
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach rotation admission")
	}
	cancel()
	var result error
	select {
	case result = <-done:
		assert.Same(t, active, b.writeBehind.active, "cancellation before admission must not rotate accepted payload")
		assert.Equal(t, pending, b.writeBehind.opCount)
		assert.Equal(t, draining, len(b.writeBehind.draining))
	case <-time.After(5 * time.Second):
		t.Error("FlushWriteBehind stayed blocked after cancellation at held buffer admission")
		unlock()
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("flush did not finish after rotation holder released")
		}
	}
	unlock()
	assert.ErrorIs(t, result, context.Canceled)
	require.NoError(t, b.FlushWriteBehind(t.Context()))
	require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(nodeKey("test:flush-admission")); return err }))
}

func TestFlushWriteBehindCancellationAfterRotationKeepsReplayOwner(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{ID: "test:flush-accepted", Properties: map[string]any{"payload": "accepted"}})
	b.db.commitGate.Lock()
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(b.db.commitGate.Unlock) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	joined := make(chan struct{})
	workerWaiting := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &b.db.commitGate {
			select {
			case workerWaiting <- seen:
			default:
			}
		}
	}))
	heldBuffer := false
	releaseBuffer := func() {
		if heldBuffer {
			heldBuffer = false
			b.writeBehind.mu.Unlock()
		}
	}
	var generation *CommitBuffer
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
	})
	go func() { defer close(joined); done <- b.FlushWriteBehind(ctx) }()
	var replayContext context.Context
	select {
	case replayContext = <-workerWaiting:
		require.NotNil(t, replayContext, "accepted replay has an explicit completion owner")
		require.Nil(t, replayContext.Done(), "accepted replay remains detached from caller and engine stop")
	case <-time.After(5 * time.Second):
		t.Fatal("accepted replay did not reach real physical gate")
	}
	b.writeBehind.mu.Lock()
	heldBuffer = true
	require.Len(t, b.writeBehind.draining, 1)
	generation = b.writeBehind.draining[0]
	require.False(t, generation.IsApplied())
	cancel()
	require.NoError(t, replayContext.Err(), "caller cancellation must not cancel accepted replay")
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("caller cancellation did not stop its buffer wait")
	}
	require.False(t, generation.IsApplied(), "cancellation must not pretend the blocked accepted replay applied")
	releaseBuffer()
	releaseGate()
	select {
	case <-generation.Settled():
	case <-time.After(5 * time.Second):
		t.Fatal("accepted replay did not finish after physical gate released")
	}
	require.True(t, generation.IsApplied())
	require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(nodeKey("test:flush-accepted")); return err }))
	require.NoError(t, b.FlushWriteBehind(t.Context()))
}

func TestRotationCaptureCancellationLeavesOwnershipUnchanged(t *testing.T) {
	w := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return nil })
	w.Close()
	t.Cleanup(func() {
		if err := w.Flush(); err != nil {
			t.Error(err)
		}
		w.Close()
	})
	const generations = bulkReadCancellationInterval*2 + 1
	for i := range generations {
		require.NoError(t, w.Append(BufferedMutation{Kind: BufferedOpNode, Node: &Node{ID: NodeID(fmt.Sprintf("test:rotation%d", i))}}))
		_, err := w.rotate(t.Context())
		require.NoError(t, err)
	}
	require.NoError(t, w.Append(BufferedMutation{Kind: BufferedOpNode, Node: &Node{ID: "test:rotation-active"}}))
	active := w.active
	oldDraining := len(w.draining)
	oldOps := w.opCount
	// Admission, first copy poll, then the next two inner copy polls. The last
	// actual poll cancels before the active generation can change ownership.
	ctx := newIndexPollCancelContext(t, 4)
	partial, err := w.rotate(ctx, true)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, partial)
	require.Same(t, active, w.active)
	require.Len(t, w.draining, oldDraining)
	require.Equal(t, oldOps, w.opCount)
	captured, err := w.rotate(t.Context(), true)
	require.NoError(t, err)
	require.Len(t, captured, generations+1)
	require.Same(t, active, captured[len(captured)-1])
	require.NoError(t, w.Flush())
}
