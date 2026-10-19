package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func frontierTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.FlushWriteBehind()); require.NoError(t, b.Close()) })
	return b
}

func appendFrontierNode(t *testing.T, b *BadgerEngine, namespace, id string) error {
	t.Helper()
	tx, err := b.BeginTransaction()
	if err != nil {
		return err
	}
	if err := tx.SetImplicit(true); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.SetNamespace(namespace); err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.CreateNode(&Node{ID: NodeID(namespace + ":" + id), Labels: []string{"Probe"}})
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// The second generation deliberately cannot apply. It contains only B and
// lies beyond DROP's fixed frontier, so DROP A must not wait for it.
func TestDeleteByPrefixFixedFrontier(t *testing.T) {
	b := frontierTestEngine(t)
	oldApply := b.writeBehind.apply
	entered := make(chan struct{})
	resume := make(chan struct{})
	futureEntered := make(chan struct{})
	futureResume := make(chan struct{})
	var first, second sync.Once
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		if buf.commits[0].namespace == "a" {
			first.Do(func() { close(entered); <-resume })
		} else {
			second.Do(func() { close(futureEntered); <-futureResume })
		}
		return oldApply(buf)
	}
	defer close(futureResume)
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	require.NoError(t, appendFrontierNode(t, b, "a", "old"))
	prepared, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, prepared.SetImplicit(true))
	require.NoError(t, prepared.SetNamespace("a"))
	_, err = prepared.CreateNode(&Node{ID: "a:prepared", Labels: []string{"Probe"}})
	require.NoError(t, err)
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _, err := b.DeleteByPrefixContext(ctx, "a:"); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("frontier never entered replay")
	}

	require.ErrorIs(t, appendFrontierNode(t, b, "a", "late"), ErrConflict, "no target ACK may cross the frontier")
	require.ErrorIs(t, prepared.Commit(), ErrConflict, "pre-frontier transactions cannot ACK after the fence")
	_, err = b.CreateNode(&Node{ID: "a:direct", Labels: []string{"Probe"}})
	require.ErrorIs(t, err, ErrConflict, "direct writes share namespace admission")
	b.writeBehind.mu.Lock()
	b.writeBehind.maxOps = 1
	b.writeBehind.mu.Unlock()
	require.NoError(t, appendFrontierNode(t, b, "b", "future"))
	require.Positive(t, b.writeBehind.PendingOpsInNamespace("a"))
	require.Positive(t, b.writeBehind.PendingOpsInNamespace("b"))
	close(resume)
	select {
	case <-futureEntered:
	case <-ctx.Done():
		t.Fatal("future generation never entered replay")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("DROP waited beyond its captured frontier")
	}
	require.Zero(t, b.writeBehind.PendingOpsInNamespace("a"))
	require.Positive(t, b.writeBehind.PendingOpsInNamespace("b"))
	nodes, err := NewNamespacedEngine(b, "a").NodeCount()
	require.NoError(t, err)
	require.Zero(t, nodes)
	nodes, err = NewNamespacedEngine(b, "b").NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), nodes)
}

func TestDeleteByPrefixContextCancellation(t *testing.T) {
	b := frontierTestEngine(t)
	oldApply := b.writeBehind.apply
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b.writeBehind.apply = func(buf *CommitBuffer) error { once.Do(func() { close(entered); <-resume }); return oldApply(buf) }
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	require.NoError(t, appendFrontierNode(t, b, "a", "old"))
	ctx, cancel := context.WithCancel(context.Background())
	earlier, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, earlier.SetNamespace("a"))
	_, err = earlier.CreateNode(&Node{ID: "a:earlier"})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, _, err := b.DeleteByPrefixContext(ctx, "a:"); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("frontier never entered replay")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled DROP did not return")
	}
	// A synchronous commit now follows every earlier ACK. Release the test's
	// paused replay after observing cancellation, before committing that writer.
	close(resume)
	require.NoError(t, earlier.Commit(), "cancelled preparation must not retire the namespace lifetime")
	require.NoError(t, appendFrontierNode(t, b, "a", "after_cancel"), "cancelled DROP must release namespace admission")
	count, err := NewNamespacedEngine(b, "a").NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "cancellation must leave A intact")
}

func TestDeleteByPrefixContextDeadlineAndApplyError(t *testing.T) {
	t.Run("barrier", func(t *testing.T) {
		b := frontierTestEngine(t)
		b.writeBarrier.RLock()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, _, err := b.DeleteByPrefixContext(ctx, "a:")
		b.writeBarrier.RUnlock()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("apply_error", func(t *testing.T) {
		b := frontierTestEngine(t)
		oldApply := b.writeBehind.apply
		applyErr := errors.New("frontier apply failed")
		b.writeBehind.apply = func(*CommitBuffer) error { return applyErr }
		require.NoError(t, appendFrontierNode(t, b, "a", "old"))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, err := b.DeleteByPrefixContext(ctx, "a:")
		require.ErrorIs(t, err, applyErr)
		b.writeBehind.Close()
		b.writeBehind.apply = oldApply
		// The stopped worker is replaced only in the test cleanup: finish the
		// unapplied generation directly before the engine closes.
		b.writeBehind.drainOne()
	})
}
