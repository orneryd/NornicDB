package search

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFilterVerdictWaiterCancellationDoesNotCancelProducer(t *testing.T) {
	ctx := context.WithValue(t.Context(), filterVerdictKey{}, &filterVerdicts{})
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	opts := &SearchOptions{Where: func(context.Context, *storage.Node) (bool, error) {
		calls.Add(1)
		close(entered)
		<-release
		return true, nil
	}}
	node := &storage.Node{ID: "a"}
	type outcome struct {
		admitted bool
		err      error
	}
	producer := make(chan outcome, 1)
	go func() { ok, err := opts.where(ctx, node); producer <- outcome{ok, err} }()
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("predicate did not start")
	}
	waiterCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	waiter := make(chan outcome, 1)
	go func() { ok, err := opts.where(waiterCtx, node); waiter <- outcome{ok, err} }()
	cancel()
	select {
	case result := <-waiter:
		require.False(t, result.admitted)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("canceled waiter did not stop")
	}
	close(release)
	released = true
	select {
	case result := <-producer:
		require.True(t, result.admitted)
		require.NoError(t, result.err)
	case <-time.After(10 * time.Second):
		t.Fatal("producer did not finish")
	}
	ok, err := opts.where(ctx, node)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(1), calls.Load())
	canceled, stop := context.WithCancel(ctx)
	stop()
	ok, err = opts.where(canceled, node)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled, "cached success never bypasses caller cancellation")
}

func TestFilterVerdictsRetainErrorsAndDatabaseIdentity(t *testing.T) {
	cause := errors.New("cannot decide")
	calls := 0
	opts := &SearchOptions{Where: func(context.Context, *storage.Node) (bool, error) { calls++; return false, cause }}
	ctx := context.WithValue(t.Context(), filterVerdictKey{}, &filterVerdicts{})
	a := &storage.Node{ID: "same"}
	metadata := a.RuntimeMetadata()
	metadata.Database = "a"
	a.SetRuntimeMetadata(metadata)
	for range 2 {
		ok, err := opts.where(ctx, a)
		require.False(t, ok)
		require.ErrorIs(t, err, cause)
		require.ErrorIs(t, err, ErrFilterEvaluation)
	}
	require.Equal(t, 1, calls, "one undecidable candidate is shared as an error, never a rejection")
	b := storage.CopyNode(a)
	metadata.Database = "b"
	b.SetRuntimeMetadata(metadata)
	_, err := opts.where(ctx, b)
	require.ErrorIs(t, err, cause)
	require.Equal(t, 2, calls, "same local ID in another database is distinct")
	opts.Where = func(context.Context, *storage.Node) (bool, error) { calls++; return true, nil }
	fresh := context.WithValue(ctx, filterVerdictKey{}, &filterVerdicts{})
	ok, err := opts.where(fresh, a)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, calls, "a later request retries in its own scope")
}

func TestFilterVerdictPreservesFailureWhenCancellationAlsoArrives(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx = context.WithValue(ctx, filterVerdictKey{}, &filterVerdicts{})
	cause := errors.New("predicate failed")
	opts := &SearchOptions{Where: func(context.Context, *storage.Node) (bool, error) { cancel(); return false, cause }}
	ok, err := opts.where(ctx, &storage.Node{ID: "a"})
	require.False(t, ok)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrFilterEvaluation)
}
