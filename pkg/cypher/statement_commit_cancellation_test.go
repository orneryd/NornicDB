package cypher

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// commitBoundaryContext cancels at the storage commit's first observation of
// it: the commit refuses, so nothing the statement wrote is stored.
type commitBoundaryContext struct {
	context.Context
	cancel context.CancelFunc
}

func newCommitBoundaryContext() *commitBoundaryContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &commitBoundaryContext{Context: ctx, cancel: cancel}
}

func (c *commitBoundaryContext) Err() error {
	// The commit observes the statement's context, which reads this one.
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".CommitContext") {
			c.cancel()
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

// The engine announces a created node once the commit that wrote it is
// visible (commit notifications run inside the commit, after its writes are
// applied). Canceling there is canceling immediately after the commit.
type commitNotificationCancel struct {
	executor *StorageExecutor
	cancel   atomic.Pointer[context.CancelFunc]
}

func newCommitNotificationCancel(t *testing.T) *commitNotificationCancel {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "commit_boundary")
	fixture := &commitNotificationCancel{executor: NewStorageExecutorWithQueryCachePolicy(store, 0, 0)}
	store.OnNodeCreated(func(*storage.Node) {
		if cancel := fixture.cancel.Load(); cancel != nil {
			(*cancel)()
		}
	})
	return fixture
}

// cancelAtCommit returns a context the next commit's notification cancels.
func (f *commitNotificationCancel) cancelAtCommit() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel.Store(&cancel)
	return ctx
}

func (f *commitNotificationCancel) committedNodes(t *testing.T) int64 {
	t.Helper()
	f.cancel.Store(nil)
	result, err := f.executor.Execute(context.Background(), "MATCH (n:CommitBoundary) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].(int64)
}

// A statement whose writes durably committed reports its result even when
// its caller cancels afterwards; one canceled before its commit fails with
// the cancellation and stores nothing. Both auto-commit statements and an
// explicit transaction's COMMIT keep that boundary.
func TestStatementCommitCancellationBoundary(t *testing.T) {
	t.Run("auto_commit_canceled_after_commit", func(t *testing.T) {
		fixture := newCommitNotificationCancel(t)
		ctx := fixture.cancelAtCommit()
		result, err := fixture.executor.Execute(ctx, "UNWIND range(1, 3) AS x CREATE (:CommitBoundary {x: x}) RETURN x ORDER BY x", nil)
		require.NoError(t, err, "a committed statement reports success")
		require.ErrorIs(t, ctx.Err(), context.Canceled, "the caller canceled while the statement ran")
		require.Len(t, result.Rows, 3)
		require.Equal(t, int64(3), fixture.committedNodes(t))
	})
	t.Run("auto_commit_canceled_before_commit", func(t *testing.T) {
		fixture := newCommitNotificationCancel(t)
		ctx := newCommitBoundaryContext()
		_, err := fixture.executor.Execute(ctx, "UNWIND range(1, 3) AS x CREATE (:CommitBoundary {x: x}) RETURN x ORDER BY x", nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, fixture.committedNodes(t))
	})
	t.Run("explicit_commit_completed", func(t *testing.T) {
		fixture := newCommitNotificationCancel(t)
		_, err := fixture.executor.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		_, err = fixture.executor.Execute(context.Background(), "UNWIND range(1, 3) AS x CREATE (:CommitBoundary {x: x})", nil)
		require.NoError(t, err)
		ctx := fixture.cancelAtCommit()
		_, err = fixture.executor.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err, "a completed COMMIT reports success")
		require.ErrorIs(t, ctx.Err(), context.Canceled, "the caller canceled during COMMIT")
		require.Equal(t, int64(3), fixture.committedNodes(t))
	})
	t.Run("explicit_commit_not_reached", func(t *testing.T) {
		fixture := newCommitNotificationCancel(t)
		_, err := fixture.executor.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		_, err = fixture.executor.Execute(context.Background(), "UNWIND range(1, 3) AS x CREATE (:CommitBoundary {x: x})", nil)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = fixture.executor.Execute(ctx, "COMMIT", nil)
		require.ErrorIs(t, err, context.Canceled)
		_, _ = fixture.executor.Execute(context.Background(), "ROLLBACK", nil)
		require.Zero(t, fixture.committedNodes(t))
	})
}
