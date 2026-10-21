package cypher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type reentrantStatementLogHandler struct {
	executor *StorageExecutor
	entered  bool
	cancel   context.CancelFunc
	result   *ExecuteResult
	err      error
}

type postExecutionMemoryHandler struct {
	cancel context.CancelFunc
	failed bool
}

func (h *postExecutionMemoryHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *postExecutionMemoryHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "slow query" && !h.failed {
		h.failed = true
		if h.cancel != nil {
			h.cancel()
		}
		if scope := memoryScopeFromContext(ctx); scope != nil {
			scope.tracker.fail(memoryLimitExceededError(transactionMemoryLimitCode, 1, scope.tracker.limit, 1, transactionMemoryMaxSetting))
		}
	}
	return nil
}
func (h *postExecutionMemoryHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *postExecutionMemoryHandler) WithGroup(string) slog.Handler      { return h }

func TestPostCommitMemoryFailureDoesNotReportRollback(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	exec := NewStorageExecutor(store)
	h := &postExecutionMemoryHandler{}
	exec.SetLogger(slog.New(h))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	result, err := exec.Execute(t.Context(), "CREATE (:PublishedBeforeLog)", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, h.failed)
	stored, err := NewStorageExecutor(store).Execute(t.Context(), "MATCH (n:PublishedBeforeLog) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
	requirePoolsEmpty(t, "nornic")
}

func TestCallerCancellationPrecedesMemoryFailure(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := newTransactionMemoryExecutor(t)
	h := &postExecutionMemoryHandler{cancel: cancel}
	exec.SetLogger(slog.New(h))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	_, err := exec.Execute(ctx, "UNWIND range(1, 10) AS x RETURN x", nil)
	require.ErrorIs(t, err, context.Canceled)
	requirePoolsEmpty(t, "nornic")
}

func (h *reentrantStatementLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *reentrantStatementLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message != "slow query" {
		return nil
	}
	if !h.entered {
		h.entered = true
		var childCtx context.Context
		childCtx, h.cancel = context.WithCancel(ctx)
		defer h.cancel()
		h.result, h.err = h.executor.Execute(childCtx, "RETURN 1 AS one", nil)
	} else {
		h.cancel()
	}
	return nil
}
func (h *reentrantStatementLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *reentrantStatementLogHandler) WithGroup(string) slog.Handler      { return h }

// Public Execute starts a new logical statement, even when its caller retained
// the context of another Execute. Only transparent routing continuations share
// a committed outcome. A reentrant read must not inherit the write's success.
func TestExecuteIndependentOutcomeDuringCommittedLog(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	h := &reentrantStatementLogHandler{executor: exec}
	exec.SetLogger(slog.New(h))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	result, err := exec.Execute(t.Context(), "CREATE (:IndependentOutcome)", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, h.entered)
	require.ErrorIs(t, h.err, context.Canceled)
	require.Nil(t, h.result)
	stored, err := NewStorageExecutor(store).Execute(t.Context(), "MATCH (n:IndependentOutcome) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}
