package cypher

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A query that the database's concurrent-query limit rejects returns the limit
// error. The statement's deferred cleanup reads the request context, so the
// rejection must not replace it with the limit check's nil context (that made
// every rejected query panic in cleanup; release defect NOR-339).
func TestExecuteRejectedByConcurrentQueryLimitReturnsLimitError(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
		require.NoError(t, err)
		defer manager.Close()
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "limited"}))
		require.NoError(t, manager.SetDatabaseLimits(t.Context(), "limited", &multidb.Limits{Query: multidb.QueryLimits{MaxConcurrentQueries: 1}}))
		store, err := manager.GetStorage("limited")
		require.NoError(t, err)
		limited, ok := store.(interface {
			GetQueryLimitChecker() interface {
				CheckQueryRate() error
				CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
				GetQueryLimits() interface{}
			}
		})
		require.True(t, ok, "database storage exposes its query limit checker")
		// Another query holds the database's only slot.
		_, release, err := limited.GetQueryLimitChecker().CheckQueryLimits(t.Context())
		require.NoError(t, err)
		defer func() {
			if release != nil {
				release()
			}
		}()

		exec := NewStorageExecutor(store)
		var panicked any
		func() {
			defer func() { panicked = recover() }()
			_, err = exec.Execute(t.Context(), "RETURN 1", nil)
		}()
		require.Nil(t, panicked, "a query rejected by the concurrent-query limit panicked")
		require.ErrorIs(t, err, multidb.ErrQueryLimitExceeded)

		// Releasing the actual held checker slot must allow a public query, not
		// merely another direct checker call.
		release()
		release = nil
		result, retryErr := exec.Execute(t.Context(), "RETURN 7 AS recovered", nil)
		require.NoError(t, retryErr)
		require.Equal(t, [][]any{{int64(7)}}, result.Rows)

		// A real scalar-function boundary runs after admission. Preserve request
		// values/deadline through the admitted child, cancel the actual caller there,
		// then prove Execute releases its slot by another healthy public query.
		type requestKey struct{}
		const requestValue = "limit-composition-request"
		deadline := time.Now().Add(time.Hour)
		parent, stop := context.WithDeadline(context.WithValue(t.Context(), requestKey{}, requestValue), deadline)
		defer stop()
		request, cancelRequest := context.WithCancel(parent)
		defer cancelRequest()
		var observedValue any
		var observedDeadline time.Time
		var hadDeadline bool
		var observedCancellation error
		gate := newStatementGate(func(work context.Context) error {
			observedValue = work.Value(requestKey{})
			observedDeadline, hadDeadline = work.Deadline()
			cancelRequest()
			observedCancellation = work.Err()
			return nil
		}).install(t)
		canceledResult, canceledErr := exec.Execute(request, "RETURN nornicdb.testgate(1) AS value", nil)
		require.EqualValues(t, 1, gate.Calls(), "real public scalar boundary must run after admission")
		require.NoError(t, gate.Err())
		require.Equal(t, requestValue, observedValue)
		require.True(t, hadDeadline)
		require.Equal(t, deadline, observedDeadline)
		require.ErrorIs(t, observedCancellation, context.Canceled)
		require.ErrorIs(t, request.Err(), context.Canceled)
		require.ErrorIs(t, canceledErr, context.Canceled)
		require.Nil(t, canceledResult)
		result, retryErr = exec.Execute(t.Context(), "RETURN 9 AS after_cancel", nil)
		require.NoError(t, retryErr)
		require.Equal(t, [][]any{{int64(9)}}, result.Rows)
	})
}
