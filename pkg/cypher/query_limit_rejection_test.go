package cypher

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A query that the database's concurrent-query limit rejects returns the limit
// error. The statement's deferred cleanup reads the request context, so the
// rejection must not replace it with the limit check's nil context (that made
// every rejected query panic in cleanup; release defect NOR-339).
func TestExecuteRejectedByConcurrentQueryLimitReturnsLimitError(t *testing.T) {
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
	defer release()

	exec := NewStorageExecutor(store)
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		_, err = exec.Execute(t.Context(), "RETURN 1", nil)
	}()
	require.Nil(t, panicked, "a query rejected by the concurrent-query limit panicked")
	require.ErrorIs(t, err, multidb.ErrQueryLimitExceeded)
}
