package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDatabaseManagerStorageExposesQueryLimitsToExecutor(t *testing.T) {
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	manager, err := multidb.NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	require.NoError(t, manager.SetDatabaseLimits("nornic", &multidb.Limits{
		Query: multidb.QueryLimits{MaxResults: 1},
		Rate:  multidb.RateLimits{MaxQueriesPerSecond: 1},
	}))
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"one", "two"} {
		_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Item"}})
		require.NoError(t, err)
	}
	executor := cypher.NewStorageExecutor(engine)

	result, err := executor.Execute(context.Background(), "MATCH (n:Item) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	_, err = executor.Execute(context.Background(), "MATCH (n:Item) RETURN n", nil)
	require.ErrorIs(t, err, multidb.ErrRateLimitExceeded)
}
