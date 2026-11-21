package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Independent public executors share the real database admission owner. A
// concurrent rejection and another statement's completion must be race-safe.
func TestConcurrentPublicAdmissionDiagnostics(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "limited"}))
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "limited", &multidb.Limits{Query: multidb.QueryLimits{MaxConcurrentQueries: 1}}))
	store, err := manager.GetStorage("limited")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const workers, attempts = 8, 50
	var completed atomic.Int64
	var group sync.WaitGroup
	failures := make(chan error, workers)
	start := make(chan struct{})
	for worker := range workers {
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for index := range attempts {
				value := int64(worker*attempts + index)
				result, err := exec.Execute(ctx, "RETURN $value AS value", map[string]any{"value": value})
				if err != nil {
					if !errors.Is(err, multidb.ErrQueryLimitExceeded) || result != nil {
						failures <- fmt.Errorf("unexpected rejection: result=%v error=%w", result, err)
						return
					}
				} else if result == nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != value {
					failures <- fmt.Errorf("wrong admitted result for %d: %v", value, result)
					return
				}
				completed.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()
	close(failures)
	for failure := range failures {
		require.NoError(t, failure)
	}
	require.EqualValues(t, workers*attempts, completed.Load())
	result, err := NewStorageExecutor(store).Execute(t.Context(), "RETURN 9 AS recovered", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(9)}}, result.Rows)
}
