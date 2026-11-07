package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A nested importing WITH reads a global after a WITH horizon. Its caller's
// cached clause scope is immutable across ordinary concurrent executions.
func TestCallScopeRuntimeCachedCallerImmutability(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-caller-cache"))
	queries := []string{
		"WITH 1 AS a CALL (a) { WITH 0 AS z CALL { WITH a RETURN a AS innerValue } RETURN innerValue AS value } RETURN value",
		"WITH 1 AS a CALL (a) { WITH 0 AS z CALL { WITH a RETURN a AS innerValue } RETURN * } RETURN innerValue AS value",
	}
	for _, query := range queries {
		type observation struct {
			result *ExecuteResult
			err    error
		}
		observations := make(chan observation, 32)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for worker := 0; worker < 32; worker++ {
			workers.Add(1)
			go func(nonce int) {
				defer workers.Done()
				<-start
				for repeat := 0; repeat < 3; repeat++ {
					result, err := executor.Execute(context.Background(), query, map[string]interface{}{"nonce": nonce*3 + repeat})
					observations <- observation{result, err}
				}
			}(worker)
		}
		close(start)
		go func() {
			workers.Wait()
			close(observations)
		}()
		for item := range observations {
			require.NoError(t, item.err, query)
			require.NotNil(t, item.result)
			require.Equal(t, []string{"value"}, item.result.Columns, query)
			require.Equal(t, [][]interface{}{{int64(1)}}, item.result.Rows, query)
		}
	}
}
