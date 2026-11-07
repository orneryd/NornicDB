package cypher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reusing semantic metadata must never retain one request's imported values.
// Exercise the public path concurrently with two visible-name sets and distinct
// parameter rows, while both scalar bodies have the same query text.
func TestCallScopeConcurrentImportIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-concurrent"))
	type observation struct {
		call   string
		base   int64
		result *ExecuteResult
		err    error
	}
	observations := make(chan observation, 24)
	var workers sync.WaitGroup
	for index := 0; index < 12; index++ {
		for _, call := range []string{"CALL (a)", "CALL (*)"} {
			workers.Add(1)
			go func(index int, call string) {
				defer workers.Done()
				base := int64(index*10 + 1)
				query := "UNWIND $xs AS a WITH a,$marker AS b " + call + " { RETURN COLLECT { WITH a AS x RETURN x } AS v } RETURN a,v ORDER BY a"
				result, err := exec.Execute(ctx, query, map[string]interface{}{"xs": []interface{}{base, base + 1}, "marker": base + 1000})
				observations <- observation{call: call, base: base, result: result, err: err}
			}(index, call)
		}
	}
	workers.Wait()
	close(observations)
	for observed := range observations {
		require.NoError(t, observed.err, observed.call)
		require.NotNil(t, observed.result, observed.call)
		require.Equal(t, []string{"a", "v"}, observed.result.Columns)
		require.Equal(t, [][]interface{}{
			{observed.base, []interface{}{observed.base}},
			{observed.base + 1, []interface{}{observed.base + 1}},
		}, observed.result.Rows, observed.call)
	}
}
