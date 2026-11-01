// Author: Codex, NOR-20 pipeline scope cancellation lane, 2026-10-31.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNOR20WithScopeCancellation: a caller cancellation raised from a plugin
// function evaluated in a WITH projection must stop the statement with the
// caller's own error.
//
// The control (same query, function does not cancel) returns the three doubled
// rows under the declared alias. Before the fix the cancel variant panicked
// with "assignment to entry in nil map": the canceled WITH projected no plan,
// the scope transfer returned a nil scope, and the following UNWIND wrote its
// alias into that nil map (pipeline_executor.go).
func TestNOR20WithScopeCancellation(t *testing.T) {
	const query = "WITH test.cancelOnce() AS c UNWIND range(1, $n) AS x RETURN x * 2 AS v"
	params := map[string]interface{}{"n": int64(3)}

	forEachParser(t, func(t *testing.T) {
		t.Run("control", func(t *testing.T) {
			calls := 0
			withPluginFunctions(t, map[string]interface{}{
				"test.cancelOnce": func() int64 {
					calls++
					return 1
				},
			})
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20-scope-control"))
			result, err := exec.Execute(context.Background(), query, params)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, []string{"v"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}}, result.Rows)
		})

		t.Run("cancel", func(t *testing.T) {
			// The caller owns the context and the cancellation: the plugin
			// function cancels it once while the WITH projection runs.
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			withPluginFunctions(t, map[string]interface{}{
				"test.cancelOnce": func() int64 {
					calls++
					if calls == 1 {
						cancel()
					}
					return 1
				},
			})
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20-scope-cancel"))
			_, err := exec.Execute(parent, query, params)
			require.Equal(t, context.Canceled, parent.Err())
			require.Equal(t, 1, calls, "nothing may be evaluated after the cancellation")
			require.Error(t, err, "a canceled statement must not report success")
			require.ErrorIs(t, err, context.Canceled)
		})
	})
}
