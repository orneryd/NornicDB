package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Changing literal values reuse immutable CALL metadata without copying the
// normalized query on each cache hit. The executable query is still fresh.
func TestCallScopeRuntimeCanonicalLookupAllocations(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical-lookup-allocations"))
	for _, width := range []int{1, 12} {
		t.Run(fmt.Sprintf("columns_%d", width), func(t *testing.T) {
			columns := make([]string, width)
			for index := range columns {
				columns[index] = fmt.Sprintf("'warm' AS canonical_lookup_column_%d", index)
			}
			query := "CALL () { RETURN " + strings.Join(columns, ", ") + " }"
			body, _, _, _ := exec.parseCallSubquery(t.Context(), query)
			model, err := exec.runtimeCallQueryScope(context.Background(), query, body, nil)
			require.NoError(t, err)
			query = strings.ReplaceAll(query, "'warm'", "'current'")
			canonical, changed := normalizeLiteralText(query)
			require.True(t, changed)
			require.Greater(t, len(canonical), 32)
			require.Less(t, len(canonical), 1024)
			key := semanticRuntimeScopeKey{kind: semanticRuntimeCall, query: query}
			value, _, normalized, found := semanticCanonicalCallScope(key)
			require.True(t, normalized)
			require.True(t, found)
			require.Same(t, model, value.model)
			allocations := testing.AllocsPerRun(100, func() {
				cached, _, normalized, hit := semanticCanonicalCallScope(key)
				if !normalized || !hit || cached.model != model {
					t.Fatal("canonical lookup did not reuse the complete scope metadata")
				}
			})
			require.Zero(t, allocations, "canonical cache-hit lookup should not allocate")
		})
	}
}
