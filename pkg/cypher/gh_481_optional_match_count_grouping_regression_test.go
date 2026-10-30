package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGH481_OptionalMatchCountGroupsPerOuterRow reproduces issue #481:
// `MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->() RETURN x.id, count(r)`
// returned a single row containing the first key and the total count instead
// of one row per outer x with the per-x relationship count (0 for misses).
func TestGH481_OptionalMatchCountGroupsPerOuterRow(t *testing.T) {
	setup := func(store storage.Engine) {
		for index := int64(1); index <= 4; index++ {
			_, err := store.CreateNode(&storage.Node{
				ID:         storage.NodeID("n" + string(rune('0'+index))),
				Labels:     []string{"A"},
				Properties: map[string]interface{}{"id": index},
			})
			require.NoError(t, err)
		}
		err := store.CreateEdge(&storage.Edge{
			ID: "e1", Type: "NEXT", StartNode: "n1", EndNode: "n2",
			Properties: map[string]interface{}{"w": int64(10)},
		})
		require.NoError(t, err)
		err = store.CreateEdge(&storage.Edge{
			ID: "e2", Type: "NEXT", StartNode: "n2", EndNode: "n3",
			Properties: map[string]interface{}{"w": int64(20)},
		})
		require.NoError(t, err)
	}

	aggregateQuery := "MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->() RETURN x.id AS id, count(r) AS outdeg ORDER BY id"
	aggregateWant := [][]interface{}{
		{int64(1), int64(1)},
		{int64(2), int64(1)},
		{int64(3), int64(0)},
		{int64(4), int64(0)},
	}

	// Non-aggregated control from the issue: was already correct.
	controlQuery := "MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->(y) RETURN x.id AS id, y.id AS y ORDER BY id"
	controlWant := [][]interface{}{
		{int64(1), int64(2)},
		{int64(2), int64(3)},
		{int64(3), nil},
		{int64(4), nil},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh481")
			setup(store)
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			t.Run("grouped count per outer row", func(t *testing.T) {
				result, err := exec.Execute(ctx, aggregateQuery, nil)
				require.NoError(t, err)
				require.Equal(t, aggregateWant, result.Rows)
			})

			t.Run("grouped count re-expresses grouping", func(t *testing.T) {
				// Grouping must not collapse to a single key; x.id and the
				// count must agree row by row, in return-item order.
				result, err := exec.Execute(ctx, "MATCH (x:A) OPTIONAL MATCH (x)-[r:NEXT]->() RETURN count(r) AS outdeg, x.id AS id ORDER BY id", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{
					{int64(1), int64(1)},
					{int64(1), int64(2)},
					{int64(0), int64(3)},
					{int64(0), int64(4)},
				}, result.Rows)
			})

			t.Run("non-aggregated control", func(t *testing.T) {
				result, err := exec.Execute(ctx, controlQuery, nil)
				require.NoError(t, err)
				require.Equal(t, controlWant, result.Rows)
			})

			t.Run("pattern comprehension workaround matches", func(t *testing.T) {
				result, err := exec.Execute(ctx, "MATCH (x:A) RETURN x.id AS id, size([(x)-[:NEXT]->(y) | y.id]) AS outdeg ORDER BY id", nil)
				require.NoError(t, err)
				require.Equal(t, aggregateWant, result.Rows)
			})

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
