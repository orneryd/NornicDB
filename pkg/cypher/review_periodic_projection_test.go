package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReviewR73PeriodicProjection(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, dynamic := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%v/dynamic=%v", explicit, dynamic), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reviewr73")
				e := NewStorageExecutor(store)
				ctx := context.Background()
				if explicit {
					_, err := e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				body := "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:ReviewR73PeriodicProjection {value:$x})',{batchSize:1}) YIELD total WITH total MATCH (n:ReviewR73PeriodicProjection) RETURN n.value AS v ORDER BY v"
				query := body
				params := map[string]interface{}{}
				if dynamic {
					params["body"] = body
					query = "CALL apoc.cypher.run($body,{}) YIELD value RETURN value.v AS v"
				}
				result, err := e.Execute(ctx, query, params)
				require.NoError(t, err)
				t.Logf("projected rows after periodic batches=%v", result.Rows)
				if explicit {
					_, err = e.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				nodes, err := store.GetNodesByLabel("ReviewR73PeriodicProjection")
				require.NoError(t, err)
				require.Len(t, nodes, 3)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows, "later clauses must see each completed batch's values")
			})
		}
	}
}
