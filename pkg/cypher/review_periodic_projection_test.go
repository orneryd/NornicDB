package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestReviewR73PeriodicVisibilityAfterBatches covers the later-clause projections
// over apoc.periodic.iterate batches. Each case keeps its executor fixture, literal
// query text, expected rows and message; explicit/dynamic stay a 2x2 matrix.
func TestReviewR73PeriodicVisibilityAfterBatches(t *testing.T) {
	for _, tc := range []struct {
		name         string
		build        func(t *testing.T) (*StorageExecutor, storage.Engine)
		label        string
		body         string
		dynamicQuery string
		logMessage   string
		wantRows     [][]interface{}
		rowsMessage  string
	}{
		{
			name: "TestReviewR73PeriodicProjection",
			build: func(t *testing.T) (*StorageExecutor, storage.Engine) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reviewr73")
				return NewStorageExecutor(store), store
			},
			label:        "ReviewR73PeriodicProjection",
			body:         "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:ReviewR73PeriodicProjection {value:$x})',{batchSize:1}) YIELD total WITH total MATCH (n:ReviewR73PeriodicProjection) RETURN n.value AS v ORDER BY v",
			dynamicQuery: "CALL apoc.cypher.run($body,{}) YIELD value RETURN value.v AS v",
			logMessage:   "projected rows after periodic batches=%v",
			wantRows:     [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			rowsMessage:  "later clauses must see each completed batch's values",
		},
		{
			name: "TestReviewR73PeriodicReadAfterBatches",
			build: func(t *testing.T) (*StorageExecutor, storage.Engine) {
				return newProcedureAdmissionExecutor(t, false)
			},
			label:        "ReviewR73PeriodicRead",
			body:         "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:ReviewR73PeriodicRead)',{batchSize:1}) YIELD total WITH total MATCH (n:ReviewR73PeriodicRead) RETURN count(n) AS found",
			dynamicQuery: "CALL apoc.cypher.run($body,{}) YIELD value RETURN value.found AS found",
			logMessage:   "row after periodic batches=%v",
			wantRows:     [][]interface{}{{int64(3)}},
			rowsMessage:  "later clauses must see the completed batches",
		},
	} {
		for _, explicit := range []bool{false, true} {
			for _, dynamic := range []bool{false, true} {
				t.Run(tc.name+"/"+fmt.Sprintf("explicit=%v/dynamic=%v", explicit, dynamic), func(t *testing.T) {
					e, store := tc.build(t)
					ctx := context.Background()
					if explicit {
						_, err := e.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					query := tc.body
					params := map[string]interface{}{}
					if dynamic {
						params["body"] = tc.body
						query = tc.dynamicQuery
					}
					result, err := e.Execute(ctx, query, params)
					require.NoError(t, err)
					t.Logf(tc.logMessage, result.Rows)
					if explicit {
						_, err = e.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					nodes, err := store.GetNodesByLabel(ctx, tc.label)
					require.NoError(t, err)
					require.Len(t, nodes, 3)
					require.Equal(t, tc.wantRows, result.Rows, tc.rowsMessage)
				})
			}
		}
	}
}
