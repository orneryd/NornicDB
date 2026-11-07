package cypher

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Scalar scope rejection and local wildcard UNION columns follow the live
// Neo4j 5.26.30 reference. A bare body does not implicitly import an outer
// variable it never reads; UNION ALL retains duplicate local results.
func TestCallScopeReviewReproductions(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call-scope-review"))
	_, err := exec.Execute(ctx, "CREATE (:Target {n:1})", nil)
	require.NoError(t, err)
	for _, call := range []string{"CALL (a) { ", "CALL (*) { "} {
		for _, prefix := range []string{"", "WITH * "} {
			query := "UNWIND [1,2] AS a " + call + prefix + "RETURN COLLECT { RETURN * } AS v } RETURN a,v ORDER BY a"
			t.Run(query, func(t *testing.T) {
				_, err := exec.Execute(ctx, query, nil)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			})
		}
	}
	for _, body := range []string{
		"MATCH (b:Target) RETURN * UNION MATCH (b:Target) RETURN b",
		"MATCH (b:Target) RETURN * UNION ALL MATCH (b:Target) RETURN b",
		"MATCH (b:Target) RETURN * UNION MATCH (b:Target) RETURN b AS b",
	} {
		query := "UNWIND [1,2] AS a CALL { " + body + " } RETURN a,b"
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"a", "b"}, result.Columns)
			want := []int64{1, 2}
			if strings.Contains(body, "UNION ALL") {
				want = []int64{1, 1, 2, 2}
			}
			var actual []int64
			for _, row := range result.Rows {
				require.Len(t, row, 2)
				actual = append(actual, row[0].(int64))
				node, ok := row[1].(*storage.Node)
				require.True(t, ok)
				require.Equal(t, []string{"Target"}, node.Labels)
				require.Equal(t, map[string]interface{}{"n": int64(1)}, node.Properties)
			}
			require.ElementsMatch(t, want, actual)
		})
	}
}

func TestCallScopeReviewReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires the pinned Neo4j scope reference")
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	t.Cleanup(func() { _ = session.Close(ctx) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call-scope-reference"))
	for _, call := range []string{"CALL (a) { ", "CALL (*) { "} {
		for _, prefix := range []string{"", "WITH * "} {
			for _, projection := range []string{"*", "a"} {
				query := "UNWIND [1,2] AS a " + call + prefix + "RETURN COLLECT { RETURN " + projection + " } AS v } RETURN a,v ORDER BY a"
				t.Run(query, func(t *testing.T) {
					var columns []string
					var rows [][]any
					result, referenceErr := session.Run(ctx, query, nil)
					if referenceErr == nil {
						columns, referenceErr = result.Keys()
						if referenceErr == nil {
							var records []*neo4j.Record
							records, referenceErr = result.Collect(ctx)
							for _, record := range records {
								rows = append(rows, record.Values)
							}
						}
					}
					actual, actualErr := exec.Execute(ctx, query, nil)
					if referenceErr != nil {
						var queryError *neo4j.Neo4jError
						require.ErrorAs(t, referenceErr, &queryError)
						require.Error(t, actualErr)
						code, _ := nornicerrors.Neo4jStatus(actualErr)
						require.Equal(t, queryError.Code, code)
						return
					}
					require.NoError(t, actualErr)
					require.Equal(t, columns, actual.Columns)
					require.Equal(t, rows, actual.Rows)
				})
			}
		}
	}
}
