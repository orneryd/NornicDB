package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallScopeTransitionRegressions(t *testing.T) {
	for _, tc := range callscopetest.TransitionRegressions() {
		t.Run(tc.Name, func(t *testing.T) {
			for _, consumer := range []string{"facts", "headers", "EXPLAIN", "PROFILE", "Execute"} {
				t.Run(consumer, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "transition"))
					var columns []string
					var err error
					var result *ExecuteResult
					switch consumer {
					case "facts":
						exec.StatementFacts(tc.Query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
							statement.Columns.Visit(func(names []string) { columns = names }, func() { t.Fatal("unknown columns") })
						}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { err = reason.Err })
					case "headers":
						columns, err = exec.StatementResultColumns(tc.Query, RawSource)
					default:
						query := tc.Query
						if consumer != "Execute" {
							query = consumer + " " + query
						}
						result, err = exec.Execute(context.Background(), query, nil)
						if result != nil {
							columns = result.Columns
						}
					}
					if tc.Code != "" {
						require.Error(t, err, tc.Query)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, tc.Code, code)
						return
					}
					require.NoError(t, err, tc.Query)
					require.Equal(t, tc.Columns, columns)
					if consumer == "Execute" || consumer == "PROFILE" {
						require.Equal(t, tc.Rows, result.Rows)
						if tc.Name == "delete_retained" {
							remaining, err := exec.Execute(context.Background(), "MATCH (n:R73Delete) RETURN count(n) AS remaining", nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{int64(0)}}, remaining.Rows, "DELETE must remove the imported node")
						}
					}
				})
			}
		})
	}
}
