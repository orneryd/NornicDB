package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Answers independently checked against Neo4j 5.26.30 in both transaction modes.
func TestCallStarScope(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"with", "WITH 11 AS x CALL (*) { WITH x WITH 2 AS y RETURN x*y AS a } RETURN a", [][]interface{}{{int64(22)}}},
		{"distinct", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"aggregation", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2,3] AS y WITH sum(y) AS total RETURN x*total AS a } RETURN a", [][]interface{}{{int64(77)}}},
		{"nested", "WITH 11 AS x CALL (*) { WITH x WITH 1 AS ignored CALL (*) { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"writes", "WITH 11 AS x CALL (*) { WITH x WITH 1 AS ignored CREATE (n:CallStar {v:x}) RETURN n.v AS a } RETURN a", [][]interface{}{{int64(11)}}},
		{"union", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2] AS y WITH DISTINCT y RETURN x*y AS a UNION WITH x UNWIND [3,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"unknown", "WITH 11 AS x CALL (*) { WITH x WITH DISTINCT x RETURN missing AS a } RETURN a", nil},
	} {
		for _, explicit := range []bool{false, true} {
			mode := "auto"
			if explicit {
				mode = "explicit"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_star"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, tc.query, nil)
				if tc.want == nil {
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.EqualError(t, err, "Variable `missing` not defined")
					return
				}
				require.NoError(t, err)
				require.Equal(t, []string{"a"}, result.Columns)
				require.Equal(t, tc.want, result.Rows)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				if tc.name == "writes" {
					stored, err := exec.Execute(ctx, "MATCH (n:CallStar) RETURN n.v AS a", nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, stored.Rows)
				}
			})
		}
	}
}

func TestCallScopeClauseSyntax(t *testing.T) {
	for _, tc := range []struct {
		query   string
		scoped  bool
		names   []string
		invalid bool
	}{
		{" \n cAlL ( * ) { RETURN 1 AS a } \t", true, nil, false},
		{"CALL () { RETURN 1 AS a }", true, []string{}, false},
		{"CALL (x, `y`) { RETURN x+y AS a }", true, []string{"x", "y"}, false},
		{"CALL (x+1) { RETURN 1 AS a }", true, []string{}, true},
		{"CALL { RETURN 1 AS a }", false, nil, false},
		{"CALL (x", false, nil, false},
		{"CALL db.labels()", false, nil, false},
		{"MATCH (n) CALL (n) { RETURN n }", false, nil, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			scope := readCallSubqueryScope(tc.query)
			require.Equal(t, tc.scoped, scope.scoped)
			require.Equal(t, tc.names, scope.names)
			require.Equal(t, tc.invalid, scope.invalid)
			if tc.scoped || tc.query == "CALL { RETURN 1 AS a }" {
				require.Equal(t, byte('{'), tc.query[scope.bodyOffset], "scope reader locates the body after whitespace and the import clause")
			}
		})
	}
}
