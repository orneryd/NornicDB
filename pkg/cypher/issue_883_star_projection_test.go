package cypher

// NornicDB #883: `WITH *, items` and `RETURN *, items` read the * as an
// expression ("could not evaluate expression: *"). The * now stands for every
// variable in scope: columns in name order, except one an item redefines,
// then the items. Answers are Neo4j 5.26.30's.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// issue883Value is a result value with nodes as N:<id> and relationships as
// R:<type>, as the Neo4j probe recorded them.
func issue883Value(value interface{}) interface{} {
	switch v := value.(type) {
	case *storage.Node:
		if id, ok := v.Properties["id"]; ok {
			return fmt.Sprint("N:", id)
		}
		return "N:None"
	case *storage.Edge:
		return "R:" + v.Type
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = issue883Value(item)
		}
		return out
	}
	return value
}

func TestIssue883StarWithItems(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Q {id:1})-[:R {w:1}]->(:Q {id:2})-[:R {w:2}]->(:Q {id:3})-[:R {w:3}]->(:Q {id:4})", nil)
			require.NoError(t, err)
			for _, tc := range []struct {
				query   string
				columns []string
				rows    [][]interface{}
			}{
				{"MATCH (s {id:1}) WITH *, 1 AS one RETURN s.id, one", []string{"s.id", "one"}, [][]interface{}{{int64(1), int64(1)}}},
				{"WITH 1 AS a WITH *, 2 AS b RETURN a, b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"MATCH (s {id:1}) RETURN *, 1 AS one", []string{"s", "one"}, [][]interface{}{{"N:1", int64(1)}}},
				{"MATCH p = (s)-[r:R*1..3]->(t) WITH *, [i IN range(0, size(r)-1) | nodes(p)[i]] AS a RETURN s.id, size(a) ORDER BY s.id, size(a) LIMIT 3", []string{"s.id", "size(a)"}, [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}, {int64(1), int64(3)}}},
				{"WITH 1 AS b, 2 AS a RETURN *, 3 AS c", []string{"a", "b", "c"}, [][]interface{}{{int64(2), int64(1), int64(3)}}},
				{"WITH 1 AS b, 2 AS a WITH *, 3 AS c RETURN *", []string{"a", "b", "c"}, [][]interface{}{{int64(2), int64(1), int64(3)}}},
				{"WITH 1 AS a RETURN *, a + 1 AS a", []string{"a"}, [][]interface{}{{int64(2)}}},
				{"WITH 1 AS a WITH *, a + 1 AS a RETURN a", []string{"a"}, [][]interface{}{{int64(2)}}},
				{"WITH 1 AS a, 2 AS b RETURN *, 5 AS a", []string{"b", "a"}, [][]interface{}{{int64(2), int64(5)}}},
				{"WITH 1 AS a, 2 AS b WITH *, 5 AS a RETURN *", []string{"a", "b"}, [][]interface{}{{int64(5), int64(2)}}},
				{"WITH 1 AS a, 2 AS c RETURN *, 5 AS b", []string{"a", "c", "b"}, [][]interface{}{{int64(1), int64(2), int64(5)}}},
				{"WITH 1 AS a RETURN *, a", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"MATCH (s:Q {id:1}) RETURN *, s", []string{"s"}, [][]interface{}{{"N:1"}}},
				{"WITH 1 AS a RETURN *, a AS b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(1)}}},
				{"WITH 1 AS a, 2 AS b RETURN *, count(*) AS a", []string{"b", "a"}, [][]interface{}{{int64(2), int64(1)}}},
				{"WITH 1 AS a WITH *, a AS a RETURN a", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"WITH 1 AS a WITH DISTINCT *, 2 AS b RETURN a, b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"WITH 1 AS a WITH *, 2 AS b WHERE b > 1 RETURN a, b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"WITH 1 AS a RETURN DISTINCT *, 2 AS b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"UNWIND [1, 2, 2] AS x WITH *, count(*) AS c RETURN x, c ORDER BY x", []string{"x", "c"}, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}}},
				{"UNWIND [1, 2, 2] AS x RETURN *, count(*) AS c ORDER BY x", []string{"x", "c"}, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}}},
				{"MATCH (s:Q) WITH *, s.id AS i ORDER BY i DESC LIMIT 2 RETURN i", []string{"i"}, [][]interface{}{{int64(4)}, {int64(3)}}},
				{"WITH 1 AS a RETURN *, 2 AS b ORDER BY b SKIP 0 LIMIT 1", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"MATCH (s:Q {id:1}) RETURN *, s.id", []string{"s", "s.id"}, [][]interface{}{{"N:1", int64(1)}}},
				{"WITH 1 AS `x y` RETURN *, 2 AS z", []string{"x y", "z"}, [][]interface{}{{int64(1), int64(2)}}},
				{"MATCH (s:Q {id:1}) WITH *, 1 AS one MATCH (s)-[:R]->(t) RETURN t.id, one", []string{"t.id", "one"}, [][]interface{}{{int64(2), int64(1)}}},
				{"OPTIONAL MATCH (z:Nope) RETURN *, 1 AS one", []string{"z", "one"}, [][]interface{}{{nil, int64(1)}}},
				{"MATCH (s:Q {id:1})-[r:R]->(t) RETURN *, r.w AS w", []string{"r", "s", "t", "w"}, [][]interface{}{{"R:R", "N:1", "N:2", int64(1)}}},
				{"UNWIND [3, 1, 2] AS x RETURN *, x * 10 AS y ORDER BY y DESC", []string{"x", "y"}, [][]interface{}{{int64(3), int64(30)}, {int64(2), int64(20)}, {int64(1), int64(10)}}},
				{"MATCH (s:Q) WHERE s.id < 3 RETURN *, s.id AS i ORDER BY i", []string{"s", "i"}, [][]interface{}{{"N:1", int64(1)}, {"N:2", int64(2)}}},
				{"MATCH (s:Q) WITH *, s.id AS i WHERE i > 2 RETURN i ORDER BY i", []string{"i"}, [][]interface{}{{int64(3)}, {int64(4)}}},
				{"WITH 1 AS a, 2 AS b WITH *, a + b AS c WITH *, c * 2 AS d RETURN a, b, c, d", []string{"a", "b", "c", "d"}, [][]interface{}{{int64(1), int64(2), int64(3), int64(6)}}},
				{"CREATE (n:Tmp883 {v: 1}) RETURN *, n.v AS v", []string{"n", "v"}, [][]interface{}{{"N:None", int64(1)}}},
				{"MERGE (n:Tmp883 {v: 2}) RETURN *, n.v AS v", []string{"n", "v"}, [][]interface{}{{"N:None", int64(2)}}},
				{"MATCH (n:Tmp883) SET n.w = n.v RETURN *, n.w AS w ORDER BY w", []string{"n", "w"}, [][]interface{}{{"N:None", int64(1)}, {"N:None", int64(2)}}},
				{"MATCH (n:Tmp883) WITH *, n.v AS v DELETE n RETURN v ORDER BY v", []string{"v"}, [][]interface{}{{int64(1)}, {int64(2)}}},
				{"CALL { RETURN 1 AS a } RETURN *, 2 AS b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}},
				{"UNWIND [1, 2] AS x CALL (x) { RETURN x * 2 AS y } RETURN *, x + y AS z ORDER BY x", []string{"x", "y", "z"}, [][]interface{}{{int64(1), int64(2), int64(3)}, {int64(2), int64(4), int64(6)}}},
				{"WITH *, 1 AS a RETURN a", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"WITH *, 1 AS a RETURN *", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"WITH 1 AS a RETURN DISTINCT *", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"WITH 1 AS a WITH DISTINCT * RETURN a", []string{"a"}, [][]interface{}{{int64(1)}}},
				{"UNWIND [1, 1, 2] AS x RETURN DISTINCT *, 1 AS one ORDER BY x", []string{"x", "one"}, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}},
				{"UNWIND [1, 1] AS x WITH DISTINCT * RETURN x", []string{"x"}, [][]interface{}{{int64(1)}}},
				{"UNWIND [1, 1, 2] AS x UNWIND [1, 1] AS y WITH DISTINCT * RETURN x, y ORDER BY x, y", []string{"x", "y"}, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}},
				{"MATCH (n:Q {id:1}) UNWIND [1, 1] AS x WITH DISTINCT * RETURN n.id AS id, x", []string{"id", "x"}, [][]interface{}{{int64(1), int64(1)}}},
				{"UNWIND [1, 1, 2] AS x WITH DISTINCT * WHERE x > 0 RETURN x ORDER BY x", []string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
				{"UNWIND [3, 1, 1, 2] AS x WITH DISTINCT * ORDER BY x LIMIT 2 RETURN x", []string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
				{"UNWIND [1, 1] AS x WITH DISTINCT * RETURN count(*) AS c", []string{"c"}, [][]interface{}{{int64(1)}}},
				{"MATCH (z:Nope) RETURN *, 1 AS one", []string{"z", "one"}, [][]interface{}{}},
				{"MATCH (z:Nope) WITH *, 1 AS one RETURN *", []string{"one", "z"}, [][]interface{}{}},
			} {
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err, tc.query)
				}
				res, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err, tc.query)
				}
				rows := make([][]interface{}, 0, len(res.Rows))
				for _, row := range res.Rows {
					values := make([]interface{}, len(row))
					for i, value := range row {
						values[i] = issue883Value(value)
					}
					rows = append(rows, values)
				}
				require.Equal(t, tc.columns, res.Columns, tc.query)
				require.Equal(t, tc.rows, rows, tc.query)
			}
			for _, query := range []string{"RETURN *", "RETURN *, 1 AS a"} {
				_, err := exec.Execute(ctx, query, nil)
				require.True(t, strings.HasPrefix(statusText(err), "Neo.ClientError.Statement.SyntaxError: "), "%s: %s", query, statusText(err))
			}
		})
	}
}

func TestIssue883ReturnColumnHelpers(t *testing.T) {
	clauses := []pipelineClause{{kind: pipelineClauseWith, text: "WITH 1 AS a"}, {kind: pipelineClauseReturn, text: "RETURN a"}}
	require.Equal(t, "", pipelineOriginalReturnText(clauses, 0))
	require.Equal(t, "", pipelineOriginalReturnText(clauses, 2))
	require.Equal(t, "RETURN a", pipelineOriginalReturnText(clauses, 1))

	// Without the statement's own text the columns stay as executed.
	final := &ExecuteResult{Columns: []string{"a"}, Rows: [][]interface{}{{int64(1)}}}
	pipelineNameReturnColumns(final, "RETURN a", "", nil)
	require.Equal(t, []string{"a"}, final.Columns)

	require.Equal(t, "a", projectionVariableText("a"))
	require.Equal(t, "`x y`", projectionVariableText("x y"))
	require.Equal(t, "`a``b`", projectionVariableText("a`b"))
}
