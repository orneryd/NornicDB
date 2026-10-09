package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Result columns as Neo4j 5.26.30 reads them (#907): UNION branches match
// columns by name, in any order, at the top level, in CALL and in EXISTS /
// COUNT / COLLECT subqueries; a COLLECT subquery returns one column; a
// RETURN inside a CALL body can't name two columns alike; and a property
// read after an aggregate's subscript (collect(n)[0].id) groups nothing.
func TestResultColumnsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "result_columns"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CQ {id: 1}), (:CQ {id: 2}), (:CP {id: 3})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"RETURN 1 AS x, 2 AS y UNION RETURN 3 AS y, 4 AS x", []string{"x", "y"}, [][]interface{}{{int64(1), int64(2)}, {int64(4), int64(3)}}},
		{"RETURN 1 AS x, 2 AS y UNION ALL RETURN 3 AS y, 4 AS x", []string{"x", "y"}, [][]interface{}{{int64(1), int64(2)}, {int64(4), int64(3)}}},
		{"CALL { RETURN 1 AS x, 2 AS y UNION RETURN 3 AS y, 4 AS x } RETURN x, y ORDER BY x", []string{"x", "y"}, [][]interface{}{{int64(1), int64(2)}, {int64(4), int64(3)}}},
		{"RETURN COUNT { RETURN 1 UNION RETURN 1 } AS c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } AS c", []string{"c"}, [][]interface{}{{int64(2)}}},
		{"RETURN COLLECT { RETURN 1 AS a UNION RETURN 2 AS a } AS l", []string{"l"}, [][]interface{}{{[]interface{}{int64(1), int64(2)}}}},
		{"RETURN EXISTS { MATCH (n:CQ) RETURN n UNION MATCH (m:CP) RETURN m AS n } AS e", []string{"e"}, [][]interface{}{{true}}},
		{"MATCH (q:CQ) WHERE EXISTS { MATCH (n:CQ) RETURN n.id AS i UNION RETURN 5 AS i } RETURN count(q) AS c", []string{"c"}, [][]interface{}{{int64(2)}}},
		{"MATCH (q:CQ) WHERE NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION MATCH (m:Nope2) RETURN m.id AS i } RETURN count(q) AS c", []string{"c"}, [][]interface{}{{int64(2)}}},
		{"UNWIND [1] AS k WITH k WHERE NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION RETURN 7 AS i } RETURN count(*) AS c", []string{"c"}, [][]interface{}{{int64(0)}}},
		{"UNWIND [1] AS k WITH k WHERE NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION MATCH (m:Nope2) RETURN m.id AS i } RETURN count(*) AS c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"RETURN [k IN [1] WHERE NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION RETURN 7 AS i }] AS l", []string{"l"}, [][]interface{}{{[]interface{}{}}}},
		{"RETURN CASE WHEN NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION RETURN 7 AS i } THEN 1 ELSE 2 END AS v", []string{"v"}, [][]interface{}{{int64(2)}}},
		{"RETURN NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION MATCH (m:Nope2) RETURN m.id AS i } AS v", []string{"v"}, [][]interface{}{{true}}},
		{"MATCH (n:CQ) WITH n ORDER BY n.id RETURN collect(n)[0].id AS v", []string{"v"}, [][]interface{}{{int64(1)}}},
		{"MATCH (n:CQ) WITH n ORDER BY n.id RETURN head(collect(n)).id AS v", []string{"v"}, [][]interface{}{{int64(1)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.columns, result.Columns)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	for _, query := range []string{
		"RETURN 1 AS x UNION RETURN 2 AS y",
		"RETURN EXISTS { RETURN 1 UNION RETURN 2 } AS e",
		"RETURN COUNT { RETURN 1 UNION RETURN 2 } AS c",
		"CALL { RETURN 1 AS a, 2 AS a } RETURN a",
		"RETURN COLLECT { UNWIND [1, 2] AS k RETURN k, k } AS l",
		"RETURN COLLECT { UNWIND [1, 2] AS k RETURN k, k + 1 AS j } AS l",
		"MATCH (n:CQ) RETURN collect(n.id)[0] + n.id AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
	// A subquery that fails while it runs fails the statement; it never
	// reads as "no rows".
	_, err = exec.Execute(ctx, "MATCH (q:CQ) WHERE EXISTS { MATCH (n:CQ) WHERE n.id = 1 / (q.id - q.id) RETURN n.id AS i UNION RETURN 5 AS i } RETURN count(q) AS c", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", code)
}

func TestUnionColumnOrder(t *testing.T) {
	order, same := unionColumnOrder([]string{"x", "y"}, []string{"x", "y"})
	require.True(t, same)
	require.Nil(t, order)
	order, same = unionColumnOrder([]string{"x", "y"}, []string{"y", "x"})
	require.True(t, same)
	require.Equal(t, []int{1, 0}, order)
	_, same = unionColumnOrder([]string{"x", "y"}, []string{"x", "z"})
	require.False(t, same)
	_, same = unionColumnOrder([]string{"x"}, []string{"x", "y"})
	require.False(t, same)
	require.Equal(t, [][]interface{}{{int64(2), int64(1)}, {nil, int64(3)}},
		reorderUnionRows([][]interface{}{{int64(1), int64(2)}, {int64(3)}}, []int{1, 0}))
	require.Equal(t, []string{"n.y"}, semanticFreeReferences(removeAggregateCalls("collect(n.x)[0].id + n.y")))
	require.Empty(t, semanticFreeReferences("[1, 2][0..1]"))
}
