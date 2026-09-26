package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPipelineEvaluatesEveryProjectionItem: the pipeline evaluates the items
// it once handed to another route - graph expressions inside operators and
// comprehensions, dynamic property access, clause keywords in any case - and
// a MATCH of several parts after another MATCH joins on its bound variables
// (Neo4j's results).
func TestPipelineEvaluatesEveryProjectionItem(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}), (x1 {name: 'x1'}), (x2 {name: 'x2'}), (:Other {name: 'o'})",
		"MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}), (x1 {name: 'x1'}), (x2 {name: 'x2'}) " +
			"CREATE (a)-[:KNOWS {since: 1}]->(x1), (a)-[:KNOWS {since: 2}]->(x2), (b)-[:KNOWS {since: 3}]->(x1), " +
			"(b)-[:KNOWS {since: 4}]->(x2), (c)-[:KNOWS {since: 5}]->(x1)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) MATCH (a)-->(x), (b)-->(x), (c)-->(x) RETURN x.name AS x",
			[][]interface{}{{"x1"}}},
		{"MATCH (a) WHERE a.name = 'A' MATCH (b) WHERE b.name = 'B' MATCH (a)-->(x), (b)-->(x) RETURN x.name AS x ORDER BY x",
			[][]interface{}{{"x1"}, {"x2"}}},
		{"MATCH (a) WHERE a.name = 'A' OR a.name = 'C' MATCH (a)-->(x) WHERE x.name = 'x2' RETURN a.name AS a",
			[][]interface{}{{"A"}}},
		{"MATCH (n {name: 'A'}) RETURN n['nam' + 'e'] AS v", [][]interface{}{{"A"}}},
		{"MATCH ({name: 'C'})-[r]->() RETURN r['sin' + 'ce'] AS v", [][]interface{}{{int64(5)}}},
		{"MATCH (n {name: 'A'}) RETURN size([(n)-->() | 1]) > 1 AS b", [][]interface{}{{true}}},
		{"MATCH (n {name: 'A'}) RETURN [x IN [n] | size([(x)-->() | 1])] AS l", [][]interface{}{{[]interface{}{int64(2)}}}},
		{"MATCH (x {name: 'x1'}) RETURN COUNT { ()-[:KNOWS]->(x) } AS c", [][]interface{}{{int64(3)}}},
		{"mAtCh (n:Other) rEtUrN n.name AS name", [][]interface{}{{"o"}}},
		{"MATCH (n:Other) WiTh n.name AS name UnWiNd [1] AS i ReTuRn name", [][]interface{}{{"o"}}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
}

// TestNormalizeMultiMatchWhereClausesMovesEveryWhere: the WHEREs of every
// required MATCH but the last move to one WHERE after the last MATCH, and a
// predicate with a top-level OR keeps its grouping.
func TestNormalizeMultiMatchWhereClausesMovesEveryWhere(t *testing.T) {
	require.Equal(t,
		"MATCH (a) MATCH (b) MATCH (a)-->(x), (b)-->(x) WHERE a.name = 'A' AND b.name = 'B' RETURN x",
		normalizeMultiMatchWhereClauses("MATCH (a) WHERE a.name = 'A' MATCH (b) WHERE b.name = 'B' MATCH (a)-->(x), (b)-->(x) RETURN x"))
	require.Equal(t,
		"MATCH (a) MATCH (a)-->(x) WHERE (a.v = 1 OR a.v = 2) AND x.v = 3 RETURN x",
		normalizeMultiMatchWhereClauses("MATCH (a) WHERE a.v = 1 OR a.v = 2 MATCH (a)-->(x) WHERE x.v = 3 RETURN x"))
	for _, unchanged := range []string{
		"MATCH (a) MATCH (b) WHERE b.v = 1 RETURN a",
		"MATCH (a) WHERE a.v = 1 OPTIONAL MATCH (a)-->(x) RETURN x",
		"MATCH (a) WHERE a.v = 1 WITH a MATCH (a)-->(x) RETURN x",
	} {
		require.Equal(t, unchanged, normalizeMultiMatchWhereClauses(unchanged))
	}
}

// TestReturnOfUndefinedVariableProperty: RETURN m.val with m unbound is
// Neo4j's "Variable `m` not defined" SyntaxError.
func TestReturnOfUndefinedVariableProperty(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {val: 1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:N) RETURN m.val", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
	require.Contains(t, err.Error(), "variable m is not defined")
}

// TestCallSubqueryAfterProcedureYieldImportsTheRow: CALL { WITH x … } after
// a procedure's YIELD runs per yielded row with the variable imported, and
// the statement's columns are the outer RETURN's (Neo4j's results; main
// dropped the procedure call and returned one null row).
func TestCallSubqueryAfterProcedureYieldImportsTheRow(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Alpha), (:Beta)", nil)
	require.NoError(t, err)
	for query, want := range map[string]struct {
		columns []string
		rows    [][]interface{}
	}{
		"CALL db.labels() YIELD label CALL { WITH label RETURN label + '!' AS y } RETURN y ORDER BY y": {
			[]string{"y"}, [][]interface{}{{"Alpha!"}, {"Beta!"}}},
		"CALL db.labels() YIELD label\nCALL {\n  WITH label\n  MATCH (n) WHERE label IN labels(n)\n  RETURN count(n) AS c\n}\nRETURN label, c ORDER BY label": {
			[]string{"label", "c"}, [][]interface{}{{"Alpha", int64(1)}, {"Beta", int64(1)}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want.columns, result.Columns, query)
		require.Equal(t, want.rows, result.Rows, query)
	}
}

// TestUnwindAliasIsOneVariable: UNWIND splits at its top-level AS (an AS
// inside a string belongs to the list), and text after the alias is a
// SyntaxError, as in Neo4j (UNWIND takes no WHERE).
func TestUnwindAliasIsOneVariable(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	result, err := exec.Execute(ctx, "UNWIND ['a value AS text', 'b'] AS s RETURN s", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a value AS text"}, {"b"}}, result.Rows)
	for query, token := range map[string]string{
		"UNWIND [1, 2] AS x WHERE x > 1 RETURN x": "WHERE",
		"UNWIND [1, 2] AS x y RETURN x":           "y",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError", query)
		require.Contains(t, err.Error(), "Invalid input '"+token+"'", query)
	}
}

// TestCallInTransactionsCountsMatchedRows: IN TRANSACTIONS counts the rows
// to batch with the subquery's MATCH, not its RETURN, which reads variables
// only the write binds; the count's own errors don't fail the statement.
func TestCallInTransactionsCountsMatchedRows(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Src {v: 1}), (:Src {v: 2}), (:Src {v: 3})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL { MATCH (s:Src) CREATE (t:Dst {v: s.v * 2}) RETURN t.v AS v } IN TRANSACTIONS OF 2 ROWS RETURN v ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"v"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}}, result.Rows)
}
