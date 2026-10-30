package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSubqueryValueHelperBranches covers the edge branches of the #652
// subquery-value helpers: quoted and malformed text, nil entities, outer
// bindings, and comprehension lists that are null or unresolved.
func TestSubqueryValueHelperBranches(t *testing.T) {
	// colonIsMapKeySeparator: a backtick-quoted key, whitespace before the
	// colon, and a colon with no key before it.
	require.True(t, colonIsMapKeySeparator(expressionBraceView("{`my key`: 1}"), len("{`my key`")))
	require.True(t, colonIsMapKeySeparator(expressionBraceView("{k : 1}"), len("{k ")))
	require.False(t, colonIsMapKeySeparator(expressionBraceView("{ : 1}"), len("{ ")))

	// patternHasRelationship skips string literals and quoted names.
	require.False(t, patternHasRelationship(originalBraceView("(a {s: 'x->y'})")))
	require.False(t, patternHasRelationship(originalBraceView("(`we->ird`)")))
	require.True(t, patternHasRelationship(originalBraceView("(a)-->(b)")))

	// Admitted source without a complete AS definition has no alias offset.
	require.Equal(t, -1, readProjectionItemSource(t.Context(), expressionBraceView("AS x"), projectionAdmitted).as)
	require.Equal(t, -1, readProjectionItemSource(t.Context(), expressionBraceView("x AS"), projectionAdmitted).as)
	require.Equal(t, -1, readProjectionItemSource(t.Context(), expressionBraceView("n.alias"), projectionAdmitted).as)

	// findSubqueryExpressions stops at an unterminated backtick.
	require.Empty(t, findSubqueryExpressions(expressionBraceView("`abc COUNT { (a) }")))

	// containsIdentifierWord needs a whole word.
	require.False(t, containsIdentifierWord("anything", ""))
	require.True(t, containsIdentifierWord("xab ab", "ab"))
	require.False(t, containsIdentifierWord("abc", "b"))

	// The evaluation row preserves explicit NULL bindings; writer entity scopes
	// contain only real entities.
	edge := &storage.Edge{ID: "r1"}
	row := entityRow(context.Background(), map[string]*storage.Node{"a": nil}, map[string]*storage.Edge{"r": edge, "s": nil})
	require.Equal(t, pipelineRow{"a": (*storage.Node)(nil), "r": edge, "s": (*storage.Edge)(nil)}, row)
	nodes, rels := entityBindings(map[string]interface{}{"r": edge, "s": (*storage.Edge)(nil), "x": 1})
	require.Empty(t, nodes)
	require.Equal(t, map[string]*storage.Edge{"r": edge}, rels)
}

// TestSubqueryValueEvaluatorBranches covers the evaluator's outer bindings,
// its error path, comprehension lists that are null or unresolved, and the
// node WHERE route's COUNT comparison.
func TestSubqueryValueEvaluatorBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SV {v: 1})-[:R]->(:SV {v: 2})", nil)
	require.NoError(t, err)

	// A correlated executor keeps the outer executor's bindings.
	exec.fabricRecordBindings = map[string]interface{}{"outer": int64(7)}
	correlated := exec.correlatedSubqueryExecutor(ctx, map[string]interface{}{"x": int64(1)})
	require.Equal(t, int64(7), correlated.fabricRecordBindings["outer"])
	require.Equal(t, int64(1), correlated.fabricRecordBindings["x"])
	exec.fabricRecordBindings = nil

	// An error in the subquery body is the statement's error.
	_, err = exec.Execute(ctx, "RETURN COUNT { UNWIND [0] AS z RETURN 1 / z AS x } AS c", nil)
	require.Error(t, err)

	// A comprehension over null holding a subquery is null.
	nullList, err := exec.Execute(ctx, "RETURN [x IN null WHERE EXISTS { MATCH (n:SV) } | x] AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, nullList.Rows)

	// NOT EXISTS over a body that reads a scalar row value.
	result, err := exec.Execute(ctx, "WITH 5 AS k MATCH (n:SV) WHERE NOT EXISTS { MATCH (n)-->(o) WHERE o.v = k } RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Rows[0][0])

	// The node WHERE route's COUNT comparison.
	nodesResult, err := exec.Execute(ctx, "MATCH (n:SV {v: 1}) RETURN n", nil)
	require.NoError(t, err)
	node := nodesResult.Rows[0][0].(*storage.Node)
	require.True(t, exec.evaluateWhere(ctx, node, "n", expressionBraceView("COUNT { (n)-->() } > 0")))
	require.True(t, exec.evaluateWhere(ctx, node, "n", expressionBraceView("0 < COUNT { (n)-->() }")))
	require.True(t, exec.evaluateWhere(ctx, node, "n", expressionBraceView("EXISTS { (n)-->() } = true")))
	require.False(t, exec.evaluateWhere(ctx, node, "n", expressionBraceView("EXISTS { (n)-->() } = false")))
}

// TestSubqueryBodiesWithCallSubqueries covers #652 bodies that contain a CALL
// subquery: a CALL inside a subquery expression is not the statement's, and a
// body the pipeline declines runs through the full executor with the row's
// values bound.
func TestSubqueryBodiesWithCallSubqueries(t *testing.T) {
	require.Equal(t, -1, firstTopLevelCallSubquery(t.Context(), "MATCH (n) RETURN COUNT { MATCH (n)-->(o) CALL (o) { RETURN 1 AS x } RETURN x } AS c"))
	require.Equal(t, -1, firstTopLevelCallSubquery(t.Context(), "RETURN 'CALL { }' AS s"))
	require.Equal(t, len("MATCH (n) "), firstTopLevelCallSubquery(t.Context(), "MATCH (n) CALL (n) { RETURN 1 AS x } RETURN x"))

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SC {v: 1})-[:R {w: 3}]->(:SC {v: 2})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"MATCH (n:SC {v: 1}) RETURN COUNT { MATCH (n)-->(o) CALL { WITH o RETURN 1 AS one } RETURN one } AS c":            int64(1),
		"MATCH (n:SC {v: 1}) WHERE EXISTS { MATCH (n)-->(o) CALL { WITH o RETURN 1 AS one } RETURN one } RETURN n.v AS c": int64(1),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}

	// The correlated runner binds nodes, relationships, nulls and scalars.
	matched, err := exec.Execute(ctx, "MATCH (a:SC {v: 1})-[r:R]->(b:SC) RETURN a, r", nil)
	require.NoError(t, err)
	a, r := matched.Rows[0][0].(*storage.Node), matched.Rows[0][1].(*storage.Edge)
	result, err := exec.executeCorrelatedSubqueryBody(ctx, originalBraceView("MATCH (a)-[r]->(b) RETURN b.v + k AS s, m AS m, e AS e"), map[string]interface{}{
		"a": a, "r": r, "k": int64(10), "m": (*storage.Node)(nil), "e": (*storage.Edge)(nil), "$ignored": 1,
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(12), nil, nil}}, result.Rows)
}
