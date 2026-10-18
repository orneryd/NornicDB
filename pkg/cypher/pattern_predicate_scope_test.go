package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Neo4j 5.26.30 and TCK Pattern1 [10]: named variables in a pattern
// predicate must already exist; only an explicit EXISTS body binds locals.
func TestPatternPredicateScopeMatchesNeo4j(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:ScopeA)-[:KNOWS]->(:ScopeB)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:ScopeA)-[r:KNOWS]->(b:ScopeB) RETURN a, r, b", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	values := map[string]interface{}{"a": result.Rows[0][0], "r": result.Rows[0][1], "b": result.Rows[0][2]}
	for _, pattern := range []string{"(a)-[:KNOWS]->(c)", "(a)-[missing]->(b)", "(c)-[:KNOWS]->(b)", "(a)-[:KNOWS]->(c)-[:KNOWS]->(b)"} {
		t.Run(pattern, func(t *testing.T) {
			failureCtx := withExpressionFailureSlot(context.Background())
			matched, recognized := exec.rowPatternPredicate(failureCtx, pattern, values)
			require.True(t, recognized)
			require.False(t, matched)
			requireSemanticPatternDetail(t, getExpressionFailure(failureCtx), "UndefinedVariable")
			_, err := exec.Execute(ctx, "MATCH (a:ScopeA)-[r:KNOWS]->(b:ScopeB) WHERE "+pattern+" RETURN a", nil)
			requireSemanticPatternDetail(t, err, "UndefinedVariable")
		})
	}
	for _, pattern := range []string{"(a)-[:KNOWS]->(b)", "(a)-[r]->(b)", "(a)-[:KNOWS]->()", "()-[r]->(b)"} {
		failureCtx := withExpressionFailureSlot(context.Background())
		matched, recognized := exec.rowPatternPredicate(failureCtx, pattern, values)
		require.True(t, recognized, pattern)
		require.True(t, matched, pattern)
		require.NoError(t, getExpressionFailure(failureCtx), pattern)
	}
	matched, recognized := exec.evaluateRowExistsPredicate(ctx, "EXISTS { (a)-[:KNOWS]->(c) }", values)
	require.True(t, recognized)
	require.True(t, matched)

	// A compiled binding adapter must report the same missing-variable error.
	failureCtx := withExpressionFailureSlot(context.Background())
	predicate := exec.getCompiledBindingWhere(failureCtx, "(a)-[:KNOWS]->(c)")
	require.False(t, predicate(binding{"a": values["a"].(*storage.Node)}, nil))
	requireSemanticPatternDetail(t, getExpressionFailure(failureCtx), "UndefinedVariable")
}

// Compare the compiled owner with a full correlated pipeline, bypassing the
// EXISTS expansion shortcut. Cover constraints that the removed matcher lost.
func TestPatternPredicateCompiledMatchesPipeline(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:PredA {id:1})-[r:T {weight:7}]->(b:PredB {name:'x'}), (a)-[:U]->(a), (c:PredC)-[:T]->(b)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:PredA)-[r:T]->(b:PredB), (c:PredC) RETURN a, r, b, c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	values := map[string]interface{}{"a": row[0], "r": row[1], "b": row[2], "c": row[3], "nm": "x", "z": (*storage.Node)(nil)}
	for _, tc := range []struct {
		pattern string
		want    bool
	}{
		{"(a)-[:T]->(b)", true},
		{"(b)<-[:T]-(a)", true},
		{"(b)-[:T]-(a)", true},
		{"(b)-[:T]->(a)", false},
		{"(a)-[:Missing]->(b)", false},
		{"(a)-[r]->(b)", true},
		{"(c)-[r]->(b)", false},
		{"()-[r]->(:PredB)", true},
		{"(a)-[:U]->(a)", true},
		{"(a)-[:T|U]->(b)", true},
		{"(a)-[:T]->(b:Missing)", false},
		{"(a)-[:T {weight:7}]->(b)", true},
		{"(a)-[:T]->(:PredB {name:nm})", true},
		{"(a)-[:T]->(:PredB {name:'y'})", false},
		{"(z)-[:T]->(b)", false},
		{"(a)-[:T]->(z)", false},
		{"(a)-[:T]->(b)<-[:T]-(c)", true},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			failureCtx := withExpressionFailureSlot(ctx)
			matched, err := planPatternPredicate(tc.pattern).evaluate(exec, failureCtx, compiledRowScope{values: &values})
			require.NoError(t, err)
			require.NoError(t, getExpressionFailure(failureCtx))
			require.Equal(t, tc.want, matched)
			general, err := exec.runCorrelatedSubquery(ctx, rowSubqueryQuery("EXISTS", tc.pattern), values)
			require.NoError(t, err)
			require.Equal(t, tc.want, len(general.Rows) > 0)
		})
	}
}
