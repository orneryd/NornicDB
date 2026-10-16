package cypher

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The correlated CALL subqueries of BenchmarkCallSubqueryClausePlan, and
// more correlated shapes (ORDER BY, OPTIONAL MATCH, UNION, a nested CALL),
// return what Neo4j 5.26.30 returns on the same graph.
func TestCallSubqueryClausePlanMatchesNeo4j(t *testing.T) {
	exec := callPlanBenchExecutor(t)
	ctx := context.Background()
	want := map[string][][]interface{}{
		"traversal_pushdown":        {{int64(900)}},
		"scoped_traversal_pushdown": {{int64(900)}},
		"optional_quantified":       {{int64(1200)}},
		"with_grouping":             {{int64(1600), int64(400)}},
		"exists_where":              {{int64(299)}},
		"count_return":              {{int64(1201)}},
		"count_in_call":             {{int64(1200)}},
		"where_fold":                {{int64(440)}},
		"limit_body":                {{int64(800), "q0"}},
	}
	for _, tc := range callPlanBenchQueries {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, want[tc.name], result.Rows, tc.name)
	}
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (p:P) WHERE p.i < 6 CALL (p) { MATCH (p)-[r:R]->(q:Q) WHERE r.w >= p.m AND q.k <> 3 RETURN q.name AS name ORDER BY name } RETURN p.i AS i, collect(name) AS names ORDER BY i", [][]interface{}{
			{int64(0), []interface{}{"q0", "q14", "q21"}},
			{int64(1), []interface{}{"q22", "q8"}},
			{int64(2), []interface{}{"q16"}},
			{int64(3), []interface{}{"q10", "q17", "q24"}},
			{int64(4), []interface{}{"q18", "q25"}},
			{int64(5), []interface{}{"q26"}},
		}},
		{"MATCH (p:P) WHERE p.i < 6 CALL { WITH p OPTIONAL MATCH (p)-[r:R]->(q:Q) WHERE r.w > p.m + 1 RETURN count(q) AS c } RETURN p.i AS i, c ORDER BY i", [][]interface{}{
			{int64(0), int64(2)}, {int64(1), int64(1)}, {int64(2), int64(0)},
			{int64(3), int64(2)}, {int64(4), int64(1)}, {int64(5), int64(0)},
		}},
		{"MATCH (p:P) WHERE p.i < 4 CALL (p) { MATCH (p)-[r:R]->(q:Q) WHERE q.k = p.m RETURN q.name AS name UNION MATCH (p)-[r:R]->(q:Q) WHERE r.w = 0 RETURN q.name AS name } RETURN p.i AS i, collect(name) AS names ORDER BY i", [][]interface{}{
			{int64(0), []interface{}{"q0"}},
			{int64(1), []interface{}{"q1"}},
			{int64(2), []interface{}{"q2"}},
			{int64(3), []interface{}{"q24", "q3"}},
		}},
		{"MATCH (p:P) WHERE p.i < 3 CALL (p) { MATCH (p)-[:R]->(q:Q) CALL (q) { MATCH (q)<-[r2:R]-(other:P) WHERE r2.w = 0 RETURN count(other) AS back } RETURN q.name AS name, back ORDER BY name } RETURN p.i AS i, collect([name, back]) AS pairs ORDER BY i", [][]interface{}{
			{int64(0), []interface{}{[]interface{}{"q0", int64(10)}, []interface{}{"q14", int64(10)}, []interface{}{"q21", int64(10)}, []interface{}{"q7", int64(10)}}},
			{int64(1), []interface{}{[]interface{}{"q1", int64(10)}, []interface{}{"q15", int64(10)}, []interface{}{"q22", int64(10)}, []interface{}{"q8", int64(10)}}},
			{int64(2), []interface{}{[]interface{}{"q16", int64(10)}, []interface{}{"q2", int64(10)}, []interface{}{"q23", int64(10)}, []interface{}{"q9", int64(10)}}},
		}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}
}

// A correlated CALL subquery parses its body's clause texts once per
// statement, not once per outer row: each analysis of a text the queries
// mark ('callplan-once') is built exactly once for 400 outer rows.
func TestCallSubqueryClausePlanParsesOncePerStatement(t *testing.T) {
	exec := callPlanBenchExecutor(t)
	ctx := context.Background()
	type build struct {
		kind pipelineClauseAnalysis
		text string
	}
	var mu sync.Mutex
	builds := map[build]int{}
	hook := func(kind pipelineClauseAnalysis, text string) {
		if strings.Contains(text, "callplan-once") {
			mu.Lock()
			builds[build{kind, text}]++
			mu.Unlock()
		}
	}
	pipelineClausePlanBuildHook.Store(&hook)
	t.Cleanup(func() { pipelineClausePlanBuildHook.Store(nil) })

	expected := map[string][]pipelineClauseAnalysis{
		"traversal_pushdown":        {clauseAnalysisCallSubquery, clauseAnalysisMatchWhere, clauseAnalysisPushdownTerms},
		"scoped_traversal_pushdown": {clauseAnalysisCallSubquery, clauseAnalysisMatchWhere, clauseAnalysisPushdownTerms},
		"optional_quantified":       {clauseAnalysisCallSubquery, clauseAnalysisPatternStructure, clauseAnalysisQuantifiedPath},
		"with_grouping":             {clauseAnalysisCallSubquery, clauseAnalysisMatchWhere, clauseAnalysisPatternBindingsMatch},
		"exists_where":              {clauseAnalysisBoundChain},
		"count_return":              {clauseAnalysisBoundChain},
		"count_in_call":             {clauseAnalysisCallSubquery, clauseAnalysisBoundChain},
		"where_fold":                {clauseAnalysisCallSubquery, clauseAnalysisMaySubquery, clauseAnalysisOperandFacts},
		"limit_body":                {clauseAnalysisCallSubquery, clauseAnalysisLimitModifier, clauseAnalysisReturnSourceColumns},
	}
	for _, tc := range callPlanBenchQueries {
		mu.Lock()
		clear(builds)
		mu.Unlock()
		exec.InvalidateCommittedWriteCaches()
		_, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.name)
		mu.Lock()
		seen := map[pipelineClauseAnalysis]bool{}
		for key, count := range builds {
			require.Equal(t, 1, count, "%s: analysis %d of %q built %d times", tc.name, key.kind, key.text, count)
			seen[key.kind] = true
		}
		mu.Unlock()
		for _, kind := range expected[tc.name] {
			require.True(t, seen[kind], "%s: analysis %d not taken from the clause plan", tc.name, kind)
		}
	}
}

// Without clause plans in the context every analysis parses its text, and
// gives what the plan gives.
func TestPipelineClausePlanWithoutPlansParses(t *testing.T) {
	where := "q.k <> 3 AND r.w >= p.m AND coalesce(q.name, '') <> 'none'"
	plain := pipelinePushdownTermsFor(context.Background(), where)
	require.Equal(t, []string{"q.k <> 3", "r.w >= p.m", "coalesce(q.name, '') <> 'none'"}, plain.terms)
	require.True(t, plain.referencesVariable(1, "p"))
	require.False(t, plain.referencesVariable(0, "p"))

	ctx := withPipelineClausePlans(context.Background())
	require.Equal(t, ctx, withPipelineClausePlans(ctx), "a statement keeps its plans")
	planned := pipelinePushdownTermsFor(ctx, where)
	require.Equal(t, plain.terms, planned.terms)
	require.Same(t, planned.references[0], pipelinePushdownTermsFor(ctx, where).references[0], "one plan per text")
	require.True(t, planned.referencesVariable(1, "p"))
	require.True(t, planned.referencesVariable(1, "p"))
	require.False(t, planned.referencesVariable(0, "p"))

	row := pipelineRow{"p": nil, "$x": int64(1)}
	require.Equal(t, "q.k <> 3 AND coalesce(q.name, '') <> 'none'", pipelineTraversalPushdownPredicate(ctx, where, row, []string{"q", "r"}))
	require.Equal(t, where, pipelineTraversalPushdownPredicate(ctx, where, row, []string{"p", "q", "r"}))
	require.Equal(t, pipelineTraversalPushdownPredicate(context.Background(), where, row, []string{"q", "r"}), pipelineTraversalPushdownPredicate(ctx, where, row, []string{"q", "r"}))

	// Past the limit, a text gets no plan and parses as without plans.
	for i := 0; i < pipelineClausePlanLimit; i++ {
		pipelineClausePlanFor(ctx, strings.Repeat("x", i+1))
	}
	require.Nil(t, pipelineClausePlanFor(ctx, "beyond the limit"))
	require.Equal(t, plain.terms, pipelinePushdownTermsFor(ctx, "q.k <> 3 AND r.w >= p.m AND coalesce(q.name, '') <> 'none' ").terms)
}

// A correlated COLLECT and an EXISTS beside an OR return what Neo4j 5.26.30
// returns, and each subquery atom compiles to its own row node, read once.
func TestRowSubqueryAtomCompilesOnce(t *testing.T) {
	exec := callPlanBenchExecutor(t)
	ctx := context.Background()
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (p:P) WHERE p.i < 5 RETURN p.i AS i, COLLECT { MATCH (p)-[r:R]->(q:Q) WHERE coalesce(q.name, 'callplan-once') <> 'none' RETURN q.name ORDER BY q.name } AS names ORDER BY i", [][]interface{}{
			{int64(0), []interface{}{"q0", "q14", "q21", "q7"}},
			{int64(1), []interface{}{"q1", "q15", "q22", "q8"}},
			{int64(2), []interface{}{"q16", "q2", "q23", "q9"}},
			{int64(3), []interface{}{"q10", "q17", "q24", "q3"}},
			{int64(4), []interface{}{"q11", "q18", "q25", "q4"}},
		}},
		{"MATCH (p:P) WHERE p.i < 5 AND (p.m = 0 OR EXISTS { (p)-[:R {w: 3}]->(:Q {k: 2}) }) RETURN collect(p.i) AS kept", [][]interface{}{
			{[]interface{}{int64(0), int64(1), int64(3)}},
		}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}

	root, _ := (&rowValueCompiler{}).compile("COUNT { MATCH (p)-[r:R]->(q:Q) WHERE q.k = 1 }")
	node, ok := root.(*rowBoundNode).inner.(*rowSubqueryNode)
	require.True(t, ok, "a subquery atom compiles to a rowSubqueryNode")
	require.Equal(t, "COUNT", node.kind)
	require.Equal(t, "MATCH (p)-[r:R]->(q:Q) WHERE q.k = 1", node.body)

	// Without a statement context the node reads its text, as the text
	// route does.
	value, resolved, err := node.eval(exec, map[string]interface{}{}, nil)
	textValue, textResolved, textErr := exec.evaluateRowValueText(node.text, map[string]interface{}{})
	require.Equal(t, textValue, value)
	require.Equal(t, textResolved, resolved)
	require.Equal(t, textErr, err)
}
