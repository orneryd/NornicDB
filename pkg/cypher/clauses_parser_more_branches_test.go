package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseUnwindNodePatternClauseInternal_MoreBranches(t *testing.T) {
	_, _, _, _, ok := parseUnwindNodePatternClauseInternal("MATCH (n:A {k:1})", "MERGE", false)
	require.False(t, ok)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MERGE n:A {k:1}", "MERGE", false)
	require.False(t, ok)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MERGE (n:A {k:1}) RETURN n", "MERGE", false)
	require.False(t, ok)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MERGE (n:A {k})", "MERGE", false)
	require.False(t, ok)

	varName, labels, assignments, anyLabel, ok := parseUnwindNodePatternClauseInternal("MERGE (n:A|B {k: row.k})", "MERGE", true)
	require.True(t, ok)
	require.Equal(t, "n", varName)
	require.Equal(t, []string{"A", "B"}, labels)
	require.True(t, anyLabel)
	require.Len(t, assignments, 1)
	require.Equal(t, "k", assignments[0].prop)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MERGE (n:A|B {k: row.k})", "MERGE", false)
	require.False(t, ok)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MERGE (1n:A {k: row.k})", "MERGE", false)
	require.False(t, ok)
}

func TestUnwindClauseParsers_MoreBranches(t *testing.T) {
	_, ok := parseUnwindWithClause("WITH")
	require.False(t, ok)
	_, ok = parseUnwindWithClause("WITH n AS")
	require.False(t, ok)

	withPlan, ok := parseUnwindWithClause("WITH n, row.k AS k, score AS s")
	require.True(t, ok)
	require.Len(t, withPlan.assignments, 2)
	require.Equal(t, "k", withPlan.assignments[0].alias)
	require.Equal(t, "row.k", withPlan.assignments[0].expr)
	require.Equal(t, "s", withPlan.assignments[1].alias)

	_, ok = parseUnwindWhereClause("WHERE")
	require.False(t, ok)
	wherePlan, ok := parseUnwindWhereClause("WHERE n.k = row.k")
	require.True(t, ok)
	require.Equal(t, "n.k = row.k", wherePlan.clause)

	alias, ok := parseUnwindBatchCountReturn("RETURN count(*) AS total")
	require.True(t, ok)
	require.Equal(t, "total", alias)
	_, ok = parseUnwindBatchCountReturn("RETURN sum(x) AS total")
	require.False(t, ok)
	alias, ok = parseUnwindBatchCountReturn("RETURN count(*)")
	require.True(t, ok)
	require.Equal(t, "count(*)", alias)

	clauses, ok := splitUnwindMergeChainClauses("MERGE (n:A {k: row.k}) ON CREATE SET n.created = true SET n.updated = true")
	require.True(t, ok)
	require.Equal(t, []string{
		"MERGE (n:A {k: row.k})",
		"ON CREATE SET n.created = true",
		"SET n.updated = true",
	}, clauses)
	_, ok = splitUnwindMergeChainClauses("XYZ n")
	require.False(t, ok)
}

func TestUnwindCollectProjectionAndRewrite_MoreBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "clauses_unwind_collect_more_cov"))

	plan, ok := exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.k) AS keys RETURN keys")
	require.True(t, ok)
	require.Equal(t, "row", plan.srcVar)
	require.Equal(t, "k", plan.prop)
	require.Equal(t, "keys", plan.alias)

	_, ok = exec.parseUnwindCollectDistinctProjection("WITH collect(row.k) AS keys RETURN keys")
	require.False(t, ok)
	_, ok = exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.k) AS keys RETURN other")
	require.False(t, ok)

	res, ok := exec.executeUnwindWithCollectProjection("row", []interface{}{
		map[string]interface{}{"k": "a"},
		map[interface{}]interface{}{"k": "a"},
		map[string]interface{}{"k": "b"},
		int64(42),
	}, "WITH collect(DISTINCT row.k) AS keys RETURN keys")
	require.True(t, ok)
	require.Equal(t, []string{"keys"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, []interface{}{"a", "b"}, res.Rows[0][0])

	_, ok = exec.executeUnwindWithCollectProjection("item", []interface{}{}, "WITH collect(DISTINCT row.k) AS keys RETURN keys")
	require.False(t, ok)

	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row RETURN count(row)", []interface{}{nil, nil}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row RETURN row", []interface{}{1, 2}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row REMOVE row.x RETURN count(row)", []interface{}{1, 2}))
}

func TestRewriteTopLevelMultiMatchToCartesianMatch_MoreBranches(t *testing.T) {
	require.Equal(t, "RETURN 1", rewriteTopLevelMultiMatchToCartesianMatch("RETURN 1"))
	require.Equal(t, "MATCH (a:A) WHERE a.id = 1", rewriteTopLevelMultiMatchToCartesianMatch("MATCH (a:A) WHERE a.id = 1"))
	require.Equal(t, "MATCH (a:A) MATCH (b:B) RETURN a, b", rewriteTopLevelMultiMatchToCartesianMatch("MATCH (a:A) MATCH (b:B) RETURN a, b"))
	require.Equal(
		t,
		"MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b",
		rewriteTopLevelMultiMatchToCartesianMatch("MATCH (a:A) MATCH (b:B) WHERE a.id = b.id RETURN a, b"),
	)
}

func TestExecuteUnion_ErrorAndInferenceBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "clauses_union_more_cov"))
	ctx := context.Background()

	_, err := exec.executeUnion(ctx, "RETURN 1 AS x UNION RETURN 2 AS x", true)
	require.Error(t, err)

	_, err = exec.executeUnion(ctx, "RETURN 1 AS x UNION ALL RETURN 2 AS x", false)
	require.Error(t, err)

	// First branch returns 0 rows (and may not preserve column metadata in some paths),
	// second branch returns one row. UNION should still succeed and preserve shape.
	res, err := exec.executeUnion(ctx,
		"MATCH (n:Missing) RETURN n.name AS name UNION ALL RETURN 'fallback' AS name",
		true,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "fallback", res.Rows[0][0])
}
