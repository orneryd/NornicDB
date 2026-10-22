package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestParseMergeProperties: a bound variable's property is evaluated; a
// string literal stays a string, whatever it reads (#514 section).
func TestParseMergeProperties(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_resolve_props_cov"))
	ctx := context.Background()

	nodeCtx := map[string]*storage.Node{
		"n": {ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "alice"}},
	}
	relCtx := map[string]*storage.Edge{
		"r": {ID: "r1", Type: "REL", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{"weight": int64(7)}},
	}

	fixtureAdmission1879Context, fixtureAdmission1879 := expressionTestContext(ctx, "{number: 42, text: 'name', literal: 'n.name', from_node: n.name, from_rel: r.weight, gone: null}")
	_ = fixtureAdmission1879Context
	out := exec.parseMergeProperties(fixtureAdmission1879, fixtureAdmission1879Context, "{number: 42, text: 'name', literal: 'n.name', from_node: n.name, from_rel: r.weight, gone: null}", nodeCtx, relCtx)
	require.EqualValues(t, int64(42), out["number"])
	require.Equal(t, "name", out["text"])
	require.Equal(t, "n.name", out["literal"])
	require.Equal(t, "alice", out["from_node"])
	require.EqualValues(t, int64(7), out["from_rel"])
	// A null value stays in the map: the MERGE validator rejects it before
	// matching (#810), which it can't do for a value that was dropped.
	require.Contains(t, out, "gone")
	require.Nil(t, out["gone"])
	fixtureAdmission1880Context, fixtureAdmission1880 := expressionTestContext(ctx, "{}")
	_ = fixtureAdmission1880Context
	require.Empty(t, exec.parseMergeProperties(fixtureAdmission1880, fixtureAdmission1880Context, "{}", nodeCtx, relCtx))
	fixtureAdmission1881Context, fixtureAdmission1881 := expressionTestContext(ctx, "")
	_ = fixtureAdmission1881Context
	require.Empty(t, exec.parseMergeProperties(fixtureAdmission1881, fixtureAdmission1881Context, "", nodeCtx, relCtx))
}

func TestExecuteMergeWithContext_AnonymousNodePatternAndChainedNoSpace(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_anon_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Anonymous merge variable should not panic and should still create node.
	res, err := exec.Execute(ctx, "MERGE (:Anon {k:'v'}) RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	// Ensure no-space chained MERGE form is handled and both nodes are created.
	res2, err := exec.Execute(ctx, "MERGE (a:NodeX {id:'1'})MERGE (b:NodeY {id:'2'}) RETURN a.id AS aid", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res2.Columns)
	require.Len(t, res2.Rows, 1)
	require.Equal(t, "1", res2.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (a:NodeX {id:'1'}), (b:NodeY {id:'2'}) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestMergeReturnClauseCountHandling_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_count_return_cov"))
	ctx := context.Background()

	n := &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "a"}}
	r := &storage.Edge{ID: "r1", Type: "R", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{}}

	// A MERGE's RETURN aggregates over all its rows (#640): two merged rows
	// count 2; count(x) skips rows where x is null.
	rows := []pipelineRow{
		exec.mergeBindingRow(ctx, map[string]*storage.Node{"n": n}, map[string]*storage.Edge{"r": r}),
		exec.mergeBindingRow(ctx, map[string]*storage.Node{"n": n}, map[string]*storage.Edge{"r": nil}),
	}
	fixtureAdmission1652Context, fixtureAdmission1652 := statementTestContext(ctx, "RETURN count(*) AS c_all, count(n) AS c_n, count(r) AS c_r")
	_ = fixtureAdmission1652Context
	result, err := exec.projectMergeReturn(fixtureAdmission1652, fixtureAdmission1652Context, rows, "RETURN count(*) AS c_all, count(n) AS c_n, count(r) AS c_r")
	require.NoError(t, err)
	require.Equal(t, []string{"c_all", "c_n", "c_r"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(2), int64(2), int64(1)}}, result.Rows)

	fixtureAdmission1653Context, fixtureAdmission1653 := statementTestContext(ctx, "RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 1")
	_ = fixtureAdmission1653Context
	result, err = exec.projectMergeReturn(fixtureAdmission1653, fixtureAdmission1653Context, rows, "RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 1")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
	fixtureAdmission1654Context, fixtureAdmission1654 := statementTestContext(ctx, "RETURN DISTINCT n.name AS name")
	_ = fixtureAdmission1654Context
	result, err = exec.projectMergeReturn(fixtureAdmission1654, fixtureAdmission1654Context, rows, "RETURN DISTINCT n.name AS name")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
}
