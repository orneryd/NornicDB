package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveMergePropsWithContext_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_resolve_props_cov"))
	ctx := context.Background()

	nodeCtx := map[string]*storage.Node{
		"n": {ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "alice"}},
	}
	relCtx := map[string]*storage.Edge{
		"r": {ID: "r1", Type: "REL", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{"weight": int64(7)}},
	}

	in := map[string]interface{}{
		"raw_non_string": int64(42),
		"raw_empty":      "  ",
		"raw_no_dot":     "name",
		"raw_unknown":    "x.name",
		"from_node":      "n.name",
		"from_rel":       "r.weight",
	}

	out := exec.resolveMergePropsWithContext(ctx, in, nodeCtx, relCtx)
	require.EqualValues(t, int64(42), out["raw_non_string"])
	require.Equal(t, "  ", out["raw_empty"])
	require.Equal(t, "name", out["raw_no_dot"])
	require.Equal(t, "x.name", out["raw_unknown"])
	require.Equal(t, "alice", out["from_node"])
	require.EqualValues(t, int64(7), out["from_rel"])

	empty := exec.resolveMergePropsWithContext(ctx, map[string]interface{}{}, nodeCtx, relCtx)
	require.Empty(t, empty)
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
	res2, err := exec.executeMergeWithContext(ctx, "MERGE (a:NodeX {id:'1'})MERGE (b:NodeY {id:'2'}) RETURN a.id AS aid", map[string]*storage.Node{}, map[string]*storage.Edge{})
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
	result, err := exec.projectMergeReturn(ctx, rows, "RETURN count(*) AS c_all, count(n) AS c_n, count(r) AS c_r")
	require.NoError(t, err)
	require.Equal(t, []string{"c_all", "c_n", "c_r"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(2), int64(2), int64(1)}}, result.Rows)

	result, err = exec.projectMergeReturn(ctx, rows, "RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 1")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
	result, err = exec.projectMergeReturn(ctx, rows, "RETURN DISTINCT n.name AS name")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
}
