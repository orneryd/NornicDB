package cypher

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipPropertySeekExplainDoesNotExecute(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "explain_only"))
	for _, query := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	ctx := withRelationshipSeekProfile(context.Background())
	result, err := exec.Execute(ctx, "EXPLAIN WITH 'x' AS k CREATE (:MustNotExist {v:COUNT { MATCH ()-[r:T]->() WHERE r.p=k }})", nil)
	require.NoError(t, err)
	require.NotNil(t, relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek"))
	require.Empty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).seeks)
	result, err = exec.Execute(context.Background(), "MATCH (n:MustNotExist) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

// Unsupported statement forms remain semantic errors. The relationship planner
// must not invent scope support in order to explain an otherwise invalid query.
func TestRelationshipPropertySeekRejectedBoundaries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rejected"))
	for _, query := range []string{
		"WITH 'x' AS k WITH 1 AS v MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p",
		"WITH 1 AS v CALL db.labels() YIELD * RETURN v",
		"FOREACH (k IN ['x'] | MATCH ()-[r:T]->() WHERE r.p=k RETURN r)",
		"MERGE (n:Output {v:COUNT { MATCH ()-[r:T]->() RETURN r }}) RETURN n",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
	}
}

func TestRelationshipPropertySeekConditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "alternatives"))
	for _, query := range []string{"CREATE ()-[:T {p:'x',q:'y'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", "CREATE RANGE INDEX rel_q FOR ()-[r:T]-() ON (r.q)"} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	query := "UNWIND [{p:'x',q:'y'},{p:null,q:'y'}] AS k MATCH ()-[r:T]->() WHERE r.p=k.p AND r.q=k.q RETURN r.p"
	ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
	actual, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x"}}, actual.Rows)
	profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	for _, mode := range []string{"EXPLAIN", "PROFILE"} {
		result, err := exec.Execute(context.Background(), mode+" "+query, nil)
		require.NoError(t, err)
		plan := result.Metadata["plan"].(*ExecutionPlan)
		choice := relationshipPlanOperator(plan.Root, "RelationshipAccessChoice")
		require.NotNil(t, choice)
		require.Len(t, choice.Children, 2)
		for _, op := range choice.Children {
			require.Equal(t, "RelationshipIndexSeek", op.OperatorType)
			if mode == "PROFILE" {
				stats := profile.seeks["r\x00"+op.Arguments["index"].(string)]
				require.NotNil(t, stats)
				require.Equal(t, stats.lookups, op.Arguments["indexLookups"])
				require.Equal(t, stats.candidates, op.Arguments["relationshipCandidates"])
				require.Equal(t, stats.rows, op.ActualRows)
			}
		}
	}
	result, err := exec.Execute(context.Background(), "PROFILE WITH null AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", nil)
	require.NoError(t, err)
	plan := result.Metadata["plan"].(*ExecutionPlan)
	require.Nil(t, relationshipPlanOperator(plan.Root, "RelationshipIndexSeek"))
	require.NotNil(t, relationshipPlanOperator(plan.Root, "Expand"))
}

// Allocation bound, not a timing assertion. The warmed public lookup must not
// allocate the graph or retain one plan per incoming row. An 8192-edge scan
// allocates multiple MiB; this returns one candidate with <1 MiB per statement.
func TestRelationshipPropertySeekAllocationBound(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "allocation_bound")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	for i := 0; i < 8192; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(i)), StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": fmt.Sprint(i)}}))
	}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", nil)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), profileExecutionKey{}, true)
	query := "CALL { WITH $x AS k RETURN * } RETURN COUNT { MATCH ()-[r:T]->() WHERE r.p=k }"
	params := map[string]interface{}{"x": "0"}
	_, err = exec.Execute(ctx, query, params)
	require.NoError(t, err)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 3; i++ {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	}
	runtime.ReadMemStats(&after)
	require.Less(t, (after.TotalAlloc-before.TotalAlloc)/3, uint64(1<<20), "lookup allocated graph-sized intermediate state")
}
