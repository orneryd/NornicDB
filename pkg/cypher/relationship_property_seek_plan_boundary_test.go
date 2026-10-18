package cypher

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
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

func TestRelationshipPropertySeekFabricScopePlans(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("seek_part"))
	store, err := manager.GetStorage("seek_part")
	require.NoError(t, err)
	setup := NewStorageExecutor(store)
	for _, query := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := setup.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	require.NoError(t, manager.CreateCompositeDatabase("seek_fabric", []multidb.ConstituentRef{{Alias: "part", DatabaseName: "seek_part", Type: "local", AccessMode: "read_write"}}))
	store, err = manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	query := "USE seek_fabric WITH $x AS k CALL (k) { USE seek_fabric.part MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p AS v } RETURN v"
	params := map[string]interface{}{"x": "x"}
	ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
	actual, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x"}}, actual.Rows)
	executed := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).seeks["r\x00rel_p"]
	require.NotNil(t, executed)
	for _, mode := range []string{"EXPLAIN", "PROFILE"} {
		result, err := exec.Execute(context.Background(), mode+" "+query, params)
		require.NoError(t, err, mode)
		seek := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek")
		require.NotNil(t, seek, mode+" must use the selected constituent's planner")
		require.Equal(t, "rel_p", seek.Arguments["index"])
		if mode == "PROFILE" {
			require.Equal(t, actual.Rows, result.Rows)
			require.Equal(t, executed.lookups, seek.Arguments["indexLookups"])
		}
	}
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
		"LOAD CSV FROM 'file:///unused.csv' AS row MATCH ()-[r:T]->() WHERE r.p=row[0] RETURN r.p",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
	}
}

// A seek can finish just before its index disappears. PROFILE must report the
// actual decision, rather than asking a later schema view whether it could seek.
type relationshipSeekChangedSchema struct {
	storage.Engine
	after   *storage.SchemaManager
	changed bool
}

func (s *relationshipSeekChangedSchema) GetSchema() *storage.SchemaManager {
	if s.changed {
		return s.after
	}
	return s.Engine.GetSchema()
}
func (s *relationshipSeekChangedSchema) RelationshipPropertySeeksAvailable() bool {
	return s.Engine.(storage.RelationshipPropertySeeker).RelationshipPropertySeeksAvailable()
}
func (s *relationshipSeekChangedSchema) SeekRelationships(ctx context.Context, typ string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	edges, answered, err := s.Engine.(storage.RelationshipPropertySeeker).SeekRelationships(ctx, typ, properties, values)
	if answered && err == nil {
		s.changed = true
	}
	return edges, answered, err
}

func TestRelationshipPropertySeekProfileUsesExecutedView(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "executed_view")
	setup := NewStorageExecutor(store)
	for _, query := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := setup.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	view := &relationshipSeekChangedSchema{Engine: store, after: storage.NewSchemaManager()}
	exec := NewStorageExecutor(view)
	result, err := exec.Execute(context.Background(), "PROFILE WITH 'x' AS k RETURN COUNT { MATCH ()-[r:T]->() WHERE r.p=k }", nil)
	require.NoError(t, err)
	require.True(t, view.changed)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	seek := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek")
	require.NotNil(t, seek, "a later schema cannot erase the executed operator")
	require.Equal(t, "rel_p", seek.Arguments["index"])
	require.Equal(t, int64(1), seek.Arguments["indexLookups"])
	require.Equal(t, int64(1), seek.Arguments["relationshipCandidates"])
	require.Equal(t, int64(10), seek.EstimatedRows)
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
