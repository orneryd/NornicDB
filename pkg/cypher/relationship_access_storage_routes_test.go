package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func assertRelationshipAccessStorageCase(t *testing.T, e *StorageExecutor, tc relationshipAccessStorageCase) {
	t.Helper()
	ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
	actual, err := e.Execute(ctx, tc.query, tc.params)
	require.NoError(t, err)
	require.Equal(t, tc.rows, actual.Rows)
	profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	require.Empty(t, profile.unregistered)
	require.Len(t, profile.templates, 1)
	for id, site := range profile.templates {
		access := site.access
		operator, index, estimate := "Expand", "", int64(100)
		switch {
		case access.vector != nil:
			operator, index, estimate = "RelationshipVectorIndexSeek", access.vector.index, access.vector.estimatedRows
			require.True(t, e.LastHotPathTrace().CosineVectorIndexFastPath)
		case access.id != nil:
			operator, estimate = "RelationshipByIdSeek", access.id.estimatedRows
			if access.id.kind == "elementId" {
				operator = "RelationshipByElementIdSeek"
			}
		case len(access.alternatives) != 0:
			require.Len(t, access.alternatives, 1)
			operator, index, estimate = "RelationshipIndexSeek", access.alternatives[0].name, access.alternatives[0].estimatedRows
		default:
			require.Positive(t, profile.fallbacks[id], "runtime must record its scan choice")
		}
		require.Equal(t, tc.operator, operator, "runtime operator")
		require.Equal(t, tc.index, index, "runtime index")
		require.Equal(t, tc.estimate, estimate, "runtime estimate")
		for _, mode := range []string{"EXPLAIN", "PROFILE"} {
			result, err := e.Execute(context.Background(), mode+" "+tc.query, tc.params)
			require.NoError(t, err, mode)
			op := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, operator)
			require.NotNil(t, op, "%s access operator", mode)
			plannedIndex, _ := op.Arguments["index"].(string)
			require.Equal(t, index, plannedIndex, "%s index", mode)
			require.Equal(t, estimate, op.EstimatedRows, "%s estimate", mode)
			if mode == "PROFILE" {
				require.Equal(t, actual.Rows, result.Rows)
			} else {
				require.Empty(t, result.Rows)
			}
		}
	}
}

func TestRelationshipAccessProgramOwnsRelationshipVectorRoute(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_vector_owner"))
	for _, query := range []string{
		"CREATE (:Entity {uuid:'a'}), (:Entity {uuid:'b'}), (:Entity {uuid:'c'})",
		"MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(b)",
		"MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'r2', fact_embedding:[0.0,1.0,0.0]}]->(c)",
		"CREATE VECTOR INDEX relationship_vector_owner FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE RANGE INDEX relationship_uuid_owner FOR ()-[e:RELATES_TO]-() ON (e.uuid)",
	} {
		_, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	probe, err := e.Execute(context.Background(), "CALL db.index.vector.queryRelationships('relationship_vector_owner', 5, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Rows)

	queries := relationshipAccessGeneratedVectorCases()
	params := queries[0].params
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) { assertRelationshipAccessStorageCase(t, e, tc) })
	}
	t.Run("removed_vector_site", func(t *testing.T) {
		query := queries[0].query
		ctx := context.WithValue(context.Background(), relationshipAccessProgramKey{}, &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}})
		ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
		actual, err := e.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"r1", 1.0}}, actual.Rows)
		require.False(t, e.LastHotPathTrace().CosineVectorIndexFastPath)
		require.NotEmpty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).unregistered)
		explain, err := e.Execute(ctx, "EXPLAIN "+query, params)
		require.NoError(t, err)
		root := explain.Metadata["plan"].(*ExecutionPlan).Root
		require.Nil(t, relationshipPlanOperator(root, "RelationshipVectorIndexSeek"))
		require.Nil(t, relationshipPlanOperator(root, "RelationshipIndexSeek"))
	})
}

func TestRelationshipAccessProgramOwnsIDAndSelectedPathRoutes(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_storage_routes"))
	for _, query := range []string{
		"CREATE (:RouteNode {id:'a'}), (:RouteNode {id:'b'}), (:RouteNode {id:'c'})",
		"MATCH (a:RouteNode {id:'a'}), (b:RouteNode {id:'b'}) CREATE (a)-[:ROUTE {p:'x'}]->(b)",
		"MATCH (b:RouteNode {id:'b'}), (c:RouteNode {id:'c'}) CREATE (b)-[:ROUTE {p:'x'}]->(c)",
		"CREATE RANGE INDEX relationship_route_owner FOR ()-[r:ROUTE]-() ON (r.p)",
	} {
		_, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	idResult, err := e.Execute(context.Background(), "MATCH (:RouteNode {id:'a'})-[r:ROUTE]->() RETURN elementId(r)", nil)
	require.NoError(t, err)
	require.Len(t, idResult.Rows, 1)

	cases := relationshipAccessGeneratedStorageCases(idResult.Rows[0][0])
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertRelationshipAccessStorageCase(t, e, tc) })
	}
	t.Run("removed_element_id_site", func(t *testing.T) {
		tc := cases[0]
		ctx := context.WithValue(context.Background(), relationshipAccessProgramKey{}, &relationshipAccessProgram{query: tc.query, byID: map[string]*relationshipAccessTemplate{}})
		ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
		actual, err := e.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err)
		require.Equal(t, tc.rows, actual.Rows)
		require.NotEmpty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).unregistered)
		explain, err := e.Execute(ctx, "EXPLAIN "+tc.query, tc.params)
		require.NoError(t, err)
		require.Nil(t, relationshipPlanOperator(explain.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipByElementIdSeek"))
	})
	t.Run("removed_selected_path_site", func(t *testing.T) {
		var selected struct {
			query string
			rows  [][]interface{}
		}
		for _, tc := range cases {
			if tc.name == "generated_selected_ANY SHORTEST_fixed_false" {
				selected.query, selected.rows = tc.query, tc.rows
				break
			}
		}
		query := selected.query
		require.NotEmpty(t, query)
		ctx := context.WithValue(context.Background(), relationshipAccessProgramKey{}, &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}})
		ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
		actual, err := e.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, selected.rows, actual.Rows)
		require.NotEmpty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).unregistered)
		explain, err := e.Execute(ctx, "EXPLAIN "+query, nil)
		require.NoError(t, err)
		require.Nil(t, relationshipPlanOperator(explain.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek"))
	})
}
