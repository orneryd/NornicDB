package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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

	queries := []string{
		"MATCH ()-[e:RELATES_TO]->() WHERE e.uuid='r1' RETURN e.uuid, vector.similarity.cosine(e.fact_embedding,$q) AS score ORDER BY score DESC LIMIT 5",
		"MATCH ()-[e:RELATES_TO]->() WHERE e.uuid='r1' WITH e, vector.similarity.cosine(e.fact_embedding,$q) AS score RETURN e.uuid, score ORDER BY score DESC LIMIT 5",
		"MATCH ()-[e:RELATES_TO]->() WHERE e.uuid=$id RETURN e.uuid, vector.similarity.cosine(e.fact_embedding,$q) AS score ORDER BY score DESC LIMIT 5",
		"MATCH ()-[e:RELATES_TO]->() WHERE e.uuid=$id WITH e, vector.similarity.cosine(e.fact_embedding,$q) AS score RETURN e.uuid, score ORDER BY score DESC LIMIT 5",
	}
	params := map[string]interface{}{"id": "r1", "q": []float64{1, 0, 0}}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			runtimeCtx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
			actual, err := e.Execute(runtimeCtx, query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"r1", 1.0}}, actual.Rows)
			require.True(t, e.LastHotPathTrace().CosineVectorIndexFastPath)
			runtimeProfile := runtimeCtx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			require.Empty(t, runtimeProfile.unregistered)
			require.Len(t, runtimeProfile.templates, 1)
			for _, site := range runtimeProfile.templates {
				require.NotNil(t, site.access.vector)
				require.Equal(t, "relationship_vector_owner", site.access.vector.index)
			}

			var accessType string
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				result, err := e.Execute(context.Background(), mode+" "+query, params)
				require.NoError(t, err)
				operator := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipVectorIndexSeek")
				require.NotNil(t, operator, "%s must project the vector access selected by execution", mode)
				require.Equal(t, int64(10), operator.EstimatedRows)
				if accessType == "" {
					accessType = operator.OperatorType
				}
				require.Equal(t, accessType, operator.OperatorType)
				if mode == "PROFILE" {
					require.Equal(t, actual.Rows, result.Rows)
				} else {
					require.Empty(t, result.Rows)
				}
			}
		})
	}
	t.Run("removed_vector_site", func(t *testing.T) {
		query := queries[0]
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

	cases := []struct {
		name, query, operator string
		estimate              int64
		params                map[string]interface{}
		rows                  [][]interface{}
	}{
		{
			name: "element_id_seed",
			query: "MATCH ()-[r:ROUTE]->() WHERE elementId(r)=$id AND r.p='x' RETURN r.p",
			operator: "RelationshipIndexSeek", estimate: 10,
			params: map[string]interface{}{"id": idResult.Rows[0][0]}, rows: [][]interface{}{{"x"}},
		},
		{
			name: "shortest_path",
			query: "MATCH p=shortestPath((a:RouteNode {id:'a'})-[r:ROUTE*]->(c:RouteNode {id:'c'})) RETURN length(p)",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		},
		{
			name: "all_shortest_paths",
			query: "MATCH p=allShortestPaths((a:RouteNode {id:'a'})-[r:ROUTE*]->(c:RouteNode {id:'c'})) RETURN length(p)",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		},
		{
			name: "shortest_path_expression",
			query: "MATCH (a:RouteNode {id:'a'}), (c:RouteNode {id:'c'}) RETURN length(shortestPath((a)-[r:ROUTE*]->(c)))",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		},
		{
			name: "selected_path",
			query: "MATCH p = ANY SHORTEST (a:RouteNode {id:'a'})-[r:ROUTE*1..2]->(c:RouteNode {id:'c'}) RETURN length(p)",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		},
		{
			name: "selected_fixed_inline_property",
			query: "MATCH p = ANY 1 (a:RouteNode {id:'a'})-[r:ROUTE {p:'x'}]->(b:RouteNode {id:'b'}) RETURN length(p)",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(1)}},
		},
		{
			name: "selected_shortest_group",
			query: "MATCH p = SHORTEST 1 GROUP (a:RouteNode {id:'a'})-[r:ROUTE*1..2]->(c:RouteNode {id:'c'}) RETURN length(p)",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtimeCtx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
			actual, err := e.Execute(runtimeCtx, tc.query, tc.params)
			require.NoError(t, err)
			require.Equal(t, tc.rows, actual.Rows)
			runtimeProfile := runtimeCtx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			require.Empty(t, runtimeProfile.unregistered)
			require.Len(t, runtimeProfile.templates, 1)
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				result, err := e.Execute(context.Background(), mode+" "+tc.query, tc.params)
				require.NoError(t, err, mode)
				operator := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, tc.operator)
				require.NotNil(t, operator, "%s access operator", mode)
				require.Equal(t, tc.estimate, operator.EstimatedRows, mode)
				if mode == "PROFILE" {
					require.Equal(t, actual.Rows, result.Rows)
				} else {
					require.Empty(t, result.Rows)
				}
			}
		})
	}
	t.Run("removed_selected_path_site", func(t *testing.T) {
		var selected struct {
			query string
			rows  [][]interface{}
		}
		for _, tc := range cases {
			if tc.name == "selected_path" {
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
