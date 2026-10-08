package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeleteProjectionCanonicalMultiplicityAndCounters(t *testing.T) {
	for _, test := range []struct {
		projection string
		columns    []string
		rows       [][]interface{}
		deleted    bool
	}{
		{"count(*) AS rows, count(DISTINCT n) AS nodes, count(r) AS edges", []string{"rows", "nodes", "edges"}, [][]interface{}{{int64(2), int64(1), int64(2)}}, false},
		{"$payload ORDER BY id(n) DESC SKIP 1 LIMIT 1", []string{"$payload"}, [][]interface{}{{map[string]interface{}{"value": float64(7)}}}, false},
		{"r.weight", nil, nil, true},
	} {
		t.Run(test.projection, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := withExpressionFailureSlot(context.WithValue(context.Background(), paramsKey, map[string]interface{}{"payload": map[string]interface{}{"value": float64(7)}}))
			_, err := exec.Execute(ctx, "CREATE (n:Victim {id: 'v'})-[r:R {weight: 1}]->(:Target), (n)-[s:R {weight: 2}]->(:Target)", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (n:Victim)-[r:R]->(m:Target) DETACH DELETE n RETURN "+test.projection, getParamsFromContext(ctx))
			if test.deleted {
				requireDeletedEntityError(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.columns, result.Columns)
			require.Equal(t, test.rows, result.Rows)
			require.Equal(t, 1, result.Stats.NodesDeleted)
			require.Equal(t, 2, result.Stats.RelationshipsDeleted)
		})
	}
}

func TestDeleteHelpers_CollectCandidatesAndProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_helpers_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (:Person {id:'p1', team:'red'}),
       (:Person {id:'p2', team:'blue'}),
       (:Person {id:'p3', team:'red'})
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX person_team_idx IF NOT EXISTS FOR (n:Person) ON (n.team)", nil)
	require.NoError(t, err)

	nodes, handled, err := exec.collectDeleteWithLimitCandidates(ctx, "RETURN 1", "n", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person)", "x", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team = 'red'", "n", 1, nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, nodes, 1)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team IN $teams", "n", 10, map[string]interface{}{"teams": []string{"blue"}})
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, nodes, 1)
	require.Equal(t, "blue", nodes[0].Properties["team"])

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team IN $teams", "n", 10, map[string]interface{}{"teams": int64(1)})
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team > 'a'", "n", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	res := &ExecuteResult{Stats: &QueryStats{NodesDeleted: 2, RelationshipsDeleted: 3}}
	node := &storage.Node{ID: "n1"}
	input := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{node}, {node}}}
	exec.applyDeleteReturnProjection(res, "MATCH (n) DELETE n RETURN count(*), count(n), 42 AS literal", "n", deleteProjectionInfo{ctx: ctx, input: input})
	require.Equal(t, []string{"count(*)", "count(n)", "literal"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 2, res.Rows[0][0])
	require.EqualValues(t, 2, res.Rows[0][1])
	require.EqualValues(t, 42, res.Rows[0][2])
	deletedCtx := withExpressionFailureSlot(ctx)
	exec.applyDeleteReturnProjection(res, "MATCH (n) DELETE n RETURN n.name", "n", deleteProjectionInfo{ctx: deletedCtx, input: input})
	requireDeletedEntityError(t, getExpressionFailure(deletedCtx))

	res = &ExecuteResult{Stats: &QueryStats{RelationshipsDeleted: 3}}
	edge := &storage.Edge{ID: "r1", Type: "R"}
	input = &ExecuteResult{Columns: []string{"r"}, Rows: [][]interface{}{{edge}, {edge}, {edge}}}
	exec.applyDeleteReturnProjection(res, "MATCH ()-[r]->() DELETE r RETURN count(r), r, type(r)", "r", deleteProjectionInfo{ctx: ctx, input: input})
	require.Equal(t, []string{"count(r)", "r", "type(r)"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 3, res.Rows[0][0])
	// The deleted relationship reads as its empty view (#907).
	require.Equal(t, &storage.Edge{ID: "r1", Type: "R"}, res.Rows[0][1])
	require.Equal(t, "R", res.Rows[0][2])

	res = &ExecuteResult{Stats: &QueryStats{NodesDeleted: 2, RelationshipsDeleted: 3}}
	info := deleteProjectionInfo{ctx: ctx, input: &ExecuteResult{Columns: []string{"n", "r"}, Rows: [][]interface{}{{node, edge}}}}
	exec.applyDeleteReturnProjection(res, "MATCH (n)-[r]->() DELETE n, r RETURN count(n), count(r), count(*)", "n, r", info)
	require.Equal(t, []string{"count(n)", "count(r)", "count(*)"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])
	require.EqualValues(t, 1, res.Rows[0][1])
	require.EqualValues(t, 1, res.Rows[0][2])
}

func TestDeleteHelpers_StreamEligibilityAndExecution(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.False(t, exec.isDeleteStreamingEligible("", "n", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n) WITH n", "n", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n,m", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n-1", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n", false))
	require.True(t, exec.isDeleteStreamingEligible("MATCH (n:Tmp)", "n", true))

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Tmp)"+" DELETE "+"n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Stats.NodesDeleted)

	verify, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	_, err = exec.Execute(ctx, "MATCH ("+" DELETE "+"n", getParamsFromContext(ctx))
	require.Error(t, err)

	// Fallback branch: rows returned but delete variable unresolved => no deletes.
	_, err = exec.Execute(ctx, "CREATE (:Ghost {id:'g1'})", nil)
	require.NoError(t, err)
	res, err = exec.Execute(ctx, "MATCH (n:Ghost)"+" DELETE "+"missingVar", getParamsFromContext(ctx))
	require.Error(t, err)
	require.Nil(t, res)
	ghosts, err := store.GetNodesByLabel("Ghost")
	require.NoError(t, err)
	require.Len(t, ghosts, 1)

	// Fallback branch with non-node values from a WITH projection.
	res, err = exec.Execute(ctx, "WITH 'does-not-exist' AS n"+" DELETE "+"n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)
}

func TestDeleteHelpers_StreamExecution_NodeEdgeAndStatsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_edges_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'}), (a)-[:R]->(b)", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Tmp {id:'a'})"+" DETACH DELETE "+"n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.NodesDeleted)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)

	verifyNodes, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, verifyNodes.Rows[0][0])
	verifyEdges, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verifyEdges.Rows[0][0])

	_, err = exec.Execute(ctx, "CREATE (c:Tmp {id:'c'}), (d:Tmp {id:'d'}), (c)-[:R]->(d)", nil)
	require.NoError(t, err)

	res, err = exec.Execute(ctx, "MATCH ()-[r:R]->()"+" DELETE "+"r", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)

	verifyEdges, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verifyEdges.Rows[0][0])
	verifyNodes, err = exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, verifyNodes.Rows[0][0])
}

func TestDeleteHelpers_StreamExecution_ExpressionDeleteVarsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_expr_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'}), (c:Tmp {id:'c'})", nil)
	require.NoError(t, err)

	// String branch in executeDeleteStreaming switch via RETURN id(n).
	res, err := exec.Execute(ctx, "MATCH (n:Tmp)"+" DELETE "+"id(n)", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Stats.NodesDeleted)

	verify, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	_, err = exec.Execute(ctx, "CREATE (x:Tmp {id:'x'}), (y:Tmp {id:'y'}), (x)-[:R]->(y)", nil)
	require.NoError(t, err)

	// Map branch with _edgeId key via map projection expression.
	res, err = exec.Execute(ctx, "MATCH ()-[r:R]->()"+" DELETE "+"{_edgeId: id(r)}", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)

	verify, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	// Map branch with _nodeId key via map projection expression.
	res, err = exec.Execute(ctx, "MATCH (n:Tmp)"+" DELETE "+"{_nodeId: id(n)}", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Stats.NodesDeleted)

	verify, err = exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])
}

func TestDeleteHelpers_ExecuteDeleteRelationshipCountProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_rel_count_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'}), (a)-[:R]->(b)", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (a:Tmp {id:'a'})-[r:R]->(b:Tmp {id:'b'}) DELETE r RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])
}

func TestDeleteHelpers_ClassifyDeleteTargetValue(t *testing.T) {
	require.Equal(t, deleteProjectionUnknown, classifyDeleteTargetValue(nil).kind)
	require.Equal(t, deleteProjectionNode, classifyDeleteTargetValue("n1").kind)
	require.Equal(t, storage.NodeID("n1"), classifyDeleteTargetValue("n1").nodeID)
	require.Equal(t, deleteProjectionNode, classifyDeleteTargetValue(map[string]interface{}{"_nodeId": "n2"}).kind)
	require.Equal(t, deleteProjectionRelationship, classifyDeleteTargetValue(map[string]interface{}{"_edgeId": "r2"}).kind)
}

func TestWherePartNodePattern(t *testing.T) {
	np := nodePatternInfo{labels: []string{"A"}}
	out := wherePartNodePattern(np, "n")
	require.Equal(t, "n", out.variable)
	require.Equal(t, []string{"A"}, out.labels)

	np2 := nodePatternInfo{variable: "x", labels: []string{"B"}}
	out2 := wherePartNodePattern(np2, "n")
	require.Equal(t, "x", out2.variable)
}
