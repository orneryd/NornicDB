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

func TestDeleteHelpers_StreamEligibilityAndExecution(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.False(t, exec.isDeleteStreamingEligible(ctx, "", "n", true))
	require.False(t, exec.isDeleteStreamingEligible(ctx, "MATCH (n) WITH n", "n", true))
	require.False(t, exec.isDeleteStreamingEligible(ctx, "MATCH (n)", "n,m", true))
	require.False(t, exec.isDeleteStreamingEligible(ctx, "MATCH (n)", "n-1", true))
	require.False(t, exec.isDeleteStreamingEligible(ctx, "MATCH (n)", "n", false))
	require.True(t, exec.isDeleteStreamingEligible(ctx, "MATCH (n:Tmp)", "n", true))

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
	ghosts, err := store.GetNodesByLabel(ctx, "Ghost")
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

	// Public marker maps are ordinary data, not relationship/node values.
	_, err = exec.Execute(ctx, "MATCH ()-[r:R]->() DELETE {_edgeId: id(r)}", nil)
	require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
	verify, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, verify.Rows[0][0])

	_, err = exec.Execute(ctx, "MATCH (n:Tmp) DELETE {_nodeId: id(n)}", nil)
	require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
	verify, err = exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, verify.Rows[0][0])

	res, err = exec.Execute(ctx, "MATCH ()-[r:R]->() DELETE r", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)
	res, err = exec.Execute(ctx, "MATCH (n:Tmp) DELETE id(n)", nil)
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
