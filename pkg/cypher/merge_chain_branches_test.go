package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteMergeWithChain_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, "MERGE (a:Node {id:'a1'}) WITH a MATCH (b:Node {id:'missing'}) MERGE (a)-[:REL]->(b) RETURN a.id AS aid", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res.Columns)
	require.Empty(t, res.Rows)

	res, err = exec.Execute(ctx, "MERGE (a:Node {id:'a2'}) WITH a OPTIONAL MATCH (b:Node {id:'missing'}) RETURN a.id AS aid, b.id AS bid", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid", "bid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a2", res.Rows[0][0])
	require.Nil(t, res.Rows[0][1])

	res, err = exec.Execute(ctx, "MERGE (a:Node {id:'a3'}) WITH a MERGE (b:Node {id:'b3'}) WITH a, b MERGE (a)-[:REL]->(b) RETURN a.id AS aid, b.id AS bid", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid", "bid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a3", res.Rows[0][0])
	require.Equal(t, "b3", res.Rows[0][1])

	verify, err := exec.Execute(ctx, "MATCH (a:Node {id:'a3'})-[r:REL]->(b:Node {id:'b3'}) RETURN count(r)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])

	// FOREACH clause inside chain segment
	res, err = exec.Execute(ctx, "MERGE (a:Node {id:'a4'}) WITH a FOREACH (i IN [1,2] | CREATE (n:Tmp {k:i})) RETURN a.id AS aid", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a4", res.Rows[0][0])

	cnt, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, cnt.Rows[0][0])
}

func TestMergeRepeatedWithPreservesScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_repeated_with"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "MERGE (n:Node {id:'1'})\nWITH n\nWITH n\nRETURN n.id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"1"}}, result.Rows)
	require.EqualValues(t, 1, result.Stats.NodesCreated)
	stored, err := exec.Execute(ctx, "MATCH (n:Node) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}

func TestProjectWithContext_ScalarFallback(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_proj_cov"))
	ctx := context.Background()
	n := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	input := []pipelineRow{{"n": n, "score": int64(7)}}
	projected, ok := exec.pipelineApplyWith(ctx, input, "WITH n AS nodeAlias, score AS s")
	require.True(t, ok)
	require.Equal(t, []pipelineRow{{"nodeAlias": n, "s": int64(7)}}, projected)
	require.Equal(t, []pipelineRow{{"n": n, "score": int64(7)}}, input)
}

func TestApplyWithProjection_Branches_Additional(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_with_proj_cov"))
	ctx := context.Background()
	n := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	input := []pipelineRow{{"n": n, "score": int64(7)}}
	projected, ok := exec.pipelineApplyWith(ctx, input, "WITH *")
	require.True(t, ok)
	require.Equal(t, input, projected)
	projected, ok = exec.pipelineApplyWith(ctx, input, "WITH n AS m, score AS s")
	require.True(t, ok)
	require.Equal(t, []pipelineRow{{"m": n, "s": int64(7)}}, projected)
}

func TestExecuteMergeWithChain_UnboundEndpoint(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_err_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	query := "MERGE (a:Node {id:'e1'}) WITH a MERGE (a)-[:REL]->(missing) RETURN a.id AS aid"
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"e1"}}, result.Rows)
	require.EqualValues(t, 2, result.Stats.NodesCreated)
	require.EqualValues(t, 1, result.Stats.RelationshipsCreated)
	repeated, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, result.Rows, repeated.Rows)
	require.Zero(t, repeated.Stats.NodesCreated)
	require.Zero(t, repeated.Stats.RelationshipsCreated)
	nodes, err := store.GetNodesByLabel("Node")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	stored, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}
