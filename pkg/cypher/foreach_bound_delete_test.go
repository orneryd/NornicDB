package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestForeach_BoundStandaloneDelete pins §6.2 bound-entity deletion: a
// standalone DELETE / DETACH DELETE whose variable resolves through the value
// scope deletes the bound entity (FOREACH over an entity list), while unbound
// targets keep the historical match-required error.
func TestForeach_BoundStandaloneDelete(t *testing.T) {
	exec, engine := newTestExecutor(t)
	ctx := context.Background()

	// Two chunks connected to a parent file.
	_, err := exec.Execute(ctx, `CREATE (f:File {root: '/w'})-[:HAS_CHUNK]->(:Chunk {n: 1}), (f)-[:HAS_CHUNK]->(:Chunk {n: 2})`, nil)
	require.NoError(t, err)

	chunks, err := engine.GetNodesByLabel("Chunk")
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	chunkItems := []interface{}{chunks[0], chunks[1]}

	// DETACH DELETE of the bound chunk entities.
	foreachCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"chunks": chunkItems})
	_, err = exec.sharedForeachForTest(foreachCtx, "FOREACH (chunk IN $chunks | DETACH DELETE chunk)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	left, err := engine.GetNodesByLabel("Chunk")
	require.NoError(t, err)
	require.Empty(t, left, "bound chunks must be deleted")

	// The parent survives a DETACH of its chunks.
	files, err := engine.GetNodesByLabel("File")
	require.NoError(t, err)
	require.Len(t, files, 1)

	// A bound edge can be deleted the same way.
	_, err = exec.Execute(ctx, `MATCH (f:File {root: '/w'}) CREATE (f)-[:LINK]->(t:Target)`, nil)
	require.NoError(t, err)
	edges, err := engine.GetEdgesByType("LINK")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edgeCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"rels": []interface{}{edges[0]}})
	_, err = exec.sharedForeachForTest(edgeCtx, "FOREACH (r IN $rels | DELETE r)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	edges, err = engine.GetEdgesByType("LINK")
	require.NoError(t, err)
	require.Empty(t, edges, "bound relationship must be deleted")

	// Reconnect the file, then a non-DETACH bound delete must go through the
	// residual relationship guard.
	_, err = exec.Execute(ctx, `MATCH (f:File {root: '/w'}) CREATE (f)-[:LINK]->(t:Target)`, nil)
	require.NoError(t, err)
	file := files[0]
	connectedCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"items": []interface{}{file}})
	_, err = exec.sharedForeachForTest(connectedCtx, "FOREACH (f IN $items | DELETE f)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.Error(t, err)

	// Unbound standalone DELETE is rejected by shared statement admission.
	_, err = exec.Execute(ctx, "DELETE n", getParamsFromContext(ctx))
	require.Error(t, err)
	require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
	require.ErrorContains(t, err, "undefined variable")
}

// TestForeach_BoundStandaloneDelete_StringIDs pins the string-ID bound target.
func TestForeach_BoundStandaloneDelete_StringIDs(t *testing.T) {
	exec, engine := newTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:StrDel {k: 1})`, nil)
	require.NoError(t, err)
	nodes, err := engine.GetNodesByLabel("StrDel")
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	strCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"items": []interface{}{string(nodes[0].ID)}})
	_, err = exec.sharedForeachForTest(strCtx, "FOREACH (id IN $items | DELETE id)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	nodes, err = engine.GetNodesByLabel("StrDel")
	require.NoError(t, err)
	require.Empty(t, nodes, "string-ID bound target must be deleted")
}

// BenchmarkForeach_BoundStandaloneDelete pins the bound standalone delete path
// (resolve binding, residual guard, node delete) on a memory engine.
func BenchmarkForeach_BoundStandaloneDelete(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		exec, engine := newTestExecutor(b)
		if _, err := exec.Execute(ctx, `CREATE (:BDel)`, nil); err != nil {
			b.Fatal(err)
		}
		nodes, err := engine.GetNodesByLabel("BDel")
		if err != nil || len(nodes) != 1 {
			b.Fatalf("setup: %v %d", err, len(nodes))
		}
		foreachCtx := context.WithValue(ctx, paramsKey, map[string]interface{}{"items": []interface{}{nodes[0]}})
		if _, err := exec.sharedForeachForTest(foreachCtx, "FOREACH (x IN $items | DELETE x)", map[string]*storage.Node{}, map[string]*storage.Edge{}); err != nil {
			b.Fatal(err)
		}
	}
}
