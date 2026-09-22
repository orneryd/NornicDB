package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVariableLengthRelationshipBindsEveryTraversedRelationship(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "variable_length_relationship_list")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:A), (b), (c) CREATE (a)-[:X]->(b), (b)-[:Y]->(c)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:A) MATCH (a)-[relationships*2]->() RETURN relationships", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0][0], 2)
}

func TestBoundRelationshipListDefinesVariableLengthPath(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound_relationship_list")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:A), (b:B), (c:C) CREATE (a)-[:Y]->(b), (b)-[:Y]->(c)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH ()-[firstRel]->()-[secondRel]->() WITH [firstRel, secondRel] AS relationships LIMIT 1 MATCH (first)-[relationships*]->(second) RETURN first, second", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Contains(t, result.Rows[0][0].(*storage.Node).Labels, "A")
	require.Contains(t, result.Rows[0][1].(*storage.Node).Labels, "C")
}

func TestCreatePipelineBuildsNodeListForRelationshipCreation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "create_aggregated_node_list")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a {var: 'start'}), (b {var: 'end'})
		WITH *
		UNWIND range(1, 20) AS i
		CREATE (n {var: i})
		WITH a, b, [a] + collect(n) + [b] AS nodeList
		UNWIND range(0, size(nodeList) - 2, 1) AS i
		WITH nodeList[i] AS n1, nodeList[i+1] AS n2
		CREATE (n1)-[:T]->(n2)`, nil)
	require.NoError(t, err)

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 21)
}

func TestVariableLengthRelationshipPatternRejectsMalformedBounds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "variable_length_invalid_bounds"))
	ctx := context.Background()

	for _, query := range []string{
		"MATCH (a)-[:TYPE..]->(b) RETURN b",
		"MATCH (a)-[:TYPE*-2]->(b) RETURN b",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireMatchSemanticDetail(t, err, "InvalidRelationshipPattern")
	}
}

func TestZeroHopTraversalBindsBothEndpointsToTheSeedNode(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zero_hop_endpoint_binding")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Root {name: 'seed'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (start:Root) MATCH (start)-[:LINK*0]->(end) RETURN end.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"seed"}}, result.Rows)
}

func BenchmarkTraceRelationshipList64(b *testing.B) {
	relationships := make([]*storage.Edge, 64)
	for index := range relationships {
		relationships[index] = &storage.Edge{
			ID:        storage.EdgeID(string(rune(index + 1))),
			StartNode: storage.NodeID(string(rune(index + 1))),
			EndNode:   storage.NodeID(string(rune(index + 2))),
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if endpoints := traceRelationshipList(relationships, "outgoing"); len(endpoints) != 1 {
			b.Fatalf("expected one endpoint pair, got %d", len(endpoints))
		}
	}
}
