package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSeedNodesFromOuterMatch_FastPathsAndFallbacks(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_nodes_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Person {name:'Alice', team:'red'}), (b:Person {name:'Bob', team:'blue'}), (c:Robot {name:'R2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)", nil)
	require.NoError(t, err)

	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person {name:'Alice'})", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	// WHERE clause should preserve semantics via generic executor.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person) WHERE n.team = 'blue'", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Bob", nodes[0].Properties["name"])

	// Relationship pattern bypasses simple fast path and falls back to executeInternal.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person)-[:KNOWS]->(m:Person)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	// WITH in outer part forces fallback execution path.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person) WITH n", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	_, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person)", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty correlated variable")

	// Variable mismatch returns empty seeds.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Person)", "missing")
	require.NoError(t, err)
	require.Empty(t, nodes)
}
