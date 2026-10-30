package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSeedNodesFromOuterMatch_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_nodes_additional_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Seed {id:'s1', team:'red'}), (:Seed {id:'s2', team:'blue'}), (:Other {id:'o1'})", nil)
	require.NoError(t, err)

	_, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed)", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty correlated variable")

	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed {team:'red'})", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "s1", nodes[0].Properties["id"])

	// Relationship pattern bypasses node-only fast path and executes through generic seed extraction.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed)-[:R]->(m)", "n")
	require.NoError(t, err)
	require.Empty(t, nodes)

	// WITH clause forces fallback path; verify projected variable extraction still works.
	nodes, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (n:Seed) WITH n", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
}
