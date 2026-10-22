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

	fixtureAdmission318Context, fixtureAdmission318 := statementTestContext(ctx, "MATCH (n:Seed)")
	_ = fixtureAdmission318
	_, err = exec.seedNodesFromOuterMatch(fixtureAdmission318, fixtureAdmission318Context, "MATCH (n:Seed)", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty correlated variable")

	fixtureAdmission319Context, fixtureAdmission319 := statementTestContext(ctx, "MATCH (n:Seed)")
	_ = fixtureAdmission319
	nodes, err := exec.seedNodesFromOuterMatch(fixtureAdmission319, fixtureAdmission319Context, "MATCH (n:Seed)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	fixtureAdmission320Context, fixtureAdmission320 := statementTestContext(ctx, "MATCH (n:Seed {team:'red'})")
	_ = fixtureAdmission320
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission320, fixtureAdmission320Context, "MATCH (n:Seed {team:'red'})", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "s1", nodes[0].Properties["id"])

	// Relationship pattern bypasses node-only fast path and executes through generic seed extraction.
	fixtureAdmission321Context, fixtureAdmission321 := statementTestContext(ctx, "MATCH (n:Seed)-[:R]->(m)")
	_ = fixtureAdmission321
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission321, fixtureAdmission321Context, "MATCH (n:Seed)-[:R]->(m)", "n")
	require.NoError(t, err)
	require.Empty(t, nodes)

	// WITH clause forces fallback path; verify projected variable extraction still works.
	fixtureAdmission322Context, fixtureAdmission322 := statementTestContext(ctx, "MATCH (n:Seed) WITH n")
	_ = fixtureAdmission322
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission322, fixtureAdmission322Context, "MATCH (n:Seed) WITH n", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
}
