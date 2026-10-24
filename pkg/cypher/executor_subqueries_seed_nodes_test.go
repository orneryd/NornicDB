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

	fixtureAdmission334Context, fixtureAdmission334 := statementTestContext(ctx, "MATCH (n:Person)")
	_ = fixtureAdmission334
	nodes, err := exec.seedNodesFromOuterMatch(fixtureAdmission334, fixtureAdmission334Context, "MATCH (n:Person)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	fixtureAdmission335Context, fixtureAdmission335 := statementTestContext(ctx, "MATCH (n:Person {name:'Alice'})")
	_ = fixtureAdmission335
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission335, fixtureAdmission335Context, "MATCH (n:Person {name:'Alice'})", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	// WHERE clause should preserve semantics via generic executor.
	fixtureAdmission336Context, fixtureAdmission336 := statementTestContext(ctx, "MATCH (n:Person) WHERE n.team = 'blue'")
	_ = fixtureAdmission336
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission336, fixtureAdmission336Context, "MATCH (n:Person) WHERE n.team = 'blue'", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Bob", nodes[0].Properties["name"])

	// Relationship pattern bypasses simple fast path and falls back to executeInternal.
	fixtureAdmission337Context, fixtureAdmission337 := statementTestContext(ctx, "MATCH (n:Person)-[:KNOWS]->(m:Person)")
	_ = fixtureAdmission337
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission337, fixtureAdmission337Context, "MATCH (n:Person)-[:KNOWS]->(m:Person)", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	// WITH in outer part forces fallback execution path.
	fixtureAdmission338Context, fixtureAdmission338 := statementTestContext(ctx, "MATCH (n:Person) WITH n")
	_ = fixtureAdmission338
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission338, fixtureAdmission338Context, "MATCH (n:Person) WITH n", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	fixtureAdmission339Context, fixtureAdmission339 := statementTestContext(ctx, "MATCH (n:Person)")
	_ = fixtureAdmission339
	_, err = exec.seedNodesFromOuterMatch(fixtureAdmission339, fixtureAdmission339Context, "MATCH (n:Person)", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty correlated variable")

	// Variable mismatch returns empty seeds.
	fixtureAdmission340Context, fixtureAdmission340 := statementTestContext(ctx, "MATCH (n:Person)")
	_ = fixtureAdmission340
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission340, fixtureAdmission340Context, "MATCH (n:Person)", "missing")
	require.NoError(t, err)
	require.Empty(t, nodes)
}
