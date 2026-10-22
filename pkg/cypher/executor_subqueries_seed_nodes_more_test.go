package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSeedNodesFromOuterMatch_IDAndIndexPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_nodes_more_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	n1ID, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Seed"}, Properties: map[string]interface{}{"ext": "e1", "team": "red"}})
	require.NoError(t, err)
	n2ID, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Seed"}, Properties: map[string]interface{}{"ext": "e2", "team": "blue"}})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE INDEX seed_ext_idx IF NOT EXISTS FOR (n:Seed) ON (n.ext)", nil)
	require.NoError(t, err)

	fixtureAdmission326Context, fixtureAdmission326 := statementTestContext(ctx, "MATCH (n) WHERE id(n) = 'n1'")
	_ = fixtureAdmission326
	nodes, err := exec.seedNodesFromOuterMatch(fixtureAdmission326, fixtureAdmission326Context, "MATCH (n) WHERE id(n) = 'n1'", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, n1ID, nodes[0].ID)

	ctxWithIDs := withParams(ctx, map[string]interface{}{"ids": []interface{}{string(n1ID), string(n2ID)}})
	fixtureAdmission327Context, fixtureAdmission327 := statementTestContext(ctxWithIDs, "MATCH (n:Seed) WHERE id(n) IN $ids")
	_ = fixtureAdmission327
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission327, fixtureAdmission327Context, "MATCH (n:Seed) WHERE id(n) IN $ids", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	ctxWithExts := withParams(ctx, map[string]interface{}{"exts": []interface{}{"e1", "e2"}})
	fixtureAdmission328Context, fixtureAdmission328 := statementTestContext(ctxWithExts, "MATCH (n:Seed) WHERE n.ext IN $exts")
	_ = fixtureAdmission328
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission328, fixtureAdmission328Context, "MATCH (n:Seed) WHERE n.ext IN $exts", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	fixtureAdmission329Context, fixtureAdmission329 := statementTestContext(ctx, "MATCH (n:Seed) WHERE n.ext = 'e2'")
	_ = fixtureAdmission329
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission329, fixtureAdmission329Context, "MATCH (n:Seed) WHERE n.ext = 'e2'", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, n2ID, nodes[0].ID)

	// WHERE clause with trailing pipeline keywords must not pollute fast-path predicate parsing.
	fixtureAdmission330Context, fixtureAdmission330 := statementTestContext(ctx, "MATCH (n:Seed) WHERE n.ext = 'e1' ORDER BY n.ext")
	_ = fixtureAdmission330
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission330, fixtureAdmission330Context, "MATCH (n:Seed) WHERE n.ext = 'e1' ORDER BY n.ext", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, n1ID, nodes[0].ID)
}

func TestSeedNodesFromOuterMatch_NonIndexableWhereFallbacks(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_nodes_more_fallbacks")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Seed {ext:'e1', score: 10}), (:Seed {ext:'e2', score: 20})", nil)
	require.NoError(t, err)

	// Non-indexable expression should execute via generic path and still return deterministic rows.
	fixtureAdmission331Context, fixtureAdmission331 := statementTestContext(ctx, "MATCH (n:Seed) WHERE coalesce(n.score, 0) > 15")
	_ = fixtureAdmission331
	nodes, err := exec.seedNodesFromOuterMatch(fixtureAdmission331, fixtureAdmission331Context, "MATCH (n:Seed) WHERE coalesce(n.score, 0) > 15", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "e2", nodes[0].Properties["ext"])

	fixtureAdmission332Context, fixtureAdmission332 := statementTestContext(ctx, "MATCH (n:Seed) RETURN n ORDER BY n.score DESC LIMIT 1")
	_ = fixtureAdmission332
	nodes, err = exec.seedNodesFromOuterMatch(fixtureAdmission332, fixtureAdmission332Context, "MATCH (n:Seed) RETURN n ORDER BY n.score DESC LIMIT 1", "n")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "e2", nodes[0].Properties["ext"])

	// Mismatched correlated variable in fallback projection should surface explicit error.
	fixtureAdmission333Context, fixtureAdmission333 := statementTestContext(context.Background(), "MATCH (n:Seed) RETURN n")
	_ = fixtureAdmission333
	_, err = exec.seedNodesFromOuterMatch(fixtureAdmission333, fixtureAdmission333Context, "MATCH (n:Seed) RETURN n", "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "outer MATCH did not project correlated variable")
}
