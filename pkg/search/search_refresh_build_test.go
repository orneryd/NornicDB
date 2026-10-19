package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Refreshes during a population are ordinary writes in the population sequence. A
// refresh superseded before its turn is skipped (its valid check), not
// recorded as written, so neither it nor the population's older snapshot is the
// last to install: the index ends at the newest refresh's read.
func TestRefreshesDuringBuildInstallOnlyTheNewest(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	svc.FixVectorDimensions()
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(admissionNode("nornic:a", "oldterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	_, err = engine.CreateNode(admissionNode("nornic:b", "endterm", []float32{0, 1, 0}, "named"))
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}))
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")

	// A node refresh reserved, then reading an intermediate state, and a
	// newer refresh reading the final one: both are handed to the build.
	stale := svc.ReserveNodeMutation("nornic:a")
	require.NoError(t, engine.UpdateNode(admissionNode("nornic:a", "midterm", []float32{0, 0, 1}, "named")))
	requireReturnsWhileBlocked(t, "a stale refresh", func() error { return svc.RefreshNodeWithMutation(stale) })
	require.NoError(t, engine.UpdateNode(admissionNode("nornic:a", "newterm", []float32{0, 1, 1}, "named")))
	requireReturnsWhileBlocked(t, "a newer refresh", func() error { return svc.RefreshNode("nornic:a") })

	// A relationship refresh that read the vector, then a newer one that reads
	// the deletion.
	staleEdge := svc.ReserveEdgeMutation("nornic:r")
	require.NoError(t, engine.UpdateEdge(&storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b", Type: "REL", Properties: map[string]any{"v": []float32{0, 1, 0}}}))
	requireReturnsWhileBlocked(t, "a stale relationship refresh", func() error { return svc.RefreshEdgeWithMutation(staleEdge) })
	require.NoError(t, engine.DeleteEdge("nornic:r"))
	requireReturnsWhileBlocked(t, "a newer relationship refresh", func() error { return svc.RefreshEdge("nornic:r") })

	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "newterm"), "the newest refresh is not installed")
	require.Empty(t, searchIDs(t, svc, "midterm"), "a superseded refresh was installed")
	require.Empty(t, searchIDs(t, svc, "oldterm"), "the build's snapshot overwrote the newest refresh")
	require.False(t, svc.HasRelationshipVectorEntries("REL", "v"), "a superseded relationship refresh resurrected a deleted vector")
	svc.nodeMutationMu.Lock()
	defer svc.nodeMutationMu.Unlock()
	require.Empty(t, svc.pendingNodeMutations, "a reservation outlived the build")
	require.Empty(t, svc.pendingEdgeMutations, "a reservation outlived the build")
}
