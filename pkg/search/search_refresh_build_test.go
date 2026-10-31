package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Refreshes during a build are ordinary writes in the build sequence. A
// refresh superseded before its turn is skipped (its valid check), not
// recorded as written, so neither it nor the build's older snapshot is the
// last to install: the index ends at the newest refresh's read.
func TestRefreshesDuringBuildInstallOnlyTheNewest(t *testing.T) {
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.ReleaseWrites)
	_, err := engine.CreateNode(buildWriteNode("nornic:a", "oldterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	_, err = engine.CreateNode(buildWriteNode("nornic:b", "endterm", []float32{0, 1, 0}))
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}))
	built := startGatedBuild(t, engine, svc)

	// A node refresh reserved, then reading an intermediate state, and a
	// newer refresh reading the final one: both are handed to the build.
	stale := svc.ReserveNodeMutation("nornic:a")
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: buildWriteNode("nornic:a", "midterm", []float32{0, 0, 1})}))
	requireReturnsDuringBuild(t, "a stale refresh", func() error { return svc.RefreshNodeWithMutation(stale) })
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: buildWriteNode("nornic:a", "newterm", []float32{0, 1, 1})}))
	requireReturnsDuringBuild(t, "a newer refresh", func() error { return svc.RefreshNode("nornic:a") })

	// A relationship refresh that read the vector, then a newer one that reads
	// the deletion.
	staleEdge := svc.ReserveEdgeMutation("nornic:r")
	require.NoError(t, engine.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b", Type: "REL", Properties: map[string]any{"v": []float32{0, 1, 0}}}}))
	requireReturnsDuringBuild(t, "a stale relationship refresh", func() error { return svc.RefreshEdgeWithMutation(staleEdge) })
	require.NoError(t, engine.DeleteEdge("nornic:r"))
	requireReturnsDuringBuild(t, "a newer relationship refresh", func() error { return svc.RefreshEdge("nornic:r") })

	engine.ReleaseWrites()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "newterm"), "the newest refresh is not installed")
	require.Empty(t, buildWriteSearchIDs(t, svc, "midterm"), "a superseded refresh was installed")
	require.Empty(t, buildWriteSearchIDs(t, svc, "oldterm"), "the build's snapshot overwrote the newest refresh")
	require.False(t, svc.HasRelationshipVectorEntries("REL", "v"), "a superseded relationship refresh resurrected a deleted vector")
	svc.nodeMutationMu.Lock()
	defer svc.nodeMutationMu.Unlock()
	require.Empty(t, svc.nodeMutations.pending, "a reservation outlived the build")
	require.Empty(t, svc.edgeMutations.pending, "a reservation outlived the build")
}
