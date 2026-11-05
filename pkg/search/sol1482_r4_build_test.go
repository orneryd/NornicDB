package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSol1482R4BuildSkipsSupersededRefresh(t *testing.T) {
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(t.Context(), buildWriteNode("nornic:a", "snapshotword", []float32{1, 0, 0}))
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), buildWriteNode("nornic:b", "endword", []float32{0, 1, 0}))
	require.NoError(t, err)
	edge := &storage.Edge{ID: "nornic:r", Type: "REL", StartNode: "nornic:a", EndNode: "nornic:b", Properties: map[string]any{"v": []float32{1, 0, 0}}}
	require.NoError(t, engine.CreateEdge(t.Context(), edge))
	built := startGatedBuild(t, engine, svc)
	require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: buildWriteNode("nornic:a", "staleword", []float32{0, 0, 1})}))
	requireReturnsDuringBuild(t, "node refresh", func() error { return svc.RefreshNode(t.Context(), "nornic:a") })
	require.NoError(t, engine.DeleteEdge(t.Context(), "nornic:r"))
	requireReturnsDuringBuild(t, "edge removal", func() error { return svc.RefreshEdge(t.Context(), "nornic:r") })
	newerNode := svc.ReserveNodeMutation("nornic:a")
	newerEdge := svc.ReserveEdgeMutation("nornic:r")
	// The newer refresh has reserved but has not read yet. A skipped queued
	// refresh must not mark its ID written and remove the build's own copy.
	require.NoError(t, engine.CreateEdge(t.Context(), edge))
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "snapshotword"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "staleword"))
	require.True(t, svc.HasRelationshipVectorEntries("REL", "v"), "a skipped edge removal was marked written")
	require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: buildWriteNode("nornic:a", "finalword", []float32{0, 1, 0})}))
	require.NoError(t, svc.RefreshNodeWithMutation(t.Context(), newerNode))
	require.NoError(t, engine.DeleteEdge(t.Context(), "nornic:r"))
	require.NoError(t, svc.RefreshEdgeWithMutation(t.Context(), newerEdge))
	require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "finalword"))
	require.False(t, svc.HasRelationshipVectorEntries("REL", "v"))
	_, err = svc.Search(context.Background(), "finalword", nil, nil)
	require.NoError(t, err)
}
