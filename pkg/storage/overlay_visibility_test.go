package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A suppressed entity whose newest body is still buffered is left out by every
// visible read and returned by every stored read, as when its body is stored:
// each reader merges the overlay through the one visibility composition
// (overlay_visibility.go). The relationship MERGE identity reads
// (GetEdgesBetween, GetEdgeBetween, MatchEdgesBetween) are among them.
func TestBufferedSuppressedEntitiesFollowReadVisibility(t *testing.T) {
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "test:a", Labels: []string{"A"}},
		{ID: "test:b", Labels: []string{"B"}},
		{ID: "test:h", Labels: []string{"H"}, Properties: map[string]any{"v": int64(1)}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
	require.NoError(t, engine.FlushWriteBehind())
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:h", true))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:r", true))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:h", Labels: []string{"H"}, Properties: map[string]any{"v": int64(2)}}, ClientNodeEdits))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": int64(2)}}))
	require.NoError(t, tx.Commit())
	ctx := context.Background()

	nodeIDs := func(nodes []*Node, err error) []NodeID {
		t.Helper()
		require.NoError(t, err)
		var ids []NodeID
		for _, node := range nodes {
			if node.ID == "test:h" {
				require.EqualValues(t, 2, node.Properties["v"], "the buffered body")
			}
			ids = append(ids, node.ID)
		}
		return ids
	}
	edgeIDs := func(edges []*Edge, err error) []EdgeID {
		t.Helper()
		require.NoError(t, err)
		var ids []EdgeID
		for _, edge := range edges {
			if edge.ID == "test:r" {
				require.EqualValues(t, 2, edge.Properties["v"], "the buffered body")
			}
			ids = append(ids, edge.ID)
		}
		return ids
	}
	streamNodes := func(stream func(func(*Node) error) error) ([]*Node, error) {
		var out []*Node
		err := stream(func(node *Node) error { out = append(out, node); return nil })
		return out, err
	}
	streamEdges := func(stream func(func(*Edge) error) error) ([]*Edge, error) {
		var out []*Edge
		err := stream(func(edge *Edge) error { out = append(out, edge); return nil })
		return out, err
	}
	all := func(*Edge) bool { return true }

	// Visible reads leave the buffered suppressed bodies out.
	_, err = engine.GetNode("test:h")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("test:r")
	require.ErrorIs(t, err, ErrNotFound)
	require.NotContains(t, nodeIDs(engine.GetNodesByLabel("H")), NodeID("test:h"))
	require.NotContains(t, nodeIDs(streamNodes(func(f func(*Node) error) error { return engine.StreamNodesByLabelProjected("H", nil, f) })), NodeID("test:h"))
	require.NotContains(t, nodeIDs(engine.AllNodes()), NodeID("test:h"))
	require.NotContains(t, nodeIDs(streamNodes(func(f func(*Node) error) error { return engine.StreamNodes(ctx, f) })), NodeID("test:h"))
	batch, err := engine.BatchGetNodes([]NodeID{"test:h", "test:a"})
	require.NoError(t, err)
	require.NotContains(t, batch, NodeID("test:h"))
	require.Contains(t, batch, NodeID("test:a"))
	require.NotContains(t, edgeIDs(engine.AllEdges()), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(streamEdges(func(f func(*Edge) error) error { return engine.StreamEdges(ctx, f) })), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(engine.GetEdgesByType("LINK")), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(streamEdges(func(f func(*Edge) error) error { return engine.StreamEdgesByType(ctx, "LINK", f) })), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(engine.GetOutgoingEdges("test:a")), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(engine.GetIncomingEdges("test:b")), EdgeID("test:r"))
	require.NotContains(t, edgeIDs(engine.GetEdgesBetween("test:a", "test:b")), EdgeID("test:r"))
	require.Nil(t, engine.GetEdgeBetween("test:a", "test:b", "LINK"))
	require.NotContains(t, edgeIDs(engine.MatchEdgesBetween("test:a", "test:b", "LINK", nil, all)), EdgeID("test:r"))

	// Stored reads return them, as the buffer holds them.
	stored, err := engine.GetStoredNode("test:h")
	require.NoError(t, err)
	require.EqualValues(t, 2, stored.Properties["v"])
	storedEdge, err := engine.GetStoredEdge("test:r")
	require.NoError(t, err)
	require.EqualValues(t, 2, storedEdge.Properties["v"])
	require.Contains(t, nodeIDs(engine.GetStoredNodesByLabel("H")), NodeID("test:h"))
	require.Contains(t, nodeIDs(streamNodes(func(f func(*Node) error) error { return engine.StreamStoredNodesByLabelProjected("H", nil, f) })), NodeID("test:h"))
	require.Contains(t, nodeIDs(streamNodes(func(f func(*Node) error) error { return StreamStoredNodes(ctx, engine, StreamNodesOptions{}, f) })), NodeID("test:h"))
	require.Contains(t, edgeIDs(engine.GetStoredEdgesByType("LINK")), EdgeID("test:r"))
	require.Contains(t, edgeIDs(streamEdges(func(f func(*Edge) error) error { return engine.StreamStoredEdgesByType(ctx, "LINK", f) })), EdgeID("test:r"))
	require.Contains(t, edgeIDs(engine.GetStoredOutgoingEdges("test:a")), EdgeID("test:r"))
	require.Contains(t, edgeIDs(engine.GetStoredIncomingEdges("test:b")), EdgeID("test:r"))
	require.Contains(t, edgeIDs(CollectStoredEdges(ctx, engine)), EdgeID("test:r"))

	// Revealed again, the visible reads return the bodies. The reveal runs
	// after the flush: on-demand suppression racing the write-behind flusher
	// on the same key is a separate, queued bug
	// (sig/handoff/bugqueue-q-suppress-flusher-race-20261021.md).
	require.NoError(t, engine.FlushWriteBehind())
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:h", false))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:r", false))
	require.Contains(t, edgeIDs(engine.GetEdgesBetween("test:a", "test:b")), EdgeID("test:r"))
	require.NotNil(t, engine.GetEdgeBetween("test:a", "test:b", "LINK"))
	require.Contains(t, nodeIDs(engine.GetNodesByLabel("H")), NodeID("test:h"))
}
