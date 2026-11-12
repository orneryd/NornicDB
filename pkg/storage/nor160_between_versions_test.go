package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR160CurrentAndHistoricalBetweenReads(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	ctx := t.Context()
	require.NoError(t, b.EnsureNamespaceMVCC("test"))
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:other", StartNode: "test:a", EndNode: "test:c", Type: "COHELD", Properties: map[string]any{"n": int64(3)}}))
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "COHELD", Properties: map[string]any{"n": int64(1)}}))
	head, err := b.GetEdgeCurrentHead(ctx, "test:e")
	require.NoError(t, err)
	peer, err := b.GetEdge(ctx, "test:e")
	require.NoError(t, err)
	before := CopyEdge(peer)
	peer.Properties["n"] = int64(2)
	require.NoError(t, b.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: peer}))
	latest, err := b.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.NoError(t, err)
	require.Len(t, latest, 1)
	require.Equal(t, int64(2), latest[0].Properties["n"])
	historic, err := b.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"}, head.Version)
	require.NoError(t, err)
	require.Len(t, historic, 1)
	require.Equal(t, EdgeID("test:e"), historic[0].ID)
	require.Equal(t, int64(1), historic[0].Properties["n"])
	historic[0].Properties["n"] = int64(99)
	repeated, err := b.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"}, head.Version)
	require.NoError(t, err)
	require.Len(t, repeated, 1)
	require.Equal(t, int64(1), repeated[0].Properties["n"])
	reverse, err := b.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:b", Other: "test:a"}, head.Version)
	require.NoError(t, err)
	require.Empty(t, reverse)
	require.NoError(t, b.DeleteEdge(ctx, "test:e"))
	latest, err = b.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.NoError(t, err)
	require.Empty(t, latest)
	historic, err = b.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"}, head.Version)
	require.NoError(t, err)
	require.Len(t, historic, 1)
	require.Equal(t, int64(1), historic[0].Properties["n"])
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = b.ReadAdjacentEdges(canceled, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = b.ReadAdjacentEdgesVisibleAt(canceled, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"}, head.Version)
	require.ErrorIs(t, err, context.Canceled)
}
