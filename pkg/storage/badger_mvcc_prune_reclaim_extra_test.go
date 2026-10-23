package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PruneMVCCVersions_ReclaimsTombstonedHeadsAndDictEntries(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })

	nodeA := NodeID("test:prune-a")
	nodeB := NodeID("test:prune-b")
	edgeID := EdgeID("test:prune-edge")

	_, err := engine.CreateNode(&Node{ID: nodeA, Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: nodeB, Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: nodeA, EndNode: nodeB, Type: "R"}))

	// Delete the node to produce tombstones for the node and adjacent edge.
	require.NoError(t, engine.DeleteNode(nodeA))

	// No retention requested: prune should reclaim tombstoned logical keys fully
	// (head + dict entries + historical versions).
	pruned, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 0})
	require.NoError(t, err)
	require.Greater(t, pruned, int64(0))

	_, err = engine.GetNodeCurrentHead(nodeA)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeCurrentHead(edgeID)
	require.ErrorIs(t, err, ErrNotFound)

	_, ok := engine.idDict.lookupNodeNumID(nodeA)
	require.False(t, ok)
	_, ok = engine.idDict.lookupEdgeNumID(edgeID)
	require.False(t, ok)
}
