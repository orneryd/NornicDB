package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReleaseNOR405PruneRecreatedEndpoint(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.DeleteNode(ctx, "test:a"))
	_, err := b.CreateNode(ctx, &Node{ID: "test:a"})
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	require.NoError(t, b.RebuildMVCCHeads(ctx))
	_, err = b.PruneMVCCVersions(ctx, MVCCPruneOptions{})
	require.NoError(t, err)
	edges, err := b.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
}
