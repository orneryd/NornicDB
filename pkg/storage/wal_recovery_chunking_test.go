package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoverFromWALWithResult_RestoresLargeEdgeSnapshotInChunks(t *testing.T) {
	dataDir := t.TempDir()
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	nodes := []*Node{
		{ID: "test:start", Labels: []string{"Recovered"}},
		{ID: "test:end", Labels: []string{"Recovered"}},
	}
	const edgeCount = 12000
	edges := make([]*Edge, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges = append(edges, &Edge{
			ID:        EdgeID(fmt.Sprintf("test:edge-%05d", i)),
			StartNode: "test:start",
			EndNode:   "test:end",
			Type:      "RECOVERED",
			Properties: map[string]any{
				"payload": strings.Repeat("x", 128),
			},
		})
	}

	snapshotPath := filepath.Join(snapshotDir, "snapshot-current.json")
	require.NoError(t, SaveSnapshot(&Snapshot{
		Sequence:  1,
		Timestamp: time.Now(),
		Nodes:     nodes,
		Edges:     edges,
		Version:   "1.0",
	}, snapshotPath))

	recovered, result, err := RecoverFromWALWithResult(filepath.Join(dataDir, "wal"), snapshotPath)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Zero(t, result.Failed)
	t.Cleanup(func() { _ = recovered.Close() })

	recoveredEdges, err := recovered.AllEdges()
	require.NoError(t, err)
	require.Len(t, recoveredEdges, edgeCount)

	edge, err := recovered.GetEdge(EdgeID("test:edge-00047"))
	require.NoError(t, err)
	require.Equal(t, NodeID("test:start"), edge.StartNode)
	require.Equal(t, NodeID("test:end"), edge.EndNode)
	require.Equal(t, strings.Repeat("x", 128), edge.Properties["payload"])
}
