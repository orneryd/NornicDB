package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRecoverIntoEngineStreamsSnapshotAndWAL(t *testing.T) {
	cleanup := config.WithWALEnabled()
	defer cleanup()

	source := NewMemoryEngine()
	namespaced := NewNamespacedEngine(source, "test")
	_, err := namespaced.CreateNode(&Node{ID: "snapshot-node", Labels: []string{"Doc"}})
	require.NoError(t, err)

	root := t.TempDir()
	snapshotPath := filepath.Join(root, "snapshot.nds")
	require.NoError(t, SaveStreamingSnapshot(context.Background(), source, snapshotPath, SnapshotOptions{Sequence: 0}))

	walDir := filepath.Join(root, "wal")
	wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{
		Node: &Node{ID: "wal-node", Labels: []string{"Doc"}},
	}, "test"))
	require.NoError(t, wal.Close())

	destination := NewMemoryEngine()
	result, status, err := RecoverIntoEngine(destination, walDir, snapshotPath)
	require.NoError(t, err)
	require.Equal(t, 1, result.Applied)
	require.True(t, status.SnapshotStreaming)
	require.Equal(t, uint64(1), status.SnapshotNodes)
	require.Equal(t, uint64(1), status.WALEntries)

	_, err = destination.GetNode("test:snapshot-node")
	require.NoError(t, err)
	_, err = destination.GetNode("test:wal-node")
	require.NoError(t, err)
}

func TestVisitWALEntriesAfterFromDirPreservesOrder(t *testing.T) {
	cleanup := config.WithWALEnabled()
	defer cleanup()

	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{
		Dir:        dir,
		SyncMode:   "immediate",
		MaxEntries: 2,
	})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{
			Node: &Node{ID: NodeID(string(rune('a' + i)))},
		}, "test"))
	}
	require.NoError(t, wal.Close())

	var sequences []uint64
	err = VisitWALEntriesAfterFromDir(dir, 2, func(entry WALEntry) error {
		sequences = append(sequences, entry.Sequence)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4, 5}, sequences)
}
