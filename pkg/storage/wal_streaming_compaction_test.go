package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAutoCompactionWritesRecoverableStreamingSnapshot(t *testing.T) {
	root := t.TempDir()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	engine := NewMemoryEngine()
	walEngine := NewWALEngine(engine, wal)
	t.Cleanup(func() { require.NoError(t, walEngine.Close()) })
	walmart := NodeID("nornic:walmart")
	target := NodeID("nornic:target")
	_, err = walEngine.CreateNode(&Node{ID: walmart, Labels: []string{"Store"}})
	require.NoError(t, err)
	_, err = walEngine.CreateNode(&Node{ID: target, Labels: []string{"Store"}})
	require.NoError(t, err)
	require.NoError(t, walEngine.CreateEdge(&Edge{ID: "nornic:near", StartNode: walmart, EndNode: target, Type: "NEAR"}))

	walEngine.snapshotDir = filepath.Join(root, "snapshots")
	require.NoError(t, walEngine.createSnapshotAndCompact())
	paths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	header, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(header), len(streamSnapshotMagic))
	require.Equal(t, streamSnapshotMagic, string(header[:len(streamSnapshotMagic)]))

	recovered, err := RecoverFromWAL(wal.config.Dir, paths[0])
	require.NoError(t, err)
	nodes, err := recovered.AllNodes()
	require.NoError(t, err)
	edges, err := recovered.AllEdges()
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Len(t, edges, 1)
}
