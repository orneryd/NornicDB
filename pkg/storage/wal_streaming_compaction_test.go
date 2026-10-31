package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestAutoCompactionWritesRecoverableStreamingSnapshot(t *testing.T) {
	defer config.WithWALEnabled()()
	root := t.TempDir()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	engine := newTestMemoryEngine(t)
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
	require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
	paths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	header, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(header), len(streamSnapshotMagic))
	require.Equal(t, streamSnapshotMagic, string(header[:len(streamSnapshotMagic)]))

	recovered, err := fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWAL(wal.config.Dir, paths[0]))
	require.NoError(t, err)
	nodes, err := recovered.AllNodes()
	require.NoError(t, err)
	edges, err := recovered.AllEdges()
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Len(t, edges, 1)
}

func TestAutoCompactionSkipsSnapshotWhenWALHasNotAdvanced(t *testing.T) {
	defer config.WithWALEnabled()()
	root := t.TempDir()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	walEngine := NewWALEngine(newTestMemoryEngine(t), wal)
	t.Cleanup(func() { require.NoError(t, walEngine.Close()) })
	walEngine.snapshotDir = filepath.Join(root, "snapshots")
	require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
	initialPaths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
	require.NoError(t, err)
	require.Empty(t, initialPaths, "an engine opened without new WAL mutations must remain idle")

	_, err = walEngine.CreateNode(&Node{ID: "nornic:first", Labels: []string{"Document"}})
	require.NoError(t, err)
	require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
	firstSequence := wal.sequence.Load()
	firstSnapshots, _ := walEngine.GetSnapshotStats()

	require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
	paths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
	require.NoError(t, err)
	idleSnapshots, _ := walEngine.GetSnapshotStats()
	require.Equal(t, firstSequence, wal.sequence.Load(), "idle compaction must not append a checkpoint")
	require.Equal(t, firstSnapshots, idleSnapshots, "idle compaction must not record a snapshot")
	require.Len(t, paths, 1, "idle compaction must not rewrite the database")

	_, err = walEngine.CreateNode(&Node{ID: "nornic:second", Labels: []string{"Document"}})
	require.NoError(t, err)
	require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
	paths, err = filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
	require.NoError(t, err)
	changedSnapshots, _ := walEngine.GetSnapshotStats()
	require.Equal(t, firstSnapshots+1, changedSnapshots)
	require.Len(t, paths, 2, "a WAL mutation must trigger the next snapshot")
}

func BenchmarkIdleAutoCompaction(b *testing.B) {
	defer config.WithWALEnabled()()
	root := b.TempDir()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate"})
	require.NoError(b, err)
	walEngine := NewWALEngine(fixtureowner.Own(b, fixtureowner.Track(b, NewMemoryEngine(), ErrStorageClosed)), wal)
	b.Cleanup(func() { require.NoError(b, walEngine.Close()) })
	walEngine.snapshotDir = filepath.Join(root, "snapshots")
	_, err = walEngine.CreateNode(&Node{ID: "nornic:document"})
	require.NoError(b, err)
	require.NoError(b, walEngine.createSnapshotAndCompact(walEngine.work.Context()))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := walEngine.createSnapshotAndCompact(walEngine.work.Context()); err != nil {
			b.Fatal(err)
		}
	}
}
