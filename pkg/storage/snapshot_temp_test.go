package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestAutoCompactionRemovesInterruptedSnapshots(t *testing.T) {
	defer config.WithWALEnabled()()
	root := t.TempDir()
	dir := filepath.Join(root, "snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	stale := filepath.Join(dir, "snapshot-20261018-142941.123456789.json.tmp")
	require.NoError(t, os.WriteFile(stale, []byte("interrupted"), 0644))
	keep := []string{"snapshot-20261018-142941.123456789.json", "snapshot-20261018-142941.123456789.snapshot.tmp", "snapshot-20261018-142941.1.json.tmp", "snapshot-bad.json.tmp", "other.tmp"}
	for _, name := range keep {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("keep"), 0644))
	}
	nested := filepath.Join(dir, "snapshot-20261018-142942.123456789.json.tmp")
	require.NoError(t, os.Mkdir(nested, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(nested, filepath.Base(stale)), []byte("keep"), 0644))
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate", SnapshotInterval: time.Hour})
	require.NoError(t, err)
	engine := NewWALEngine(newTestMemoryEngine(t), wal)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.EnableAutoCompaction(dir))
	require.NoFileExists(t, stale, "startup must reclaim interrupted automatic snapshots")
	for _, name := range keep {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, "keep", string(raw))
	}
	require.DirExists(t, nested)
	require.FileExists(t, filepath.Join(nested, filepath.Base(stale)))
	_, err = engine.CreateNode(&Node{ID: "nornic:cleanup", Properties: map[string]any{"name": "retained"}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(stale, []byte("interrupted"), 0644))
	require.NoError(t, engine.createSnapshotAndCompact())
	require.NoFileExists(t, stale, "a later compaction also reclaims interrupted files")
	// Cleanup also runs on idle ticks, without requiring another full snapshot.
	require.NoError(t, os.WriteFile(stale, []byte("interrupted"), 0644))
	require.NoError(t, engine.createSnapshotAndCompact())
	require.NoFileExists(t, stale)
}
