package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestReleaseNOR412SalvageRetainsPostSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL("", &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	defer w.Close()
	b := newTestMemoryEngine(t)
	n := &Node{ID: "test:a", Labels: []string{"L"}}
	_, err = b.CreateNode(t.Context(), n)
	require.NoError(t, err)
	require.NoError(t, w.Append(OpCreateNode, WALNodeData{Node: n}))
	snapshot, err := w.CreateSnapshot(b)
	require.NoError(t, err)
	n2 := &Node{ID: "test:b", Labels: []string{"L"}}
	require.NoError(t, w.Append(OpCreateNode, WALNodeData{Node: n2}))
	require.NoError(t, w.Sync())
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, w.ManageSnapshot(snapshot.Sequence, WALSnapshotTruncate))
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "valid acknowledged post-snapshot entry must survive salvage")
	for _, e := range entries {
		require.Greater(t, e.Sequence, snapshot.Sequence)
	}
}
