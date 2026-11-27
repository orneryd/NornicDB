package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptWALDirectoryConvertsRetiredUnlistedSegment(t *testing.T) {
	dir := t.TempDir()
	marker := "retired-plaintext-marker-6268f050"
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpCheckpoint, map[string]any{"marker": marker}, "test"))
	require.NoError(t, wal.Close())
	segment := filepath.Join(walSegmentsDir(dir), "seg-1-1.wal")
	require.NoError(t, os.Rename(walActivePath(dir), segment))
	_, err = accessWALManifest(dir, walPublishManifest, &WALManifest{Version: walManifestVersion, RetiredThrough: 1})
	require.NoError(t, err)
	raw, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.True(t, bytes.Contains(raw, []byte(marker)))

	key := encryptedWALTestKey(19)
	require.NoError(t, EncryptWALDirectory(t.Context(), dir, key))
	raw, err = os.ReadFile(segment)
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, []byte(marker)), "retired segment omitted from the manifest must not retain plaintext")
	entries, err := ReadWALEntriesWithKey(t.Context(), segment, nil, key)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Contains(t, string(entries[0].Data), marker)
}
