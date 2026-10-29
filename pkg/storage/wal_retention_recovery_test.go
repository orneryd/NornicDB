package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALCompactedEmptyManifestDoesNotResurrectOrReuseSequence(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, mode := range []WALSnapshotMode{WALSnapshotTruncate, WALSnapshotRetention} {
		for _, stale := range []bool{false, true} {
			name := mode.String() + "/deleted"
			if stale {
				name = mode.String() + "/crash-before-segment-delete"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				cfg := DefaultWALConfig()
				cfg.MaxEntries = 1
				wal, err := NewWAL(dir, cfg)
				require.NoError(t, err)
				require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
				manifest, err := accessWALManifest(dir, walReadManifest, nil)
				require.NoError(t, err)
				require.Len(t, manifest.Segments, 1)
				path := filepath.Join(walSegmentsDir(dir), manifest.Segments[0].Path)
				prior, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, wal.ManageSnapshot(wal.Sequence(), mode))
				require.NoError(t, wal.Close())
				if stale {
					require.NoError(t, os.WriteFile(path, prior, 0600))
				}
				entries, err := ReadWALEntriesFromDir(dir)
				require.NoError(t, err)
				require.Empty(t, entries, "a snapshot-covered segment must stay retired")
				wal, err = NewWAL(dir, cfg)
				require.NoError(t, err)
				require.Equal(t, uint64(1), wal.Sequence(), "compaction must not reuse sequence numbers")
				require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
				require.NoError(t, wal.Close())
				entries, err = ReadWALEntriesFromDir(dir)
				require.NoError(t, err)
				require.Len(t, entries, 1)
				require.Equal(t, uint64(2), entries[0].Sequence)
			})
		}
	}
}
