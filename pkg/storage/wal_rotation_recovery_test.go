package storage

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

// Rotation recovery owns one fixture for missing-newest and retired-oldest
// manifest entries. Both paths must preserve their exact sequence population.
func TestWALRotationManifestRecovery(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		tail  bool
	}{
		{"recover_unlisted_tail", 2, true},
		{"do_not_resurrect_older_unlisted_segment", 3, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.MaxEntries = 1
			wal, err := NewWAL(dir, cfg)
			require.NoError(t, err)
			for i := 1; i <= test.count; i++ {
				require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": i}))
			}
			require.NoError(t, wal.Close())
			manifest, err := accessWALManifest(dir, walReadManifest, nil)
			require.NoError(t, err)
			require.Len(t, manifest.Segments, test.count)
			if test.tail {
				manifest.Segments = manifest.Segments[:1]
			} else {
				manifest.Segments = manifest.Segments[1:]
			}
			_, err = accessWALManifest(dir, walPublishManifest, manifest)
			require.NoError(t, err)
			entries, err := ReadWALEntriesAfter(dir, 0, WALReadDirectory)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			if !test.tail {
				require.Equal(t, uint64(2), entries[0].Sequence)
				require.Equal(t, uint64(3), entries[1].Sequence)
				return
			}
			require.Equal(t, uint64(2), entries[1].Sequence)
			var visited []uint64
			require.NoError(t, VisitWALEntriesAfterFromDir(dir, 0, func(e WALEntry) error { visited = append(visited, e.Sequence); return nil }))
			require.Equal(t, []uint64{1, 2}, visited)
			wal, err = NewWAL(dir, cfg)
			require.NoError(t, err)
			require.Equal(t, uint64(2), wal.Sequence())
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 3}))
			require.NoError(t, wal.Close())
			manifest, err = accessWALManifest(dir, walReadManifest, nil)
			require.NoError(t, err)
			require.Len(t, manifest.Segments, 3)
			entries, err = ReadWALEntriesAfter(dir, 0, WALReadDirectory)
			require.NoError(t, err)
			require.Len(t, entries, 3)
			require.Equal(t, uint64(3), entries[2].Sequence)
		})
	}
}

func TestWALRotationRejectsOverlappingUnlistedTail(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.MaxEntries = 2
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
	require.NoError(t, wal.Close())
	m, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, m.Segments, 1)
	raw, err := os.ReadFile(filepath.Join(walSegmentsDir(dir), m.Segments[0].Path))
	require.NoError(t, err)
	name := fmt.Sprintf("seg-%020d-%020d.wal", uint64(2), uint64(3))
	require.NoError(t, os.WriteFile(filepath.Join(walSegmentsDir(dir), name), raw, 0600))
	_, err = ReadWALEntriesAfter(dir, 0, WALReadDirectory)
	require.ErrorContains(t, err, "overlaps the manifest")
}
