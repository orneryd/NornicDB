package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A manifest read failure is not evidence that no later WAL entries exist.
// Truncation must leave the WAL intact rather than replacing it with an empty log.
func TestTruncateAfterSnapshotRejectsInvalidManifestWithoutChangingWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"before": true}))
	require.NoError(t, wal.Sync())
	before, err := os.ReadFile(walActivePath(dir))
	require.NoError(t, err)
	require.NotEmpty(t, before)
	{
		_, manifestErr := accessWALManifest(dir, walPublishManifest, &WALManifest{
			Version:  walManifestVersion,
			Segments: []WALSegment{{FirstSeq: 1, LastSeq: 1, Path: "../outside.wal"}},
		})
		require.NoError(t, manifestErr)
	}

	err = wal.ManageSnapshot(0, WALSnapshotTruncate)
	require.ErrorContains(t, err, "invalid segment path")
	after, err := os.ReadFile(walActivePath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"after": true}))
}

func TestTruncateAfterSnapshotPreservesUnreadableWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"before": true}))
	require.NoError(t, wal.Sync())
	path := walActivePath(dir)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 9)
	crcOffset := 9 + int(binary.LittleEndian.Uint32(raw[5:9]))
	raw[crcOffset] ^= 0xff
	require.NoError(t, os.WriteFile(path, raw, 0600))

	err = wal.ManageSnapshot(0, WALSnapshotTruncate)
	require.Error(t, err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}

// Segments newer than the snapshot must remain independently recoverable while
// the active WAL is replaced; compaction cannot delete their only durable copy.
func TestTruncateAfterSnapshotPreservesUncoveredSegments(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.MaxEntries = 1
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
	before, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, before.Segments, 2)

	require.NoError(t, wal.ManageSnapshot(1, WALSnapshotTruncate))
	after, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, after.Segments, 1)
	require.Equal(t, before.Segments[1].Path, after.Segments[0].Path)
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(2), entries[0].Sequence)
}

func TestTruncateAfterSnapshotKeepsSegmentSpanningSnapshot(t *testing.T) {
	for _, manifestState := range []string{"current", "missing", "understated range"} {
		t.Run(manifestState, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.MaxEntries = 2
			wal, err := NewWAL(dir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
			manifest, err := accessWALManifest(dir, walReadManifest, nil)
			require.NoError(t, err)
			require.Len(t, manifest.Segments, 1)
			segment := filepath.Join(walSegmentsDir(dir), manifest.Segments[0].Path)
			switch manifestState {
			case "missing":
				require.NoError(t, os.Remove(walManifestPath(dir)))
			case "understated range":
				manifest.Segments[0].LastSeq = 1
				{
					_, manifestErr := accessWALManifest(dir, walPublishManifest, manifest)
					require.NoError(t, manifestErr)
				}
			}
			require.NoError(t, wal.ManageSnapshot(1, WALSnapshotTruncate))
			require.FileExists(t, segment)
			entries, err := ReadWALEntriesAfter(dir, 1, WALReadDirectory)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, uint64(2), entries[0].Sequence)
		})
	}
}

func TestTruncateAfterSnapshotKeepsSegmentsAcrossReplacementFailures(t *testing.T) {
	for _, afterRename := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_rename=%t", afterRename), func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.MaxEntries = 1
			wal, err := NewWAL(dir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
			cfg.MaxEntries = 0
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 3}))
			manifest, err := accessWALManifest(dir, walReadManifest, nil)
			require.NoError(t, err)
			require.Len(t, manifest.Segments, 2)
			uncovered := filepath.Join(walSegmentsDir(dir), manifest.Segments[1].Path)
			failure := errors.New("injected replacement failure")
			wal.truncateReplace = func(from, to string) error {
				if afterRename {
					require.NoError(t, os.Rename(from, to))
				}
				return failure
			}
			err = wal.ManageSnapshot(1, WALSnapshotTruncate)
			require.ErrorIs(t, err, failure)
			require.FileExists(t, uncovered)
			entries, err := ReadWALEntriesAfter(dir, 1, WALReadDirectory)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			require.Equal(t, []uint64{2, 3}, []uint64{entries[0].Sequence, entries[1].Sequence})
		})
	}
}
