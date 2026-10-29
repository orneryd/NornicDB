package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALEntriesFromDirWithSegments(t *testing.T) {
	cleanup := config.WithWALEnabled()
	defer cleanup()

	dir := t.TempDir()
	cfg := &WALConfig{
		Dir:         dir,
		SyncMode:    "none",
		MaxEntries:  2,
		MaxFileSize: 0,
	}

	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		node := &Node{ID: NodeID(fmt.Sprintf("test:%d", i))}
		_, err := wal.AppendWithDatabaseReturningSeq(OpCreateNode, WALNodeData{
			Node: node,
			TxID: "tx-seg",
		}, "test")
		require.NoError(t, err)
	}

	require.NoError(t, wal.Close())

	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 5)

	for i := 1; i < len(entries); i++ {
		if entries[i].Sequence <= entries[i-1].Sequence {
			t.Fatalf("expected ascending sequences, got %d then %d", entries[i-1].Sequence, entries[i].Sequence)
		}
	}

	matches, err := FindWALEntriesByTxID(dir, "tx-seg", 0)
	require.NoError(t, err)
	require.Len(t, matches, 5)
}

func TestReadWALEntriesFromDir_RejectsManifestPathTraversal(t *testing.T) {
	dir := t.TempDir()

	manifest := &WALManifest{
		Version: walManifestVersion,
		Segments: []WALSegment{
			{
				FirstSeq: 1,
				LastSeq:  1,
				Path:     "../wal.log",
			},
		},
	}
	require.NoError(t, writeWALManifest(dir, manifest))

	_, err := ReadWALEntriesFromDir(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid segment path")
}

func TestWALManifestHelpers(t *testing.T) {
	t.Run("load missing manifest returns empty default", func(t *testing.T) {
		manifest, err := loadWALManifest(t.TempDir())
		require.NoError(t, err)
		require.Equal(t, walManifestVersion, manifest.Version)
		require.Empty(t, manifest.Segments)
	})

	t.Run("write and load sorts segments and backfills version", func(t *testing.T) {
		dir := t.TempDir()
		manifest := &WALManifest{
			Segments: []WALSegment{
				{FirstSeq: 20, LastSeq: 30, Path: "seg-20-30.wal"},
				{FirstSeq: 1, LastSeq: 19, Path: "seg-1-19.wal"},
			},
		}

		require.NoError(t, writeWALManifest(dir, manifest))
		require.Equal(t, walManifestVersion, manifest.Version)

		loaded, err := loadWALManifest(dir)
		require.NoError(t, err)
		require.Equal(t, walManifestVersion, loaded.Version)
		require.Len(t, loaded.Segments, 2)
		require.Equal(t, uint64(1), loaded.Segments[0].FirstSeq)
		require.Equal(t, uint64(20), loaded.Segments[1].FirstSeq)
	})

	t.Run("write rejects nil manifest", func(t *testing.T) {
		err := writeWALManifest(t.TempDir(), nil)
		require.ErrorContains(t, err, "manifest is nil")
	})

	t.Run("load returns decode error for malformed manifest", func(t *testing.T) {
		dir := t.TempDir()
		segmentsDir := walSegmentsDir(dir)
		require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
		require.NoError(t, os.WriteFile(walManifestPath(dir), []byte("{bad-json"), 0o644))

		_, err := loadWALManifest(dir)
		require.Error(t, err)
	})

	t.Run("write returns mkdir error when segments path is a file", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(walSegmentsDir(dir), []byte("not-dir"), 0o644))
		err := writeWALManifest(dir, &WALManifest{Version: walManifestVersion})
		require.Error(t, err)
	})

	t.Run("write returns rename error when manifest path is a directory", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(walManifestPath(dir), 0o755))
		err := writeWALManifest(dir, &WALManifest{Version: walManifestVersion})
		require.Error(t, err)
	})
}

func TestWALSegmentPathHelpers(t *testing.T) {
	t.Run("scan segments ignores invalid files and sorts valid ones", func(t *testing.T) {
		dir := t.TempDir()
		segmentDir := walSegmentsDir(dir)
		require.NoError(t, os.MkdirAll(segmentDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "seg-10-12.wal"), []byte("a"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "seg-1-9.wal"), []byte("abc"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "not-a-segment.txt"), []byte("skip"), 0o644))
		require.NoError(t, os.Mkdir(filepath.Join(segmentDir, "nested"), 0o755))

		segments, err := scanSegmentsFromDir(dir)
		require.NoError(t, err)
		require.Len(t, segments, 2)
		require.Equal(t, uint64(1), segments[0].FirstSeq)
		require.Equal(t, int64(3), segments[0].SizeBytes)
		require.Equal(t, "seg-10-12.wal", segments[1].Path)
	})

	t.Run("missing segment dir returns nil slice", func(t *testing.T) {
		segments, err := scanSegmentsFromDir(t.TempDir())
		require.NoError(t, err)
		require.Nil(t, segments)
	})

	t.Run("parse and resolve segment names validates inputs", func(t *testing.T) {
		first, last, ok := parseSegmentFilename("seg-7-9.wal")
		require.True(t, ok)
		require.Equal(t, uint64(7), first)
		require.Equal(t, uint64(9), last)

		_, _, ok = parseSegmentFilename("bad-name.wal")
		require.False(t, ok)

		dir := t.TempDir()
		resolved, err := resolveWALSegmentPath(dir, "seg-7-9.wal")
		require.NoError(t, err)
		require.Equal(t, filepath.Join(walSegmentsDir(dir), "seg-7-9.wal"), resolved)

		for _, name := range []string{"", "..", "../wal.log", "nested/seg-1-2.wal", "bad-name.wal"} {
			_, err := resolveWALSegmentPath(dir, name)
			require.Error(t, err, name)
		}

		_, err = resolveWALSegmentPath(dir, filepath.Join(dir, "seg-1-2.wal"))
		require.ErrorContains(t, err, "absolute")
	})
}

func TestReadWALEntriesFromDir_FallsBackToSegmentScan(t *testing.T) {
	cleanup := config.WithWALEnabled()
	defer cleanup()

	dir := t.TempDir()
	cfg := &WALConfig{
		Dir:         dir,
		SyncMode:    "none",
		MaxEntries:  2,
		MaxFileSize: 0,
	}

	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := wal.AppendWithDatabaseReturningSeq(OpCreateNode, WALNodeData{
			Node: &Node{ID: NodeID(fmt.Sprintf("scan:%d", i))},
			TxID: "tx-scan",
		}, "test")
		require.NoError(t, err)
	}
	require.NoError(t, wal.Close())

	require.NoError(t, os.Remove(walManifestPath(dir)))

	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 5)
}

func TestWALEntryRangeAndTxIDQueries(t *testing.T) {
	cleanup := config.WithWALEnabled()
	defer cleanup()

	dir := t.TempDir()
	cfg := &WALConfig{
		Dir:         dir,
		SyncMode:    "none",
		MaxEntries:  2,
		MaxFileSize: 0,
	}

	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		txID := "tx-odd"
		if i%2 == 0 {
			txID = "tx-even"
		}
		_, err := wal.AppendWithDatabaseReturningSeq(OpCreateNode, WALNodeData{
			Node: &Node{ID: NodeID(fmt.Sprintf("query:%d", i))},
			TxID: txID,
		}, "test")
		require.NoError(t, err)
	}
	require.NoError(t, wal.Close())

	after, err := ReadWALEntriesAfter(dir, 3, WALReadDirectory)
	require.NoError(t, err)
	require.Len(t, after, 3)
	require.Equal(t, uint64(4), after[0].Sequence)

	inRange, err := ReadWALEntriesRangeFromDir(dir, 2, 4)
	require.NoError(t, err)
	require.Len(t, inRange, 3)
	require.Equal(t, uint64(2), inRange[0].Sequence)
	require.Equal(t, uint64(4), inRange[2].Sequence)

	openEnded, err := ReadWALEntriesRangeFromDir(dir, 5, 0)
	require.NoError(t, err)
	require.Len(t, openEnded, 2)
	require.Equal(t, uint64(5), openEnded[0].Sequence)

	matches, err := FindWALEntriesByTxID(dir, "tx-even", 2)
	require.NoError(t, err)
	require.Len(t, matches, 2)
	for _, entry := range matches {
		require.Equal(t, "tx-even", GetEntryTxID(entry))
	}

	_, err = FindWALEntriesByTxID(dir, "", 1)
	require.ErrorContains(t, err, "tx_id is required")
}
