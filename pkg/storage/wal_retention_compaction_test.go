package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// createdNodeIDs lists the node IDs (as the WAL records them, without the
// database prefix) of the create_node entries readable in the WAL directory
// (sealed segments and the active WAL), as db.txlog reads them.
func createdNodeIDs(t *testing.T, walDir string) []string {
	t.Helper()
	var ids []string
	require.NoError(t, VisitWALEntriesAfterFromDir(walDir, 0, func(entry WALEntry) error {
		if entry.Operation != OpCreateNode {
			return nil
		}
		var data WALNodeData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		ids = append(ids, string(data.Node.ID))
		return nil
	}))
	sort.Strings(ids)
	return ids
}

// newRetentionWALEngine opens a WAL engine whose WAL has the given retention,
// with snapshots under root/snapshots as auto-compaction saves them.
func newRetentionWALEngine(t *testing.T, root string, maxAge time.Duration, maxSegments int) (*WALEngine, *WAL) {
	t.Helper()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate", RetentionMaxAge: maxAge, RetentionMaxSegments: maxSegments})
	require.NoError(t, err)
	walEngine := NewWALEngine(newTestMemoryEngine(t), wal)
	walEngine.snapshotDir = filepath.Join(root, "snapshots")
	t.Cleanup(func() { require.NoError(t, walEngine.Close()) })
	return walEngine, wal
}

func createWALNodes(t *testing.T, walEngine *WALEngine, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		_, err := walEngine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("nornic:n%02d", i)), Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
}

func nodeIDRange(from, to int) []string {
	ids := make([]string, 0, to-from+1)
	for i := from; i <= to; i++ {
		ids = append(ids, fmt.Sprintf("n%02d", i))
	}
	return ids
}

// Auto-compaction keeps the entries a snapshot covers when WAL retention is
// configured (NORNICDB_WAL_RETENTION_MAX_AGE / _MAX_SEGMENTS): they are sealed
// into segments, readable for the retention period, and recovery from the
// latest snapshot still replays only what came after it. Without retention
// the covered entries are dropped, as before.
func TestAutoCompactionHonoursWALRetention(t *testing.T) {
	defer config.WithWALEnabled()()

	t.Run("max age keeps every entry inside the window", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, time.Hour, 0)
		createWALNodes(t, walEngine, 1, 3)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		createWALNodes(t, walEngine, 4, 5)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		createWALNodes(t, walEngine, 6, 6)
		require.Equal(t, nodeIDRange(1, 6), createdNodeIDs(t, wal.config.Dir))

		paths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
		require.NoError(t, err)
		sort.Strings(paths)
		recovered, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(wal.config.Dir, paths[len(paths)-1]))
		require.NoError(t, err)
		nodes, err := recovered.AllNodes()
		require.NoError(t, err)
		require.Len(t, nodes, 6)
		require.Zero(t, result.Failed)
	})

	t.Run("max segments keeps the newest covered segments", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, 0, 1)
		createWALNodes(t, walEngine, 1, 2)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		createWALNodes(t, walEngine, 3, 4)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		createWALNodes(t, walEngine, 5, 5)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		// The first two compactions' segments are covered by the last
		// snapshot; one is kept, the newest.
		require.Equal(t, nodeIDRange(5, 5), createdNodeIDs(t, wal.config.Dir))
		manifest, err := accessWALManifest(wal.config.Dir, walReadManifest, nil)
		require.NoError(t, err)
		require.Len(t, manifest.Segments, 1)
	})

	t.Run("nothing sealed yet keeps nothing to remove", func(t *testing.T) {
		_, wal := newRetentionWALEngine(t, t.TempDir(), time.Hour, 0)
		require.NoError(t, wal.ManageSnapshot(1, WALSnapshotTruncate))
		manifest, err := accessWALManifest(wal.config.Dir, walReadManifest, nil)
		require.NoError(t, err)
		require.Empty(t, manifest.Segments)
	})

	t.Run("an invalid segments path fails before sealing", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, time.Hour, 0)
		createWALNodes(t, walEngine, 1, 1)
		// A file where the segments directory goes.
		require.NoError(t, os.RemoveAll(walSegmentsDir(wal.config.Dir)))
		require.NoError(t, os.WriteFile(walSegmentsDir(wal.config.Dir), []byte("x"), 0o644))
		require.ErrorContains(t, wal.ManageSnapshot(wal.sequence.Load(), WALSnapshotTruncate), "cannot truncate before validating WAL")
	})

	t.Run("without retention covered entries are dropped", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, 0, 0)
		createWALNodes(t, walEngine, 1, 3)
		require.NoError(t, walEngine.createSnapshotAndCompact(walEngine.work.Context()))
		createWALNodes(t, walEngine, 4, 4)
		require.Equal(t, nodeIDRange(4, 4), createdNodeIDs(t, wal.config.Dir))
	})
}

// Standalone retention runs the segment-retention algorithm only. Before any
// segment is sealed it must leave the active WAL and the manifest untouched.
func TestStandaloneRetentionLeavesUnsealedWALUnchanged(t *testing.T) {
	defer config.WithWALEnabled()()
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(t.TempDir(), "wal"), SyncMode: "immediate", RetentionMaxAge: time.Nanosecond, RetentionMaxSegments: 1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 2}))
	require.NoError(t, wal.Sync())

	dir := wal.config.Dir
	before, err := os.ReadFile(walActivePath(dir))
	require.NoError(t, err)
	require.NotEmpty(t, before)
	_, manifestErrBefore := os.Stat(walManifestPath(dir))
	require.True(t, os.IsNotExist(manifestErrBefore), "a fresh WAL publishes no segment manifest")
	entriesBefore, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entriesBefore, 2)

	require.NoError(t, wal.ManageSnapshot(wal.Sequence(), WALSnapshotRetention))

	after, err := os.ReadFile(walActivePath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after, "standalone retention must not rewrite the active WAL")
	_, manifestErrAfter := os.Stat(walManifestPath(dir))
	require.True(t, os.IsNotExist(manifestErrAfter), "standalone retention must not publish a seal")
	sealed, globErr := filepath.Glob(filepath.Join(walSegmentsDir(dir), "seg-*.wal"))
	require.NoError(t, globErr)
	require.Empty(t, sealed, "standalone retention must not rotate the active segment")
	entriesAfter, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entriesAfter, 2)
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 3}))
}

func TestManageSnapshotRejectsUnknownModeWithoutChangingWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := filepath.Join(t.TempDir(), "wal")
	wal, err := NewWAL("", &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
	wal.mu.Lock()
	err = wal.rotateSegmentLocked(wal.Sequence())
	wal.mu.Unlock()
	require.NoError(t, err)
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 1)
	segment := filepath.Join(walSegmentsDir(dir), manifest.Segments[0].Path)
	before, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)

	err = wal.ManageSnapshot(wal.Sequence(), WALSnapshotMode(255))
	require.ErrorContains(t, err, "invalid WAL snapshot mode")
	after, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.FileExists(t, segment)
}

func TestManageSnapshotReturnsClosedWhenCloseWinsBeforeLock(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, mode := range []WALSnapshotMode{WALSnapshotTruncate, WALSnapshotRetention} {
		t.Run(mode.String(), func(t *testing.T) {
			wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(t.TempDir(), "wal"), SyncMode: "immediate"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
			entered, release := make(chan struct{}), make(chan struct{})
			wal.beforeSnapshotLock = func() { close(entered); <-release }
			result := make(chan error, 1)
			go func() { result <- wal.ManageSnapshot(wal.Sequence(), mode) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				close(release)
				_ = wal.Close()
				t.Fatal("snapshot did not reach the lock boundary")
			}
			closeErr := wal.Close()
			close(release)
			require.NoError(t, closeErr)
			require.ErrorIs(t, <-result, ErrWALClosed)
		})
	}
}
