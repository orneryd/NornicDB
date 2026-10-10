package storage

import (
	"encoding/json"
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
		require.NoError(t, json.Unmarshal(entry.Data, &data))
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
	walEngine := NewWALEngine(NewMemoryEngine(), wal)
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
		require.NoError(t, walEngine.createSnapshotAndCompact())
		createWALNodes(t, walEngine, 4, 5)
		require.NoError(t, walEngine.createSnapshotAndCompact())
		createWALNodes(t, walEngine, 6, 6)
		require.Equal(t, nodeIDRange(1, 6), createdNodeIDs(t, wal.config.Dir))

		paths, err := filepath.Glob(filepath.Join(walEngine.snapshotDir, "snapshot-*.json"))
		require.NoError(t, err)
		sort.Strings(paths)
		recovered, result, err := RecoverFromWALWithResult(wal.config.Dir, paths[len(paths)-1])
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
		require.NoError(t, walEngine.createSnapshotAndCompact())
		createWALNodes(t, walEngine, 3, 4)
		require.NoError(t, walEngine.createSnapshotAndCompact())
		createWALNodes(t, walEngine, 5, 5)
		require.NoError(t, walEngine.createSnapshotAndCompact())
		// The first two compactions' segments are covered by the last
		// snapshot; one is kept, the newest.
		require.Equal(t, nodeIDRange(5, 5), createdNodeIDs(t, wal.config.Dir))
		manifest, err := loadWALManifest(wal.config.Dir)
		require.NoError(t, err)
		require.Len(t, manifest.Segments, 1)
	})

	t.Run("nothing sealed yet keeps nothing to remove", func(t *testing.T) {
		_, wal := newRetentionWALEngine(t, t.TempDir(), time.Hour, 0)
		require.NoError(t, wal.TruncateAfterSnapshot(1))
		manifest, err := loadWALManifest(wal.config.Dir)
		require.NoError(t, err)
		require.Empty(t, manifest.Segments)
	})

	t.Run("a segment that can't be sealed fails the truncation", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, time.Hour, 0)
		createWALNodes(t, walEngine, 1, 1)
		// A file where the segments directory goes.
		require.NoError(t, os.RemoveAll(walSegmentsDir(wal.config.Dir)))
		require.NoError(t, os.WriteFile(walSegmentsDir(wal.config.Dir), []byte("x"), 0o644))
		require.ErrorContains(t, wal.TruncateAfterSnapshot(wal.sequence.Load()), "failed to seal segment before retention")
	})

	t.Run("without retention covered entries are dropped", func(t *testing.T) {
		root := t.TempDir()
		walEngine, wal := newRetentionWALEngine(t, root, 0, 0)
		createWALNodes(t, walEngine, 1, 3)
		require.NoError(t, walEngine.createSnapshotAndCompact())
		createWALNodes(t, walEngine, 4, 4)
		require.Equal(t, nodeIDRange(4, 4), createdNodeIDs(t, wal.config.Dir))
	})
}
