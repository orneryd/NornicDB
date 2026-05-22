package nornicdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRecoverBadgerFromSnapshotAndWAL_AllowsWALOnlyWhenNoSnapshots(t *testing.T) {
	// Create a fake "data dir" with ONLY a WAL (no snapshots directory/files).
	dataDir, err := os.MkdirTemp("", "nornicdb-auto-recover-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })

	walDir := filepath.Join(dataDir, "wal")
	wal, err := storage.NewWAL(walDir, nil)
	require.NoError(t, err)

	// Write a simple WAL-only history.
	require.NoError(t, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{
		Node: &storage.Node{
			ID:     storage.NodeID("node-1"), // unprefixed; database is carried in entry.Database
			Labels: []string{"Test"},
			Properties: map[string]any{
				"key": "value",
			},
		},
	}, "test"))
	require.NoError(t, wal.Close())

	// Invoke recovery. This will rename dataDir → dataDir.corrupted-* and rebuild.
	badgerOpts := storage.BadgerOptions{DataDir: dataDir}
	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, badgerOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	// Verify the recovered Badger store contains the node.
	node, err := recovered.GetNode(storage.NodeID("test:node-1"))
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, "value", node.Properties["key"])

	// Sanity: run a small transactional read as well (exercise Badger reads).
	tx, err := recovered.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NoError(t, tx.Rollback())
}

func TestStorageRecoveryHelpers_HeuristicsAndArtifacts(t *testing.T) {
	t.Run("autoRecoverOnCorruptionEnabled env variants", func(t *testing.T) {
		orig, had := os.LookupEnv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION")
		t.Cleanup(func() {
			if had {
				_ = os.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", orig)
			} else {
				_ = os.Unsetenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION")
			}
		})

		_ = os.Unsetenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION")
		require.True(t, autoRecoverOnCorruptionEnabled())

		cases := map[string]bool{
			"1":     true,
			"true":  true,
			"yes":   true,
			"on":    true,
			"0":     false,
			"false": false,
			"off":   false,
		}
		for val, expected := range cases {
			require.NoError(t, os.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", val))
			require.Equal(t, expected, autoRecoverOnCorruptionEnabled(), "env=%s", val)
		}
	})

	t.Run("looksLikeCorruption matches known signatures", func(t *testing.T) {
		require.False(t, looksLikeCorruption(nil))
		require.True(t, looksLikeCorruption(fmt.Errorf("manifest checksum mismatch")))
		require.True(t, looksLikeCorruption(fmt.Errorf("badger value log truncate required")))
		require.True(t, looksLikeCorruption(fmt.Errorf("schema: rebuild unique values: decode node: property key id 179 not in dictionary for namespace %q", "nornic")))
		require.False(t, looksLikeCorruption(fmt.Errorf("permission denied")))
	})

	t.Run("latestSnapshotPath picks newest snapshot", func(t *testing.T) {
		dir := t.TempDir()
		old := filepath.Join(dir, "snapshot-old.json")
		newer := filepath.Join(dir, "snapshot-new.json")
		require.NoError(t, os.WriteFile(old, []byte("{}"), 0644))
		require.NoError(t, os.WriteFile(newer, []byte("{}"), 0644))

		past := time.Now().Add(-2 * time.Hour)
		require.NoError(t, os.Chtimes(old, past, past))
		got, err := latestSnapshotPath(dir)
		require.NoError(t, err)
		require.Equal(t, newer, got)
	})

	t.Run("hasRecoverableArtifacts from snapshots and wal files", func(t *testing.T) {
		base := t.TempDir()
		require.False(t, hasRecoverableArtifacts(base))

		snapDir := filepath.Join(base, "snapshots")
		require.NoError(t, os.MkdirAll(snapDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot-1.json"), []byte("{}"), 0644))
		require.True(t, hasRecoverableArtifacts(base))

		base2 := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(base2, "wal"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(base2, "wal", "wal.log"), []byte("x"), 0644))
		require.True(t, hasRecoverableArtifacts(base2))

		base3 := t.TempDir()
		segDir := filepath.Join(base3, "wal", "segments")
		require.NoError(t, os.MkdirAll(segDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(segDir, "seg-1-2.wal"), []byte("x"), 0644))
		require.True(t, hasRecoverableArtifacts(base3))
	})
}

func TestRecoverBadgerFromSnapshotAndWAL_HandlesEmptyRecoveryInputs(t *testing.T) {
	dataDir := t.TempDir()
	opts := storage.BadgerOptions{DataDir: dataDir}
	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, opts)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.NotEmpty(t, backupDir)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	nodes, err := recovered.AllNodes()
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestRecoverBadgerFromSnapshotAndWAL_SnapshotAndWALReplay(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)

	mem := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(mem, "test")
	_, err = ns.CreateNode(&storage.Node{
		ID:     "seed-1",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "from-snapshot",
		},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{
		ID:     "seed-2",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "from-snapshot-2",
		},
	})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&storage.Edge{
		ID:        "edge-1",
		StartNode: "seed-1",
		EndNode:   "seed-2",
		Type:      "LINK",
	}))

	snapshot, err := wal.CreateSnapshot(mem)
	require.NoError(t, err)
	snapshotPath := filepath.Join(snapshotDir, "snapshot-current.json")
	require.NoError(t, storage.SaveSnapshot(snapshot, snapshotPath))

	// Post-snapshot write must be replayed from WAL.
	require.NoError(t, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{
		Node: &storage.Node{
			ID:     "after-1",
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"content": "from-wal",
			},
		},
	}, "test"))
	require.NoError(t, wal.Close())

	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, storage.BadgerOptions{DataDir: dataDir})
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.NotEmpty(t, backupDir)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	snapNode, err := recovered.GetNode(storage.NodeID("test:seed-1"))
	require.NoError(t, err)
	require.Equal(t, "from-snapshot", snapNode.Properties["content"])

	walNode, err := recovered.GetNode(storage.NodeID("test:after-1"))
	require.NoError(t, err)
	require.Equal(t, "from-wal", walNode.Properties["content"])

	edge, err := recovered.GetEdge(storage.EdgeID("test:edge-1"))
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("test:seed-1"), edge.StartNode)
	require.Equal(t, storage.NodeID("test:seed-2"), edge.EndNode)
}

func TestRecoverBadgerFromSnapshotAndWAL_RestoresLargeNodeSetInChunks(t *testing.T) {
	dataDir := t.TempDir()
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	const nodeCount = 12000
	nodes := make([]*storage.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("test:node-%03d", i)),
			Labels: []string{"Recovered"},
			Properties: map[string]any{
				"payload": strings.Repeat("x", 128),
			},
		})
	}
	require.NoError(t, storage.SaveSnapshot(&storage.Snapshot{
		Sequence:  1,
		Timestamp: time.Now(),
		Nodes:     nodes,
		Version:   "1.0",
	}, filepath.Join(snapshotDir, "snapshot-current.json")))

	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, storage.BadgerOptions{
		DataDir:   dataDir,
		LowMemory: true,
	})
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.NotEmpty(t, backupDir)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	recoveredNodes, err := recovered.AllNodes()
	require.NoError(t, err)
	require.Len(t, recoveredNodes, nodeCount)

	node, err := recovered.GetNode(storage.NodeID("test:node-047"))
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("x", 128), node.Properties["payload"])
}

func TestRecoverBadgerFromSnapshotAndWAL_RestoresLargeEdgeSetInChunks(t *testing.T) {
	dataDir := t.TempDir()
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))

	nodes := []*storage.Node{
		{ID: "test:start", Labels: []string{"Recovered"}},
		{ID: "test:end", Labels: []string{"Recovered"}},
	}
	const edgeCount = 12000
	edges := make([]*storage.Edge, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges = append(edges, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("test:edge-%05d", i)),
			StartNode: "test:start",
			EndNode:   "test:end",
			Type:      "RECOVERED",
			Properties: map[string]any{
				"payload": strings.Repeat("x", 128),
			},
		})
	}
	require.NoError(t, storage.SaveSnapshot(&storage.Snapshot{
		Sequence:  1,
		Timestamp: time.Now(),
		Nodes:     nodes,
		Edges:     edges,
		Version:   "1.0",
	}, filepath.Join(snapshotDir, "snapshot-current.json")))

	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, storage.BadgerOptions{
		DataDir:   dataDir,
		LowMemory: true,
	})
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.NotEmpty(t, backupDir)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	recoveredEdges, err := recovered.AllEdges()
	require.NoError(t, err)
	require.Len(t, recoveredEdges, edgeCount)

	edge, err := recovered.GetEdge(storage.EdgeID("test:edge-00047"))
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("test:start"), edge.StartNode)
	require.Equal(t, storage.NodeID("test:end"), edge.EndNode)
	require.Equal(t, strings.Repeat("x", 128), edge.Properties["payload"])
}

func TestRecoverBadgerFromSnapshotAndWAL_WALReplayError(t *testing.T) {
	// Use a file path as dataDir so walDir cannot be opened as a proper directory.
	filePath := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0644))

	opts := storage.BadgerOptions{DataDir: filePath}
	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(filePath, opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wal replay failed")
	require.Nil(t, recovered)
	require.Empty(t, backupDir)
}

func TestRecoverBadgerFromSnapshotAndWAL_PreserveDirRenameFailure(t *testing.T) {
	// Missing directory makes the preserve step (os.Rename) fail after replay stage.
	dataDir := filepath.Join(t.TempDir(), "missing-data-dir")
	opts := storage.BadgerOptions{DataDir: dataDir}
	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to preserve corrupted data dir")
	require.Nil(t, recovered)
	require.Empty(t, backupDir)
}
