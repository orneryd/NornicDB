// Package storage provides WAL compaction tests.
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildAtomicIntegrityPayload(t *testing.T, seq uint64) []byte {
	t.Helper()
	entry := WALEntry{
		Sequence:  seq,
		Operation: OpCreateNode,
		Data:      mustMarshal(WALNodeData{Node: &Node{ID: NodeID(prefixTestID(fmt.Sprintf("ic-%d", seq)))}}),
	}
	entry.Checksum = crc32Checksum(entry.Data)
	payload, err := json.Marshal(entry)
	require.NoError(t, err)
	return payload
}

type snapshotErrorEngine struct {
	*MemoryEngine
	allNodesErr error
	allEdgesErr error
}

func (e *snapshotErrorEngine) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.MemoryEngine.AllNodes()
}

func (e *snapshotErrorEngine) AllEdges() ([]*Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.MemoryEngine.AllEdges()
}

// A snapshot collects stored nodes and relationships (CollectStoredNodes,
// CollectStoredEdges): the fake fails those reads as it fails AllNodes and
// AllEdges.
func (e *snapshotErrorEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(*Node) error) error {
	if e.allNodesErr != nil {
		return e.allNodesErr
	}
	return e.MemoryEngine.StreamNodesWithOptions(ctx, opts, fn)
}

func (e *snapshotErrorEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, fn func(*Edge) error) error {
	if e.allEdgesErr != nil {
		return e.allEdgesErr
	}
	return e.MemoryEngine.StreamStoredEdgesByType(ctx, edgeType, fn)
}

// TestWALCompactionConfig tests WAL configuration options for compaction.

func TestWALCompactionConfig(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("default_snapshot_interval_is_1_hour", func(t *testing.T) {
		cfg := DefaultWALConfig()
		assert.Equal(t, 1*time.Hour, cfg.SnapshotInterval)
	})

	t.Run("snapshot_interval_can_be_configured", func(t *testing.T) {
		cfg := &WALConfig{
			Dir:              t.TempDir(),
			SyncMode:         "none",
			SnapshotInterval: 5 * time.Minute,
		}
		assert.Equal(t, 5*time.Minute, cfg.SnapshotInterval)
	})

	t.Run("max_file_size_defaults_to_100MB", func(t *testing.T) {
		cfg := DefaultWALConfig()
		assert.Equal(t, int64(100*1024*1024), cfg.MaxFileSize)
	})

	t.Run("max_entries_defaults_to_100000", func(t *testing.T) {
		cfg := DefaultWALConfig()
		assert.Equal(t, int64(100000), cfg.MaxEntries)
	})

	t.Run("snapshot_retention_defaults", func(t *testing.T) {
		cfg := DefaultWALConfig()
		assert.Equal(t, 3, cfg.SnapshotRetentionMaxCount)
		assert.Equal(t, time.Duration(0), cfg.SnapshotRetentionMaxAge)
	})
}

func TestCheckWALIntegrity_AtomicBranches(t *testing.T) {
	t.Run("invalid payload size is reported", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		header := make([]byte, 9)
		binary.LittleEndian.PutUint32(header[0:4], walMagic)
		header[4] = walFormatVersion
		binary.LittleEndian.PutUint32(header[5:9], walMaxEntrySize+1)
		require.NoError(t, os.WriteFile(path, header, 0o644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.False(t, report.Healthy)
		require.NotEmpty(t, report.Errors)
	})

	t.Run("truncated payload is reported", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		header := make([]byte, 9)
		binary.LittleEndian.PutUint32(header[0:4], walMagic)
		header[4] = walFormatVersion
		binary.LittleEndian.PutUint32(header[5:9], 32)
		require.NoError(t, os.WriteFile(path, header, 0o644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotEmpty(t, report.Errors)
		assert.Contains(t, strings.Join(report.Errors, " "), "truncated payload")
	})

	t.Run("missing trailer is reported", func(t *testing.T) {
		payload := buildAtomicIntegrityPayload(t, 1)
		var buf bytes.Buffer
		_, err := writeAtomicRecordV2(&buf, payload)
		require.NoError(t, err)
		record := buf.Bytes()
		require.Greater(t, len(record), 8)

		path := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(path, record[:len(record)-8], 0o644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotEmpty(t, report.Errors)
		assert.Contains(t, strings.Join(report.Errors, " "), "missing trailer")
	})

	t.Run("invalid magic after good record is reported", func(t *testing.T) {
		payload := buildAtomicIntegrityPayload(t, 1)
		var buf bytes.Buffer
		_, err := writeAtomicRecordV2(&buf, payload)
		require.NoError(t, err)
		// Append malformed header-like bytes with wrong magic.
		_, err = buf.Write([]byte{0x00, 0x01, 0x02, 0x03, walFormatVersion, 0, 0, 0, 0})
		require.NoError(t, err)

		path := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))

		report, err := CheckWALIntegrity(path)
		require.NoError(t, err)
		require.NotEmpty(t, report.Errors)
		assert.Contains(t, strings.Join(report.Errors, " "), "invalid magic")
	})
}

func TestSnapshotAndSaveSnapshot_ErrorBranches(t *testing.T) {
	t.Run("CreateSnapshot returns ErrWALClosed when closed", func(t *testing.T) {
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		require.NoError(t, wal.Close())
		_, err = wal.CreateSnapshot(newTestMemoryEngine(t))
		require.ErrorIs(t, err, ErrWALClosed)
	})

	t.Run("CreateSnapshot returns wrapped all-nodes error", func(t *testing.T) {
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		defer wal.Close()
		engine := &snapshotErrorEngine{
			MemoryEngine: newTestMemoryEngine(t),
			allNodesErr:  fmt.Errorf("nodes-fail"),
		}
		_, err = wal.CreateSnapshot(engine)
		require.ErrorContains(t, err, "failed to get nodes")
	})

	t.Run("CreateSnapshot returns wrapped all-edges error", func(t *testing.T) {
		wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		defer wal.Close()
		engine := &snapshotErrorEngine{
			MemoryEngine: newTestMemoryEngine(t),
			allEdgesErr:  fmt.Errorf("edges-fail"),
		}
		_, err = wal.CreateSnapshot(engine)
		require.ErrorContains(t, err, "failed to get edges")
	})

	t.Run("SaveSnapshot returns directory creation error", func(t *testing.T) {
		blocked := filepath.Join(t.TempDir(), "blocked-parent")
		require.NoError(t, os.WriteFile(blocked, []byte("file"), 0o644))
		err := SaveSnapshot(&Snapshot{Version: "1.0"}, filepath.Join(blocked, "snapshot.json"))
		require.ErrorContains(t, err, "failed to create snapshot directory")
	})

	t.Run("SaveSnapshot returns file creation error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snapshots", "snapshot.json")
		require.NoError(t, os.MkdirAll(filepath.Join(filepath.Dir(path), filepath.Base(path)+".tmp"), 0o755))
		err := SaveSnapshot(&Snapshot{Version: "1.0"}, path)
		require.ErrorContains(t, err, "failed to create snapshot file")
	})

	t.Run("SaveSnapshot returns rename error when destination path is a directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snapshots", "snapshot.json")
		require.NoError(t, os.MkdirAll(path, 0o755))
		err := SaveSnapshot(&Snapshot{Version: "1.0"}, path)
		require.ErrorContains(t, err, "failed to rename snapshot")
	})
}

// TestPruneOldSnapshotFiles tests snapshot file retention pruning.
func TestPruneOldSnapshotFiles(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	// Create 4 snapshot files (minimal valid JSON)
	content := []byte(`{"Sequence":1,"Timestamp":"2020-01-01T00:00:00Z","Nodes":[],"Edges":[],"Version":1}`)
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("snapshot-2020010%d-120000.json", i)
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, content, 0644))
		// Ensure mtime order: 1 oldest, 4 newest
		require.NoError(t, os.Chtimes(path, time.Now().Add(-time.Duration(4-i)*time.Hour), time.Now().Add(-time.Duration(4-i)*time.Hour)))
	}

	// Keep only 2 most recent
	cfg := &WALConfig{SnapshotRetentionMaxCount: 2}
	err := PruneOldSnapshotFiles(dir, cfg)
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var count int
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "snapshot-") && strings.HasSuffix(e.Name(), ".json") {
			count++
		}
	}
	assert.Equal(t, 2, count, "should keep only 2 snapshots")
}

func TestPruneOldSnapshotFiles_AdditionalBranches(t *testing.T) {
	t.Run("nil config is no-op", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-a.json"), []byte(`{}`), 0644))
		require.NoError(t, PruneOldSnapshotFiles(dir, nil))
	})

	t.Run("max age prunes stale snapshots", func(t *testing.T) {
		dir := t.TempDir()
		oldPath := filepath.Join(dir, "snapshot-old.json")
		newPath := filepath.Join(dir, "snapshot-new.json")
		require.NoError(t, os.WriteFile(oldPath, []byte(`{}`), 0644))
		require.NoError(t, os.WriteFile(newPath, []byte(`{}`), 0644))
		now := time.Now()
		require.NoError(t, os.Chtimes(oldPath, now.Add(-48*time.Hour), now.Add(-48*time.Hour)))
		require.NoError(t, os.Chtimes(newPath, now, now))

		require.NoError(t, PruneOldSnapshotFiles(dir, &WALConfig{
			SnapshotRetentionMaxAge:   24 * time.Hour,
			SnapshotRetentionMaxCount: 10,
		}))

		_, oldErr := os.Stat(oldPath)
		require.True(t, os.IsNotExist(oldErr))
		_, newErr := os.Stat(newPath)
		require.NoError(t, newErr)
	})

	t.Run("non-positive max count keeps all remaining snapshots", func(t *testing.T) {
		dir := t.TempDir()
		for i := 0; i < 3; i++ {
			require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("snapshot-keep-%d.json", i)), []byte(`{}`), 0644))
		}
		require.NoError(t, PruneOldSnapshotFiles(dir, &WALConfig{
			SnapshotRetentionMaxCount: 0,
		}))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 3)
	})
}

// TestWALTruncateAfterSnapshot tests WAL truncation functionality.
func TestWALTruncateAfterSnapshot(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("truncate_removes_entries_before_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Create 100 nodes
		for i := 1; i <= 100; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
			_, err := walEngine.CreateNode(node)
			require.NoError(t, err)
		}

		// Create snapshot
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)

		snapshotPath := filepath.Join(dir, "snapshot.json")
		err = SaveSnapshot(snapshot, snapshotPath)
		require.NoError(t, err)

		// Get WAL size before truncation
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		// Truncate WAL
		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		// WAL should be empty or near-empty (only entries after snapshot)
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		assert.Less(t, sizeAfter, sizeBefore, "WAL should be smaller after truncation")
		assert.Less(t, sizeAfter, int64(1000), "WAL should be nearly empty after truncation")

		walEngine.Close()
	})

	t.Run("truncate_preserves_entries_after_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Create 50 nodes before snapshot
		for i := 1; i <= 50; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
			walEngine.CreateNode(node)
		}

		// Create snapshot at seq 50
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		snapshotSeq := snapshot.Sequence

		// Add 50 more nodes AFTER snapshot
		for i := 51; i <= 100; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
			walEngine.CreateNode(node)
		}

		// Truncate WAL (should keep entries after snapshot)
		err = wal.TruncateAfterSnapshot(snapshotSeq)
		require.NoError(t, err)

		// Read remaining entries
		walPath := filepath.Join(walDir, "wal.log")
		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)

		// Should have ~50 entries (from after snapshot)
		// Plus checkpoint entry, so 51
		assert.GreaterOrEqual(t, len(entries), 50, "Should preserve entries after snapshot")
		assert.LessOrEqual(t, len(entries), 52, "Should not have entries before snapshot")

		walEngine.Close()
	})

	t.Run("truncate_recoverable_after_crash", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// Session 1: Create data, snapshot, and truncate
		func() {
			cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
			wal, err := NewWAL("", cfg)
			require.NoError(t, err)

			engine := newTestMemoryEngine(t)
			walEngine := NewWALEngine(engine, wal)

			// Create nodes
			for i := 1; i <= 100; i++ {
				node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
				walEngine.CreateNode(node)
			}

			// Create and save snapshot
			snapshot, err := wal.CreateSnapshot(engine)
			require.NoError(t, err)

			require.NoError(t, os.MkdirAll(snapshotDir, 0755))
			snapshotPath := filepath.Join(snapshotDir, "snapshot.json")
			require.NoError(t, SaveSnapshot(snapshot, snapshotPath))

			// Truncate WAL
			require.NoError(t, wal.TruncateAfterSnapshot(snapshot.Sequence))

			// Add more nodes after truncation
			for i := 101; i <= 150; i++ {
				node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
				walEngine.CreateNode(node)
			}

			walEngine.Close()
		}()

		// Session 2: Recover from snapshot + truncated WAL
		snapshotPath := filepath.Join(snapshotDir, "snapshot.json")
		recovered, err := RecoverFromWAL(walDir, snapshotPath)
		require.NoError(t, err)
		recoveredNS := NewNamespacedEngine(recovered, "test")

		// Should have all 150 nodes
		count, err := recoveredNS.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(150), count, "Should recover all nodes from snapshot + WAL")

		// Verify specific nodes
		n1, err := recoveredNS.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, n1, "Node 1 should be recovered")

		n150, err := recoveredNS.GetNode("n150")
		assert.NoError(t, err)
		assert.NotNil(t, n150, "Node 150 should be recovered")
	})
}

// TestWALAutoCompactionEnabled tests that auto-compaction is properly enabled.
func TestWALAutoCompactionEnabled(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("enable_auto_compaction_creates_snapshot_directory", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate", SnapshotInterval: 1 * time.Hour}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Snapshot directory should be created
		_, err = os.Stat(snapshotDir)
		require.NoError(t, err, "Snapshot directory should be created")

		walEngine.DisableAutoCompaction()
		walEngine.Close()
	})

	t.Run("cannot_enable_auto_compaction_twice", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate", SnapshotInterval: 1 * time.Hour}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Enable first time - should succeed
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Enable second time - should fail
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already enabled")

		walEngine.DisableAutoCompaction()
		walEngine.Close()
	})

	t.Run("auto_compaction_runs_at_configured_interval", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// Configure very short interval for testing
		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 50 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Add some data before enabling compaction
		for i := 1; i <= 20; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
			walEngine.CreateNode(node)
		}

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Wait for at least 1 compaction cycle (poll to reduce flakiness on slow CI)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			totalSnapshots, lastSnapshot := walEngine.GetSnapshotStats()
			if totalSnapshots >= 1 && !lastSnapshot.IsZero() {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		// Check snapshot stats - at least 1 snapshot should have been created
		totalSnapshots, lastSnapshot := walEngine.GetSnapshotStats()
		assert.GreaterOrEqual(t, totalSnapshots, int64(1), "Should have at least 1 snapshot")
		assert.False(t, lastSnapshot.IsZero(), "Last snapshot time should be set")

		// Check snapshot files were created - at least 1
		deadline = time.Now().Add(2 * time.Second)
		var files []os.DirEntry
		for time.Now().Before(deadline) {
			files, err = os.ReadDir(snapshotDir)
			require.NoError(t, err)
			if len(files) >= 1 {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		assert.GreaterOrEqual(t, len(files), 1, "Should have at least 1 snapshot file")

		walEngine.DisableAutoCompaction()
		walEngine.Close()
	})

	t.Run("disable_auto_compaction_stops_snapshots", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 100 * time.Millisecond, // Longer interval to reduce race window
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Wait for one snapshot cycle to complete
		time.Sleep(150 * time.Millisecond)

		// Disable auto-compaction immediately
		walEngine.DisableAutoCompaction()

		// Get snapshot count right after disable
		countAtDisable, _ := walEngine.GetSnapshotStats()

		// Wait for what would have been more snapshots
		time.Sleep(300 * time.Millisecond)

		// Snapshot count should not have increased significantly
		// (allow 1 extra due to potential race at disable time)
		countAfter, _ := walEngine.GetSnapshotStats()
		assert.LessOrEqual(t, countAfter, countAtDisable+1, "Snapshot count should not increase significantly after disable")

		walEngine.Close()
	})
}

// TestWALCompactionUnderLoad tests compaction behavior under concurrent writes.
func TestWALCompactionUnderLoad(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("compaction_safe_during_concurrent_writes", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 50 * time.Millisecond, // Frequent compaction
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Spawn concurrent writers
		var wg sync.WaitGroup
		writeCount := 100
		writers := 5

		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				for i := 0; i < writeCount; i++ {
					nodeID := fmt.Sprintf("w%d_n%d", writerID, i)
					node := &Node{ID: NodeID(prefixTestID(nodeID)), Labels: []string{"Test"}}
					_, err := walEngine.CreateNode(node)
					if err != nil {
						t.Errorf("Writer %d failed to create node %d: %v", writerID, i, err)
					}
					// Small delay to allow compaction to run
					if i%20 == 0 {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(w)
		}

		wg.Wait()

		walEngine.DisableAutoCompaction()

		// Verify all nodes were created
		for w := 0; w < writers; w++ {
			for i := 0; i < writeCount; i++ {
				nodeID := NodeID(prefixTestID(fmt.Sprintf("w%d_n%d", w, i)))
				node, err := walEngine.GetNode(nodeID)
				assert.NoError(t, err, "Node %s should exist", nodeID)
				assert.NotNil(t, node, "Node %s should not be nil", nodeID)
			}
		}

		totalNodes, err := walEngine.GetInnerEngine().NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(writers*writeCount), totalNodes, "Should have all nodes")

		walEngine.Close()
	})
}

// TestWALCompactionDiskSpace tests that compaction actually saves disk space.
func TestWALCompactionDiskSpace(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("compaction_reduces_wal_size", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 100 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Write a lot of data to grow the WAL
		for i := 1; i <= 500; i++ {
			node := &Node{
				ID:     NodeID(prefixTestID(fmt.Sprintf("n%d", i))),
				Labels: []string{"Test"},
				Properties: map[string]interface{}{
					"name":        fmt.Sprintf("Node %d with some extra content to increase size", i),
					"description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
					"counter":     i,
				},
			}
			walEngine.CreateNode(node)
		}

		// Get WAL size before compaction
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		t.Logf("WAL size before compaction: %d bytes", sizeBefore)

		// Enable auto-compaction and wait for it to run
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Wait for compaction
		time.Sleep(300 * time.Millisecond)

		walEngine.DisableAutoCompaction()

		// Get WAL size after compaction
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		t.Logf("WAL size after compaction: %d bytes", sizeAfter)
		t.Logf("Reduction: %.2f%%", float64(sizeBefore-sizeAfter)/float64(sizeBefore)*100)

		// WAL should be significantly smaller (at least 80% reduction expected)
		assert.Less(t, sizeAfter, sizeBefore/2, "WAL should be at least 50% smaller after compaction")

		// But data should still be intact
		count, err := engine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(500), count, "All nodes should still exist")

		walEngine.Close()
	})
}

// TestWALSnapshotRecovery tests recovery scenarios after compaction.
func TestWALSnapshotRecovery(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("recovery_uses_latest_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// Session 1: Create data with multiple snapshots
		func() {
			cfg := &WALConfig{
				Dir:              walDir,
				SyncMode:         "immediate",
				SnapshotInterval: 50 * time.Millisecond,
			}
			wal, err := NewWAL("", cfg)
			require.NoError(t, err)

			engine := newTestMemoryEngine(t)
			walEngine := NewWALEngine(engine, wal)

			err = walEngine.EnableAutoCompaction(snapshotDir)
			require.NoError(t, err)

			// Create nodes in batches with delays to allow multiple snapshots
			for batch := 0; batch < 5; batch++ {
				for i := 0; i < 20; i++ {
					nodeID := prefixTestID(fmt.Sprintf("b%d_n%d", batch, i))
					node := &Node{ID: NodeID(nodeID), Labels: []string{"Test"}}
					_, err := walEngine.CreateNode(node)
					require.NoError(t, err)
				}
				time.Sleep(80 * time.Millisecond) // Allow snapshot between batches
			}

			walEngine.DisableAutoCompaction()
			walEngine.Close()
		}()

		// Find the latest snapshot (by name; snapshot-YYYYMMDD-HHMMSS.json sorts chronologically)
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(files), 2, "Should have multiple snapshots")
		names := make([]string, 0, len(files))
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".json") && !strings.HasSuffix(f.Name(), ".tmp") {
				names = append(names, f.Name())
			}
		}
		require.GreaterOrEqual(t, len(names), 2, "Should have multiple snapshot files")
		sort.Strings(names)
		latestSnapshot := filepath.Join(snapshotDir, names[len(names)-1])

		// Session 2: Recover
		recovered, err := RecoverFromWAL(walDir, latestSnapshot)
		require.NoError(t, err)

		// Should have all 100 nodes (5 batches x 20 nodes)
		count, err := recovered.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(100), count, "Should recover all nodes")
	})

	t.Run("recovery_without_snapshot_uses_full_wal", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		// Session 1: Create data without snapshots
		func() {
			cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
			wal, err := NewWAL("", cfg)
			require.NoError(t, err)

			engine := newTestMemoryEngine(t)
			walEngine := NewWALEngine(engine, wal)

			for i := 1; i <= 50; i++ {
				node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
				walEngine.CreateNode(node)
			}

			walEngine.Close()
		}()

		// Session 2: Recover without snapshot
		recovered, err := RecoverFromWAL(walDir, "")
		require.NoError(t, err)

		count, err := recovered.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(50), count, "Should recover all nodes from WAL")
	})
}

// TestWALCompactionEdgeCases tests edge cases in compaction.
func TestWALCompactionEdgeCases(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("compaction_with_empty_wal", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 50 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := newTestMemoryEngine(t)
		walEngine := NewWALEngine(engine, wal)

		// Enable auto-compaction on empty WAL
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Wait for compaction cycle
		time.Sleep(100 * time.Millisecond)

		// Should not crash, snapshots might be created (empty ones)
		walEngine.DisableAutoCompaction()
		walEngine.Close()
	})

	t.Run("truncate_closed_wal_returns_error", func(t *testing.T) {
		dir := t.TempDir()

		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		// Close the WAL
		wal.Close()

		// Truncate should return error
		err = wal.TruncateAfterSnapshot(100)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrWALClosed)
	})

	t.Run("checkpoint_creates_marker", func(t *testing.T) {
		dir := t.TempDir()

		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		seqBefore := wal.Sequence()

		// Create checkpoint
		err = wal.Checkpoint()
		require.NoError(t, err)

		seqAfter := wal.Sequence()
		assert.Equal(t, seqBefore+1, seqAfter, "Checkpoint should increment sequence")
	})
}

// BenchmarkWALCompaction benchmarks compaction performance.
func BenchmarkWALCompaction(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	b.Run("truncate_after_snapshot", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()

			dir := b.TempDir()
			cfg := &WALConfig{Dir: dir, SyncMode: "none"}
			wal, _ := NewWAL("", cfg)

			engine := NewMemoryEngine()
			walEngine := NewWALEngine(engine, wal)

			// Create data
			for j := 0; j < 1000; j++ {
				node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", j)))}
				walEngine.CreateNode(node)
			}

			// Create snapshot
			snapshot, _ := wal.CreateSnapshot(engine)

			b.StartTimer()

			// Benchmark truncation
			wal.TruncateAfterSnapshot(snapshot.Sequence)

			b.StopTimer()
			walEngine.Close()
		}
	})

	b.Run("create_snapshot", func(b *testing.B) {
		dir := b.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, _ := NewWAL("", cfg)

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Create test data
		for j := 0; j < 10000; j++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", j)))}
			engine.CreateNode(node)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := wal.CreateSnapshot(engine)
			if err != nil {
				b.Fatal(err)
			}
		}

		walEngine.Close()
	})
}
