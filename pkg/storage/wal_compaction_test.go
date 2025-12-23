// Package storage provides WAL compaction tests.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

		engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
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

			engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Add some data before enabling compaction
		for i := 1; i <= 20; i++ {
			node := &Node{ID: NodeID(prefixTestID(fmt.Sprintf("n%d", i))), Labels: []string{"Test"}}
			walEngine.CreateNode(node)
		}

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)

		// Wait for at least 1 compaction cycle (be more lenient with timing)
		time.Sleep(200 * time.Millisecond)

		// Check snapshot stats - at least 1 snapshot should have been created
		totalSnapshots, lastSnapshot := walEngine.GetSnapshotStats()
		assert.GreaterOrEqual(t, totalSnapshots, int64(1), "Should have at least 1 snapshot")
		assert.False(t, lastSnapshot.IsZero(), "Last snapshot time should be set")

		// Check snapshot files were created - at least 1
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
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

		engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
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

		totalNodes, err := walEngine.GetEngine().NodeCount()
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

		engine := NewMemoryEngine()
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

			engine := NewMemoryEngine()
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

		// Find the latest snapshot
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(files), 2, "Should have multiple snapshots")

		// Use the latest snapshot (sorted by time, last entry)
		latestSnapshot := filepath.Join(snapshotDir, files[len(files)-1].Name())

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

			engine := NewMemoryEngine()
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

		engine := NewMemoryEngine()
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
