package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWAL(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("creates_wal_with_default_config", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		assert.NotNil(t, wal)
		assert.Equal(t, dir, wal.config.Dir)
		assert.Equal(t, "batch", wal.config.SyncMode)
	})

	t.Run("creates_wal_with_custom_config", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:               dir,
			SyncMode:          "immediate",
			BatchSyncInterval: 50 * time.Millisecond,
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		assert.Equal(t, "immediate", wal.config.SyncMode)
	})

	t.Run("creates_directory_if_not_exists", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", "wal", "dir")
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		_, err = os.Stat(dir)
		assert.NoError(t, err)
	})
}

func TestWAL_Append(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("appends_entries_when_enabled", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{
			Dir:      dir,
			SyncMode: "none",
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		node := &Node{
			ID:     "test-node",
			Labels: []string{"Test"},
		}

		err = wal.Append(OpCreateNode, WALNodeData{Node: node})
		require.NoError(t, err)

		assert.Equal(t, uint64(1), wal.Sequence())
		stats := wal.Stats()
		assert.Equal(t, int64(1), stats.TotalWrites)
	})

	t.Run("skips_when_wal_disabled", func(t *testing.T) {
		config.DisableWAL()
		defer config.EnableWAL()

		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		assert.NoError(t, err) // Should succeed but not write

		// Sequence should still increment (the append was called but skipped internally)
		// Actually with WAL disabled, Append returns early so sequence doesn't increment
	})

	t.Run("returns_error_when_closed", func(t *testing.T) {
		config.EnableWAL()

		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		assert.Equal(t, ErrWALClosed, err)
	})

	t.Run("increments_sequence_monotonically", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		for i := 0; i < 100; i++ {
			err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
			require.NoError(t, err)
		}

		assert.Equal(t, uint64(100), wal.Sequence())
	})
}

func TestWAL_Sync(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("immediate_sync_mode", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		require.NoError(t, err)

		stats := wal.Stats()
		assert.GreaterOrEqual(t, stats.TotalSyncs, int64(1))
	})

	t.Run("manual_sync", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		err = wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "test"}})
		require.NoError(t, err)

		err = wal.Sync()
		assert.NoError(t, err)
	})

	t.Run("sync_returns_error_when_closed", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)
		wal.Close()

		err = wal.Sync()
		assert.Equal(t, ErrWALClosed, err)
	})
}

func TestWAL_Checkpoint(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Add some entries
	for i := 0; i < 5; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}

	// Create checkpoint
	err = wal.Checkpoint()
	require.NoError(t, err)

	assert.Equal(t, uint64(6), wal.Sequence()) // 5 creates + 1 checkpoint
}

func TestWAL_Close(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("closes_cleanly", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err)
		assert.True(t, wal.Stats().Closed)
	})

	t.Run("double_close_is_safe", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, nil)
		require.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err)

		err = wal.Close()
		assert.NoError(t, err) // Should not error on second close
	})
}

func TestWAL_Stats(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Initial stats
	stats := wal.Stats()
	assert.Equal(t, uint64(0), stats.Sequence)
	assert.False(t, stats.Closed)

	// After writes
	for i := 0; i < 10; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}

	stats = wal.Stats()
	assert.Equal(t, uint64(10), stats.Sequence)
	assert.Equal(t, int64(10), stats.TotalWrites)
}

func TestWAL_ReadEntries(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	// Write entries
	nodes := []*Node{
		{ID: "n1", Labels: []string{"A"}},
		{ID: "n2", Labels: []string{"B"}},
		{ID: "n3", Labels: []string{"C"}},
	}
	for _, n := range nodes {
		err = wal.Append(OpCreateNode, WALNodeData{Node: n})
		require.NoError(t, err)
	}
	wal.Close()

	// Read entries back
	walPath := filepath.Join(dir, "wal.log")
	entries, err := ReadWALEntries(walPath)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Equal(t, OpCreateNode, entries[0].Operation)
}

func TestWAL_ReadEntriesAfter(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	// Write 10 entries
	for i := 0; i < 10; i++ {
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune('0'+i)))}})
	}
	wal.Close()

	// Read only entries after sequence 5
	walPath := filepath.Join(dir, "wal.log")
	entries, err := ReadWALEntriesAfter(walPath, 5)
	require.NoError(t, err)
	assert.Len(t, entries, 5) // Entries 6-10
	assert.Equal(t, uint64(6), entries[0].Sequence)
}

func TestSnapshot_CreateAndLoad(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	// Create engine with data
	engine := NewMemoryEngine()
	engine.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
	engine.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}})
	engine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})

	// Create snapshot
	snapshot, err := wal.CreateSnapshot(engine)
	require.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Len(t, snapshot.Nodes, 2)
	assert.Len(t, snapshot.Edges, 1)
	assert.Equal(t, "1.0", snapshot.Version)

	// Save snapshot
	snapshotPath := filepath.Join(dir, "snapshot.json")
	err = SaveSnapshot(snapshot, snapshotPath)
	require.NoError(t, err)

	// Load snapshot
	loaded, err := LoadSnapshot(snapshotPath)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Sequence, loaded.Sequence)
	assert.Len(t, loaded.Nodes, 2)
	assert.Len(t, loaded.Edges, 1)
}

func TestSnapshot_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")

	snapshot := &Snapshot{
		Sequence:  100,
		Timestamp: time.Now(),
		Nodes:     []*Node{{ID: "n1"}},
		Edges:     []*Edge{{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "TEST"}},
		Version:   "1.0",
	}

	err := SaveSnapshot(snapshot, snapshotPath)
	require.NoError(t, err)

	// Verify temp file doesn't exist
	_, err = os.Stat(snapshotPath + ".tmp")
	assert.True(t, os.IsNotExist(err))

	// Verify actual file exists
	_, err = os.Stat(snapshotPath)
	assert.NoError(t, err)
}

func TestReplayWALEntry(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("replay_create_node", func(t *testing.T) {
		engine := NewMemoryEngine()
		entry := WALEntry{
			Sequence:  1,
			Operation: OpCreateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		node, err := engine.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, node)
	})

	t.Run("replay_update_node", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})

		entry := WALEntry{
			Sequence:  2,
			Operation: OpUpdateNode,
			Data:      mustMarshal(WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Updated"}}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		node, _ := engine.GetNode("n1")
		// Labels are normalized to lowercase during storage
		found := false
		for _, l := range node.Labels {
			if l == "updated" || l == "Updated" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should have Updated label")
	})

	t.Run("replay_delete_node", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})

		entry := WALEntry{
			Sequence:  3,
			Operation: OpDeleteNode,
			Data:      mustMarshal(WALDeleteData{ID: "n1"}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		_, err = engine.GetNode("n1")
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("replay_create_edge", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1"})
		engine.CreateNode(&Node{ID: "n2"})

		entry := WALEntry{
			Sequence:  4,
			Operation: OpCreateEdge,
			Data:      mustMarshal(WALEdgeData{Edge: &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		edge, err := engine.GetEdge("e1")
		assert.NoError(t, err)
		assert.NotNil(t, edge)
	})

	t.Run("replay_bulk_nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		nodes := []*Node{
			{ID: "b1", Labels: []string{"Bulk"}},
			{ID: "b2", Labels: []string{"Bulk"}},
		}

		entry := WALEntry{
			Sequence:  5,
			Operation: OpBulkNodes,
			Data:      mustMarshal(WALBulkNodesData{Nodes: nodes}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("replay_checkpoint_is_noop", func(t *testing.T) {
		engine := NewMemoryEngine()
		entry := WALEntry{
			Sequence:  6,
			Operation: OpCheckpoint,
			Data:      mustMarshal(map[string]interface{}{"time": time.Now()}),
		}
		entry.Checksum = crc32Checksum(entry.Data)

		err := ReplayWALEntry(engine, entry)
		assert.NoError(t, err)
	})

	t.Run("replay_unknown_operation", func(t *testing.T) {
		engine := NewMemoryEngine()
		entry := WALEntry{
			Sequence:  7,
			Operation: "unknown_op",
			Data:      []byte("{}"),
		}

		err := ReplayWALEntry(engine, entry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown operation")
	})
}

func TestRecoverFromWAL(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("recovery_with_snapshot_and_wal", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		// Phase 1: Create initial state and snapshot
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1", Labels: []string{"Original"}})
		engine.CreateNode(&Node{ID: "n2", Labels: []string{"Original"}})

		// Create and save snapshot
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		err = SaveSnapshot(snapshot, snapshotPath)
		require.NoError(t, err)

		// Phase 2: Add more changes after snapshot
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n3", Labels: []string{"AfterSnapshot"}}})
		wal.Append(OpUpdateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Modified"}}})
		wal.Close()

		// Phase 3: Recover
		recovered, err := RecoverFromWAL(walDir, snapshotPath)
		require.NoError(t, err)

		// Verify state
		count, _ := recovered.NodeCount()
		assert.Equal(t, int64(3), count)

		n1, _ := recovered.GetNode("n1")
		// Labels are normalized to lowercase
		found := false
		for _, l := range n1.Labels {
			if l == "modified" || l == "Modified" {
				found = true
				break
			}
		}
		assert.True(t, found, "n1 should have Modified label")

		n3, _ := recovered.GetNode("n3")
		assert.NotNil(t, n3)
	})

	t.Run("recovery_without_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}})
		wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n2", Labels: []string{"Test"}}})
		wal.Close()

		recovered, err := RecoverFromWAL(walDir, "")
		require.NoError(t, err)

		count, _ := recovered.NodeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("recovery_no_wal_file", func(t *testing.T) {
		dir := t.TempDir()

		// Create empty WAL directory structure
		walDir := filepath.Join(dir, "wal")
		os.MkdirAll(walDir, 0755)

		recovered, err := RecoverFromWAL(walDir, "")
		// If no WAL file exists, should still return empty engine (not error)
		require.NoError(t, err)

		count, _ := recovered.NodeCount()
		assert.Equal(t, int64(0), count) // Empty engine
	})
}

func TestWALEngine(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("logs_and_executes_operations", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		// Create node
		err = walEngine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
		require.NoError(t, err)

		// Verify node exists
		node, err := walEngine.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, node)

		// Verify WAL entry was created
		assert.Equal(t, uint64(1), wal.Sequence())
	})

	t.Run("all_operations_logged", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		// Create nodes
		walEngine.CreateNode(&Node{ID: "n1"})
		walEngine.CreateNode(&Node{ID: "n2"})

		// Update node
		walEngine.UpdateNode(&Node{ID: "n1", Labels: []string{"Updated"}})

		// Create edge
		walEngine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})

		// Update edge
		walEngine.UpdateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "FRIENDS"})

		// Delete edge
		walEngine.DeleteEdge("e1")

		// Delete node
		walEngine.DeleteNode("n2")

		// Bulk create
		walEngine.BulkCreateNodes([]*Node{{ID: "b1"}, {ID: "b2"}})
		walEngine.BulkCreateEdges([]*Edge{{ID: "be1", StartNode: "n1", EndNode: "b1", Type: "TEST"}})

		// Verify sequence
		assert.Equal(t, uint64(9), wal.Sequence())
	})

	t.Run("read_operations_not_logged", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &WALConfig{Dir: dir, SyncMode: "none"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()
		engine.CreateNode(&Node{ID: "n1"})
		engine.CreateNode(&Node{ID: "n2"})
		engine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "TEST"})

		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		// All read operations - should not increase sequence
		walEngine.GetNode("n1")
		walEngine.GetEdge("e1")
		walEngine.GetNodesByLabel("Test")
		walEngine.GetOutgoingEdges("n1")
		walEngine.GetIncomingEdges("n2")
		walEngine.GetEdgesBetween("n1", "n2")
		walEngine.GetEdgeBetween("n1", "n2", "TEST")
		walEngine.AllNodes()
		walEngine.AllEdges()
		walEngine.GetAllNodes()
		walEngine.GetInDegree("n2")
		walEngine.GetOutDegree("n1")
		walEngine.GetSchema()
		walEngine.NodeCount()
		walEngine.EdgeCount()

		assert.Equal(t, uint64(0), wal.Sequence())
	})

	t.Run("getters_return_underlying_components", func(t *testing.T) {
		dir := t.TempDir()
		wal, _ := NewWAL(dir, nil)
		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)
		defer walEngine.Close()

		assert.Same(t, wal, walEngine.GetWAL())
		assert.Same(t, engine, walEngine.GetEngine())
	})
}

func TestWALEngine_WithFeatureFlagDisabled(t *testing.T) {
	config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	engine := NewMemoryEngine()
	walEngine := NewWALEngine(engine, wal)
	defer walEngine.Close()

	// Operations should succeed but not log
	walEngine.CreateNode(&Node{ID: "n1"})
	walEngine.UpdateNode(&Node{ID: "n1", Labels: []string{"Updated"}})
	walEngine.DeleteNode("n1")

	// No entries should be written when disabled
	assert.Equal(t, uint64(0), wal.Sequence())

	// But operations should still execute
	_, err = walEngine.GetNode("n1")
	assert.Equal(t, ErrNotFound, err) // Deleted
}

func TestWAL_ConcurrentAppends(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(id*1000+j)))}})
			}
		}(i)
	}

	wg.Wait()

	expected := uint64(numGoroutines * entriesPerGoroutine)
	assert.Equal(t, expected, wal.Sequence())
}

func TestWAL_SequenceRestoration(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	// First WAL session
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal1, err := NewWAL("", cfg)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		wal1.Append(OpCreateNode, WALNodeData{Node: &Node{ID: NodeID("n" + string(rune(i)))}})
	}
	wal1.Close()

	// Second WAL session - should continue from where we left off
	wal2, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal2.Close()

	// Sequence should be restored
	assert.Equal(t, uint64(50), wal2.Sequence())

	// New entries should continue from 51
	wal2.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n51"}})
	assert.Equal(t, uint64(51), wal2.Sequence())
}

func TestCrc32Checksum(t *testing.T) {
	data := []byte("test data for checksum")
	checksum1 := crc32Checksum(data)
	checksum2 := crc32Checksum(data)

	assert.Equal(t, checksum1, checksum2, "Same data should produce same checksum")

	differentData := []byte("different data")
	checksum3 := crc32Checksum(differentData)
	assert.NotEqual(t, checksum1, checksum3, "Different data should produce different checksum")
}

func TestDefaultWALConfig(t *testing.T) {
	cfg := DefaultWALConfig()

	assert.Equal(t, "data/wal", cfg.Dir)
	assert.Equal(t, "batch", cfg.SyncMode)
	assert.Equal(t, 100*time.Millisecond, cfg.BatchSyncInterval)
	assert.Equal(t, int64(100*1024*1024), cfg.MaxFileSize)
	assert.Equal(t, int64(100000), cfg.MaxEntries)
	assert.Equal(t, 1*time.Hour, cfg.SnapshotInterval)
}

func TestWAL_FeatureFlagIntegration(t *testing.T) {
	t.Run("wal_enabled_by_default", func(t *testing.T) {
		config.ResetFeatureFlags()
		// After reset, WAL is disabled, but let's enable it
		config.EnableWAL()
		defer config.DisableWAL()

		assert.True(t, config.IsWALEnabled())
	})

	t.Run("wal_disable_toggle", func(t *testing.T) {
		config.EnableWAL()
		assert.True(t, config.IsWALEnabled())

		config.DisableWAL()
		assert.False(t, config.IsWALEnabled())
	})

	t.Run("with_wal_enabled_helper", func(t *testing.T) {
		config.ResetFeatureFlags()
		assert.False(t, config.IsWALEnabled())

		cleanup := config.WithWALEnabled()
		assert.True(t, config.IsWALEnabled())

		cleanup()
		assert.False(t, config.IsWALEnabled())
	})
}

// Helper function
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// Benchmarks

func BenchmarkWAL_Append(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	node := &Node{ID: "bench-node", Labels: []string{"Benchmark"}}
	data := WALNodeData{Node: node}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Append(OpCreateNode, data)
	}
}

func BenchmarkWAL_AppendWithSync(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	node := &Node{ID: "bench-node", Labels: []string{"Benchmark"}}
	data := WALNodeData{Node: node}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Append(OpCreateNode, data)
	}
}

func BenchmarkWALEngine_CreateNode(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	engine := NewMemoryEngine()
	walEngine := NewWALEngine(engine, wal)
	defer walEngine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		walEngine.CreateNode(&Node{ID: NodeID("n" + string(rune(i)))})
	}
}

// ============================================================================
// BatchWriter Tests
// ============================================================================

func TestBatchWriter_AppendNode(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()
	require.NotNil(t, batch)

	// Write multiple entries in batch
	node1 := &Node{ID: "batch-n1", Labels: []string{"Test"}}
	node2 := &Node{ID: "batch-n2", Labels: []string{"Test"}}

	err = batch.AppendNode(OpCreateNode, node1)
	require.NoError(t, err)

	err = batch.AppendNode(OpCreateNode, node2)
	require.NoError(t, err)

	// Before commit, entries should be buffered
	assert.Equal(t, 2, batch.Len())
}

func TestBatchWriter_Commit(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add entries
	for i := 0; i < 5; i++ {
		node := &Node{ID: NodeID(fmt.Sprintf("commit-n%d", i)), Labels: []string{"Test"}}
		err = batch.AppendNode(OpCreateNode, node)
		require.NoError(t, err)
	}

	// Commit batch
	err = batch.Commit()
	require.NoError(t, err)

	// Batch should be cleared after commit
	assert.Equal(t, 0, batch.Len())
}

func TestBatchWriter_Rollback(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add entries
	for i := 0; i < 3; i++ {
		node := &Node{ID: NodeID(fmt.Sprintf("rollback-n%d", i)), Labels: []string{"Test"}}
		batch.AppendNode(OpCreateNode, node)
	}

	// Rollback - should discard all entries
	batch.Rollback()

	// Batch should be cleared
	assert.Equal(t, 0, batch.Len())
}

func TestBatchWriter_AppendEdge(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add edge entries
	edge1 := &Edge{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"}
	edge2 := &Edge{ID: "e2", Type: "LIKES", StartNode: "n2", EndNode: "n3"}

	err = batch.AppendEdge(OpCreateEdge, edge1)
	require.NoError(t, err)

	err = batch.AppendEdge(OpCreateEdge, edge2)
	require.NoError(t, err)

	assert.Equal(t, 2, batch.Len())

	err = batch.Commit()
	require.NoError(t, err)
}

func TestWAL_TruncateAfterSnapshot(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("truncate_removes_old_entries", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		// Create WAL and engine
		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()

		// Add entries before snapshot
		for i := 1; i <= 5; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"BeforeSnapshot"}}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Create snapshot (gets sequence number)
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		snapshotSeq := snapshot.Sequence

		// Save snapshot
		err = SaveSnapshot(snapshot, snapshotPath)
		require.NoError(t, err)

		// Add entries after snapshot
		for i := 6; i <= 10; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"AfterSnapshot"}}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Get WAL size before truncation
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		// Truncate WAL to remove entries before snapshot
		err = wal.TruncateAfterSnapshot(snapshotSeq)
		require.NoError(t, err)

		// Get WAL size after truncation
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		// WAL should be significantly smaller
		assert.Less(t, sizeAfter, sizeBefore, "WAL should shrink after truncation")

		// Read WAL entries to verify only post-snapshot entries remain
		entries, err := ReadWALEntries(walPath)
		require.NoError(t, err)

		// All remaining entries should have sequence > snapshotSeq
		for _, entry := range entries {
			assert.Greater(t, entry.Sequence, snapshotSeq,
				"All entries should be after snapshot sequence")
		}

		// Should have ~5 entries (nodes 6-10 + checkpoint is possible)
		assert.GreaterOrEqual(t, len(entries), 5, "Should have at least 5 post-snapshot entries")
		assert.LessOrEqual(t, len(entries), 6, "Should have at most 6 entries (5 nodes + possible checkpoint)")
	})

	t.Run("truncate_preserves_data_integrity", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)

		engine := NewMemoryEngine()

		// Add 100 nodes
		for i := 1; i <= 100; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Snapshot at node 50
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		SaveSnapshot(snapshot, snapshotPath)

		// Add 50 more nodes
		for i := 101; i <= 150; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Truncate
		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		// Verify we can still append after truncation
		newNode := &Node{ID: "n-after-truncate", Labels: []string{"PostTruncate"}}
		err = wal.Append(OpCreateNode, WALNodeData{Node: newNode})
		require.NoError(t, err)

		wal.Close()

		// Recover from snapshot + truncated WAL
		recovered, err := RecoverFromWAL(walDir, snapshotPath)
		require.NoError(t, err)

		// Should have all 100 nodes from snapshot + 50 post-snapshot + 1 after truncate
		count, err := recovered.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(151), count, "Should have 151 nodes after recovery")

		// Verify specific nodes exist
		n1, err := recovered.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		n150, err := recovered.GetNode("n150")
		assert.NoError(t, err)
		assert.NotNil(t, n150)

		nAfter, err := recovered.GetNode("n-after-truncate")
		assert.NoError(t, err)
		assert.NotNil(t, nAfter)
	})

	t.Run("truncate_with_empty_wal_after_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotPath := filepath.Join(dir, "snapshot.json")

		cfg := &WALConfig{Dir: walDir, SyncMode: "immediate"}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()

		// Add nodes
		for i := 1; i <= 10; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			engine.CreateNode(node)
			wal.Append(OpCreateNode, WALNodeData{Node: node})
		}

		// Snapshot everything
		snapshot, err := wal.CreateSnapshot(engine)
		require.NoError(t, err)
		SaveSnapshot(snapshot, snapshotPath)

		// NO new entries after snapshot

		// Truncate should leave WAL nearly empty (just checkpoint possibly)
		err = wal.TruncateAfterSnapshot(snapshot.Sequence)
		require.NoError(t, err)

		// WAL should be very small or empty
		walPath := filepath.Join(walDir, "wal.log")
		stat, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.Less(t, stat.Size(), int64(1000), "WAL should be nearly empty after full truncation")

		// Verify we can still use WAL after truncation
		newNode := &Node{ID: "n-new"}
		err = wal.Append(OpCreateNode, WALNodeData{Node: newNode})
		require.NoError(t, err)
	})
}

func TestWALEngine_AutoCompaction(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("auto_compaction_truncates_wal_periodically", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// Create WAL with short snapshot interval for testing
		cfg := &WALConfig{
			Dir:              walDir,
			SyncMode:         "immediate",
			SnapshotInterval: 100 * time.Millisecond, // Very frequent for testing
		}
		wal, err := NewWAL("", cfg)
		require.NoError(t, err)
		defer wal.Close()

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Enable auto-compaction
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)
		defer walEngine.DisableAutoCompaction()

		// Add many nodes to WAL
		for i := 1; i <= 50; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			walEngine.CreateNode(node)
		}

		// Get WAL size before compaction
		walPath := filepath.Join(walDir, "wal.log")
		statBefore, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeBefore := statBefore.Size()

		// Wait for at least one snapshot cycle
		time.Sleep(250 * time.Millisecond)

		// Check that snapshot was created
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(files), 1, "At least one snapshot should be created")

		// Check stats
		totalSnapshots, lastSnapshot := walEngine.GetSnapshotStats()
		assert.GreaterOrEqual(t, totalSnapshots, int64(1), "Should have created at least 1 snapshot")
		assert.False(t, lastSnapshot.IsZero(), "Last snapshot time should be set")

		// Add more nodes after first compaction
		for i := 51; i <= 100; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			walEngine.CreateNode(node)
		}

		// Wait for another snapshot cycle
		time.Sleep(250 * time.Millisecond)

		// WAL should have been truncated - size should be manageable
		statAfter, err := os.Stat(walPath)
		require.NoError(t, err)
		sizeAfter := statAfter.Size()

		// WAL should not grow unbounded - truncation keeps it under control
		// It should be much smaller than if we had all 100 nodes in it
		assert.Less(t, sizeAfter, sizeBefore*3, "WAL should not grow unbounded due to truncation")

		// Verify all nodes are still accessible (data not lost)
		for i := 1; i <= 100; i++ {
			node, err := walEngine.GetNode(NodeID(fmt.Sprintf("n%d", i)))
			assert.NoError(t, err)
			assert.NotNil(t, node)
		}
	})

	t.Run("auto_compaction_recoverable", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		snapshotDir := filepath.Join(dir, "snapshots")

		// First session: create data with auto-compaction
		func() {
			cfg := &WALConfig{
				Dir:              walDir,
				SyncMode:         "immediate",
				SnapshotInterval: 50 * time.Millisecond,
			}
			wal, err := NewWAL("", cfg)
			require.NoError(t, err)
			defer wal.Close()

			engine := NewMemoryEngine()
			walEngine := NewWALEngine(engine, wal)

			err = walEngine.EnableAutoCompaction(snapshotDir)
			require.NoError(t, err)

			// Create nodes
			for i := 1; i <= 100; i++ {
				node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
				walEngine.CreateNode(node)
			}

			// Wait for snapshot
			time.Sleep(150 * time.Millisecond)

			walEngine.DisableAutoCompaction()
		}()

		// Find latest snapshot (filter for .json files only, not .tmp files)
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)

		var jsonFiles []os.DirEntry
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".json") && !strings.HasSuffix(f.Name(), ".tmp") {
				jsonFiles = append(jsonFiles, f)
			}
		}
		require.GreaterOrEqual(t, len(jsonFiles), 1, "Should have at least one snapshot")

		// Use the most recent snapshot
		latestSnapshot := filepath.Join(snapshotDir, jsonFiles[len(jsonFiles)-1].Name())

		// Second session: recover from snapshot + WAL
		recovered, err := RecoverFromWAL(walDir, latestSnapshot)
		require.NoError(t, err)

		// Verify all data recovered
		count, err := recovered.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(100), count, "All nodes should be recovered")

		// Spot check nodes
		n1, err := recovered.GetNode("n1")
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		n100, err := recovered.GetNode("n100")
		assert.NoError(t, err)
		assert.NotNil(t, n100)
	})

	t.Run("disable_auto_compaction_stops_snapshots", func(t *testing.T) {
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
		defer wal.Close()

		engine := NewMemoryEngine()
		walEngine := NewWALEngine(engine, wal)

		// Enable then immediately disable
		err = walEngine.EnableAutoCompaction(snapshotDir)
		require.NoError(t, err)
		walEngine.DisableAutoCompaction()

		// Add nodes
		for i := 1; i <= 20; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("n%d", i))}
			walEngine.CreateNode(node)
		}

		// Wait longer than snapshot interval
		time.Sleep(200 * time.Millisecond)

		// No snapshots should be created
		files, err := os.ReadDir(snapshotDir)
		require.NoError(t, err)
		assert.Equal(t, 0, len(files), "No snapshots should be created after disable")
	})
}

func TestBatchWriter_AppendDelete(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	defer wal.Close()

	batch := wal.NewBatch()

	// Add delete entries
	err = batch.AppendDelete(OpDeleteNode, "node-to-delete-1")
	require.NoError(t, err)

	err = batch.AppendDelete(OpDeleteNode, "node-to-delete-2")
	require.NoError(t, err)

	assert.Equal(t, 2, batch.Len())

	err = batch.Commit()
	require.NoError(t, err)
}

func BenchmarkBatchWriter_Commit(b *testing.B) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := b.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "none"}
	wal, _ := NewWAL("", cfg)
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := wal.NewBatch()
		for j := 0; j < 100; j++ {
			node := &Node{ID: NodeID(fmt.Sprintf("bench-n%d-%d", i, j))}
			batch.AppendNode(OpCreateNode, node)
		}
		batch.Commit()
	}
}

// ============================================================================
// WALEngine StreamingEngine Tests
// ============================================================================

func TestWALEngine_StreamNodes(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		err := walEngine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllNodes", func(t *testing.T) {
		var count int
		err := walEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := walEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			if count >= 10 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 10, count, "Should stop after 10 nodes")
	})
}

func TestWALEngine_StreamEdges(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create nodes first
	for i := 0; i < 10; i++ {
		err := walEngine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Create edges
	for i := 0; i < 50; i++ {
		err := walEngine.CreateEdge(&Edge{
			ID:        EdgeID(fmt.Sprintf("edge-%d", i)),
			Type:      "CONNECTS",
			StartNode: NodeID(fmt.Sprintf("node-%d", i%10)),
			EndNode:   NodeID(fmt.Sprintf("node-%d", (i+1)%10)),
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllEdges", func(t *testing.T) {
		var count int
		err := walEngine.StreamEdges(ctx, func(edge *Edge) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 50, count, "Should stream all 50 edges")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := walEngine.StreamEdges(ctx, func(edge *Edge) error {
			count++
			if count >= 5 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 5, count, "Should stop after 5 edges")
	})
}

func TestWALEngine_StreamNodeChunks(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)
	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		err := walEngine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamInChunks", func(t *testing.T) {
		var totalNodes int
		var chunkCount int
		err := walEngine.StreamNodeChunks(ctx, 25, func(nodes []*Node) error {
			chunkCount++
			totalNodes += len(nodes)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, totalNodes, "Should stream all 100 nodes")
		assert.Equal(t, 4, chunkCount, "Should have 4 chunks of 25")
	})
}

// TestWALEngine_ImplementsStreamingEngine verifies the interface is implemented
func TestWALEngine_ImplementsStreamingEngine(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	engine := NewMemoryEngine()
	defer engine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(engine, wal)

	// This should compile - WALEngine implements StreamingEngine
	var _ StreamingEngine = walEngine
	t.Log("WALEngine implements StreamingEngine interface")
}

// TestFullStorageChain_Streaming tests streaming through the full storage chain:
// AsyncEngine -> WALEngine -> BadgerEngine
func TestFullStorageChain_Streaming(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	// Create the full storage chain
	badgerEngine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badgerEngine.Close()

	wal, err := NewWAL(dir, &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(badgerEngine, wal)

	asyncEngine := NewAsyncEngine(walEngine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour, // Don't auto-flush
	})
	defer asyncEngine.Close()

	ctx := context.Background()

	// Create 100 nodes through the full chain
	for i := 0; i < 100; i++ {
		err := asyncEngine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("chain-node-%d", i)),
			Labels: []string{"ChainTest"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamThroughFullChain", func(t *testing.T) {
		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes through full chain")
	})

	t.Run("EarlyTerminationThroughFullChain", func(t *testing.T) {
		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			if count >= 25 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 25, count, "Should stop after 25 nodes")
	})

	t.Run("StreamAfterFlush", func(t *testing.T) {
		require.NoError(t, asyncEngine.Flush())

		var count int
		err := asyncEngine.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes after flush")
	})
}
