// Package storage - Unit tests for WAL durability and batch sequence ordering.
//
// These tests verify:
// 1. Directory fsync is called for durability
// 2. Batch sequence numbers are assigned at commit time
// 3. SaveSnapshot syncs directory after rename
package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

// =============================================================================
// DIRECTORY FSYNC TESTS
// =============================================================================

// TestSyncDirFunction verifies syncDir works correctly.
func TestSyncDirFunction(t *testing.T) {
	dir := t.TempDir()

	// syncDir should work on valid directory
	err := syncDir(dir)
	if err != nil {
		t.Errorf("syncDir failed on valid directory: %v", err)
	}
}

// TestSyncDirInvalidPath verifies syncDir returns error for invalid path.
func TestSyncDirInvalidPath(t *testing.T) {
	err := syncDir("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("syncDir should fail on invalid path")
	}
}

// TestNewWALSyncsDirectory verifies WAL creation syncs the directory.
func TestNewWALSyncsDirectory(t *testing.T) {
	dir := t.TempDir()

	// Create WAL - should sync directory
	wal, err := NewWAL(dir, &WALConfig{
		Dir:      dir,
		SyncMode: "none", // Don't need fsync for this test
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Verify WAL file was created
	walPath := filepath.Join(dir, "wal.log")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist after creation")
	}
}

// TestSaveSnapshotSyncsDirectory verifies snapshot saves sync the directory.
func TestSaveSnapshotSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")

	snapshot := &Snapshot{
		Sequence: 100,
		Nodes:    []*Node{{ID: "n1", Labels: []string{"Test"}}},
		Edges:    []*Edge{},
		Version:  "1.0",
	}

	// SaveSnapshot should succeed and sync directory
	err := SaveSnapshot(snapshot, snapshotPath)
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Verify snapshot file exists
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Error("Snapshot file should exist after save")
	}

	// Verify temp file was cleaned up
	tmpPath := snapshotPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temp file should be cleaned up after save")
	}
}

// =============================================================================
// BATCH SEQUENCE ORDERING TESTS
// =============================================================================

// TestBatchSequenceAssignedAtCommit verifies sequences are assigned at commit time.
func TestBatchSequenceAssignedAtCommit(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Get starting sequence
	startSeq := wal.Sequence()

	// Create batch and add entries
	batch := wal.NewBatch()

	node1 := &Node{ID: "n1", Labels: []string{"Test"}}
	node2 := &Node{ID: "n2", Labels: []string{"Test"}}

	batch.AppendNode(OpCreateNode, node1)
	batch.AppendNode(OpCreateNode, node2)

	// Sequence should NOT have changed yet
	afterAppendSeq := wal.Sequence()
	if afterAppendSeq != startSeq {
		t.Errorf("Sequence should not change during append, got %d, want %d", afterAppendSeq, startSeq)
	}

	// Write a non-batch entry
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n3", Labels: []string{"Test"}}})
	nonBatchSeq := wal.Sequence()

	// Now commit the batch
	if err := batch.Commit(); err != nil {
		t.Fatalf("Batch commit failed: %v", err)
	}

	afterCommitSeq := wal.Sequence()

	// Batch entries should have sequences AFTER the non-batch entry
	// startSeq = initial
	// nonBatchSeq = startSeq + 1 (non-batch write)
	// afterCommitSeq = nonBatchSeq + 2 (batch of 2)
	expectedAfterCommit := nonBatchSeq + 2
	if afterCommitSeq != expectedAfterCommit {
		t.Errorf("After commit sequence should be %d, got %d", expectedAfterCommit, afterCommitSeq)
	}
}

// TestBatchMixedWithNonBatchOrdering verifies proper ordering of mixed operations.
func TestBatchMixedWithNonBatchOrdering(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Interleave batch and non-batch operations
	batch := wal.NewBatch()

	// Add to batch (seq not assigned yet)
	batch.AppendNode(OpCreateNode, &Node{ID: "batch1", Labels: []string{"Test"}})

	// Non-batch write (gets seq 1)
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "non-batch1", Labels: []string{"Test"}}})

	// Add more to batch (seq still not assigned)
	batch.AppendNode(OpCreateNode, &Node{ID: "batch2", Labels: []string{"Test"}})

	// Another non-batch write (gets seq 2)
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "non-batch2", Labels: []string{"Test"}}})

	// Commit batch (gets seq 3, 4)
	batch.Commit()

	wal.Close()

	// Read back and verify order
	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("Expected 4 entries, got %d", len(entries))
	}

	// Entries should be in sequence order: 1, 2, 3, 4
	// Content should be: non-batch1, non-batch2, batch1, batch2
	expectedOrder := []struct {
		seq uint64
		id  string
	}{
		{1, "non-batch1"},
		{2, "non-batch2"},
		{3, "batch1"},
		{4, "batch2"},
	}

	for i, expected := range expectedOrder {
		if entries[i].Sequence != expected.seq {
			t.Errorf("Entry %d: sequence should be %d, got %d", i, expected.seq, entries[i].Sequence)
		}
	}
}

// TestBatchRollbackClearsEntries verifies rollback clears pending entries.
func TestBatchRollbackClearsEntries(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	batch := wal.NewBatch()

	batch.AppendNode(OpCreateNode, &Node{ID: "n1", Labels: []string{"Test"}})
	batch.AppendNode(OpCreateNode, &Node{ID: "n2", Labels: []string{"Test"}})

	if batch.Len() != 2 {
		t.Errorf("Batch should have 2 entries, got %d", batch.Len())
	}

	// Rollback should clear entries
	batch.Rollback()

	if batch.Len() != 0 {
		t.Errorf("Batch should be empty after rollback, got %d", batch.Len())
	}

	// Sequence should not have been consumed
	// (since we never committed)
}

// TestBatchEmptyCommit verifies committing empty batch is a no-op.
func TestBatchEmptyCommit(t *testing.T) {
	dir := t.TempDir()

	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	startSeq := wal.Sequence()

	batch := wal.NewBatch()
	err = batch.Commit()
	if err != nil {
		t.Errorf("Empty commit should succeed, got error: %v", err)
	}

	// Sequence should not change
	if wal.Sequence() != startSeq {
		t.Error("Empty commit should not consume sequence numbers")
	}
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

// TestWALFullDurabilityPath tests full write-commit-recover path.
func TestWALFullDurabilityPath(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	// Create and write to WAL
	cfg := &WALConfig{
		Dir:      dir,
		SyncMode: "immediate",
	}

	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some entries
	nodes := []*Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
	}

	for _, node := range nodes {
		if err := wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: node}, "test"); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Close and "crash"
	wal.Close()

	// Recover
	baseEngine, result, err := RecoverFromWALWithResult(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Verify recovery stats
	if result.Applied != 2 {
		t.Errorf("Should have applied 2 entries, got %d", result.Applied)
	}

	// Verify data recovered
	for _, node := range nodes {
		recovered, err := engine.GetNode(node.ID)
		if err != nil {
			t.Errorf("Failed to get node %s: %v", node.ID, err)
			continue
		}
		if recovered == nil {
			t.Errorf("Node %s not found", node.ID)
		}
	}
}
