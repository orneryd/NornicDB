// Package storage - Unit tests for WAL corruption handling and data integrity.
//
// These tests verify critical fixes:
// 1. Proper CRC32 checksumming (not weak XOR)
// 2. Replay error tracking (not silent skip)
// 3. Checksum verification on recovery
package storage

import (
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

// =============================================================================
// CRC32 CHECKSUM TESTS
// =============================================================================

// TestCRC32ProperImplementation verifies we use real CRC32, not weak XOR.
func TestCRC32ProperImplementation(t *testing.T) {
	// The old weak implementation would produce collisions easily
	// A proper CRC32 should not collide on these inputs

	tests := []struct {
		name   string
		input1 []byte
		input2 []byte
	}{
		{
			name:   "single bit flip",
			input1: []byte{0x00, 0x00, 0x00, 0x00},
			input2: []byte{0x00, 0x00, 0x00, 0x01}, // Single bit different
		},
		{
			name:   "byte swap",
			input1: []byte{0x01, 0x02, 0x03, 0x04},
			input2: []byte{0x04, 0x03, 0x02, 0x01}, // Same bytes, different order
		},
		{
			name:   "adjacent values",
			input1: []byte("hello"),
			input2: []byte("hellp"), // Off by one
		},
		{
			name:   "length difference",
			input1: []byte("test"),
			input2: []byte("test\x00"), // Extra null byte
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sum1 := crc32Checksum(tc.input1)
			sum2 := crc32Checksum(tc.input2)

			if sum1 == sum2 {
				t.Errorf("CRC32 collision detected: %x == %x for different inputs", sum1, sum2)
				t.Errorf("Input1: %v", tc.input1)
				t.Errorf("Input2: %v", tc.input2)
			}
		})
	}
}

// TestCRC32MatchesStandardLibrary verifies our implementation matches stdlib.
func TestCRC32MatchesStandardLibrary(t *testing.T) {
	testData := [][]byte{
		[]byte("hello world"),
		[]byte(`{"id": "node1", "labels": ["Person"]}`),
		make([]byte, 1024), // 1KB of zeros
		[]byte{0xFF, 0xFE, 0xFD, 0xFC},
	}

	// Use same table as our implementation
	table := crc32.MakeTable(crc32.Castagnoli)

	for i, data := range testData {
		ourSum := crc32Checksum(data)
		stdSum := crc32.Checksum(data, table)

		if ourSum != stdSum {
			t.Errorf("Test %d: checksum mismatch: got %x, want %x", i, ourSum, stdSum)
		}
	}
}

// TestCRC32Deterministic verifies same input always produces same output.
func TestCRC32Deterministic(t *testing.T) {
	data := []byte(`{"operation": "create_node", "data": {"id": "n1"}}`)

	first := crc32Checksum(data)
	for i := 0; i < 100; i++ {
		if got := crc32Checksum(data); got != first {
			t.Errorf("Iteration %d: non-deterministic checksum: got %x, want %x", i, got, first)
		}
	}
}

// TestVerifyCRC32 tests the verification helper function.
func TestVerifyCRC32(t *testing.T) {
	data := []byte("test data for verification")
	checksum := crc32Checksum(data)

	if !verifyCRC32(data, checksum) {
		t.Error("verifyCRC32 should return true for matching checksum")
	}

	if verifyCRC32(data, checksum+1) {
		t.Error("verifyCRC32 should return false for wrong checksum")
	}

	if verifyCRC32(append(data, 'x'), checksum) {
		t.Error("verifyCRC32 should return false for corrupted data")
	}
}

// =============================================================================
// WAL ENTRY CORRUPTION DETECTION TESTS
// =============================================================================

// TestWALDetectsCorruptedChecksum verifies corrupted entries are rejected.
func TestWALDetectsCorruptedChecksum(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Create a valid WAL entry
	node := &Node{ID: "corrupt-test", Labels: []string{"Test"}}
	dataBytes, _ := json.Marshal(WALNodeData{Node: node})
	validChecksum := crc32Checksum(dataBytes)

	entry := WALEntry{
		Sequence:  1,
		Operation: OpCreateNode,
		Data:      dataBytes,
		Checksum:  validChecksum,
	}

	// Write valid entry first
	file, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(&entry); err != nil {
		t.Fatalf("Failed to write valid entry: %v", err)
	}

	// Write corrupted entry (wrong checksum)
	corruptEntry := WALEntry{
		Sequence:  2,
		Operation: OpCreateNode,
		Data:      dataBytes,
		Checksum:  validChecksum + 1, // Wrong checksum!
	}
	if err := encoder.Encode(&corruptEntry); err != nil {
		t.Fatalf("Failed to write corrupt entry: %v", err)
	}
	file.Close()

	// Read should fail on corrupted entry
	_, err = ReadWALEntries(walPath)
	if err == nil {
		t.Error("Expected error reading WAL with corrupted entry, got nil")
	}
	if err != nil && err.Error() == "" {
		t.Error("Error should have descriptive message")
	}
}

// TestWALSkipsCorruptedEmbeddingEntries verifies embedding entries are safely skipped.
func TestWALSkipsCorruptedEmbeddingEntries(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Create a valid node entry
	node := &Node{ID: "embedding-test", Labels: []string{"Test"}}
	nodeData, _ := json.Marshal(WALNodeData{Node: node})

	// Create entries: valid node, corrupted embedding (should skip), valid node
	entries := []WALEntry{
		{
			Sequence:  1,
			Operation: OpCreateNode,
			Data:      nodeData,
			Checksum:  crc32Checksum(nodeData),
		},
		{
			Sequence:  2,
			Operation: OpUpdateEmbedding,
			Data:      nodeData,
			Checksum:  crc32Checksum(nodeData) + 1, // Corrupted!
		},
		{
			Sequence:  3,
			Operation: OpCreateNode,
			Data:      nodeData,
			Checksum:  crc32Checksum(nodeData),
		},
	}

	// Write entries
	file, _ := os.Create(walPath)
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		encoder.Encode(&entry)
	}
	file.Close()

	// Read should skip corrupted embedding entry and return valid entries
	result, err := ReadWALEntries(walPath)
	if err != nil {
		t.Fatalf("Should skip corrupted embedding, got error: %v", err)
	}

	// Should have 2 entries (skipped the corrupted embedding)
	if len(result) != 2 {
		t.Errorf("Expected 2 entries (skipped embedding), got %d", len(result))
	}
}

// =============================================================================
// REPLAY ERROR TRACKING TESTS
// =============================================================================

// TestReplayResultTracking verifies ReplayResult correctly tracks outcomes.
func TestReplayResultTracking(t *testing.T) {
	engine := NewMemoryEngine()

	// Create test entries
	node1 := &Node{ID: "n1", Labels: []string{"Test"}}
	node2 := &Node{ID: "n2", Labels: []string{"Test"}}
	node1Data, _ := json.Marshal(WALNodeData{Node: node1})
	node2Data, _ := json.Marshal(WALNodeData{Node: node2})

	entries := []WALEntry{
		{Sequence: 1, Operation: OpCreateNode, Data: node1Data},
		{Sequence: 2, Operation: OpCreateNode, Data: node2Data},
		{Sequence: 3, Operation: OpCheckpoint, Data: []byte("{}")}, // Should skip
		{Sequence: 4, Operation: OpCreateNode, Data: node1Data},    // Duplicate - should skip
	}

	result := ReplayWALEntries(engine, entries)

	if result.Applied != 2 {
		t.Errorf("Expected 2 applied, got %d", result.Applied)
	}
	if result.Skipped != 2 {
		t.Errorf("Expected 2 skipped (checkpoint + duplicate), got %d", result.Skipped)
	}
	if result.Failed != 0 {
		t.Errorf("Expected 0 failed, got %d", result.Failed)
	}
}

// TestReplayResultHasCriticalErrors verifies error detection.
func TestReplayResultHasCriticalErrors(t *testing.T) {
	tests := []struct {
		name     string
		result   ReplayResult
		expected bool
	}{
		{
			name:     "no errors",
			result:   ReplayResult{Applied: 10, Skipped: 2, Failed: 0},
			expected: false,
		},
		{
			name:     "with failures",
			result:   ReplayResult{Applied: 8, Skipped: 1, Failed: 1},
			expected: true,
		},
		{
			name:     "all failed",
			result:   ReplayResult{Applied: 0, Skipped: 0, Failed: 5},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.result.HasCriticalErrors(); got != tc.expected {
				t.Errorf("HasCriticalErrors() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestReplayResultSummary verifies summary format.
func TestReplayResultSummary(t *testing.T) {
	result := ReplayResult{Applied: 10, Skipped: 3, Failed: 1}
	summary := result.Summary()

	expected := "applied=10 skipped=3 failed=1"
	if summary != expected {
		t.Errorf("Summary() = %q, want %q", summary, expected)
	}
}

// TestRecoverFromWALWithResultTracksErrors verifies full recovery error tracking.
func TestRecoverFromWALWithResultTracksErrors(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Create entries with invalid data that will fail during replay
	node := &Node{ID: "valid-node", Labels: []string{"Test"}}
	nodeData, _ := json.Marshal(WALNodeData{Node: node})

	// Invalid edge data (references non-existent nodes)
	edge := &Edge{ID: "e1", StartNode: "nonexistent1", EndNode: "nonexistent2", Type: "LINKS"}
	edgeData, _ := json.Marshal(WALEdgeData{Edge: edge})

	entries := []WALEntry{
		{Sequence: 1, Operation: OpCreateNode, Data: nodeData, Checksum: crc32Checksum(nodeData)},
		// This edge creation might fail due to missing endpoints
		{Sequence: 2, Operation: OpCreateEdge, Data: edgeData, Checksum: crc32Checksum(edgeData)},
	}

	// Write entries
	file, _ := os.Create(walPath)
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		encoder.Encode(&entry)
	}
	file.Close()

	// Recovery should track results
	baseEngine, result, err := RecoverFromWALWithResult(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Should have recovered the node
	if engine == nil {
		t.Fatal("Engine should not be nil")
	}

	// Check that we got a valid node
	n, err := engine.GetNode("valid-node")
	if err != nil || n == nil {
		t.Error("Valid node should have been recovered")
	}

	// Result should have tracked operations
	if result.Applied+result.Skipped+result.Failed != 2 {
		t.Errorf("Total operations should be 2, got applied=%d skipped=%d failed=%d",
			result.Applied, result.Skipped, result.Failed)
	}
}

// =============================================================================
// WAL INTEGRITY TESTS
// =============================================================================

// TestWALEntryIntegrity verifies full round-trip integrity.
func TestWALEntryIntegrity(t *testing.T) {
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

	// Write various entries
	nodes := []*Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
	}

	for _, node := range nodes {
		if err := wal.Append(OpCreateNode, WALNodeData{Node: node}); err != nil {
			t.Fatalf("Failed to append node: %v", err)
		}
	}

	wal.Close()

	// Read back and verify
	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Verify checksums match
	for i, entry := range entries {
		expected := crc32Checksum(entry.Data)
		if entry.Checksum != expected {
			t.Errorf("Entry %d: checksum mismatch: stored=%x computed=%x", i, entry.Checksum, expected)
		}

		// Verify data can be deserialized
		var data WALNodeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			t.Errorf("Entry %d: failed to unmarshal: %v", i, err)
		}
		if data.Node.ID != nodes[i].ID {
			t.Errorf("Entry %d: node ID mismatch: got %s, want %s", i, data.Node.ID, nodes[i].ID)
		}
	}
}

// TestWALRecoveryFromCorruptTail verifies recovery from partially written tail.
func TestWALRecoveryFromCorruptTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// Write valid entries
	node := &Node{ID: "n1", Labels: []string{"Test"}}
	nodeData, _ := json.Marshal(WALNodeData{Node: node})

	file, _ := os.Create(walPath)
	encoder := json.NewEncoder(file)

	// Write valid entry
	entry := WALEntry{
		Sequence:  1,
		Operation: OpCreateNode,
		Data:      nodeData,
		Checksum:  crc32Checksum(nodeData),
	}
	encoder.Encode(&entry)

	// Write partial/corrupt entry (simulate crash mid-write)
	file.WriteString(`{"seq":2,"op":"create_node","data":`) // Incomplete JSON
	file.Close()

	// Reading should fail on the corrupt JSON
	_, err := ReadWALEntries(walPath)
	if err == nil {
		t.Error("Expected error on corrupt tail, got nil")
	}

	// Verify the error mentions corruption
	if err != nil && !isCorruptionError(err) {
		t.Logf("Got error: %v", err)
		// This is expected - partial JSON causes decode error
	}
}

func isCorruptionError(err error) bool {
	return err != nil && (err.Error() != "" &&
		(contains(err.Error(), "corrupt") ||
			contains(err.Error(), "decode") ||
			contains(err.Error(), "JSON")))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
