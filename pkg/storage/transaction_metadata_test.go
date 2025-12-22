package storage

import (
	"strings"
	"testing"
)

func TestTransaction_SetMetadata(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Set metadata
	metadata := map[string]interface{}{
		"app":       "test-app",
		"userId":    12345,
		"action":    "create-user",
		"requestId": "req-abc-123",
	}

	err := tx.SetMetadata(metadata)
	if err != nil {
		t.Fatalf("SetMetadata failed: %v", err)
	}

	// Verify metadata was set
	retrieved := tx.GetMetadata()
	if retrieved["app"] != "test-app" {
		t.Errorf("Expected app='test-app', got %v", retrieved["app"])
	}
	if retrieved["userId"] != 12345 {
		t.Errorf("Expected userId=12345, got %v", retrieved["userId"])
	}
	if retrieved["action"] != "create-user" {
		t.Errorf("Expected action='create-user', got %v", retrieved["action"])
	}
}

func TestTransaction_SetMetadata_Merge(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Set initial metadata
	tx.SetMetadata(map[string]interface{}{
		"app": "test-app",
		"userId": 123,
	})

	// Set additional metadata (should merge)
	tx.SetMetadata(map[string]interface{}{
		"action": "create",
		"userId": 456, // Override
	})

	retrieved := tx.GetMetadata()
	if retrieved["app"] != "test-app" {
		t.Error("app should still be present")
	}
	if retrieved["userId"] != 456 {
		t.Error("userId should be overridden to 456")
	}
	if retrieved["action"] != "create" {
		t.Error("action should be added")
	}
}

func TestTransaction_SetMetadata_TooLarge(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Create metadata > 2048 chars
	largeString := strings.Repeat("x", 2100)
	metadata := map[string]interface{}{
		"data": largeString,
	}

	err := tx.SetMetadata(metadata)
	if err == nil {
		t.Error("Should reject metadata > 2048 chars")
	}

	if !strings.Contains(err.Error(), "too large") {
		t.Errorf("Error message should mention 'too large', got: %v", err)
	}
}

func TestTransaction_SetMetadata_ClosedTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Commit transaction
	tx.Commit()

	// Try to set metadata on closed transaction
	err := tx.SetMetadata(map[string]interface{}{"test": "value"})
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransaction_GetMetadata_Copy(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	tx.SetMetadata(map[string]interface{}{
		"app": "test",
	})

	// Get metadata copy
	metadata := tx.GetMetadata()
	metadata["app"] = "modified"

	// Original should be unchanged
	retrieved := tx.GetMetadata()
	if retrieved["app"] != "test" {
		t.Error("Original metadata should not be affected by modifications to the copy")
	}
}

func TestTransaction_Commit_LogsMetadata(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Set metadata
	tx.SetMetadata(map[string]interface{}{
		"app":    "test-app",
		"userId": 123,
	})

	// Create a node
	node := &Node{
		ID: NodeID(prefixTestID("test-node")),
		Labels: []string{"Test"},
	}
	tx.CreateNode(node)

	// Commit should log metadata (check console output)
	err := tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify node was created
	retrieved, err := engine.GetNode(NodeID(prefixTestID("test-node")))
	if err != nil {
		t.Errorf("Node should exist after commit: %v", err)
	}
	if retrieved == nil {
		t.Error("Node should not be nil")
	}
}

func TestTransaction_Metadata_EmptyByDefault(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	metadata := tx.GetMetadata()
	if len(metadata) != 0 {
		t.Errorf("Metadata should be empty by default, got %d items", len(metadata))
	}
}

func TestTransaction_Metadata_WithOperations(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Set metadata before operations
	tx.SetMetadata(map[string]interface{}{
		"operation": "bulk-import",
		"batchId":   "batch-001",
	})

	// Perform operations
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("node-" + string(rune('0'+i)))),
			Labels: []string{"Imported"},
			Properties: map[string]interface{}{
				"index": i,
			},
		}
		tx.CreateNode(node)
	}

	// Commit with metadata
	err := tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all nodes were created
	count := 0
	for i := 0; i < 5; i++ {
		nodeID := NodeID(prefixTestID("node-" + string(rune('0'+i))))
		if _, err := engine.GetNode(nodeID); err == nil {
			count++
		}
	}

	if count != 5 {
		t.Errorf("Expected 5 nodes, got %d", count)
	}
}
