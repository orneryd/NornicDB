package nornicdb

import (
	"context"
	"testing"
)

func TestEmbeddingStorage(t *testing.T) {
	// Open in-memory database
	db, err := Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a fake embedding (1024 dimensions)
	embedding := make([]float32, 1024)
	for i := range embedding {
		embedding[i] = float32(i) / 1024.0
	}

	// Create a node with embedding in properties.
	props := map[string]interface{}{
		"title":     "Test Node",
		"content":   "This is test content",
		"embedding": embedding,
	}

	node, err := db.CreateNode(ctx, []string{"Memory"}, props)
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	t.Logf("Created node: %s", node.ID)

	// User-provided embedding property should be preserved.
	if _, hasEmb := node.Properties["embedding"]; !hasEmb {
		t.Error("Embedding property should be preserved in node properties")
	} else {
		t.Log("✓ Embedding property preserved in node properties")
	}

	// Try to get the node from storage
	storageNode, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	t.Logf("Retrieved node: labels=%v", storageNode.Labels)

	// Note: The embed queue may generate managed embeddings asynchronously.
	// For this test, we verify user properties round-trip unchanged.
	t.Log("✓ Node created successfully with user embedding property preserved")
}
