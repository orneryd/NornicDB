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
	// NOTE: User-provided embeddings are now ignored - this tests that behavior
	embedding := make([]float32, 1024)
	for i := range embedding {
		embedding[i] = float32(i) / 1024.0
	}

	// Create a node with embedding in properties (should be silently ignored)
	props := map[string]interface{}{
		"title":     "Test Node",
		"content":   "This is test content",
		"embedding": embedding, // This will be stripped - embeddings are internal-only
	}

	node, err := db.CreateNode(ctx, []string{"Memory"}, props)
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	t.Logf("Created node: %s", node.ID)

	// Embedding should be stripped from properties (embeddings are internal-only)
	if _, hasEmb := node.Properties["embedding"]; hasEmb {
		t.Error("Embedding should be stripped from properties")
	} else {
		t.Log("✓ Embedding correctly stripped from properties (internal-only)")
	}

	// Try to get the node from storage
	storageNode, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	t.Logf("Retrieved node: labels=%v", storageNode.Labels)

	// Note: The embed queue will generate embeddings asynchronously
	// For this test, we just verify the node was created without the user embedding
	t.Log("✓ Node created successfully (embeddings generated asynchronously by embed queue)")
}
