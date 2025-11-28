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

	// Create a node with embedding in properties
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

	// Check if embedding was removed from properties (stored separately)
	if _, hasEmb := node.Properties["embedding"]; hasEmb {
		t.Log("Embedding is still in properties (expected to be removed)")
	} else {
		t.Log("✓ Embedding removed from properties")
	}

	// Try to get the node from storage and check its embedding
	storageNode, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	t.Logf("Retrieved node: labels=%v", storageNode.Labels)

	// Try hybrid search with the same embedding
	results, err := db.HybridSearch(ctx, "test content", embedding, nil, 10)
	if err != nil {
		t.Fatalf("HybridSearch failed: %v", err)
	}
	t.Logf("HybridSearch returned %d results", len(results))

	for i, r := range results {
		t.Logf("  %d. %s (score: %.4f)", i+1, r.Node.Properties["title"], r.Score)
	}

	if len(results) == 0 {
		// Try text-only search
		textResults, err := db.Search(ctx, "test content", nil, 10)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		t.Logf("Text Search returned %d results", len(textResults))
		for i, r := range textResults {
			t.Logf("  %d. %s (score: %.4f)", i+1, r.Node.Properties["title"], r.Score)
		}
	}
}
