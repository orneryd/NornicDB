//go:build integration
// +build integration

package embed

import (
	"context"
	"os"
	"testing"
	"time"
)

// Run with: go test -tags=integration -v ./pkg/embed/...
// Requires llama.cpp server running on localhost:11434

func TestLlamaCppEmbeddings(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Set INTEGRATION_TEST=1 to run")
	}

	// Configure for llama.cpp (uses OpenAI-compatible format)
	config := &Config{
		Provider:   "openai", // llama.cpp uses OpenAI format
		APIURL:     "http://localhost:11434",
		APIPath:    "/v1/embeddings",
		Model:      "bge-m3",
		Dimensions: 1024,
		Timeout:    30 * time.Second,
	}

	embedder := NewOpenAI(config)
	ctx := context.Background()

	// Test single embedding
	t.Run("SingleEmbed", func(t *testing.T) {
		vec, err := embedder.Embed(ctx, "hello world")
		if err != nil {
			t.Fatalf("Embed failed: %v", err)
		}
		if len(vec) != 1024 {
			t.Errorf("Expected 1024 dimensions, got %d", len(vec))
		}
		t.Logf("✓ Single embedding: %d dimensions", len(vec))
	})

	// Test batch embedding
	t.Run("BatchEmbed", func(t *testing.T) {
		texts := []string{
			"graph database stores relationships",
			"vector search finds similar content",
			"machine learning algorithms",
		}
		vecs, err := embedder.EmbedBatch(ctx, texts)
		if err != nil {
			t.Fatalf("EmbedBatch failed: %v", err)
		}
		if len(vecs) != 3 {
			t.Errorf("Expected 3 embeddings, got %d", len(vecs))
		}
		for i, vec := range vecs {
			if len(vec) != 1024 {
				t.Errorf("Embedding %d: expected 1024 dims, got %d", i, len(vec))
			}
		}
		t.Logf("✓ Batch embedding: %d texts", len(vecs))
	})

	// Test similarity (sanity check)
	t.Run("Similarity", func(t *testing.T) {
		vec1, _ := embedder.Embed(ctx, "cat")
		vec2, _ := embedder.Embed(ctx, "kitten")
		vec3, _ := embedder.Embed(ctx, "automobile")

		sim12 := vector.CosineSimilarity(vec1, vec2)
		sim13 := vector.CosineSimilarity(vec1, vec3)

		t.Logf("cat vs kitten: %.4f", sim12)
		t.Logf("cat vs automobile: %.4f", sim13)

		if sim12 <= sim13 {
			t.Errorf("Expected cat-kitten (%.4f) > cat-automobile (%.4f)", sim12, sim13)
		}
		t.Logf("✓ Similarity ordering correct")
	})
}
