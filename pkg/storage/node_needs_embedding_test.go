package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeNeedsEmbedding(t *testing.T) {
	t.Run("nil node returns false", func(t *testing.T) {
		assert.False(t, NodeNeedsEmbedding(nil))
	})

	t.Run("node with embedding returns false", func(t *testing.T) {
		node := &Node{
			ID:         "test-1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice"},
			Embedding:  []float32{0.1, 0.2, 0.3},
		}
		assert.False(t, NodeNeedsEmbedding(node))
	})

	t.Run("node without embedding returns true", func(t *testing.T) {
		node := &Node{
			ID:         "test-2",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Bob"},
			Embedding:  nil,
		}
		assert.True(t, NodeNeedsEmbedding(node))
	})

	t.Run("node with empty embedding returns true", func(t *testing.T) {
		node := &Node{
			ID:         "test-3",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Charlie"},
			Embedding:  []float32{},
		}
		assert.True(t, NodeNeedsEmbedding(node))
	})

	t.Run("internal node (underscore label) returns false", func(t *testing.T) {
		node := &Node{
			ID:         "test-4",
			Labels:     []string{"_Internal"},
			Properties: map[string]interface{}{"data": "internal"},
			Embedding:  nil,
		}
		assert.False(t, NodeNeedsEmbedding(node))
	})

	t.Run("node with embedding_skipped returns false", func(t *testing.T) {
		node := &Node{
			ID:     "test-5",
			Labels: []string{"Empty"},
			Properties: map[string]interface{}{
				"embedding_skipped": "no content",
			},
			Embedding: nil,
		}
		assert.False(t, NodeNeedsEmbedding(node))
	})

	// REGRESSION TEST: has_embedding property should NOT affect the result
	// Only the actual Embedding array matters
	t.Run("REGRESSION: has_embedding=true but no embedding array returns true", func(t *testing.T) {
		// This was the bug - nodes with has_embedding=true but no actual embedding
		// were being skipped, preventing regeneration
		node := &Node{
			ID:     "test-6",
			Labels: []string{"File"},
			Properties: map[string]interface{}{
				"name":          "test.txt",
				"has_embedding": true, // Property says true, but no actual embedding
			},
			Embedding: nil, // No actual embedding array
		}
		// Should return true because we need to generate the embedding
		assert.True(t, NodeNeedsEmbedding(node), "node with has_embedding=true but no embedding array should need embedding")
	})

	t.Run("REGRESSION: has_embedding=false but no embedding array returns true", func(t *testing.T) {
		// Another regression case - has_embedding=false should not prevent processing
		// unless embedding_skipped is also set
		node := &Node{
			ID:     "test-7",
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"content":       "Some content to embed",
				"has_embedding": false,
			},
			Embedding: nil,
		}
		// Should return true - has_embedding property is ignored
		assert.True(t, NodeNeedsEmbedding(node), "node with has_embedding=false should still need embedding if no embedding_skipped")
	})

	t.Run("REGRESSION: has_chunks=true but no embedding array returns true", func(t *testing.T) {
		// File nodes that were partially processed (has_chunks but no has_embedding)
		// should be re-processed
		node := &Node{
			ID:     "test-8",
			Labels: []string{"File"},
			Properties: map[string]interface{}{
				"name":       "large_file.txt",
				"has_chunks": true,
				// Note: no has_embedding property - chunking was interrupted
			},
			Embedding: nil,
		}
		// Should return true - needs to complete the chunking process
		assert.True(t, NodeNeedsEmbedding(node), "node with has_chunks=true but incomplete processing should need embedding")
	})

	t.Run("node with content and no embedding returns true", func(t *testing.T) {
		node := &Node{
			ID:     "test-9",
			Labels: []string{"Memory"},
			Properties: map[string]interface{}{
				"content":   "This is a memory that needs embedding",
				"createdAt": "2024-01-01",
			},
			Embedding: nil,
		}
		assert.True(t, NodeNeedsEmbedding(node))
	})

	t.Run("FileChunk node without embedding returns true", func(t *testing.T) {
		node := &Node{
			ID:     "chunk-1",
			Labels: []string{"FileChunk"},
			Properties: map[string]interface{}{
				"content":     "Chunk content",
				"chunk_index": 0,
				"parent_file": "file-123",
			},
			Embedding: nil,
		}
		assert.True(t, NodeNeedsEmbedding(node))
	})
}

func TestNodeNeedsEmbedding_ClearedEmbeddings(t *testing.T) {
	// Simulate the regeneration scenario where embeddings are cleared
	t.Run("after clearing embeddings, node should need embedding", func(t *testing.T) {
		// Start with a node that has an embedding
		node := &Node{
			ID:     "regen-1",
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"name":                 "Alice",
				"has_embedding":        true,
				"embedding_model":      "test-model",
				"embedding_dimensions": 512,
				"embedded_at":          "2024-01-01",
			},
			Embedding: []float32{0.1, 0.2, 0.3},
		}
		assert.False(t, NodeNeedsEmbedding(node), "node with embedding should not need embedding")

		// Clear the embedding (simulating ClearAllEmbeddings)
		node.Embedding = nil

		// Now it should need embedding again, regardless of has_embedding property
		assert.True(t, NodeNeedsEmbedding(node), "node with cleared embedding should need embedding")
	})

	t.Run("File node after clearing should need embedding", func(t *testing.T) {
		// File node that was chunked
		node := &Node{
			ID:     "file-regen",
			Labels: []string{"File"},
			Properties: map[string]interface{}{
				"name":          "document.md",
				"has_chunks":    true,
				"chunk_count":   5,
				"has_embedding": true,
				"embedded_at":   "2024-01-01",
			},
			Embedding: nil, // File nodes don't have embeddings, their chunks do
		}

		// This is tricky - File nodes with has_chunks=true don't have their own embedding
		// The chunks have embeddings. So this node should NOT need embedding itself.
		// But if we're regenerating, we need to process it to recreate the chunks.
		// For now, we return true so it gets processed and chunks get recreated.
		assert.True(t, NodeNeedsEmbedding(node), "File node should be processed to recreate chunks")
	})
}
