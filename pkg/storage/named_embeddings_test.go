package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetDefaultEmbedding tests the GetDefaultEmbedding migration behavior.
func TestGetDefaultEmbedding(t *testing.T) {
	t.Run("returns NamedEmbeddings[default] when present", func(t *testing.T) {
		node := &Node{
			ID: NodeID("test-1"),
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
				"title":   {0.4, 0.5, 0.6},
			},
		}

		emb := node.GetDefaultEmbedding()
		require.NotNil(t, emb)
		assert.Equal(t, []float32{0.1, 0.2, 0.3}, emb)
	})

	t.Run("returns ChunkEmbeddings[0] when NamedEmbeddings[default] missing (migration)", func(t *testing.T) {
		node := &Node{
			ID: NodeID("test-2"),
			ChunkEmbeddings: [][]float32{
				{0.4, 0.5, 0.6},
				{0.7, 0.8, 0.9},
			},
		}

		emb := node.GetDefaultEmbedding()
		require.NotNil(t, emb)
		assert.Equal(t, []float32{0.4, 0.5, 0.6}, emb)
	})

	t.Run("prefers NamedEmbeddings[default] over ChunkEmbeddings[0]", func(t *testing.T) {
		node := &Node{
			ID: NodeID("test-3"),
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
			},
			ChunkEmbeddings: [][]float32{
				{0.4, 0.5, 0.6},
			},
		}

		emb := node.GetDefaultEmbedding()
		require.NotNil(t, emb)
		assert.Equal(t, []float32{0.1, 0.2, 0.3}, emb) // Should prefer NamedEmbeddings
	})

	t.Run("returns nil when no embeddings present", func(t *testing.T) {
		node := &Node{
			ID: NodeID("test-4"),
		}

		emb := node.GetDefaultEmbedding()
		assert.Nil(t, emb)
	})

	t.Run("returns nil when NamedEmbeddings exists but default missing and ChunkEmbeddings empty", func(t *testing.T) {
		node := &Node{
			ID: NodeID("test-5"),
			NamedEmbeddings: map[string][]float32{
				"title": {0.4, 0.5, 0.6}, // "default" not present
			},
		}

		emb := node.GetDefaultEmbedding()
		assert.Nil(t, emb)
	})

	t.Run("returns nil when ChunkEmbeddings empty", func(t *testing.T) {
		node := &Node{
			ID:              NodeID("test-6"),
			ChunkEmbeddings: [][]float32{},
		}

		emb := node.GetDefaultEmbedding()
		assert.Nil(t, emb)
	})

	t.Run("handles nil node", func(t *testing.T) {
		var node *Node
		emb := node.GetDefaultEmbedding()
		assert.Nil(t, emb)
	})
}

// TestNamedEmbeddingsSerialization tests that NamedEmbeddings are properly
// serialized and deserialized.
func TestNamedEmbeddingsSerialization(t *testing.T) {
	t.Run("serializes and deserializes NamedEmbeddings", func(t *testing.T) {
		original := &Node{
			ID:     NodeID("test-serialize"),
			Labels: []string{"Document"},
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3, 0.4},
				"title":   {0.5, 0.6, 0.7, 0.8},
				"content": {0.9, 1.0, 1.1, 1.2},
			},
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		require.NotNil(t, decoded.NamedEmbeddings)
		assert.Equal(t, len(original.NamedEmbeddings), len(decoded.NamedEmbeddings))
		assert.Equal(t, original.NamedEmbeddings["default"], decoded.NamedEmbeddings["default"])
		assert.Equal(t, original.NamedEmbeddings["title"], decoded.NamedEmbeddings["title"])
		assert.Equal(t, original.NamedEmbeddings["content"], decoded.NamedEmbeddings["content"])
	})

	t.Run("handles empty NamedEmbeddings map", func(t *testing.T) {
		original := &Node{
			ID:             NodeID("test-empty"),
			NamedEmbeddings: map[string][]float32{},
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		// Empty map should be preserved (not nil)
		assert.NotNil(t, decoded.NamedEmbeddings)
		assert.Equal(t, 0, len(decoded.NamedEmbeddings))
	})

	t.Run("handles nil NamedEmbeddings", func(t *testing.T) {
		original := &Node{
			ID: NodeID("test-nil"),
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		// nil map should remain nil
		assert.Nil(t, decoded.NamedEmbeddings)
	})
}

// TestNamedEmbeddingsCopy tests that NamedEmbeddings are properly deep-copied.
func TestNamedEmbeddingsCopy(t *testing.T) {
	t.Run("deep copies NamedEmbeddings", func(t *testing.T) {
		original := &Node{
			ID: NodeID("test-copy"),
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
				"title":   {0.4, 0.5, 0.6},
			},
		}

		copied := CopyNode(original)

		// Verify copy
		require.NotNil(t, copied.NamedEmbeddings)
		assert.Equal(t, original.NamedEmbeddings["default"], copied.NamedEmbeddings["default"])
		assert.Equal(t, original.NamedEmbeddings["title"], copied.NamedEmbeddings["title"])

		// Verify independence
		original.NamedEmbeddings["default"][0] = 9.9
		assert.Equal(t, float32(0.1), copied.NamedEmbeddings["default"][0], "copy should be independent")
	})

	t.Run("handles nil NamedEmbeddings in copy", func(t *testing.T) {
		original := &Node{
			ID: NodeID("test-copy-nil"),
		}

		copied := CopyNode(original)
		assert.Nil(t, copied.NamedEmbeddings)
	})
}

