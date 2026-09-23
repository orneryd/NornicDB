package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatchWhereSize tests MATCH (n) WHERE size(n.prop) op value.
// size() returns string/list length; these queries must filter nodes correctly.
func TestMatchWhereSize(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Nodes: small content (5 chars), large content (15000 chars), no content
	smallContent := "hello"
	largeContent := strings.Repeat("x", 15000)

	n1 := &storage.Node{
		ID:     "n1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content": smallContent,
			"name":    "small",
		},
	}
	n2 := &storage.Node{
		ID:     "n2",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content": largeContent,
			"name":    "large",
		},
	}
	n3 := &storage.Node{
		ID:         "n3",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "empty"},
	}
	_, err := store.CreateNode(n1)
	require.NoError(t, err)
	_, err = store.CreateNode(n2)
	require.NoError(t, err)
	_, err = store.CreateNode(n3)
	require.NoError(t, err)

	t.Run("WHERE size(n.content) > 10000 returns only large", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE size(n.content) > 10000 RETURN n.name AS name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "should return exactly one node (large content)")
		assert.Equal(t, "large", result.Rows[0][0])
	})

	t.Run("WHERE size(n.content) > 100 returns only large", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE size(n.content) > 100 RETURN n.name AS name ORDER BY n.name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "large", result.Rows[0][0])
	})

	t.Run("WHERE size(n.content) >= 5 returns small and large", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE size(n.content) >= 5 RETURN n.name AS name ORDER BY n.name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		assert.Equal(t, "large", result.Rows[0][0])
		assert.Equal(t, "small", result.Rows[1][0])
	})

	t.Run("WHERE size(n.content) < 100 excludes null content", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE size(n.content) < 100 RETURN n.name AS name ORDER BY n.name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"small"}}, result.Rows)
	})
}

// TestMatchWhereExists tests MATCH (n) WHERE exists(n.prop).
func TestMatchWhereExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	n1 := &storage.Node{
		ID:     "n1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content":          "some text",
			"openai_embedding": []float32{0.1, 0.2},
		},
	}
	n2 := &storage.Node{
		ID:         "n2",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"content": "no embedding"},
	}
	n3 := &storage.Node{
		ID:     "n3",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content":          "other",
			"openai_embedding": []float32{0.3},
		},
	}
	_, err := store.CreateNode(n1)
	require.NoError(t, err)
	_, err = store.CreateNode(n2)
	require.NoError(t, err)
	_, err = store.CreateNode(n3)
	require.NoError(t, err)

	t.Run("WHERE exists(n.openai_embedding) returns only nodes with embedding", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE exists(n.openai_embedding) RETURN n.content AS content ORDER BY n.content", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "should return n1 and n3 only")
		assert.Equal(t, "other", result.Rows[0][0])
		assert.Equal(t, "some text", result.Rows[1][0])
	})

	t.Run("WHERE NOT exists(n.openai_embedding) returns nodes without embedding", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE NOT exists(n.openai_embedding) RETURN n.content AS content", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "no embedding", result.Rows[0][0])
	})
}

// TestMatchWhereSizeAndExists tests combined WHERE size(n.content) > X AND exists(n.openai_embedding).
func TestMatchWhereSizeAndExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	largeContent := strings.Repeat("x", 20000)

	// Large content with embedding
	n1 := &storage.Node{
		ID:     "n1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content":          largeContent,
			"openai_embedding": []float32{0.1},
			"name":             "large-with-embedding",
		},
	}
	// Large content without embedding
	n2 := &storage.Node{
		ID:     "n2",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content": largeContent,
			"name":    "large-no-embedding",
		},
	}
	// Small content with embedding
	n3 := &storage.Node{
		ID:     "n3",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"content":          "short",
			"openai_embedding": []float32{0.2},
			"name":             "small-with-embedding",
		},
	}
	_, err := store.CreateNode(n1)
	require.NoError(t, err)
	_, err = store.CreateNode(n2)
	require.NoError(t, err)
	_, err = store.CreateNode(n3)
	require.NoError(t, err)

	t.Run("WHERE size(n.content) > 10000 AND exists(n.openai_embedding) returns only large with embedding", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE size(n.content) > 10000 AND exists(n.openai_embedding) RETURN n.name AS name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "should return only node with large content and openai_embedding")
		assert.Equal(t, "large-with-embedding", result.Rows[0][0])
	})
}
