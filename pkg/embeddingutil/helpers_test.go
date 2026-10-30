package embeddingutil

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildText(t *testing.T) {
	props := map[string]interface{}{
		"content":         "Real content",
		"tags":            []interface{}{"tag1", "tag2"},
		"metadata":        map[string]interface{}{"key": "value"},
		"id":              "123",
		"embedding_model": "test-model",
		"embedded_at":     "2024-01-01",
	}

	text := BuildText(props, []string{"Article"}, nil)

	assert.Contains(t, text, "labels: Article")
	assert.Contains(t, text, "content: Real content")
	assert.Contains(t, text, "tags: tag1, tag2")
	assert.Contains(t, text, "metadata: {\"key\":\"value\"}")
	assert.NotContains(t, text, "id:")
	assert.NotContains(t, text, "embedding_model:")
	assert.NotContains(t, text, "embedded_at:")
}

func TestBuildText_WithOptions(t *testing.T) {
	props := map[string]interface{}{
		"content":     "Main content",
		"title":       "Title here",
		"internal_id": "skip-me",
	}

	text := BuildText(props, []string{"Doc"}, &EmbedTextOptions{
		Include:       []string{"content", "title"},
		Exclude:       []string{"title"},
		IncludeLabels: false,
	})

	assert.Equal(t, "content: Main content", text)
}

func TestBuildText_Fallback(t *testing.T) {
	assert.Equal(t, "node", BuildText(map[string]interface{}{}, nil, &EmbedTextOptions{IncludeLabels: false}))
}

func TestMetadataPropertyKeyAndInvalidation(t *testing.T) {
	for _, key := range []string{
		"embedding",
		"has_embedding",
		"embedding_skipped",
		"embedding_model",
		"embedding_dimensions",
		"embedded_at",
		"has_chunks",
		"chunk_count",
		"createdAt",
		"updatedAt",
		"id",
	} {
		require.True(t, IsMetadataPropertyKey(key), "expected metadata key: %s", key)
	}
	require.False(t, IsMetadataPropertyKey("name"))

	InvalidateManagedEmbeddings(nil)

	node := &storage.Node{
		ID:              "n-1",
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		EmbedMeta: map[string]any{
			"has_embedding": true,
		},
	}
	InvalidateManagedEmbeddings(node)
	require.Nil(t, node.ChunkEmbeddings)
	require.Nil(t, node.EmbedMeta)
}

func TestEmbedTextOptionsFromConfig(t *testing.T) {
	cfg := config.LoadFromEnv()
	cfg.EmbeddingWorker.PropertiesInclude = []string{"content", "title"}
	cfg.EmbeddingWorker.PropertiesExclude = []string{"internal_id"}
	cfg.EmbeddingWorker.IncludeLabels = false

	opts := EmbedTextOptionsFromConfig(cfg)
	require.NotNil(t, opts)
	assert.Equal(t, []string{"content", "title"}, opts.Include)
	assert.Equal(t, []string{"internal_id"}, opts.Exclude)
	assert.False(t, opts.IncludeLabels)
}

func TestApplyManagedEmbedding(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	node := &storage.Node{ID: "n-1"}

	ApplyManagedEmbedding(node, [][]float32{{0.1, 0.2, 0.3}}, "mxbai-embed-large", 3, now)
	require.Len(t, node.ChunkEmbeddings, 1)
	assert.Equal(t, float32(0.1), node.ChunkEmbeddings[0][0])
	require.NotNil(t, node.EmbedMeta)
	assert.Equal(t, 1, node.EmbedMeta["chunk_count"])
	assert.Equal(t, "mxbai-embed-large", node.EmbedMeta["embedding_model"])
	assert.Equal(t, 3, node.EmbedMeta["embedding_dimensions"])
	assert.Equal(t, true, node.EmbedMeta["has_embedding"])
	assert.Equal(t, "2026-04-01T12:00:00Z", node.EmbedMeta["embedded_at"])
}

// TestBuildTextOrderAndPolicy: properties appear in key order, so the same
// content always gives the same text, and TextPolicy mirrors the options
// (#963).
func TestBuildTextOrderAndPolicy(t *testing.T) {
	props := map[string]interface{}{"zeta": "z", "alpha": "a", "mid": "m", "id": "x"}
	opts := &EmbedTextOptions{}
	want := "alpha: a\nmid: m\nzeta: z"
	for i := 0; i < 20; i++ {
		if got := BuildText(props, nil, opts); got != want {
			t.Fatalf("BuildText = %q, want %q", got, want)
		}
	}
	if p := TextPolicy(nil); !p.IncludeLabels || p.Include != nil {
		t.Fatalf("TextPolicy(nil) = %+v", p)
	}
	if p := TextPolicy(&EmbedTextOptions{Include: []string{"text"}, Exclude: []string{"x"}}); p.IncludeLabels || p.Include[0] != "text" || p.Exclude[0] != "x" {
		t.Fatalf("TextPolicy = %+v", p)
	}
}
