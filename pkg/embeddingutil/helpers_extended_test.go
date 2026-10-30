package embeddingutil

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildText_EmptyProperties_ReturnsNode(t *testing.T) {
	t.Parallel()
	result := BuildText(map[string]interface{}{}, nil, &EmbedTextOptions{IncludeLabels: false})
	assert.Equal(t, "node", result)
}

func TestBuildText_NilOptions_DefaultsToLabels(t *testing.T) {
	t.Parallel()
	result := BuildText(map[string]interface{}{"name": "test"}, []string{"Doc"}, nil)
	assert.Contains(t, result, "labels: Doc")
	assert.Contains(t, result, "name: test")
}

func TestBuildText_IncludeFilter(t *testing.T) {
	t.Parallel()
	props := map[string]interface{}{
		"title":       "Important Doc",
		"content":     "Body text here",
		"internal_id": "xyz-123",
	}
	opts := &EmbedTextOptions{
		Include:       []string{"title", "content"},
		IncludeLabels: false,
	}
	result := BuildText(props, nil, opts)
	assert.Contains(t, result, "title: Important Doc")
	assert.Contains(t, result, "content: Body text here")
	assert.NotContains(t, result, "internal_id")
}

func TestBuildText_ExcludeFilter(t *testing.T) {
	t.Parallel()
	props := map[string]interface{}{
		"name":   "Alice",
		"secret": "password123",
	}
	opts := &EmbedTextOptions{
		Exclude:       []string{"secret"},
		IncludeLabels: false,
	}
	result := BuildText(props, nil, opts)
	assert.Contains(t, result, "name: Alice")
	assert.NotContains(t, result, "secret")
	assert.NotContains(t, result, "password123")
}

func TestBuildText_MetadataKeysExcluded(t *testing.T) {
	t.Parallel()
	props := map[string]interface{}{
		"name":                 "Doc",
		"embedding":            []float32{0.1, 0.2},
		"has_embedding":        true,
		"embedding_model":      "bge-m3",
		"embedding_dimensions": 1024,
	}
	result := BuildText(props, nil, &EmbedTextOptions{IncludeLabels: false})
	assert.Contains(t, result, "name: Doc")
	assert.NotContains(t, result, "bge-m3")
	assert.NotContains(t, result, "1024")
}

func TestBuildText_ValueTypes(t *testing.T) {
	t.Parallel()
	props := map[string]interface{}{
		"str":     "hello",
		"num_int": 42,
		"num_f64": 3.14,
		"flag":    true,
		"nilval":  nil,
		"arr":     []interface{}{"a", "b", 3},
		"obj":     map[string]interface{}{"x": 1}, // json.Marshal path
	}
	result := BuildText(props, nil, &EmbedTextOptions{IncludeLabels: false})
	assert.Contains(t, result, "str: hello")
	assert.Contains(t, result, "num_int: 42")
	assert.Contains(t, result, "num_f64: 3.14")
	assert.Contains(t, result, "flag: true")
	assert.Contains(t, result, "nilval: null")
	assert.Contains(t, result, "arr: a, b, 3")
	assert.Contains(t, result, `obj: {"x":1}`)
}

func TestBuildText_LabelsDisabled(t *testing.T) {
	t.Parallel()
	result := BuildText(map[string]interface{}{"name": "test"}, []string{"Doc", "File"}, &EmbedTextOptions{IncludeLabels: false})
	assert.NotContains(t, result, "labels:")
	assert.Contains(t, result, "name: test")
}

func TestEmbedTextOptionsFromConfig_NilConfig(t *testing.T) {
	t.Parallel()
	opts := EmbedTextOptionsFromConfig(nil)
	require.NotNil(t, opts)
	assert.True(t, opts.IncludeLabels)
	assert.Empty(t, opts.Include)
	assert.Empty(t, opts.Exclude)
}

func TestEmbedTextOptionsFromConfig_WithConfig(t *testing.T) {
	t.Parallel()
	cfg := config.LoadDefaults()
	cfg.EmbeddingWorker.PropertiesInclude = []string{"title", "content"}
	cfg.EmbeddingWorker.PropertiesExclude = []string{"secret"}
	cfg.EmbeddingWorker.IncludeLabels = false

	opts := EmbedTextOptionsFromConfig(cfg)
	require.NotNil(t, opts)
	assert.Equal(t, []string{"title", "content"}, opts.Include)
	assert.Equal(t, []string{"secret"}, opts.Exclude)
	assert.False(t, opts.IncludeLabels)
}

func TestEmbedTextOptionsFromFields(t *testing.T) {
	t.Parallel()
	opts := EmbedTextOptionsFromFields([]string{"a"}, []string{"b"}, true)
	assert.Equal(t, []string{"a"}, opts.Include)
	assert.Equal(t, []string{"b"}, opts.Exclude)
	assert.True(t, opts.IncludeLabels)
}

func TestApplyManagedEmbedding_NilNode(t *testing.T) {
	t.Parallel()
	// Should not panic
	ApplyManagedEmbedding(nil, [][]float32{{0.1}}, "model", 3, time.Now())
}

func TestApplyManagedEmbedding_SetsFields(t *testing.T) {
	t.Parallel()
	node := &storage.Node{ID: "n1"}
	embeddings := [][]float32{{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}}
	now := time.Now()
	ApplyManagedEmbedding(node, embeddings, "bge-m3", 3, now)

	assert.Equal(t, embeddings, node.ChunkEmbeddings)
	require.NotNil(t, node.EmbedMeta)
	assert.Equal(t, 2, node.EmbedMeta["chunk_count"])
	assert.Equal(t, "bge-m3", node.EmbedMeta["embedding_model"])
	assert.Equal(t, 3, node.EmbedMeta["embedding_dimensions"])
	assert.Equal(t, true, node.EmbedMeta["has_embedding"])
	assert.Equal(t, now.Format(time.RFC3339), node.EmbedMeta["embedded_at"])
}

func TestApplyManagedEmbedding_EmptyEmbeddings(t *testing.T) {
	t.Parallel()
	node := &storage.Node{ID: "n1"}
	ApplyManagedEmbedding(node, [][]float32{}, "model", 0, time.Now())

	assert.Empty(t, node.ChunkEmbeddings)
	assert.Equal(t, false, node.EmbedMeta["has_embedding"])
	assert.Equal(t, 0, node.EmbedMeta["chunk_count"])
}

func TestApplyManagedEmbedding_OverwritesExisting(t *testing.T) {
	t.Parallel()
	node := &storage.Node{
		ID:              "n1",
		ChunkEmbeddings: [][]float32{{1, 2}},
		EmbedMeta:       map[string]any{"embedding_model": "old-model"},
	}
	ApplyManagedEmbedding(node, [][]float32{{3, 4, 5}}, "new-model", 3, time.Now())

	assert.Len(t, node.ChunkEmbeddings, 1)
	assert.Equal(t, float32(3), node.ChunkEmbeddings[0][0])
	assert.Equal(t, "new-model", node.EmbedMeta["embedding_model"])
}
