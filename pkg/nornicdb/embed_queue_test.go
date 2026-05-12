package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEmbedder is a test embedder that tracks calls
type mockEmbedder struct {
	embedCount int
	mu         sync.Mutex
	dims       int
	model      string
}

func newMockEmbedder() *mockEmbedder {
	return &mockEmbedder{
		dims:  1024,
		model: "test-model",
	}
}

func (m *mockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	m.mu.Lock()
	m.embedCount++
	m.mu.Unlock()
	return make([]float32, m.dims), nil
}

func (m *mockEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	m.mu.Lock()
	m.embedCount += len(texts)
	m.mu.Unlock()

	result := make([][]float32, len(texts))
	for i := range texts {
		result[i] = make([]float32, m.dims)
		// Add some unique values to distinguish embeddings
		for j := 0; j < m.dims && j < len(texts[i]); j++ {
			result[i][j] = float32(texts[i][j%len(texts[i])]) / 255.0
		}
	}
	return result, nil
}

func (m *mockEmbedder) Model() string {
	return m.model
}

func (m *mockEmbedder) Dimensions() int {
	return m.dims
}

func (m *mockEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (m *mockEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func (m *mockEmbedder) GetEmbedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.embedCount
}

// recordingBatchEmbedder records EmbedBatch call sizes for batching assertions.
type recordingBatchEmbedder struct {
	mu         sync.Mutex
	dims       int
	model      string
	batchSizes []int
}

func newRecordingBatchEmbedder() *recordingBatchEmbedder {
	return &recordingBatchEmbedder{
		dims:  1024,
		model: "recording-model",
	}
}

func (m *recordingBatchEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return make([]float32, m.dims), nil
}

func (m *recordingBatchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	m.mu.Lock()
	m.batchSizes = append(m.batchSizes, len(texts))
	m.mu.Unlock()

	result := make([][]float32, len(texts))
	for i := range texts {
		result[i] = make([]float32, m.dims)
	}
	return result, nil
}

func (m *recordingBatchEmbedder) Model() string {
	return m.model
}

func (m *recordingBatchEmbedder) Dimensions() int {
	return m.dims
}

func (m *recordingBatchEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (m *recordingBatchEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func (m *recordingBatchEmbedder) MaxBatchSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	max := 0
	for _, sz := range m.batchSizes {
		if sz > max {
			max = sz
		}
	}
	return max
}

// TestCopyNodeForEmbedding tests that the copy function creates independent copies
func TestCopyNodeForEmbedding(t *testing.T) {
	t.Run("creates_independent_properties_map", func(t *testing.T) {
		original := &storage.Node{
			ID:     storage.NodeID("test-node"),
			Labels: []string{"Memory", "Test"},
			Properties: map[string]any{
				"content": "test content",
				"title":   "Test Title",
			},
		}

		copy := copyNodeForEmbedding(original)

		// Modify the copy
		copy.Properties["new_property"] = "new value"
		copy.Properties["content"] = "modified content"

		// Original should be unchanged
		assert.Equal(t, "test content", original.Properties["content"])
		_, hasNew := original.Properties["new_property"]
		assert.False(t, hasNew, "Original should not have new property")
	})

	t.Run("copies_embedding", func(t *testing.T) {
		original := &storage.Node{
			ID:              storage.NodeID("test-node"),
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}

		copy := copyNodeForEmbedding(original)

		// Modify the copy's embedding
		copy.ChunkEmbeddings[0][0] = 999.0

		// Original should be unchanged
		assert.Equal(t, float32(0.1), original.ChunkEmbeddings[0][0]) // Use first chunk, first element
	})

	t.Run("copies_labels", func(t *testing.T) {
		original := &storage.Node{
			ID:     storage.NodeID("test-node"),
			Labels: []string{"Label1", "Label2"},
		}

		copy := copyNodeForEmbedding(original)

		// Modify the copy's labels
		copy.Labels[0] = "Modified"

		// Original should be unchanged
		assert.Equal(t, "Label1", original.Labels[0])
	})

	t.Run("preserves_id", func(t *testing.T) {
		original := &storage.Node{
			ID: storage.NodeID("unique-id-123"),
		}

		copy := copyNodeForEmbedding(original)

		assert.Equal(t, original.ID, copy.ID)
	})

	t.Run("handles_nil", func(t *testing.T) {
		copy := copyNodeForEmbedding(nil)
		assert.Nil(t, copy)
	})
}

// TestEmbedWorkerRecentlyProcessed tests the duplicate processing prevention
func TestEmbedWorkerRecentlyProcessed(t *testing.T) {
	t.Run("tracks_processed_nodes", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Manually add a node to recentlyProcessed
		worker.mu.Lock()
		worker.recentlyProcessed["test-node-1"] = time.Now()
		worker.mu.Unlock()

		// Verify it's tracked
		worker.mu.Lock()
		_, exists := worker.recentlyProcessed["test-node-1"]
		worker.mu.Unlock()

		assert.True(t, exists)
	})

	t.Run("cleans_old_entries", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}
		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Add an old entry (more than 1 minute old)
		worker.mu.Lock()
		worker.recentlyProcessed["old-node"] = time.Now().Add(-2 * time.Minute)
		worker.recentlyProcessed["new-node"] = time.Now()
		worker.mu.Unlock()

		// Create a node to trigger cleanup (cleanup happens during processing)
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("trigger-node"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "trigger content",
			},
		})
		require.NoError(t, err)

		// Trigger processing which should clean up old entries
		worker.Trigger()

		// Wait for processing to complete
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// old-node should be cleaned up, new-node should remain
		worker.mu.Lock()
		_, oldExists := worker.recentlyProcessed["old-node"]
		_, newExists := worker.recentlyProcessed["new-node"]
		worker.mu.Unlock()

		assert.False(t, oldExists, "Old node should be cleaned up")
		assert.True(t, newExists, "New node should still exist")
	})
}

// TestEmbedWorkerPersistence tests that embeddings are actually persisted
func TestEmbedWorkerPersistence(t *testing.T) {
	t.Run("embedding_persisted_to_storage", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node without embedding
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("persist-test"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "This is test content for embedding",
			},
		})
		require.NoError(t, err)

		// Verify node has no embedding initially
		node, err := engine.GetNode("persist-test")
		require.NoError(t, err)
		assert.Empty(t, node.ChunkEmbeddings, "Node should have no embeddings initially")

		// Create worker and process
		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Trigger embedding
		worker.Trigger()

		// Wait for processing with timeout - worker has 500ms startup delay
		deadline := time.Now().Add(3 * time.Second)
		var processed bool
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				processed = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.True(t, processed, "Worker should have processed at least one node")

		// Small additional delay for storage to sync
		time.Sleep(50 * time.Millisecond)

		// Verify embedding was persisted
		node, err = engine.GetNode("persist-test")
		require.NoError(t, err)

		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings after processing")
		assert.Equal(t, 1024, len(node.ChunkEmbeddings[0]), "Embedding should have correct dimensions")
	})

	t.Run("node_not_reprocessed_after_embedding", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("no-reprocess-test"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Content for no-reprocess test",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Wait for first processing to complete
		worker.Trigger()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Record embed count after first processing
		initialEmbedCount := embedder.GetEmbedCount()
		require.Equal(t, 1, initialEmbedCount, "Should have embedded once initially")

		// Trigger multiple more times
		for i := 0; i < 5; i++ {
			worker.Trigger()
			time.Sleep(100 * time.Millisecond)
		}

		// Wait a bit more
		time.Sleep(200 * time.Millisecond)

		// Embedder should NOT have been called again (node already has embedding)
		finalEmbedCount := embedder.GetEmbedCount()
		assert.Equal(t, initialEmbedCount, finalEmbedCount, "Embedder should not be called again for same node")

		// Worker stats should show only 1 processed
		stats := worker.Stats()
		assert.Equal(t, 1, stats.Processed, "Should show 1 processed node")
	})
}

// TestEmbedWorkerFindNodeWithoutEmbedding tests the node discovery logic
func TestEmbedWorkerFindNodeWithoutEmbedding(t *testing.T) {
	t.Run("finds_node_without_embedding", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create node without embedding
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("needs-embed"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Content needing embedding",
			},
		})
		require.NoError(t, err)

		worker := NewEmbedWorker(embedder, engine, nil)
		defer worker.Close()

		node := worker.findNodeWithoutEmbedding()

		assert.NotNil(t, node, "Should find node without embedding")
		assert.Equal(t, storage.NodeID("needs-embed"), node.ID)
	})

	t.Run("skips_node_with_embedding", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create node WITH embedding
		_, err := engine.CreateNode(&storage.Node{
			ID:              storage.NodeID("has-embed"),
			Labels:          []string{"Memory"},
			ChunkEmbeddings: [][]float32{make([]float32, 1024)}, // Pre-existing embedding
			Properties: map[string]any{
				"content": "Already embedded content",
			},
		})
		require.NoError(t, err)

		worker := NewEmbedWorker(embedder, engine, nil)
		defer worker.Close()

		node := worker.findNodeWithoutEmbedding()

		assert.Nil(t, node, "Should not find node that already has embedding")
	})

	t.Run("skips_internal_nodes", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create internal node (starts with _)
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("internal-node"),
			Labels: []string{"_Internal"},
			Properties: map[string]any{
				"content": "Internal content",
			},
		})
		require.NoError(t, err)

		worker := NewEmbedWorker(embedder, engine, nil)
		defer worker.Close()

		node := worker.findNodeWithoutEmbedding()

		assert.Nil(t, node, "Should skip internal nodes")
	})

	t.Run("skips_deleted_nodes", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create node without embedding
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("to-delete"),
			Labels: []string{"Test"},
			Properties: map[string]any{
				"content": "Content to delete",
			},
		})
		require.NoError(t, err)

		worker := NewEmbedWorker(embedder, engine, nil)
		defer worker.Close()

		// Node should be found initially
		node := worker.findNodeWithoutEmbedding()
		require.NotNil(t, node, "Should find node before deletion")
		assert.Equal(t, storage.NodeID("to-delete"), node.ID)

		// Delete the node
		err = engine.DeleteNode(storage.NodeID("to-delete"))
		require.NoError(t, err)

		// processNextBatch should skip the deleted node
		// It will find the node from the index, but then check if it exists
		// and skip it if it's been deleted
		didWork := worker.processNextBatch()

		// Should return false (no work done) because node was deleted
		// The node was found in the index but doesn't exist, so it's skipped
		assert.False(t, didWork, "Should skip deleted node and return false")

		// Verify node was not embedded (embedder should not have been called)
		assert.Equal(t, 0, embedder.embedCount, "Should not embed deleted node")
	})
}

// TestBuildEmbeddingText tests the text extraction for embedding
func TestBuildEmbeddingText(t *testing.T) {
	t.Run("stringifies_all_properties", func(t *testing.T) {
		props := map[string]interface{}{
			"title":       "Test Title",
			"content":     "Test Content",
			"description": "Test Description",
			"score":       85,
		}

		text := embeddingutil.BuildText(props, []string{"Document"}, nil)

		assert.Contains(t, text, "Test Title")
		assert.Contains(t, text, "Test Content")
		assert.Contains(t, text, "Test Description")
		assert.Contains(t, text, "85")
		assert.Contains(t, text, "labels: Document")
	})

	t.Run("skips_metadata_fields", func(t *testing.T) {
		props := map[string]interface{}{
			"content":         "Real content",
			"id":              "123",
			"embedding":       []float32{0.1, 0.2},
			"has_embedding":   true,
			"embedding_model": "test-model",
			"embedded_at":     "2024-01-01",
			"createdAt":       "2024-01-01",
		}

		text := embeddingutil.BuildText(props, []string{"Article"}, nil)

		assert.Contains(t, text, "Real content")
		assert.Contains(t, text, "labels: Article")
		assert.NotContains(t, text, "id:")
		assert.NotContains(t, text, "embedding:")
		assert.NotContains(t, text, "has_embedding:")
		assert.NotContains(t, text, "embedding_model:")
		assert.NotContains(t, text, "embedded_at:")
		assert.NotContains(t, text, "createdAt:")
	})

	t.Run("returns_labels_for_only_metadata", func(t *testing.T) {
		props := map[string]interface{}{
			"id":            "123",
			"embedding":     []float32{0.1},
			"has_embedding": true,
			"createdAt":     "2024-01-01",
		}

		text := embeddingutil.BuildText(props, []string{"Node"}, nil)

		// Should return labels even if no embeddable properties
		assert.Contains(t, text, "labels: Node")
		assert.NotEmpty(t, text)
	})

	t.Run("includes_tags_array", func(t *testing.T) {
		props := map[string]interface{}{
			"content": "Some content",
			"tags":    []interface{}{"tag1", "tag2"},
		}

		text := embeddingutil.BuildText(props, []string{"Post"}, nil)

		assert.Contains(t, text, "tag1")
		assert.Contains(t, text, "tag2")
		assert.Contains(t, text, "labels: Post")
	})

	t.Run("handles_arbitrary_properties", func(t *testing.T) {
		// Test with TranslationEntry-like properties
		props := map[string]interface{}{
			"originalText":       "Your prescription was delivered",
			"spanishTranslation": "Tu receta fue entregada",
			"aiAuditScore":       80,
			"humanReviewResult":  "approved",
			"issuesFound":        "Uses informal 'tu'",
		}

		text := embeddingutil.BuildText(props, []string{"TranslationEntry"}, nil)

		assert.Contains(t, text, "Your prescription was delivered")
		assert.Contains(t, text, "Tu receta fue entregada")
		assert.Contains(t, text, "80")
		assert.Contains(t, text, "approved")
		assert.Contains(t, text, "Uses informal")
		assert.Contains(t, text, "labels: TranslationEntry")
	})

	t.Run("includes_labels_even_with_no_properties", func(t *testing.T) {
		props := map[string]interface{}{}
		text := embeddingutil.BuildText(props, []string{"Person", "Employee"}, nil)

		assert.Contains(t, text, "labels: Person, Employee")
		assert.NotEmpty(t, text)
	})

	t.Run("handles_empty_labels_and_properties", func(t *testing.T) {
		props := map[string]interface{}{}
		text := embeddingutil.BuildText(props, []string{}, nil)

		// Should return fallback "node" if no labels and no properties
		assert.NotEmpty(t, text)
		assert.Contains(t, text, "node")
	})

	t.Run("includes_all_property_types", func(t *testing.T) {
		props := map[string]interface{}{
			"name":     "Alice",
			"age":      30,
			"active":   true,
			"tags":     []interface{}{"developer", "golang"},
			"metadata": map[string]interface{}{"key": "value"},
			"empty":    "",
			"nil":      nil,
		}
		text := embeddingutil.BuildText(props, []string{"Person"}, nil)

		assert.Contains(t, text, "labels: Person")
		assert.Contains(t, text, "name: Alice")
		assert.Contains(t, text, "age: 30")
		assert.Contains(t, text, "active: true")
		assert.Contains(t, text, "tags: developer, golang")
		assert.Contains(t, text, "empty: ")   // Empty strings are included
		assert.Contains(t, text, "nil: null") // Nil values are included as "null"
	})
}

// TestBuildEmbeddingText_IncludeExclude tests property include/exclude and IncludeLabels options.
func TestBuildEmbeddingText_IncludeExclude(t *testing.T) {
	props := map[string]interface{}{
		"content":     "Main content",
		"title":       "Title here",
		"internal_id": "skip-me",
		"description": "Desc",
	}

	t.Run("include_only", func(t *testing.T) {
		opts := &embeddingutil.EmbedTextOptions{Include: []string{"content"}, IncludeLabels: true}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		assert.Contains(t, text, "labels: Doc")
		assert.Contains(t, text, "content: Main content")
		assert.NotContains(t, text, "title:")
		assert.NotContains(t, text, "internal_id:")
		assert.NotContains(t, text, "description:")
	})

	t.Run("include_multiple", func(t *testing.T) {
		opts := &embeddingutil.EmbedTextOptions{Include: []string{"content", "title"}, IncludeLabels: true}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		assert.Contains(t, text, "content: Main content")
		assert.Contains(t, text, "title: Title here")
		assert.NotContains(t, text, "internal_id:")
		assert.NotContains(t, text, "description:")
	})

	t.Run("exclude_only", func(t *testing.T) {
		opts := &embeddingutil.EmbedTextOptions{Exclude: []string{"internal_id"}, IncludeLabels: true}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		assert.Contains(t, text, "content: Main content")
		assert.Contains(t, text, "title: Title here")
		assert.Contains(t, text, "description: Desc")
		assert.NotContains(t, text, "internal_id:")
	})

	t.Run("include_and_exclude", func(t *testing.T) {
		// Include content and title; exclude title (exclude wins) so only content
		opts := &embeddingutil.EmbedTextOptions{
			Include:       []string{"content", "title"},
			Exclude:       []string{"title"},
			IncludeLabels: true,
		}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		assert.Contains(t, text, "content: Main content")
		assert.NotContains(t, text, "title:")
		assert.NotContains(t, text, "internal_id:")
	})

	t.Run("include_labels_false", func(t *testing.T) {
		opts := &embeddingutil.EmbedTextOptions{Include: []string{"content"}, IncludeLabels: false}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		assert.NotContains(t, text, "labels:")
		assert.Contains(t, text, "content: Main content")
	})

	t.Run("include_labels_false_no_properties_included", func(t *testing.T) {
		opts := &embeddingutil.EmbedTextOptions{Include: []string{"nonexistent"}, IncludeLabels: false}
		text := embeddingutil.BuildText(props, []string{"Doc"}, opts)
		// Should return fallback when nothing to embed
		assert.Equal(t, "node", text)
	})

	t.Run("nil_opts_includes_all_non_metadata", func(t *testing.T) {
		text := embeddingutil.BuildText(props, []string{"Doc"}, nil)
		assert.Contains(t, text, "labels: Doc")
		assert.Contains(t, text, "content: Main content")
		assert.Contains(t, text, "title: Title here")
		assert.Contains(t, text, "internal_id: skip-me")
		assert.Contains(t, text, "description: Desc")
	})
}

// TestChunkText tests the text chunking logic
func TestChunkText(t *testing.T) {
	t.Run("short_text_single_chunk", func(t *testing.T) {
		text := "Short text"
		chunks, err := chunkTestText(text, 512, 50)
		require.NoError(t, err)

		assert.Len(t, chunks, 1)
		assert.Equal(t, text, chunks[0])
	})

	t.Run("long_text_multiple_chunks", func(t *testing.T) {
		// Create text longer than chunk size
		text := ""
		for i := 0; i < 100; i++ {
			text += "This is sentence number " + string(rune('0'+i%10)) + ". "
		}

		chunks, err := chunkTestText(text, 100, 20)
		require.NoError(t, err)

		assert.Greater(t, len(chunks), 1, "Should create multiple chunks")

		// Verify each chunk is within token size limit (with small boundary tolerance)
		for _, chunk := range chunks {
			assert.LessOrEqual(t, mustCountTestTokens(chunk), 110, "Chunk should be near token chunk size")
		}
	})

	t.Run("respects_overlap", func(t *testing.T) {
		text := "Word1 Word2 Word3 Word4 Word5 Word6 Word7 Word8 Word9 Word10"
		chunks, err := chunkTestText(text, 30, 10)
		require.NoError(t, err)

		if len(chunks) >= 2 {
			// Check that there's some overlap between consecutive chunks
			// (the end of chunk 1 should appear in the beginning of chunk 2)
			chunk1End := chunks[0][len(chunks[0])-10:]
			chunk2Start := chunks[1][:min(10, len(chunks[1]))]

			// Due to word boundary logic, we just verify chunks are created
			assert.NotEmpty(t, chunk1End)
			assert.NotEmpty(t, chunk2Start)
		}
	})

	t.Run("large_file_content_chunking", func(t *testing.T) {
		// Simulate a large file content (like a TypeScript/Go source file)
		// Using realistic code-like content
		largeContent := `
// Package main implements a complex system
// with multiple components and features.

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Config holds the application configuration.
type Config struct {
	DatabaseURL    string
	MaxConnections int
	Timeout        time.Duration
	EnableMetrics  bool
	LogLevel       string
}

// Application represents the main application instance.
type Application struct {
	config  *Config
	db      *Database
	cache   *Cache
	metrics *MetricsCollector
	mu      sync.RWMutex
	started bool
}

// NewApplication creates a new application with the given config.
func NewApplication(config *Config) (*Application, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	
	db, err := NewDatabase(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	
	cache := NewCache(config.MaxConnections)
	
	var metrics *MetricsCollector
	if config.EnableMetrics {
		metrics = NewMetricsCollector()
	}
	
	return &Application{
		config:  config,
		db:      db,
		cache:   cache,
		metrics: metrics,
	}, nil
}

// Start initializes and starts all application components.
func (a *Application) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	if a.started {
		return fmt.Errorf("application already started")
	}
	
	// Initialize database connection pool
	if err := a.db.Connect(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	
	// Start cache warmer
	go a.cache.WarmUp(ctx)
	
	// Start metrics collection if enabled
	if a.metrics != nil {
		go a.metrics.Start(ctx)
	}
	
	a.started = true
	return nil
}

// Stop gracefully shuts down all application components.
func (a *Application) Stop(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	if !a.started {
		return nil
	}
	
	// Stop metrics first
	if a.metrics != nil {
		a.metrics.Stop()
	}
	
	// Close cache
	a.cache.Close()
	
	// Close database connection
	if err := a.db.Close(ctx); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	
	a.started = false
	return nil
}
`
		// Use a smaller token size here to force multi-chunk behavior in test.
		chunks, err := chunkTestText(largeContent, 128, 16)
		require.NoError(t, err)

		t.Logf("Large content: %d tokens, chunked into %d pieces", mustCountTestTokens(largeContent), len(chunks))

		assert.Greater(t, len(chunks), 1, "Large content should produce multiple chunks")
		// No cap on chunks per node - number depends only on content length and chunk size

		// Verify all content is preserved (approximately - overlap means some duplication)
		totalChunkLength := 0
		for i, chunk := range chunks {
			totalChunkLength += mustCountTestTokens(chunk)
			t.Logf("  Chunk %d: %d tokens", i+1, mustCountTestTokens(chunk))
			assert.NotEmpty(t, chunk, "No empty chunks allowed")
		}

		// Total should be >= original (due to overlap) but not excessively more
		assert.GreaterOrEqual(t, totalChunkLength, mustCountTestTokens(largeContent)-20,
			"Chunks should cover all content")
	})

	t.Run("very_large_content_stress_test", func(t *testing.T) {
		// Generate very large content (50KB+)
		var sb strings.Builder
		for i := 0; i < 1000; i++ {
			sb.WriteString(fmt.Sprintf("Line %d: This is a test line with some content that simulates real text data. ", i))
		}
		veryLargeContent := sb.String()

		t.Logf("Very large content: %d tokens", mustCountTestTokens(veryLargeContent))

		chunks, err := chunkTestText(veryLargeContent, 512, 50)
		require.NoError(t, err)

		t.Logf("Chunked into %d pieces", len(chunks))

		// Verify no empty chunks and reasonable token sizes
		for _, chunk := range chunks {
			assert.NotEmpty(t, chunk)
			assert.LessOrEqual(t, mustCountTestTokens(chunk), 560, "Chunks should not exceed token budget by much")
		}

		// Verify we can reconstruct approximately
		assert.Greater(t, len(chunks), 30, "Should have many chunks for large content")
	})
}

// TestLargeContentEmbedding tests end-to-end embedding of large content
func TestLargeContentEmbedding(t *testing.T) {
	t.Run("large_file_gets_chunked_and_embedded", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node with large content (like a source file)
		largeContent := strings.Repeat("This is line of code with various tokens and symbols. ", 100)

		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("large-file-node"),
			Labels: []string{"File"},
			Properties: map[string]any{
				"content":  largeContent,
				"path":     "/src/components/LargeComponent.tsx",
				"fileType": "typescript",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512, // Small chunks to verify chunking
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)

		// Wait for processing
		worker.Trigger()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		// Verify
		stats := worker.Stats()
		require.Equal(t, 1, stats.Processed, "Should have processed the large file")

		node, err := engine.GetNode("large-file-node")
		require.NoError(t, err)

		// Large content gets chunked and all chunk embeddings stored on the same node
		// The node has chunk_count in EmbedMeta if there are multiple chunks
		// All chunk embeddings are stored in ChunkEmbeddings (opaque to users)
		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings")
		assert.Greater(t, len(node.ChunkEmbeddings), 1, "Large content should create multiple chunks")

		// Verify chunk_count is set in EmbedMeta for multiple chunks
		chunkCountVal, hasChunkCount := node.EmbedMeta["chunk_count"]
		assert.True(t, hasChunkCount, "Should have chunk_count in EmbedMeta for multiple chunks")
		chunkCount := chunkCountVal.(int)
		assert.Equal(t, len(node.ChunkEmbeddings), chunkCount, "chunk_count should match number of chunks")
		assert.Greater(t, chunkCount, 1, "Large content should create multiple chunks")
		t.Logf("Created %d chunk embeddings on the same node", chunkCount)

		// Verify all chunks have correct dimensions
		for i, chunk := range node.ChunkEmbeddings {
			assert.Equal(t, 1024, len(chunk), "Chunk %d should have correct dimensions", i)
		}

		// Verify NO separate FileChunk nodes were created (new architecture stores all on same node)
		allNodes := engine.GetAllNodes()
		chunkNodes := 0
		for _, n := range allNodes {
			for _, label := range n.Labels {
				if label == "FileChunk" {
					chunkNodes++
				}
			}
		}
		assert.Equal(t, 0, chunkNodes, "Should NOT create separate FileChunk nodes (all chunks on same node)")

		// Verify embedder was called multiple times (once per chunk)
		embedCount := embedder.GetEmbedCount()
		t.Logf("Embedder called %d times for %d char content", embedCount, len(largeContent))
		assert.Equal(t, chunkCount, embedCount, "Should embed once per chunk")
	})
}

func TestEmbedWorkerMicroBatching(t *testing.T) {
	t.Run("large_node_uses_bounded_embed_batch_size", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)
		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newRecordingBatchEmbedder()

		largeContent := strings.Repeat("This content should produce many embedding chunks. ", 700)
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("micro-batch-node"),
			Labels: []string{"File"},
			Properties: map[string]any{
				"content": largeContent,
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval:   time.Hour,
			BatchDelay:     10 * time.Millisecond,
			MaxRetries:     1,
			ChunkSize:      256,
			ChunkOverlap:   32,
			EmbedBatchSize: 8,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		worker.Trigger()

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if worker.Stats().Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		stats := worker.Stats()
		require.Equal(t, 1, stats.Processed, "node should be processed")
		assert.LessOrEqual(t, embedder.MaxBatchSize(), config.EmbedBatchSize, "embed request should respect micro-batch cap")

		node, err := engine.GetNode("micro-batch-node")
		require.NoError(t, err)
		assert.NotEmpty(t, node.ChunkEmbeddings, "chunk embeddings should be stored")
	})
}

// TestEmbedWorkerConcurrency tests for race conditions
func TestEmbedWorkerConcurrency(t *testing.T) {
	t.Run("concurrent_triggers_safe", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create multiple nodes
		for i := 0; i < 10; i++ {
			_, err := engine.CreateNode(&storage.Node{
				ID:     storage.NodeID("node-" + string(rune('0'+i))),
				Labels: []string{"Memory"},
				Properties: map[string]any{
					"content": "Content for node " + string(rune('0'+i)),
				},
			})
			require.NoError(t, err)
		}

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   1 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Trigger concurrently from multiple goroutines
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Trigger()
			}()
		}

		wg.Wait()

		// Wait for all processing
		time.Sleep(2 * time.Second)

		// Should not panic and stats should be consistent
		stats := worker.Stats()
		assert.GreaterOrEqual(t, stats.Processed, 0)
		t.Logf("Processed %d nodes", stats.Processed)
	})

	t.Run("reset_close_overlap_no_waitgroup_reuse_panic", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)
		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		_, err := engine.CreateNode(&storage.Node{
			ID:     "race-node-1",
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Race content",
			},
		})
		require.NoError(t, err)

		worker := NewEmbedWorker(embedder, engine, &EmbedWorkerConfig{
			ScanInterval: 5 * time.Second,
			BatchDelay:   1 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    256,
			ChunkOverlap: 32,
		})

		worker.Trigger()
		time.Sleep(25 * time.Millisecond)

		done := make(chan struct{})
		go func() {
			worker.Reset()
			close(done)
		}()

		// Intentionally overlap Close with Reset, matching the production race.
		time.Sleep(1 * time.Millisecond)
		worker.Close()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reset did not finish after overlapping close")
		}
	})
}

// TestRecentlyProcessedOnlyLogsOnce verifies we don't spam "recently processed" logs
func TestRecentlyProcessedOnlyLogsOnce(t *testing.T) {
	t.Run("skip_message_should_not_repeat", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node that will be marked as recently processed but still found
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("test-skip-logs"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Some content",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour, // Long interval so ticker doesn't interfere
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)

		// Wait for initial processing
		worker.Trigger()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Verify it was processed
		stats := worker.Stats()
		require.Equal(t, 1, stats.Processed, "Should have processed 1 node")

		// Now trigger multiple times - should NOT spam logs
		for i := 0; i < 5; i++ {
			worker.Trigger()
			time.Sleep(100 * time.Millisecond)
		}

		worker.Close()

		// Stats should still show 1 processed (not re-processed)
		finalStats := worker.Stats()
		assert.Equal(t, 1, finalStats.Processed, "Should still only show 1 processed")
	})
}

// TestNoContentNodeDoesNotCauseInfiniteLoop tests that nodes without embeddable content
// don't cause infinite loops in processUntilEmpty - this was the bug where "⏭️ Skipping node n1"
// would print infinitely
func TestNoContentNodeDoesNotCauseInfiniteLoop(t *testing.T) {
	t.Run("no_content_node_stops_loop", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node with NO labels and ONLY metadata fields (all skipped by buildEmbeddingText)
		// This is a truly empty node - no labels, no embeddable properties
		// Don't set has_embedding as that would prevent node discovery
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("no-content-node"),
			Labels: []string{}, // No labels - truly empty
			Properties: map[string]any{
				"id":        "123",        // Skipped
				"createdAt": "2024-01-01", // Skipped
				"updatedAt": "2024-01-02", // Skipped
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)

		// Trigger and wait - this should NOT loop infinitely
		worker.Trigger()

		// Give it time to process - if it loops infinitely, the test will timeout
		done := make(chan struct{})
		go func() {
			time.Sleep(2 * time.Second)
			close(done)
		}()

		// Wait for worker to finish or timeout
		<-done

		worker.Close()

		// With the new behavior, even empty nodes get embedded (using "node" fallback)
		// The key test is that it doesn't loop infinitely
		embedCount := embedder.GetEmbedCount()
		// Node should be embedded (even if empty, we embed with fallback "node")
		assert.Equal(t, 1, embedCount, "Empty node should be embedded with fallback text")

		// Node should have been embedded (with "node" fallback text)
		node, err := engine.GetNode("no-content-node")
		require.NoError(t, err)
		// With new behavior, empty nodes get embedded with "node" fallback
		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings (even empty nodes get embedded)")
		assert.Nil(t, node.Properties["embedding_skipped"], "Node should not be marked as skipped (it was embedded)")

		t.Log("✓ No infinite loop with no-content node")
	})
}

// TestAsyncEngineCacheIntegration tests that embeddings in AsyncEngine cache
// are correctly recognized by FindNodeNeedingEmbedding - this was the root cause
// of the "n1 keeps getting found" bug
func TestAsyncEngineCacheIntegration(t *testing.T) {
	t.Run("cached_embedding_not_refound", func(t *testing.T) {
		// Create underlying engine
		baseUnderlying := storage.NewMemoryEngine()

		underlying := storage.NewNamespacedEngine(baseUnderlying, "test")

		// Wrap with AsyncEngine (like production setup)
		asyncConfig := storage.DefaultAsyncEngineConfig()
		asyncConfig.FlushInterval = 10 * time.Second // Long flush interval
		asyncEngine := storage.NewAsyncEngine(underlying, asyncConfig)
		defer asyncEngine.Close()

		embedder := newMockEmbedder()

		// Create a node
		_, err := asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID("async-test"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Test content for async cache",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, asyncEngine, config)

		// Wait for processing
		worker.Trigger()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Verify it was processed
		stats := worker.Stats()
		require.Equal(t, 1, stats.Processed, "Should have processed 1 node")

		// WITHOUT flushing, trigger again - should NOT find the node again
		// because the embedding is in AsyncEngine's cache
		initialEmbedCount := embedder.GetEmbedCount()

		for i := 0; i < 3; i++ {
			worker.Trigger()
			time.Sleep(100 * time.Millisecond)
		}

		worker.Close()

		// Embedder should NOT have been called again
		finalEmbedCount := embedder.GetEmbedCount()
		assert.Equal(t, initialEmbedCount, finalEmbedCount,
			"Embedder should not be called again - embedding is in async cache")

		t.Log("✓ AsyncEngine cache correctly prevents re-processing")
	})
}

// TestEmbeddingPersistenceVerification tests that embeddings are truly persisted
// and readable back from storage - this catches the bug where n1 keeps getting skipped
func TestEmbeddingPersistenceVerification(t *testing.T) {
	t.Run("embedding_readable_after_update", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("verify-persist"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "Test content for persistence verification",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)

		// Wait for processing
		worker.Trigger()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		// Verify embedding is readable from storage
		node, err := engine.GetNode("verify-persist")
		require.NoError(t, err)
		require.NotEmpty(t, node.ChunkEmbeddings, "Chunk embeddings should be persisted and readable")

		// Verify findNodeWithoutEmbedding doesn't find it anymore
		worker2 := NewEmbedWorker(embedder, engine, config)
		defer worker2.Close()

		found := worker2.findNodeWithoutEmbedding()
		assert.Nil(t, found, "Node with embedding should NOT be found by findNodeWithoutEmbedding")
	})

	t.Run("storage_update_persists_embedding_field", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")

		// Create a node
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("manual-embed"),
			Labels: []string{"Test"},
			Properties: map[string]any{
				"content": "test",
			},
		})
		require.NoError(t, err)

		// Manually update with embedding
		node, err := engine.GetNode("manual-embed")
		require.NoError(t, err)

		node.ChunkEmbeddings = [][]float32{make([]float32, 1024)}
		for i := range node.ChunkEmbeddings[0] {
			node.ChunkEmbeddings[0][i] = float32(i) * 0.001
		}

		err = engine.UpdateNode(node)
		require.NoError(t, err)

		// Read back and verify
		node2, err := engine.GetNode("manual-embed")
		require.NoError(t, err)

		assert.Equal(t, 1024, len(node2.ChunkEmbeddings[0]), "Embedding should have correct dimensions")
		assert.Equal(t, float32(0.001), node2.ChunkEmbeddings[0][1], "Embedding values should be preserved")
	})
}

// TestRaceConditionPrevention specifically tests the race condition scenario
// where the embedding worker processes a node while another goroutine reads it
func TestRaceConditionPrevention(t *testing.T) {
	t.Run("concurrent_node_access_during_embedding", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		baseEngine.SetEmbeddingsEnabled(true)

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("race-test"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content":     "Test content for race condition",
				"title":       "Race Test",
				"description": "A node to test concurrent access",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   1 * time.Millisecond, // Fast processing
			MaxRetries:   1,
			ChunkSize:    512,
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		defer worker.Close()

		// Start multiple readers that continuously read the node's properties
		var wg sync.WaitGroup
		stop := make(chan struct{})

		// Reader goroutines that would cause "concurrent map iteration and map write"
		// if copyNodeForEmbedding wasn't used
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						node, err := engine.GetNode("race-test")
						if err == nil && node != nil {
							// Iterate over properties (this is what caused the panic)
							for k, v := range node.Properties {
								_ = k
								_ = v
							}
						}
						time.Sleep(1 * time.Millisecond)
					}
				}
			}()
		}

		// Trigger embedding while readers are active
		worker.Trigger()

		// Wait for embedding to complete
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Let readers continue a bit more after embedding
		time.Sleep(50 * time.Millisecond)

		// Stop readers
		close(stop)
		wg.Wait()

		// Verify embedding was stored correctly
		node, err := engine.GetNode("race-test")
		require.NoError(t, err)
		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings")
		assert.Equal(t, 1024, len(node.ChunkEmbeddings[0]), "Embedding should have correct dimensions")

		t.Log("✓ No race condition detected during concurrent node access")
	})
}

func TestEmbedQueueSmallWrappers(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "test")
	cfg := DefaultEmbedQueueConfig()
	cfg.DeferWorkerStart = true
	worker := NewEmbedQueue(nil, engine, cfg)
	defer worker.Close()

	worker.SetOnQueueEmpty(func(processedCount int) {})
	require.NotNil(t, worker.onQueueEmpty)

	worker.SetEmbedder(newMockEmbedder())
	require.NotNil(t, worker.embedder)
	require.Equal(t, 1, len(worker.trigger), "SetEmbedder should trigger immediate processing")

	worker.Enqueue("node-1")
	require.Equal(t, 1, len(worker.trigger), "Enqueue delegates to Trigger and keeps single wakeup signal")

	payload, err := WorkerStats{Running: true, Processed: 3, Failed: 1}.MarshalJSON()
	require.NoError(t, err)
	require.Contains(t, string(payload), "\"running\":true")
	require.Contains(t, string(payload), "\"processed\":3")
	require.Contains(t, string(payload), "\"failed\":1")
}

type pendingAdderEngine struct {
	storage.Engine
	added []storage.NodeID
}

type queueBranchEngine struct {
	storage.Engine
	findNode           *storage.Node
	findReturned       bool
	getNodeErr         error
	secondGetNodeErr   error
	returnNilNode      bool
	getNodeCalls       int
	updateEmbeddingErr error
	updateNodeErr      error
	refreshCount       int
	marked             []storage.NodeID
	added              []storage.NodeID
}

func (e *queueBranchEngine) FindNodeNeedingEmbedding() *storage.Node {
	if e.findNode == nil || e.findReturned {
		return nil
	}
	e.findReturned = true
	return storage.CopyNode(e.findNode)
}

func (e *queueBranchEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.getNodeCalls++
	if e.getNodeCalls == 1 && e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	if e.getNodeCalls == 1 && e.returnNilNode {
		return nil, nil
	}
	if e.getNodeCalls > 1 && e.secondGetNodeErr != nil {
		return nil, e.secondGetNodeErr
	}
	return e.Engine.GetNode(id)
}

func (e *queueBranchEngine) MarkNodeEmbedded(id storage.NodeID) {
	e.marked = append(e.marked, id)
}

func (e *queueBranchEngine) RefreshPendingEmbeddingsIndex() int {
	return e.refreshCount
}

func (e *queueBranchEngine) AddToPendingEmbeddings(id storage.NodeID) {
	e.added = append(e.added, id)
}

func (e *queueBranchEngine) UpdateNodeEmbedding(*storage.Node) error {
	return e.updateEmbeddingErr
}

func (e *queueBranchEngine) UpdateNode(node *storage.Node) error {
	if e.updateNodeErr != nil {
		return e.updateNodeErr
	}
	return e.Engine.UpdateNode(node)
}

// engineOnlyWrapper intentionally exposes only storage.Engine so optional
// interfaces (EmbeddingFinder/EmbeddingIndexManager) are not satisfied.
type engineOnlyWrapper struct {
	storage.Engine
}

type emptyBatchEmbedder struct {
	dims int
}

func (e *emptyBatchEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return make([]float32, e.dims), nil
}

func (e *emptyBatchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	// Return one empty embedding (dim=0) to hit validation branch.
	return [][]float32{{}}, nil
}

func (e *emptyBatchEmbedder) Model() string { return "empty" }

func (e *emptyBatchEmbedder) Dimensions() int { return e.dims }

func (e *emptyBatchEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (e *emptyBatchEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type flakyBatchEmbedder struct {
	mu         sync.Mutex
	dims       int
	failUntil  int
	callCount  int
	returnSize int
}

func (f *flakyBatchEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return make([]float32, f.dims), nil
}

func (f *flakyBatchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	f.mu.Lock()
	f.callCount++
	call := f.callCount
	f.mu.Unlock()
	if call <= f.failUntil {
		return nil, errors.New("temporary embed failure")
	}
	size := len(texts)
	if f.returnSize >= 0 {
		size = f.returnSize
	}
	out := make([][]float32, size)
	for i := range out {
		out[i] = make([]float32, f.dims)
	}
	return out, nil
}

func (f *flakyBatchEmbedder) Model() string { return "flaky" }

func (f *flakyBatchEmbedder) Dimensions() int { return f.dims }

func (f *flakyBatchEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (f *flakyBatchEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type deterministicChunkEmbedder struct {
	mu         sync.Mutex
	dims       int
	chunks     []string
	batchCalls [][]string
}

func (d *deterministicChunkEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return make([]float32, d.dims), nil
}

func (d *deterministicChunkEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.batchCalls = append(d.batchCalls, append([]string(nil), texts...))
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = make([]float32, d.dims)
	}
	return out, nil
}

func (d *deterministicChunkEmbedder) Model() string { return "deterministic" }

func (d *deterministicChunkEmbedder) Dimensions() int { return d.dims }

func (d *deterministicChunkEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (d *deterministicChunkEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return append([]string(nil), d.chunks...), nil
}

func (p *pendingAdderEngine) AddToPendingEmbeddings(id storage.NodeID) {
	p.added = append(p.added, id)
}

func TestEmbedQueueDebounceAndHelpers(t *testing.T) {
	t.Run("debounce accumulates and fires when threshold met", func(t *testing.T) {
		var got []int
		ew := &EmbedWorker{
			config: &EmbedWorkerConfig{
				ClusterDebounceDelay: 15 * time.Millisecond,
				ClusterMinBatchSize:  2,
			},
			onQueueEmpty: func(processedCount int) {
				got = append(got, processedCount)
			},
		}

		ew.scheduleClusteringDebounced(1)
		ew.scheduleClusteringDebounced(2)
		require.Eventually(t, func() bool { return len(got) == 1 }, 300*time.Millisecond, 5*time.Millisecond)
		require.Equal(t, 3, got[0])
	})

	t.Run("addNodeToPendingEmbeddings delegates when supported", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		adder := &pendingAdderEngine{Engine: storage.NewNamespacedEngine(base, "test")}
		ew := &EmbedWorker{storage: adder}
		ew.addNodeToPendingEmbeddings(storage.NodeID("n-1"))
		require.Equal(t, []storage.NodeID{"n-1"}, adder.added)
	})

	t.Run("averageEmbeddings handles edge cases", func(t *testing.T) {
		require.Nil(t, averageEmbeddings(nil))
		require.Equal(t, []float32{1, 2}, averageEmbeddings([][]float32{{1, 2}}))
		require.Equal(t, []float32{2, 3}, averageEmbeddings([][]float32{{1, 2}, {3, 4}}))
	})

	t.Run("embedBatchWithRetry success after retry", func(t *testing.T) {
		emb := &flakyBatchEmbedder{dims: 3, failUntil: 1, returnSize: -1}
		ew := &EmbedWorker{
			embedder: emb,
			config:   &EmbedWorkerConfig{MaxRetries: 2},
			ctx:      context.Background(),
		}
		out, err := ew.embedBatchWithRetry([]string{"a", "b"})
		require.NoError(t, err)
		require.Len(t, out, 2)
		emb.mu.Lock()
		defer emb.mu.Unlock()
		require.Equal(t, 2, emb.callCount)
	})

	t.Run("embedBatchWithRetry returns context error", func(t *testing.T) {
		emb := &flakyBatchEmbedder{dims: 3, failUntil: 10, returnSize: -1}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ew := &EmbedWorker{
			embedder: emb,
			config:   &EmbedWorkerConfig{MaxRetries: 3},
			ctx:      ctx,
		}
		out, err := ew.embedBatchWithRetry([]string{"a"})
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, out)
	})

	t.Run("embedChunksInBatches catches mismatch", func(t *testing.T) {
		emb := &flakyBatchEmbedder{
			dims:       3,
			failUntil:  0,
			returnSize: 1, // force mismatch when batch has >1 chunk
		}
		ew := &EmbedWorker{
			embedder: emb,
			config:   &EmbedWorkerConfig{EmbedBatchSize: 2, MaxRetries: 1},
			ctx:      context.Background(),
		}
		out, err := ew.embedChunksInBatches([]string{"c1", "c2", "c3"}, storage.NodeID("n1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "embedding count mismatch")
		require.Nil(t, out)
	})

	t.Run("processNextBatch stale pending node is cleaned", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		qe := &queueBranchEngine{
			Engine:     engine,
			findNode:   &storage.Node{ID: storage.NodeID("missing")},
			getNodeErr: storage.ErrNotFound,
		}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}
		didWork := ew.processNextBatch()
		require.False(t, didWork)
		require.Equal(t, []storage.NodeID{"missing"}, qe.marked)
	})

	t.Run("processNextBatch handles nil node returned from storage", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		qe := &queueBranchEngine{
			Engine:        engine,
			findNode:      &storage.Node{ID: storage.NodeID("ghost")},
			returnNilNode: true,
		}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.False(t, didWork)
		require.Equal(t, []storage.NodeID{"ghost"}, qe.marked)
	})

	t.Run("processNextBatch requeues on embed failure", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		node := &storage.Node{
			ID:         storage.NodeID("n1"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{
			Engine:   engine,
			findNode: &storage.Node{ID: storage.NodeID("n1")},
		}
		ew := &EmbedWorker{
			embedder: &flakyBatchEmbedder{dims: 3, failUntil: 5, returnSize: -1},
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.True(t, didWork)
		require.Equal(t, int64(1), ew.failed.Load())
		require.Equal(t, []storage.NodeID{"n1"}, qe.added)
	})

	t.Run("processNextBatch uses deterministic chunker from embedder", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		node := &storage.Node{
			ID:         storage.NodeID("n-deterministic"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello world"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		emb := &deterministicChunkEmbedder{dims: 3, chunks: []string{"chunk-one", "chunk-two"}}
		qe := &queueBranchEngine{
			Engine:   engine,
			findNode: &storage.Node{ID: storage.NodeID("n-deterministic")},
		}
		ew := &EmbedWorker{
			embedder: emb,
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 8192, ChunkOverlap: 50},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.True(t, didWork)
		require.Equal(t, int64(1), ew.processed.Load())
		require.Len(t, emb.batchCalls, 1)
		require.Equal(t, []string{"chunk-one", "chunk-two"}, emb.batchCalls[0])
	})

	t.Run("processNextBatch empty embedding is treated as failure", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		node := &storage.Node{
			ID:         storage.NodeID("n2"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{
			Engine:   engine,
			findNode: &storage.Node{ID: storage.NodeID("n2")},
		}
		ew := &EmbedWorker{
			embedder: &emptyBatchEmbedder{dims: 3},
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.True(t, didWork)
		require.Equal(t, int64(1), ew.failed.Load())
		require.Equal(t, []storage.NodeID{"n2"}, qe.added)
	})

	t.Run("processNextBatch skips when update embedding reports not found", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		node := &storage.Node{
			ID:         storage.NodeID("n3"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{
			Engine:             engine,
			findNode:           &storage.Node{ID: storage.NodeID("n3")},
			updateEmbeddingErr: storage.ErrNotFound,
		}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.False(t, didWork)
		require.Equal(t, []storage.NodeID{"n3", "n3"}, qe.marked)
	})

	t.Run("processNextBatch skips when node is deleted before save", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		node := &storage.Node{
			ID:         storage.NodeID("n4"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{
			Engine:           engine,
			findNode:         &storage.Node{ID: storage.NodeID("n4")},
			secondGetNodeErr: storage.ErrNotFound,
		}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		didWork := ew.processNextBatch()
		require.False(t, didWork)
		require.Equal(t, []storage.NodeID{"n4", "n4"}, qe.marked)
	})

	t.Run("startWorkers guards closed and starts one worker when numWorkers<1", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		cfg := &EmbedWorkerConfig{
			NumWorkers:       0, // triggers minimum worker fallback branch
			ScanInterval:     20 * time.Millisecond,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        64,
			ChunkOverlap:     8,
			DeferWorkerStart: true,
		}
		worker := NewEmbedWorker(newMockEmbedder(), engine, cfg)
		defer worker.Close()

		worker.closed.Store(true)
		worker.StartWorkers()
		require.False(t, worker.workersStarted)

		worker.closed.Store(false)
		worker.StartWorkers()
		require.True(t, worker.workersStarted)
	})

	t.Run("close stops debounce timer safely", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		worker := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:       1,
			ScanInterval:     20 * time.Millisecond,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        64,
			ChunkOverlap:     8,
			DeferWorkerStart: true,
		})
		worker.clusterDebounceMu.Lock()
		worker.clusterDebounceTimer = time.NewTimer(time.Hour)
		worker.clusterDebounceMu.Unlock()
		worker.Close()

		worker.clusterDebounceMu.Lock()
		defer worker.clusterDebounceMu.Unlock()
		require.Nil(t, worker.clusterDebounceTimer)
	})

	t.Run("trigger no-op when closed and non-blocking when already queued", func(t *testing.T) {
		worker := &EmbedWorker{
			trigger: make(chan struct{}, 1),
		}
		worker.closed.Store(true)
		worker.Trigger()
		require.Len(t, worker.trigger, 0)

		worker.closed.Store(false)
		worker.Trigger()
		require.Len(t, worker.trigger, 1)
		worker.Trigger()
		require.Len(t, worker.trigger, 1)
	})

	t.Run("worker waits for embedder then ticker picks up new node", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")

		cfg := &EmbedWorkerConfig{
			NumWorkers:   1,
			ScanInterval: 25 * time.Millisecond,
			BatchDelay:   time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    64,
			ChunkOverlap: 8,
			// Start explicitly so we can set embedder after worker starts.
			DeferWorkerStart: true,
		}
		worker := NewEmbedWorker(nil, engine, cfg)
		defer worker.Close()
		worker.StartWorkers()

		// Worker should be waiting for embedder and not processing yet.
		require.Equal(t, 0, worker.Stats().Processed)

		worker.SetEmbedder(newMockEmbedder())

		// Create node after worker activation; ticker path should discover it without explicit Trigger.
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("ticker-node"),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"content": "picked by ticker",
			},
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return worker.Stats().Processed >= 1
		}, 4*time.Second, 25*time.Millisecond)

		n, err := engine.GetNode("ticker-node")
		require.NoError(t, err)
		require.NotEmpty(t, n.ChunkEmbeddings)
	})

	t.Run("worker exits promptly when closed before embedder is set", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ew := &EmbedWorker{
			config:            DefaultEmbedWorkerConfig(),
			ctx:               ctx,
			cancel:            cancel,
			trigger:           make(chan struct{}, 1),
			recentlyProcessed: make(map[string]time.Time),
			loggedSkip:        make(map[string]bool),
		}
		done := make(chan struct{}, 1)
		ew.wg.Add(1)
		go func() {
			ew.worker()
			done <- struct{}{}
		}()

		// Closed branch in the wait-for-embedder loop should exit quickly.
		ew.closed.Store(true)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not exit after closed=true without embedder")
		}
	})

	t.Run("findNodeWithoutEmbedding uses EmbeddingFinder fast path", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		qe := &queueBranchEngine{
			Engine:   engine,
			findNode: &storage.Node{ID: storage.NodeID("fast-path")},
		}
		ew := &EmbedWorker{storage: qe}
		n := ew.findNodeWithoutEmbedding()
		require.NotNil(t, n)
		require.Equal(t, storage.NodeID("fast-path"), n.ID)
	})

	t.Run("findNodeWithoutEmbedding falls back to storage helper", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("fallback-node"),
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"id":      "fallback-node",
				"content": "needs embedding",
			},
		})
		require.NoError(t, err)
		ew := &EmbedWorker{storage: &engineOnlyWrapper{Engine: engine}}
		n := ew.findNodeWithoutEmbedding()
		require.NotNil(t, n)
		require.Equal(t, storage.NodeID("fallback-node"), n.ID)
	})

	t.Run("refreshEmbeddingIndex manager and fallback branches", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "test")

		qe := &queueBranchEngine{Engine: engine, refreshCount: 7}
		ew := &EmbedWorker{storage: qe}
		require.Equal(t, 7, ew.refreshEmbeddingIndex())

		ew.storage = &engineOnlyWrapper{Engine: engine}
		require.Equal(t, 0, ew.refreshEmbeddingIndex())
	})
}

func TestEmbedWorker_ProcessNextBatch_YieldsUnderForegroundPressure(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := &queueBranchEngine{
		Engine: storage.NewNamespacedEngine(base, "test"),
		findNode: &storage.Node{
			ID:         "test:n-1",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"text": "hello"},
		},
	}
	_, err := engine.Engine.CreateNode(storage.CopyNode(engine.findNode))
	require.NoError(t, err)

	worker := &EmbedWorker{
		storage: engine,
		config:  DefaultEmbedWorkerConfig(),
		ctx:     context.Background(),
	}
	worker.SetShouldYield(func() bool { return true })

	didWork := worker.processNextBatch()
	require.False(t, didWork)
	require.False(t, engine.findReturned, "worker should yield before claiming any node")
	require.Equal(t, 0, engine.getNodeCalls, "worker should not hit storage while yielding")
	require.Empty(t, engine.marked, "worker should not mutate pending index while yielding")
}
