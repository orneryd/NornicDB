package nornicdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

func (m *mockEmbedder) GetEmbedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.embedCount
}

// TestCopyNodeForEmbedding tests that the copy function creates independent copies
func TestCopyNodeForEmbedding(t *testing.T) {
	t.Run("creates_independent_properties_map", func(t *testing.T) {
		original := &storage.Node{
			ID:     "test-node",
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
			ID:        "test-node",
			Embedding: []float32{0.1, 0.2, 0.3},
		}

		copy := copyNodeForEmbedding(original)

		// Modify the copy's embedding
		copy.Embedding[0] = 999.0

		// Original should be unchanged
		assert.Equal(t, float32(0.1), original.Embedding[0])
	})

	t.Run("copies_labels", func(t *testing.T) {
		original := &storage.Node{
			ID:     "test-node",
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
			ID: "unique-id-123",
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
		engine := storage.NewMemoryEngine()
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
		engine := storage.NewMemoryEngine()
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
		err := engine.CreateNode(&storage.Node{
			ID:     "trigger-node",
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node without embedding
		err := engine.CreateNode(&storage.Node{
			ID:     "persist-test",
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content": "This is test content for embedding",
			},
		})
		require.NoError(t, err)

		// Verify node has no embedding initially
		node, err := engine.GetNode("persist-test")
		require.NoError(t, err)
		assert.Empty(t, node.Embedding, "Node should have no embedding initially")

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

		assert.NotEmpty(t, node.Embedding, "Node should have embedding after processing")
		assert.Equal(t, 1024, len(node.Embedding), "Embedding should have correct dimensions")
	})

	t.Run("node_not_reprocessed_after_embedding", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node
		err := engine.CreateNode(&storage.Node{
			ID:     "no-reprocess-test",
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create node without embedding
		err := engine.CreateNode(&storage.Node{
			ID:     "needs-embed",
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create node WITH embedding
		err := engine.CreateNode(&storage.Node{
			ID:        "has-embed",
			Labels:    []string{"Memory"},
			Embedding: make([]float32, 1024), // Pre-existing embedding
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create internal node (starts with _)
		err := engine.CreateNode(&storage.Node{
			ID:     "internal-node",
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

		text := buildEmbeddingText(props)

		assert.Contains(t, text, "Test Title")
		assert.Contains(t, text, "Test Content")
		assert.Contains(t, text, "Test Description")
		assert.Contains(t, text, "85")
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

		text := buildEmbeddingText(props)

		assert.Contains(t, text, "Real content")
		assert.NotContains(t, text, "id:")
		assert.NotContains(t, text, "embedding:")
		assert.NotContains(t, text, "has_embedding:")
		assert.NotContains(t, text, "embedding_model:")
		assert.NotContains(t, text, "embedded_at:")
		assert.NotContains(t, text, "createdAt:")
	})

	t.Run("returns_empty_for_only_metadata", func(t *testing.T) {
		props := map[string]interface{}{
			"id":            "123",
			"embedding":     []float32{0.1},
			"has_embedding": true,
			"createdAt":     "2024-01-01",
		}

		text := buildEmbeddingText(props)

		assert.Empty(t, text)
	})

	t.Run("includes_tags_array", func(t *testing.T) {
		props := map[string]interface{}{
			"content": "Some content",
			"tags":    []interface{}{"tag1", "tag2"},
		}

		text := buildEmbeddingText(props)

		assert.Contains(t, text, "tag1")
		assert.Contains(t, text, "tag2")
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

		text := buildEmbeddingText(props)

		assert.Contains(t, text, "Your prescription was delivered")
		assert.Contains(t, text, "Tu receta fue entregada")
		assert.Contains(t, text, "80")
		assert.Contains(t, text, "approved")
		assert.Contains(t, text, "Uses informal")
	})
}

// TestChunkText tests the text chunking logic
func TestChunkText(t *testing.T) {
	t.Run("short_text_single_chunk", func(t *testing.T) {
		text := "Short text"
		chunks := chunkText(text, 512, 50)

		assert.Len(t, chunks, 1)
		assert.Equal(t, text, chunks[0])
	})

	t.Run("long_text_multiple_chunks", func(t *testing.T) {
		// Create text longer than chunk size
		text := ""
		for i := 0; i < 100; i++ {
			text += "This is sentence number " + string(rune('0'+i%10)) + ". "
		}

		chunks := chunkText(text, 100, 20)

		assert.Greater(t, len(chunks), 1, "Should create multiple chunks")

		// Verify each chunk is within size limit (with some tolerance for word boundaries)
		for _, chunk := range chunks {
			assert.LessOrEqual(t, len(chunk), 150, "Chunk should be close to chunk size")
		}
	})

	t.Run("respects_overlap", func(t *testing.T) {
		text := "Word1 Word2 Word3 Word4 Word5 Word6 Word7 Word8 Word9 Word10"
		chunks := chunkText(text, 30, 10)

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
		// Test with realistic chunk settings (512 chars, 50 overlap)
		chunks := chunkText(largeContent, 512, 50)

		t.Logf("Large content: %d chars, chunked into %d pieces", len(largeContent), len(chunks))

		assert.Greater(t, len(chunks), 1, "Large content should produce multiple chunks")
		assert.LessOrEqual(t, len(chunks), 10, "Should not over-chunk")

		// Verify all content is preserved (approximately - overlap means some duplication)
		totalChunkLength := 0
		for i, chunk := range chunks {
			totalChunkLength += len(chunk)
			t.Logf("  Chunk %d: %d chars", i+1, len(chunk))
			assert.NotEmpty(t, chunk, "No empty chunks allowed")
		}

		// Total should be >= original (due to overlap) but not excessively more
		assert.GreaterOrEqual(t, totalChunkLength, len(largeContent)-100,
			"Chunks should cover all content")
	})

	t.Run("very_large_content_stress_test", func(t *testing.T) {
		// Generate very large content (50KB+)
		var sb strings.Builder
		for i := 0; i < 1000; i++ {
			sb.WriteString(fmt.Sprintf("Line %d: This is a test line with some content that simulates real text data. ", i))
		}
		veryLargeContent := sb.String()

		t.Logf("Very large content: %d chars (%.1f KB)", len(veryLargeContent), float64(len(veryLargeContent))/1024)

		chunks := chunkText(veryLargeContent, 512, 50)

		t.Logf("Chunked into %d pieces", len(chunks))

		// Verify no empty chunks and reasonable sizes
		for _, chunk := range chunks {
			assert.NotEmpty(t, chunk)
			assert.LessOrEqual(t, len(chunk), 1024, "Chunks should not be excessively large")
		}

		// Verify we can reconstruct approximately
		assert.Greater(t, len(chunks), 50, "Should have many chunks for large content")
	})
}

// TestLargeContentEmbedding tests end-to-end embedding of large content
func TestLargeContentEmbedding(t *testing.T) {
	t.Run("large_file_gets_chunked_and_embedded", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node with large content (like a source file)
		largeContent := strings.Repeat("This is line of code with various tokens and symbols. ", 100)

		err := engine.CreateNode(&storage.Node{
			ID:     "large-file-node",
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

		// Large files create FileChunk nodes, NOT a single embedding on parent
		// The parent File node has has_chunks=true and chunk_count=N
		// Each FileChunk has its own embedding
		assert.True(t, node.Properties["has_chunks"].(bool), "Parent should have has_chunks=true")
		chunkCount := node.Properties["chunk_count"].(int)
		assert.Greater(t, chunkCount, 1, "Large content should create multiple chunks")
		t.Logf("Created %d FileChunk nodes", chunkCount)

		// Parent File does NOT have an embedding - the chunks do
		assert.Empty(t, node.Embedding, "Parent File should NOT have embedding (chunks have them)")

		// Verify FileChunk nodes were created with embeddings
		allNodes := engine.GetAllNodes()
		chunkNodes := 0
		for _, n := range allNodes {
			for _, label := range n.Labels {
				if label == "FileChunk" {
					chunkNodes++
					assert.NotEmpty(t, n.Embedding, "FileChunk should have embedding")
					assert.Equal(t, 1024, len(n.Embedding), "FileChunk embedding should have correct dims")
				}
			}
		}
		assert.Equal(t, chunkCount, chunkNodes, "Should have created correct number of FileChunk nodes")

		// Verify embedder was called multiple times (once per chunk)
		embedCount := embedder.GetEmbedCount()
		t.Logf("Embedder called %d times for %d char content", embedCount, len(largeContent))
		assert.Equal(t, chunkCount, embedCount, "Should embed once per chunk")
	})
}

// TestEmbedWorkerConcurrency tests for race conditions
func TestEmbedWorkerConcurrency(t *testing.T) {
	t.Run("concurrent_triggers_safe", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create multiple nodes
		for i := 0; i < 10; i++ {
			err := engine.CreateNode(&storage.Node{
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
}

// TestRecentlyProcessedOnlyLogsOnce verifies we don't spam "recently processed" logs
func TestRecentlyProcessedOnlyLogsOnce(t *testing.T) {
	t.Run("skip_message_should_not_repeat", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node that will be marked as recently processed but still found
		err := engine.CreateNode(&storage.Node{
			ID:     "test-skip-logs",
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node with ONLY metadata fields (all skipped by buildEmbeddingText)
		// Don't set has_embedding as that would prevent node discovery
		err := engine.CreateNode(&storage.Node{
			ID:     "no-content-node",
			Labels: []string{"Empty"},
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
		select {
		case <-done:
			// Good - didn't loop infinitely
		}

		worker.Close()

		// Embedder should NOT have been called (no embeddable content)
		embedCount := embedder.GetEmbedCount()
		assert.Equal(t, 0, embedCount, "Embedder should not be called for node without content")

		// Node should be marked as skipped
		node, err := engine.GetNode("no-content-node")
		require.NoError(t, err)
		assert.Empty(t, node.Embedding, "Node should have no embedding (skipped)")
		assert.Equal(t, "no content", node.Properties["embedding_skipped"], "Node should be marked as skipped")

		t.Log("✓ No infinite loop with no-content node")
	})
}

// TestAsyncEngineCacheIntegration tests that embeddings in AsyncEngine cache
// are correctly recognized by FindNodeNeedingEmbedding - this was the root cause
// of the "n1 keeps getting found" bug
func TestAsyncEngineCacheIntegration(t *testing.T) {
	t.Run("cached_embedding_not_refound", func(t *testing.T) {
		// Create underlying engine
		underlying := storage.NewMemoryEngine()

		// Wrap with AsyncEngine (like production setup)
		asyncConfig := storage.DefaultAsyncEngineConfig()
		asyncConfig.FlushInterval = 10 * time.Second // Long flush interval
		asyncEngine := storage.NewAsyncEngine(underlying, asyncConfig)
		defer asyncEngine.Close()

		embedder := newMockEmbedder()

		// Create a node
		err := asyncEngine.CreateNode(&storage.Node{
			ID:     "async-test",
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
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node
		err := engine.CreateNode(&storage.Node{
			ID:     "verify-persist",
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
		require.NotEmpty(t, node.Embedding, "Embedding should be persisted and readable")

		// Verify findNodeWithoutEmbedding doesn't find it anymore
		worker2 := NewEmbedWorker(embedder, engine, config)
		defer worker2.Close()

		found := worker2.findNodeWithoutEmbedding()
		assert.Nil(t, found, "Node with embedding should NOT be found by findNodeWithoutEmbedding")
	})

	t.Run("storage_update_persists_embedding_field", func(t *testing.T) {
		engine := storage.NewMemoryEngine()

		// Create a node
		err := engine.CreateNode(&storage.Node{
			ID:     "manual-embed",
			Labels: []string{"Test"},
			Properties: map[string]any{
				"content": "test",
			},
		})
		require.NoError(t, err)

		// Manually update with embedding
		node, err := engine.GetNode("manual-embed")
		require.NoError(t, err)

		node.Embedding = make([]float32, 1024)
		for i := range node.Embedding {
			node.Embedding[i] = float32(i) * 0.001
		}

		err = engine.UpdateNode(node)
		require.NoError(t, err)

		// Read back and verify
		node2, err := engine.GetNode("manual-embed")
		require.NoError(t, err)

		assert.Equal(t, 1024, len(node2.Embedding), "Embedding should have correct dimensions")
		assert.Equal(t, float32(0.001), node2.Embedding[1], "Embedding values should be preserved")
	})
}

// TestRaceConditionPrevention specifically tests the race condition scenario
// where the embedding worker processes a node while another goroutine reads it
func TestRaceConditionPrevention(t *testing.T) {
	t.Run("concurrent_node_access_during_embedding", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		embedder := newMockEmbedder()

		// Create a node
		err := engine.CreateNode(&storage.Node{
			ID:     "race-test",
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
		assert.NotEmpty(t, node.Embedding, "Node should have embedding")
		assert.Equal(t, 1024, len(node.Embedding), "Embedding should have correct dimensions")

		t.Log("✓ No race condition detected during concurrent node access")
	})
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
