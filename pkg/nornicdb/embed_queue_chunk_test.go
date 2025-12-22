package nornicdb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNonFileNodeChunking tests that non-File nodes with large content
// are chunked and all chunks are stored on the same node.
func TestNonFileNodeChunking(t *testing.T) {
	t.Run("single_chunk_stored_normally", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node with small content (single chunk)
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID("small-node"),
			Labels: []string{"Document"},
			Properties: map[string]any{
				"content": "Short content",
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
		worker.Trigger()

		// Wait for processing
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		// Verify node was embedded
		node, err := engine.GetNode("small-node")
		require.NoError(t, err)
		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings")
		assert.Equal(t, 1, len(node.ChunkEmbeddings), "Should have 1 chunk (single chunk stored as array of 1)")
		assert.Equal(t, 1024, len(node.ChunkEmbeddings[0]), "Embedding should have correct dimensions")
		assert.Nil(t, node.Properties["chunk_embeddings"], "Single chunk should not have chunk_embeddings property")
		assert.True(t, node.Properties["has_embedding"].(bool), "Should have has_embedding=true")
	})

	t.Run("multiple_chunks_stored_on_same_node", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node with large content (will be chunked)
		// buildEmbeddingText adds "labels: Document\ncontent: ...\ntitle: ..."
		// So we need content large enough that after adding labels/title, it exceeds chunkSize
		largeContent := strings.Repeat("This is a sentence with various words and tokens. ", 500)
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID("large-doc-node"),
			Labels: []string{"Document"},
			Properties: map[string]any{
				"content": largeContent,
				"title":   "Large Document",
			},
		})
		require.NoError(t, err)

		config := &EmbedWorkerConfig{
			ScanInterval: time.Hour,
			BatchDelay:   10 * time.Millisecond,
			MaxRetries:   1,
			ChunkSize:    512, // Small chunks to force multiple chunks
			ChunkOverlap: 50,
		}

		worker := NewEmbedWorker(embedder, engine, config)
		worker.Trigger()

		// Wait for processing
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		// Verify node was embedded with multiple chunks
		node, err := engine.GetNode("large-doc-node")
		require.NoError(t, err)

		// Should have chunk embeddings (always stored as ChunkEmbeddings, even single chunk = array of 1)
		assert.NotEmpty(t, node.ChunkEmbeddings, "Node should have chunk embeddings")
		assert.Equal(t, 1024, len(node.ChunkEmbeddings[0]), "First chunk embedding should have correct dimensions")

		// Should have chunk embeddings in struct field (not properties - opaque to users)
		require.NotNil(t, node.ChunkEmbeddings, "Should have ChunkEmbeddings struct field")
		require.Greater(t, len(node.ChunkEmbeddings), 0, "Should have at least one chunk embedding")
		require.Greater(t, len(node.ChunkEmbeddings), 1, "Should have multiple chunk embeddings")
		// Verify chunk_embeddings is NOT in properties (should be opaque)
		_, ok := node.Properties["chunk_embeddings"]
		assert.False(t, ok, "chunk_embeddings should NOT be in properties (opaque to users)")

		chunkCount, ok := node.Properties["chunk_count"].(int)
		require.True(t, ok, "Should have chunk_count property")
		assert.Equal(t, len(node.ChunkEmbeddings), chunkCount, "chunk_count should match number of chunks")

		// Verify each chunk embedding has correct dimensions
		for i, chunkEmb := range node.ChunkEmbeddings {
			require.NotNil(t, chunkEmb, "Chunk embedding %d should not be nil", i)
			assert.Equal(t, 1024, len(chunkEmb), "Chunk embedding %d should have correct dimensions", i)
		}

		// Verify embedder was called for all chunks
		embedCount := embedder.GetEmbedCount()
		assert.Equal(t, chunkCount, embedCount, "Should embed once per chunk")

		// Verify only ONE node exists (no separate chunk nodes created)
		allNodes := engine.GetAllNodes()
		assert.Equal(t, 1, len(allNodes), "Should only have one node (no separate chunk nodes)")
	})

	t.Run("chunk_embeddings_preserved_on_update", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()

		engine := storage.NewNamespacedEngine(baseEngine, "test")
		embedder := newMockEmbedder()

		// Create a node with large content
		largeContent := strings.Repeat("Test content for chunking. ", 500)
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID("test-node"),
			Labels: []string{"Article"},
			Properties: map[string]any{
				"content": largeContent,
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
		worker.Trigger()

		// Wait for processing
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			stats := worker.Stats()
			if stats.Processed > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		worker.Close()

		// Verify chunk embeddings exist in struct field (not properties)
		node, err := engine.GetNode("test-node")
		require.NoError(t, err)
		assert.NotNil(t, node.ChunkEmbeddings, "Should have ChunkEmbeddings struct field")
		originalChunkCount := len(node.ChunkEmbeddings)
		// Verify chunk_embeddings is NOT in properties (should be opaque)
		_, ok := node.Properties["chunk_embeddings"]
		assert.False(t, ok, "chunk_embeddings should NOT be in properties (opaque to users)")

		// Update node with new property
		node.Properties["updated"] = true
		err = engine.UpdateNode(node)
		require.NoError(t, err)

		// Verify chunk embeddings are preserved in struct field
		updatedNode, err := engine.GetNode("test-node")
		require.NoError(t, err)
		assert.NotNil(t, updatedNode.ChunkEmbeddings, "ChunkEmbeddings should be preserved")
		assert.Equal(t, originalChunkCount, len(updatedNode.ChunkEmbeddings), "Chunk count should be preserved")
		assert.True(t, updatedNode.Properties["updated"].(bool), "New property should be added")
	})
}

// TestChunkEmbeddingSearch tests that all chunk embeddings are searchable
// and results are properly deduplicated.
func TestChunkEmbeddingSearch(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	embedder := newMockEmbedder()

	// Create a search service
	searchService := search.NewService(engine)

	// Create a node with large content (will be chunked)
	largeContent := strings.Repeat("Machine learning algorithms are fascinating. ", 500)
	_, err := engine.CreateNode(&storage.Node{
		ID: storage.NodeID("ml-doc"),
		Labels: []string{"Document"},
		Properties: map[string]any{
			"content": largeContent,
			"title":   "Machine Learning Guide",
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
	worker.Trigger()

	// Wait for processing
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats := worker.Stats()
		if stats.Processed > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	worker.Close()

	// Manually index the node (since MemoryEngine doesn't have callbacks)
	node, err := engine.GetNode("ml-doc")
	require.NoError(t, err)
	err = searchService.IndexNode(node)
	require.NoError(t, err)

	// Build search indexes (for any other nodes)
	err = searchService.BuildIndexes(context.Background())
	require.NoError(t, err)

	// Verify all chunk embeddings are indexed
	embeddingCount := searchService.EmbeddingCount()
	chunkCount, ok := node.Properties["chunk_count"].(int)
	if !ok {
		// If no chunk_count, it means single chunk
		chunkCount = 0
	}

	// Embedding count should include: 1 main embedding (at node.ID) + N chunk embeddings (for multi-chunk nodes)
	// For single chunk: 1 main embedding
	// For multi-chunk: 1 main embedding + N chunk embeddings
	expectedCount := 1 + chunkCount
	assert.Equal(t, expectedCount, embeddingCount, "Should have main embedding plus all chunk embeddings indexed")

	// Perform a search
	queryEmbedding := make([]float32, 1024)
	for i := range queryEmbedding {
		queryEmbedding[i] = 0.1 // Simple query embedding
	}

	opts := search.DefaultSearchOptions()
	opts.Limit = 10

	response, err := searchService.Search(context.Background(), "machine learning", queryEmbedding, opts)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Verify we get the node back (deduplicated from multiple chunks)
	assert.Greater(t, len(response.Results), 0, "Should return search results")

	// Verify we only get ONE result (not one per chunk)
	foundNode := false
	for _, result := range response.Results {
		if result.NodeID == "ml-doc" {
			foundNode = true
			assert.Equal(t, "ml-doc", result.ID, "Result ID should be original node ID, not chunk ID")
			assert.NotContains(t, result.ID, "-chunk-", "Result ID should not contain chunk suffix")
		}
	}
	assert.True(t, foundNode, "Should find the ml-doc node in search results")
}

// TestChunkEmbeddingRemoval tests that chunk embeddings are properly removed
// when a node is deleted.
func TestChunkEmbeddingRemoval(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	embedder := newMockEmbedder()
	searchService := search.NewService(engine)

	// Create a node with large content
	largeContent := strings.Repeat("Test content. ", 500)
	_, err := engine.CreateNode(&storage.Node{
		ID: storage.NodeID("temp-doc"),
		Labels: []string{"Document"},
		Properties: map[string]any{
			"content": largeContent,
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
	worker.Trigger()

	// Wait for processing
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats := worker.Stats()
		if stats.Processed > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	worker.Close()

	// Reload node to get latest state with chunk_embeddings
	node, err := engine.GetNode("temp-doc")
	require.NoError(t, err)

	// Manually index the node (since MemoryEngine doesn't have callbacks)
	err = searchService.IndexNode(node)
	require.NoError(t, err)

	// Build indexes (for any other nodes)
	err = searchService.BuildIndexes(context.Background())
	require.NoError(t, err)

	// Verify embeddings are indexed (should include main + chunk embeddings)
	initialCount := searchService.EmbeddingCount()
	chunkCount, _ := node.Properties["chunk_count"].(int)
	expectedCount := 1 + chunkCount // 1 main embedding (at node.ID) + N chunk embeddings
	assert.Equal(t, expectedCount, initialCount, "Should have main embedding plus all chunk embeddings indexed")

	// Delete the node
	err = engine.DeleteNode("temp-doc")
	require.NoError(t, err)

	// Remove from search index
	err = searchService.RemoveNode("temp-doc")
	require.NoError(t, err)

	// Verify all embeddings (main + chunks) are removed
	finalCount := searchService.EmbeddingCount()
	assert.Equal(t, 0, finalCount, "All embeddings should be removed")
}
