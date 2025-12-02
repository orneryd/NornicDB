package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallDbIndexVectorCreateNodeIndex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("create_vector_index", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('embeddings_idx', 'Document', 'embedding', 384, 'cosine')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "embeddings_idx", result.Rows[0][0])
		assert.Equal(t, "Document", result.Rows[0][1])
		assert.Equal(t, "embedding", result.Rows[0][2])
		assert.Equal(t, 384, result.Rows[0][3])
		assert.Equal(t, "cosine", result.Rows[0][4])
	})

	t.Run("create_with_default_similarity", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx2', 'Node', 'vec', 128)", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "cosine", result.Rows[0][4]) // Default similarity
	})

	t.Run("create_with_euclidean", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx3', 'Item', 'features', 256, 'euclidean')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "euclidean", result.Rows[0][4])
	})
}

func TestCallDbCreateSetNodeVectorProperty(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a node first
	err := engine.CreateNode(&storage.Node{
		ID:         "node1",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"title": "Test"},
	})
	require.NoError(t, err)

	t.Run("set_vector_property", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.create.setNodeVectorProperty('node1', 'embedding', [0.1, 0.2, 0.3, 0.4])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Verify the vector was set
		node, err := engine.GetNode("node1")
		require.NoError(t, err)

		embedding, ok := node.Properties["embedding"].([]float64)
		require.True(t, ok, "embedding should be []float64")
		assert.Equal(t, []float64{0.1, 0.2, 0.3, 0.4}, embedding)
	})

	t.Run("update_vector_property", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.create.setNodeVectorProperty('node1', 'embedding', [0.5, 0.6, 0.7, 0.8])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Verify the vector was updated
		node, err := engine.GetNode("node1")
		require.NoError(t, err)

		embedding, ok := node.Properties["embedding"].([]float64)
		require.True(t, ok)
		assert.Equal(t, []float64{0.5, 0.6, 0.7, 0.8}, embedding)
	})

	t.Run("node_not_found", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.create.setNodeVectorProperty('nonexistent', 'embedding', [1.0, 2.0])", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node not found")
	})
}

func TestCallDbCreateSetRelationshipVectorProperty(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes and relationship first
	err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&storage.Edge{
		ID:         "rel1",
		StartNode:  "a",
		EndNode:    "b",
		Type:       "CONNECTS",
		Properties: map[string]interface{}{},
	})
	require.NoError(t, err)

	t.Run("set_relationship_vector", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.create.setRelationshipVectorProperty('rel1', 'features', [1.0, 2.0, 3.0])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Verify the vector was set
		rel, err := engine.GetEdge("rel1")
		require.NoError(t, err)

		features, ok := rel.Properties["features"].([]float64)
		require.True(t, ok, "features should be []float64")
		assert.Equal(t, []float64{1.0, 2.0, 3.0}, features)
	})

	t.Run("relationship_not_found", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.create.setRelationshipVectorProperty('nonexistent', 'features', [1.0])", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "relationship not found")
	})
}

func TestVectorIndexQueryNodesWithProcedure(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// This tests the existing queryNodes procedure with index created via CALL
	t.Run("query_vector_index", func(t *testing.T) {
		// Create vector index first
		_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('test_idx', 'Doc', 'vec', 4, 'cosine')", nil)
		require.NoError(t, err)

		// Query (will return empty since no embeddings stored via this mechanism)
		result, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('test_idx', 5, [0.1, 0.2, 0.3, 0.4]) YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("string_query_without_embedder", func(t *testing.T) {
		// String query without embedder should return helpful error
		_, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('test_idx', 5, 'search text') YIELD node, score", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no embedder configured")
	})

	t.Run("string_query_with_embedder", func(t *testing.T) {
		// Create a mock embedder
		mockEmbedder := &mockQueryEmbedder{
			embedding: []float32{0.1, 0.2, 0.3, 0.4},
		}
		exec.SetEmbedder(mockEmbedder)

		// Now string query should work (embeds the string first)
		result, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('test_idx', 5, 'machine learning') YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		// Verify the embedder was called
		assert.Equal(t, "machine learning", mockEmbedder.lastQuery)
	})
}

// mockQueryEmbedder is a test embedder for string queries
type mockQueryEmbedder struct {
	embedding []float32
	lastQuery string
}

func (m *mockQueryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	m.lastQuery = text
	return m.embedding, nil
}

// TestVectorSearchQueryModes tests all three query modes:
// 1. Direct vector array (Neo4j compatible)
// 2. String query (NornicDB server-side embedding)
// 3. Parameter reference ($queryVector)
func TestVectorSearchQueryModes(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create test nodes with embeddings via Cypher SET (like Mimir does with Neo4j)
	// Node 1: about machine learning
	_, err := exec.Execute(ctx, `CREATE (n:Document {id: 'doc1', title: 'ML Guide', content: 'Machine learning basics'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (n:Document {id: 'doc1'}) SET n.embedding = [0.9, 0.1, 0.0, 0.0]`, nil)
	require.NoError(t, err)

	// Node 2: about databases
	_, err = exec.Execute(ctx, `CREATE (n:Document {id: 'doc2', title: 'DB Guide', content: 'Database fundamentals'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (n:Document {id: 'doc2'}) SET n.embedding = [0.0, 0.9, 0.1, 0.0]`, nil)
	require.NoError(t, err)

	// Node 3: about web development
	_, err = exec.Execute(ctx, `CREATE (n:Document {id: 'doc3', title: 'Web Guide', content: 'Web development intro'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (n:Document {id: 'doc3'}) SET n.embedding = [0.0, 0.0, 0.9, 0.1]`, nil)
	require.NoError(t, err)

	t.Run("query_with_direct_vector_array", func(t *testing.T) {
		// Query for ML-related documents using direct vector (Neo4j style)
		mlQuery := [4]float32{0.85, 0.15, 0.0, 0.0} // Similar to doc1
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('doc_idx', 10, [0.85, 0.15, 0.0, 0.0]) YIELD node, score", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should find nodes with embeddings, doc1 should be most similar
		assert.Greater(t, len(result.Rows), 0, "Should find at least one document")
		if len(result.Rows) > 0 {
			topNode := result.Rows[0][0].(map[string]interface{})
			assert.Equal(t, "doc1", topNode["id"], "doc1 should be most similar to ML query")
			score := result.Rows[0][1].(float64)
			assert.Greater(t, score, 0.8, "Score should be high for similar vectors")
		}
		_ = mlQuery // Suppress unused warning
	})

	t.Run("query_with_string_auto_embedded", func(t *testing.T) {
		// Set up mock embedder that returns ML-like vector for any query
		mockEmbedder := &mockQueryEmbedder{
			embedding: []float32{0.85, 0.15, 0.0, 0.0}, // Returns ML-like vector
		}
		exec.SetEmbedder(mockEmbedder)

		// Query using string - server embeds it automatically
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('doc_idx', 10, 'machine learning tutorial') YIELD node, score", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify embedder was called with the query string
		assert.Equal(t, "machine learning tutorial", mockEmbedder.lastQuery)

		// Should find nodes, doc1 should be most similar
		assert.Greater(t, len(result.Rows), 0, "Should find documents")
		if len(result.Rows) > 0 {
			topNode := result.Rows[0][0].(map[string]interface{})
			assert.Equal(t, "doc1", topNode["id"], "doc1 should match ML query")
		}
	})

	t.Run("query_with_double_quoted_string", func(t *testing.T) {
		mockEmbedder := &mockQueryEmbedder{
			embedding: []float32{0.0, 0.85, 0.15, 0.0}, // Returns DB-like vector
		}
		exec.SetEmbedder(mockEmbedder)

		// Query using double-quoted string
		result, err := exec.Execute(ctx,
			`CALL db.index.vector.queryNodes('doc_idx', 10, "database query") YIELD node, score`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify embedder was called
		assert.Equal(t, "database query", mockEmbedder.lastQuery)

		// doc2 should be most similar
		if len(result.Rows) > 0 {
			topNode := result.Rows[0][0].(map[string]interface{})
			assert.Equal(t, "doc2", topNode["id"], "doc2 should match DB query")
		}
	})

	t.Run("query_with_parameter_reference", func(t *testing.T) {
		// Parameter queries return empty result (params resolved at higher level)
		// This tests that the syntax is accepted
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('doc_idx', 10, $queryVector) YIELD node, score", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// Empty because parameters aren't resolved at this level
		assert.Empty(t, result.Rows, "Parameter queries return empty at executor level")
	})

	t.Run("string_query_error_without_embedder", func(t *testing.T) {
		// Create fresh executor without embedder
		freshExec := NewStorageExecutor(engine)

		// Should fail gracefully with helpful message
		_, err := freshExec.Execute(ctx,
			"CALL db.index.vector.queryNodes('idx', 5, 'test query') YIELD node, score", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no embedder configured")
		assert.Contains(t, err.Error(), "use vector array")
	})

	t.Run("dimension_mismatch_silently_filters", func(t *testing.T) {
		// Query with different dimension vector - should return 0 results (not error)
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('idx', 10, [0.5, 0.5]) YIELD node, score", nil) // 2-dim vs 4-dim
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Empty(t, result.Rows, "Dimension mismatch should filter out all nodes")
	})

	t.Run("limit_results", func(t *testing.T) {
		// Query with limit of 2
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('idx', 2, [0.5, 0.5, 0.5, 0.5]) YIELD node, score", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.LessOrEqual(t, len(result.Rows), 2, "Should respect limit")
	})
}

// TestVectorSearchEndToEnd simulates the Mimir workflow:
// 1. Mimir generates embedding client-side
// 2. Mimir stores embedding via Cypher SET
// 3. Mimir queries via db.index.vector.queryNodes with vector
func TestVectorSearchEndToEnd(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Simulate Mimir storing a node with embedding (as it does with Neo4j today)
	_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'node-abc123', type: 'decision', content: 'Use PostgreSQL', title: 'Database Choice'})`, nil)
	require.NoError(t, err)

	// Mimir generates embedding and stores it (single SET for simplicity)
	_, err = exec.Execute(ctx, `MATCH (n:Node {id: 'node-abc123'}) SET n.embedding = [0.7, 0.2, 0.05, 0.05]`, nil)
	require.NoError(t, err)

	// Additional properties
	_, err = exec.Execute(ctx, `MATCH (n:Node {id: 'node-abc123'}) SET n.has_embedding = true`, nil)
	require.NoError(t, err)

	// Verify embedding was stored
	result, err := exec.Execute(ctx, `MATCH (n:Node {id: 'node-abc123'}) RETURN n`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Query like Mimir does: with pre-computed query vector
	searchResult, err := exec.Execute(ctx, `CALL db.index.vector.queryNodes('node_embedding_index', 10, [0.65, 0.25, 0.05, 0.05]) YIELD node, score`, nil)
	require.NoError(t, err)
	require.NotNil(t, searchResult)

	// Should find our node with high similarity
	require.GreaterOrEqual(t, len(searchResult.Rows), 1, "Should find the stored node")
	if len(searchResult.Rows) > 0 {
		score := searchResult.Rows[0][1].(float64)
		assert.Greater(t, score, 0.9, "Should have high similarity score")
	}
}

// TestMultiLineSetWithArray tests that SET clauses with arrays and multiple properties work
func TestMultiLineSetWithArray(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a node
	_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-multi'})`, nil)
	require.NoError(t, err)

	// Multi-line SET with array - this is how Mimir sets embeddings with metadata
	_, err = exec.Execute(ctx, `
		MATCH (n:Node {id: 'test-multi'})
		SET n.embedding = [0.7, 0.2, 0.05, 0.05],
		    n.embedding_dimensions = 4,
		    n.embedding_model = 'mxbai-embed-large',
		    n.has_embedding = true
	`, nil)
	require.NoError(t, err)

	// Verify all properties were set
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	node := nodes[0]

	// Check embedding was set (routed to node.Embedding)
	assert.Equal(t, 4, len(node.Embedding), "Embedding should have 4 dimensions")
	assert.InDelta(t, 0.7, node.Embedding[0], 0.01, "First embedding value")

	// Check other properties were set - integers stored as int64 (Neo4j compatible)
	assert.Equal(t, int64(4), node.Properties["embedding_dimensions"])
	assert.Equal(t, "mxbai-embed-large", node.Properties["embedding_model"])
	assert.Equal(t, true, node.Properties["has_embedding"])
}

func TestCallDbIndexVectorQueryRelationships(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("query_relationship_vectors", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_idx', 5, [0.1, 0.2]) YIELD relationship, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		// Returns empty for now since relationship vectors aren't indexed
		assert.Empty(t, result.Rows)
	})
}

func TestCallDbIndexFulltextQueryRelationships(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes and relationship with text property
	err := engine.CreateNode(&storage.Node{ID: "x", Labels: []string{"Node"}})
	require.NoError(t, err)
	err = engine.CreateNode(&storage.Node{ID: "y", Labels: []string{"Node"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&storage.Edge{
		ID:         "rel_text",
		StartNode:  "x",
		EndNode:    "y",
		Type:       "DESCRIBES",
		Properties: map[string]interface{}{"description": "This is a test relationship with searchable text"},
	})
	require.NoError(t, err)

	t.Run("query_relationship_fulltext", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.index.fulltext.queryRelationships('rel_text_idx', 'searchable') YIELD relationship, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}

// =============================================================================
// Tests for New Index Creation Procedures (Neo4j 100% Compatibility)
// =============================================================================

func TestCallDbIndexVectorCreateRelationshipIndex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("create_relationship_vector_index", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.createRelationshipIndex('rel_vec_idx', 'SIMILAR_TO', 'similarity', 768, 'cosine')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "rel_vec_idx", result.Rows[0][0])
		assert.Equal(t, "SIMILAR_TO", result.Rows[0][1])
		assert.Equal(t, "similarity", result.Rows[0][2])
		assert.Equal(t, 768, result.Rows[0][3])
		assert.Equal(t, "cosine", result.Rows[0][4])
	})

	t.Run("create_with_euclidean_similarity", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.createRelationshipIndex('rel_vec_idx2', 'CONNECTS', 'embedding', 1024, 'euclidean')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "euclidean", result.Rows[0][4])
	})

	t.Run("create_with_default_similarity", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.createRelationshipIndex('rel_vec_idx3', 'RELATES', 'vec', 256)", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "cosine", result.Rows[0][4]) // Default
	})

	t.Run("missing_arguments_error", func(t *testing.T) {
		_, err := exec.Execute(ctx,
			"CALL db.index.vector.createRelationshipIndex('idx', 'REL', 'prop')", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least 4 arguments")
	})
}

func TestCallDbIndexFulltextCreateNodeIndex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("create_single_label_single_property", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('ft_idx1', 'Document', 'content')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "ft_idx1", result.Rows[0][0])
		labels := result.Rows[0][1].([]string)
		assert.Contains(t, labels, "Document")
	})

	t.Run("create_with_array_labels", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('ft_idx2', ['Article', 'Blog', 'Post'], ['title', 'content'])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		labels := result.Rows[0][1].([]string)
		assert.Len(t, labels, 3)
		assert.Contains(t, labels, "Article")
		assert.Contains(t, labels, "Blog")
		assert.Contains(t, labels, "Post")

		properties := result.Rows[0][2].([]string)
		assert.Len(t, properties, 2)
		assert.Contains(t, properties, "title")
		assert.Contains(t, properties, "content")
	})

	t.Run("create_with_single_quoted_strings", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			`CALL db.index.fulltext.createNodeIndex('ft_idx3', 'Memory', 'text')`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("missing_arguments_error", func(t *testing.T) {
		_, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('idx', 'Label')", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least 3 arguments")
	})
}

func TestCallDbIndexFulltextCreateRelationshipIndex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("create_single_type_single_property", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createRelationshipIndex('rel_ft_idx1', 'MENTIONS', 'description')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "rel_ft_idx1", result.Rows[0][0])
	})

	t.Run("create_with_array_types", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createRelationshipIndex('rel_ft_idx2', ['REFERENCES', 'CITES', 'LINKS_TO'], ['note', 'context'])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		relTypes := result.Rows[0][1].([]string)
		assert.Len(t, relTypes, 3)
		assert.Contains(t, relTypes, "REFERENCES")
		assert.Contains(t, relTypes, "CITES")
		assert.Contains(t, relTypes, "LINKS_TO")
	})

	t.Run("missing_arguments_error", func(t *testing.T) {
		_, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createRelationshipIndex('idx', 'REL')", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least 3 arguments")
	})
}

func TestCallDbIndexFulltextDrop(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("drop_existing_index", func(t *testing.T) {
		// Create index first
		_, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('to_drop', 'Node', 'prop')", nil)
		require.NoError(t, err)

		// Drop it
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.drop('to_drop')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "to_drop", result.Rows[0][0])
		assert.Equal(t, true, result.Rows[0][1])
	})

	t.Run("drop_nonexistent_index_succeeds", func(t *testing.T) {
		// Drop should succeed even if index doesn't exist (idempotent)
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.drop('nonexistent_index')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "nonexistent_index", result.Rows[0][0])
		assert.Equal(t, true, result.Rows[0][1]) // NornicDB returns true (no-op)
	})

	t.Run("drop_with_quoted_name", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			`CALL db.index.fulltext.drop("my_special_index")`, nil)
		require.NoError(t, err)
		assert.Equal(t, "my_special_index", result.Rows[0][0])
	})
}

func TestCallDbIndexVectorDrop(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("drop_existing_vector_index", func(t *testing.T) {
		// Create index first
		_, err := exec.Execute(ctx,
			"CALL db.index.vector.createNodeIndex('vec_to_drop', 'Doc', 'embedding', 384, 'cosine')", nil)
		require.NoError(t, err)

		// Drop it
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.drop('vec_to_drop')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "vec_to_drop", result.Rows[0][0])
		assert.Equal(t, true, result.Rows[0][1])
	})

	t.Run("drop_nonexistent_vector_index_succeeds", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.drop('nonexistent_vec_idx')", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "nonexistent_vec_idx", result.Rows[0][0])
		assert.Equal(t, true, result.Rows[0][1])
	})
}

// =============================================================================
// Integration Tests: Real-World Index Workflows
// =============================================================================

func TestFulltextIndexWorkflow(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("complete_fulltext_workflow", func(t *testing.T) {
		// 1. Create fulltext index for documents
		result, err := exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('documents_search', ['Document', 'Article'], ['title', 'content', 'summary'])", nil)
		require.NoError(t, err)
		assert.Equal(t, "documents_search", result.Rows[0][0])

		// 2. Create some documents
		_, err = exec.Execute(ctx, `
			CREATE (d1:Document {id: 'doc1', title: 'Introduction to Machine Learning', content: 'Machine learning is a subset of AI'})
		`, nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, `
			CREATE (d2:Article {id: 'doc2', title: 'Deep Learning Tutorial', content: 'Neural networks enable deep learning'})
		`, nil)
		require.NoError(t, err)

		// 3. Query the fulltext index
		result, err = exec.Execute(ctx,
			"CALL db.index.fulltext.queryNodes('documents_search', 'machine learning') YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// 4. Clean up - drop the index
		result, err = exec.Execute(ctx,
			"CALL db.index.fulltext.drop('documents_search')", nil)
		require.NoError(t, err)
		assert.Equal(t, true, result.Rows[0][1])
	})
}

func TestVectorIndexWorkflow(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("complete_vector_workflow", func(t *testing.T) {
		// 1. Create vector index for embeddings
		result, err := exec.Execute(ctx,
			"CALL db.index.vector.createNodeIndex('memory_embeddings', 'Memory', 'embedding', 1024, 'cosine')", nil)
		require.NoError(t, err)
		assert.Equal(t, "memory_embeddings", result.Rows[0][0])
		assert.Equal(t, 1024, result.Rows[0][3])

		// 2. Create relationship vector index
		result, err = exec.Execute(ctx,
			"CALL db.index.vector.createRelationshipIndex('edge_similarity', 'RELATES_TO', 'weight_vector', 256, 'euclidean')", nil)
		require.NoError(t, err)
		assert.Equal(t, "edge_similarity", result.Rows[0][0])

		// 3. Create a memory node directly in storage with known ID
		err = engine.CreateNode(&storage.Node{
			ID:         "mem1",
			Labels:     []string{"Memory"},
			Properties: map[string]interface{}{"content": "Test memory"},
		})
		require.NoError(t, err)

		// 4. Set embedding via procedure (uses storage ID, not property)
		result, err = exec.Execute(ctx,
			"CALL db.create.setNodeVectorProperty('mem1', 'embedding', [0.1, 0.2, 0.3, 0.4])", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1) // Returns the updated node

		// 5. Query vector index
		result, err = exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('memory_embeddings', 5, [0.15, 0.25, 0.35, 0.45]) YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// 6. Drop indexes
		result, err = exec.Execute(ctx, "CALL db.index.vector.drop('memory_embeddings')", nil)
		require.NoError(t, err)
		assert.Equal(t, true, result.Rows[0][1])

		result, err = exec.Execute(ctx, "CALL db.index.vector.drop('edge_similarity')", nil)
		require.NoError(t, err)
		assert.Equal(t, true, result.Rows[0][1])
	})
}

func TestMimirCompatibleWorkflow(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("mimir_style_index_management", func(t *testing.T) {
		// This simulates how Mimir would manage indexes in NornicDB

		// 1. Create default indexes that Mimir expects
		_, err := exec.Execute(ctx,
			"CALL db.index.vector.createNodeIndex('node_embedding_index', 'Node', 'embedding', 1024, 'cosine')", nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx,
			"CALL db.index.fulltext.createNodeIndex('node_fulltext_index', ['Node', 'Memory', 'Decision'], ['content', 'title', 'description'])", nil)
		require.NoError(t, err)

		// 2. Verify indexes were created by listing them
		result, err := exec.Execute(ctx, "CALL db.indexes()", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// 3. Store a decision node like Mimir would
		_, err = exec.Execute(ctx, `
			CREATE (n:Decision {
				id: 'decision-123',
				type: 'decision',
				title: 'Use PostgreSQL for primary database',
				content: 'Selected PostgreSQL due to ACID compliance and JSON support',
				created: datetime()
			})
		`, nil)
		require.NoError(t, err)

		// 4. Set embedding (simulating what Mimir's embedding service does)
		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = float32(i) / 1024.0
		}

		// Using Cypher SET instead of procedure for embedding (Mimir style)
		_, err = exec.Execute(ctx, `
			MATCH (n:Decision {id: 'decision-123'})
			SET n.embedding = [0.1, 0.2, 0.3, 0.4],
			    n.has_embedding = true
		`, nil)
		require.NoError(t, err)

		// 5. Search - both fulltext and vector
		result, err = exec.Execute(ctx,
			"CALL db.index.fulltext.queryNodes('node_fulltext_index', 'PostgreSQL') YIELD node, score", nil)
		require.NoError(t, err)

		result, err = exec.Execute(ctx,
			"CALL db.index.vector.queryNodes('node_embedding_index', 10, [0.1, 0.2, 0.3, 0.4]) YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}
