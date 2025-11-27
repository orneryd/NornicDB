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
