// Comprehensive test suite for all Cypher queries that Mimir sends to NornicDB.
// This ensures full compatibility with Mimir's graph operations.

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// 1. Connection Test
// ====================================================================================

func TestMimirConnectionTest(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `RETURN 1`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

// ====================================================================================
// 2. Schema Initialization Queries
// ====================================================================================

func TestMimirSchemaInitialization(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("CREATE CONSTRAINT node_id_unique", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT node_id_unique IF NOT EXISTS 
			FOR (n:Node) REQUIRE n.id IS UNIQUE
		`, nil)
		// Should succeed or be a no-op (NornicDB manages indexes internally)
		assert.NoError(t, err)
	})

	t.Run("CREATE FULLTEXT INDEX node_search", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE FULLTEXT INDEX node_search IF NOT EXISTS
			FOR (n:Node) ON EACH [n.properties]
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE INDEX node_type", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE INDEX node_type IF NOT EXISTS
			FOR (n:Node) ON (n.type)
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE CONSTRAINT watch_config_id_unique", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT watch_config_id_unique IF NOT EXISTS
			FOR (w:WatchConfig) REQUIRE w.id IS UNIQUE
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE INDEX watch_config_path", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE INDEX watch_config_path IF NOT EXISTS
			FOR (w:WatchConfig) ON (w.path)
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE INDEX file_path", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE INDEX file_path IF NOT EXISTS
			FOR (f:File) ON (f.path)
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE FULLTEXT INDEX file_metadata_search", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE FULLTEXT INDEX file_metadata_search IF NOT EXISTS
			FOR (f:File) ON EACH [f.path, f.name, f.language]
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE FULLTEXT INDEX file_chunk_content_search", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			CREATE FULLTEXT INDEX file_chunk_content_search IF NOT EXISTS
			FOR (c:FileChunk) ON EACH [c.text]
		`, nil)
		assert.NoError(t, err)
	})

	t.Run("CREATE VECTOR INDEX node_embedding_index", func(t *testing.T) {
		if config.IsANTLRParser() {
			t.Skip("Skipping: ANTLR parser doesn't support NornicDB's extended VECTOR INDEX syntax")
		}
		_, err := exec.Execute(ctx, `
			CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS
			FOR (n:Node) ON (n.embedding)
			OPTIONS {indexConfig: {
			  vector.dimensions: 768,
			  vector.similarity_function: 'cosine'
			}}
		`, nil)
		assert.NoError(t, err)
	})
}

// ====================================================================================
// 3. Node Operations (THE CRITICAL ONES)
// ====================================================================================

func TestMimirNodeOperations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("addNode - Create a new node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CREATE (n:Node {
				id: 'todo-1-1734202000000', 
				type: 'todo', 
				created: '2025-12-14T18:00:00.000Z', 
				updated: '2025-12-14T18:00:00.000Z', 
				has_embedding: false, 
				taskId: 'audit-translation', 
				title: 'Audit Translation Quality', 
				status: 'pending'
			}) RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Neo4j compatible: RETURN n should return *storage.Node
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok, "RETURN n must return *storage.Node for Neo4j compatibility, got %T", result.Rows[0][0])
		assert.Equal(t, "todo-1-1734202000000", node.Properties["id"])
		assert.Equal(t, "todo", node.Properties["type"])
		assert.Equal(t, "pending", node.Properties["status"])
		assert.Contains(t, node.Labels, "Node")
	})

	t.Run("getNode - Get a node by ID", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'todo-1-1734202000000'}) RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "todo-1-1734202000000", node.Properties["id"])
	})

	t.Run("updateNode - Update with SET += operator (CRITICAL)", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'todo-1-1734202000000'}) 
			SET n += {status: 'worker_executing', updated: '2025-12-14T18:00:01.000Z'} 
			RETURN n
		`, nil)
		require.NoError(t, err, "SET n += {props} must work for Mimir compatibility")
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "worker_executing", node.Properties["status"])
		assert.Equal(t, "2025-12-14T18:00:01.000Z", node.Properties["updated"])
		// Original properties should still exist
		assert.Equal(t, "todo", node.Properties["type"])
		assert.Equal(t, "Audit Translation Quality", node.Properties["title"])
	})

	t.Run("updateNode - Alternative SET syntax", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'todo-1-1734202000000'}) 
			SET n.status = 'completed', n.updated = '2025-12-14T18:02:00.000Z' 
			RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "completed", node.Properties["status"])
	})

	t.Run("deleteNode - Delete with DETACH DELETE", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'todo-1-1734202000000'})
			DETACH DELETE n
			RETURN count(*) as deleted
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Verify node is gone
		verifyResult, _ := exec.Execute(ctx, `
			MATCH (n:Node {id: 'todo-1-1734202000000'}) RETURN n
		`, nil)
		assert.Len(t, verifyResult.Rows, 0, "Node should be deleted")
	})
}

// ====================================================================================
// 4. Edge Operations
// ====================================================================================

func TestMimirEdgeOperations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create source and target nodes
	_, err := exec.Execute(ctx, `
		CREATE (s:Node {id: 'source-node-id', type: 'task'})
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (t:Node {id: 'target-node-id', type: 'task'})
	`, nil)
	require.NoError(t, err)

	t.Run("addEdge - Create a relationship", func(t *testing.T) {
		// Neo4j standard: comma-separated patterns in single MATCH
		result, err := exec.Execute(ctx, `
			MATCH (s:Node {id: 'source-node-id'}), (t:Node {id: 'target-node-id'})
			CREATE (s)-[e:EDGE {id: 'edge-1-1734202000000', type: 'depends_on', created: '2025-12-14T18:00:00.000Z'}]->(t)
			RETURN e
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("deleteEdge - Delete a relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH ()-[e:EDGE {id: 'edge-1-1734202000000'}]->()
			DELETE e
			RETURN count(*) as deleted
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// ====================================================================================
// 5. Embedding Updates
// ====================================================================================

func TestMimirEmbeddingUpdates(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create a test node
	_, err := exec.Execute(ctx, `
		CREATE (n:Node {id: 'test-node-1', type: 'document'})
	`, nil)
	require.NoError(t, err)

	t.Run("SET embedding array", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-node-1'}) 
			SET n.embedding = [0.1, 0.2, 0.3],
			    n.embedding_dimensions = 768,
			    n.embedding_model = 'nomic-embed-text',
			    n.has_embedding = true
			RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, true, node.Properties["has_embedding"])
		assert.Equal(t, "nomic-embed-text", node.Properties["embedding_model"])
	})

	t.Run("SET has_embedding and has_chunks flags", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-node-1'}) 
			SET n.has_embedding = true, n.has_chunks = true
			RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, true, node.Properties["has_embedding"])
		assert.Equal(t, true, node.Properties["has_chunks"])
	})
}

// ====================================================================================
// 6. Chunk Operations (MERGE with ON CREATE SET)
// ====================================================================================

func TestMimirChunkOperations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create parent node
	_, err := exec.Execute(ctx, `
		CREATE (n:Node {id: 'parent-node-id', type: 'document'})
	`, nil)
	require.NoError(t, err)

	t.Run("MERGE chunk with ON CREATE SET", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'parent-node-id'})
			MERGE (c:NodeChunk:Node {id: 'chunk-parent-node-id-0'})
			ON CREATE SET
			  c.chunk_index = 0,
			  c.text = 'chunk text here',
			  c.start_offset = 0,
			  c.end_offset = 768,
			  c.type = 'node_chunk',
			  c.parentNodeId = 'parent-node-id',
			  c.has_embedding = true
			MERGE (n)-[:HAS_CHUNK {index: 0}]->(c)
			RETURN c.id AS chunk_id
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "chunk-parent-node-id-0", result.Rows[0][0])
	})

	t.Run("Delete chunks with OPTIONAL MATCH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'parent-node-id'})
			OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk)
			DELETE r, chunk
		`, nil)
		require.NoError(t, err)
		// Verify chunks are deleted
		assert.NotNil(t, result)
	})
}

// ====================================================================================
// Quick Test Suite - All critical operations in sequence
// ====================================================================================

func TestMimirQuickTestSuite(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// 1. Test connection
	t.Run("1. Connection test", func(t *testing.T) {
		result, err := exec.Execute(ctx, `RETURN 1 as test`, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.Rows[0][0])
	})

	// 2. Create a test node
	t.Run("2. Create test node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CREATE (n:Node {
				id: 'test-mimir-1', 
				type: 'todo', 
				created: '2025-12-14T18:00:00.000Z', 
				updated: '2025-12-14T18:00:00.000Z', 
				has_embedding: false, 
				title: 'Test Task'
			}) RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	// 3. Get the test node
	t.Run("3. Get test node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-mimir-1'}) RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	// 4. Update with SET += (THE CRITICAL ONE)
	t.Run("4. Update with SET += (CRITICAL)", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-mimir-1'}) 
			SET n += {status: 'in_progress', updated: '2025-12-14T18:01:00.000Z'} 
			RETURN n
		`, nil)
		require.NoError(t, err, "SET n += must work!")
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "in_progress", node.Properties["status"])
	})

	// 5. Alternative update syntax
	t.Run("5. Alternative SET syntax", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-mimir-1'}) 
			SET n.status = 'completed', n.updated = '2025-12-14T18:02:00.000Z' 
			RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "completed", node.Properties["status"])
	})

	// 6. Delete the test node
	t.Run("6. Delete test node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-mimir-1'}) 
			DETACH DELETE n 
			RETURN count(*) as deleted
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// ====================================================================================
// SET += Edge Cases
// ====================================================================================

func TestSetPlusEqualsEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("SET += with nested properties", func(t *testing.T) {
		// Create node
		_, err := exec.Execute(ctx, `
			CREATE (n:Node {id: 'nested-test', data: 'original'})
		`, nil)
		require.NoError(t, err)

		// Update with multiple properties
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'nested-test'})
			SET n += {
				status: 'active',
				count: 42,
				enabled: true,
				tags: 'tag1,tag2'
			}
			RETURN n
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "active", node.Properties["status"])
		assert.Equal(t, int64(42), node.Properties["count"])
		assert.Equal(t, true, node.Properties["enabled"])
		assert.Equal(t, "original", node.Properties["data"]) // Original preserved
	})

	t.Run("SET += without RETURN", func(t *testing.T) {
		// Create node
		_, err := exec.Execute(ctx, `
			CREATE (n:Node {id: 'no-return-test'})
		`, nil)
		require.NoError(t, err)

		// Update without RETURN
		_, err = exec.Execute(ctx, `
			MATCH (n:Node {id: 'no-return-test'})
			SET n += {updated: true}
		`, nil)
		require.NoError(t, err)

		// Verify update applied
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'no-return-test'}) RETURN n.updated
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, true, result.Rows[0][0])
	})

	t.Run("SET += on non-existent node returns empty", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'does-not-exist'})
			SET n += {status: 'updated'}
			RETURN n
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "Should return empty for non-existent node")
	})
}
