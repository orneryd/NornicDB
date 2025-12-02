package cypher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMimirExactQueries tests the EXACT queries from Mimir's index-api.ts
// with data that matches production: mostly .md files with File:Node labels
func TestMimirExactQueries(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create File:Node nodes similar to production
	// Production has 313 files, 311 are .md, 2 are other
	// Let's create 10 files: 8 .md, 1 .ts, 1 .js
	setupQueries := []string{
		`CREATE (:File:Node {path: '/test/doc1.md', extension: '.md', name: 'doc1.md'})`,
		`CREATE (:File:Node {path: '/test/doc2.md', extension: '.md', name: 'doc2.md'})`,
		`CREATE (:File:Node {path: '/test/doc3.md', extension: '.md', name: 'doc3.md'})`,
		`CREATE (:File:Node {path: '/test/doc4.md', extension: '.md', name: 'doc4.md'})`,
		`CREATE (:File:Node {path: '/test/doc5.md', extension: '.md', name: 'doc5.md'})`,
		`CREATE (:File:Node {path: '/test/doc6.md', extension: '.md', name: 'doc6.md'})`,
		`CREATE (:File:Node {path: '/test/doc7.md', extension: '.md', name: 'doc7.md'})`,
		`CREATE (:File:Node {path: '/test/doc8.md', extension: '.md', name: 'doc8.md'})`,
		`CREATE (:File:Node {path: '/test/app.ts', extension: '.ts', name: 'app.ts'})`,
		`CREATE (:File:Node {path: '/test/util.js', extension: '.js', name: 'util.js'})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// Verify setup
	nodes, err := store.GetNodesByLabel("File")
	require.NoError(t, err)
	t.Logf("Created %d File nodes", len(nodes))
	for _, n := range nodes {
		t.Logf("  Node %s: labels=%v, ext=%v", n.ID, n.Labels, n.Properties["extension"])
	}

	t.Run("EXACT Mimir stats query (index-api.ts:642-658)", func(t *testing.T) {
		// EXACT query from Mimir - copy-pasted from index-api.ts
		query := `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c,
			  CASE WHEN c IS NOT NULL AND c.embedding IS NOT NULL THEN 1 ELSE 0 END as chunkHasEmbedding,
			  CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END as fileHasEmbedding
			WITH 
			  COUNT(DISTINCT f) as totalFiles,
			  COUNT(DISTINCT c) as totalChunks,
			  SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) as totalEmbeddings,
			  COLLECT(DISTINCT f.extension) as extensions
			RETURN 
			  totalFiles,
			  totalChunks,
			  totalEmbeddings,
			  extensions
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Query should execute without error")
		require.Len(t, result.Rows, 1, "Should return exactly 1 row")

		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row: %v", result.Rows[0])

		// Find columns
		var totalFiles, totalChunks, totalEmbeddings int64
		for i, col := range result.Columns {
			val := result.Rows[0][i]
			t.Logf("  %s = %v (type: %T)", col, val, val)
			switch col {
			case "totalFiles":
				totalFiles = exactTestToInt64(val)
			case "totalChunks":
				totalChunks = exactTestToInt64(val)
			case "totalEmbeddings":
				totalEmbeddings = exactTestToInt64(val)
			}
		}

		assert.Equal(t, int64(10), totalFiles, "Should have 10 files")
		assert.Equal(t, int64(0), totalChunks, "Should have 0 chunks (none created)")
		assert.Equal(t, int64(0), totalEmbeddings, "Should have 0 embeddings (none set)")
	})

	t.Run("EXACT Mimir extension query (index-api.ts:666-672)", func(t *testing.T) {
		// EXACT query from Mimir - copy-pasted from index-api.ts
		query := `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			WITH f.extension as ext, COUNT(f) as count
			RETURN ext, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Query should execute without error")

		t.Logf("Extension query result: %d rows", len(result.Rows))
		t.Logf("Columns: %v", result.Columns)

		byExtension := make(map[string]int64)
		for _, row := range result.Rows {
			ext := fmt.Sprintf("%v", row[0])
			count := exactTestToInt64(row[1])
			t.Logf("  %s: %d", ext, count)
			byExtension[ext] = count
		}

		// Expected: .md=8, .ts=1, .js=1
		assert.Equal(t, int64(8), byExtension[".md"], "Should have 8 .md files")
		assert.Equal(t, int64(1), byExtension[".ts"], "Should have 1 .ts file")
		assert.Equal(t, int64(1), byExtension[".js"], "Should have 1 .js file")
	})

	t.Run("EXACT Mimir byType query (index-api.ts:682-689)", func(t *testing.T) {
		// EXACT query from Mimir - copy-pasted from index-api.ts
		query := `
			MATCH (f:File)
			WITH f, [label IN labels(f) WHERE label <> 'File'] as filteredLabels
			UNWIND filteredLabels as label
			WITH label, COUNT(f) as count
			RETURN label as type, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Query should execute without error")

		t.Logf("byType query result: %d rows", len(result.Rows))
		t.Logf("Columns: %v", result.Columns)

		byType := make(map[string]int64)
		for _, row := range result.Rows {
			typeLabel := fmt.Sprintf("%v", row[0])
			count := exactTestToInt64(row[1])
			t.Logf("  %s: %d", typeLabel, count)
			byType[typeLabel] = count
		}

		// Expected: Node=10 (all files have File:Node labels, File is filtered out)
		assert.Equal(t, int64(10), byType["Node"], "Should have 10 Node labels (File filtered out)")
		assert.Equal(t, int64(0), byType["File"], "File label should be filtered out")
	})
}

// TestMimirExactQueriesWithEmbeddings tests with files that have embeddings
func TestMimirExactQueriesWithEmbeddings(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create files
	setupQueries := []string{
		`CREATE (:File:Node {path: '/test/doc1.md', extension: '.md', name: 'doc1.md'})`,
		`CREATE (:File:Node {path: '/test/doc2.md', extension: '.md', name: 'doc2.md'})`,
		`CREATE (:File:Node {path: '/test/doc3.md', extension: '.md', name: 'doc3.md'})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// Set embeddings on 2 of 3 files (simulating NornicDB embed worker)
	nodes, err := store.GetNodesByLabel("File")
	require.NoError(t, err)
	require.Len(t, nodes, 3)

	// Set embedding on first 2 nodes
	for i := 0; i < 2; i++ {
		nodes[i].Embedding = []float32{0.1, 0.2, 0.3}
		nodes[i].Properties["has_embedding"] = true
		nodes[i].Properties["embedding"] = true // Marker for IS NOT NULL check
		err = store.UpdateNode(nodes[i])
		require.NoError(t, err)
	}

	t.Run("EXACT Mimir stats query with embeddings", func(t *testing.T) {
		query := `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c,
			  CASE WHEN c IS NOT NULL AND c.embedding IS NOT NULL THEN 1 ELSE 0 END as chunkHasEmbedding,
			  CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END as fileHasEmbedding
			WITH 
			  COUNT(DISTINCT f) as totalFiles,
			  COUNT(DISTINCT c) as totalChunks,
			  SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) as totalEmbeddings,
			  COLLECT(DISTINCT f.extension) as extensions
			RETURN 
			  totalFiles,
			  totalChunks,
			  totalEmbeddings,
			  extensions
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Query should execute without error")
		require.Len(t, result.Rows, 1, "Should return exactly 1 row")

		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row: %v", result.Rows[0])

		var totalFiles, totalChunks, totalEmbeddings int64
		for i, col := range result.Columns {
			val := result.Rows[0][i]
			t.Logf("  %s = %v (type: %T)", col, val, val)
			switch col {
			case "totalFiles":
				totalFiles = exactTestToInt64(val)
			case "totalChunks":
				totalChunks = exactTestToInt64(val)
			case "totalEmbeddings":
				totalEmbeddings = exactTestToInt64(val)
			}
		}

		assert.Equal(t, int64(3), totalFiles, "Should have 3 files")
		assert.Equal(t, int64(0), totalChunks, "Should have 0 chunks")
		// 2 files have embedding property set to true
		assert.Equal(t, int64(2), totalEmbeddings, "Should have 2 embeddings")
	})
}

// TestMimirE2EWithAsyncStorageAndEmbeddings is a full end-to-end test that:
// 1. Uses AsyncEngine + BadgerDB (like production)
// 2. Creates File nodes via Cypher
// 3. Creates FileChunk nodes with HAS_CHUNK relationships
// 4. Sets embeddings on Files and Chunks manually
// 5. Verifies the exact Mimir stats queries return correct counts
//
// This test verifies the fix for the bug where embeddings weren't persisting
// through the AsyncEngine flush to BadgerDB.
func TestMimirE2EWithAsyncStorageAndEmbeddings(t *testing.T) {
	// Create temp directory for BadgerDB
	tmpDir, err := os.MkdirTemp("", "mimir-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create BadgerEngine -> AsyncEngine stack (like production)
	badger, err := storage.NewBadgerEngine(filepath.Join(tmpDir, "data"))
	require.NoError(t, err)
	defer badger.Close()

	config := storage.DefaultAsyncEngineConfig()
	config.FlushInterval = 100 * time.Millisecond
	async := storage.NewAsyncEngine(badger, config)
	defer async.Close()

	exec := NewStorageExecutor(async)
	ctx := context.Background()

	// ==========================================================================
	// Step 1: Create 10 File nodes via Cypher (like Mimir does)
	// ==========================================================================
	setupQueries := []string{
		`CREATE (:File:Node {id: 'file1', path: '/test/doc1.md', extension: '.md', name: 'doc1.md', content: 'content 1'})`,
		`CREATE (:File:Node {id: 'file2', path: '/test/doc2.md', extension: '.md', name: 'doc2.md', content: 'content 2'})`,
		`CREATE (:File:Node {id: 'file3', path: '/test/doc3.md', extension: '.md', name: 'doc3.md', content: 'content 3'})`,
		`CREATE (:File:Node {id: 'file4', path: '/test/doc4.md', extension: '.md', name: 'doc4.md', content: 'content 4'})`,
		`CREATE (:File:Node {id: 'file5', path: '/test/doc5.md', extension: '.md', name: 'doc5.md', content: 'content 5'})`,
		`CREATE (:File:Node {id: 'file6', path: '/test/doc6.md', extension: '.md', name: 'doc6.md', content: 'content 6'})`,
		`CREATE (:File:Node {id: 'file7', path: '/test/doc7.md', extension: '.md', name: 'doc7.md', content: 'content 7'})`,
		`CREATE (:File:Node {id: 'file8', path: '/test/doc8.md', extension: '.md', name: 'doc8.md', content: 'content 8'})`,
		`CREATE (:File:Node {id: 'file9', path: '/test/app.ts', extension: '.ts', name: 'app.ts', content: 'typescript'})`,
		`CREATE (:File:Node {id: 'file10', path: '/test/util.js', extension: '.js', name: 'util.js', content: 'javascript'})`,
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// ==========================================================================
	// Step 2: Create FileChunk nodes AND HAS_CHUNK relationships via Cypher
	//   MATCH (f:File {path: $path})
	//   MERGE (c:FileChunk:Node {id: $chunkId})
	//   SET c.chunk_index = $chunkIndex, c.text = $text, ...
	//   MERGE (f)-[:HAS_CHUNK {index: $chunkIndex}]->(c)
	// ==========================================================================
	// We'll create chunks for files 1-5 (2 chunks each = 10 total chunks)
	chunkQueries := []string{
		// File 1 chunks
		`MATCH (f:File {path: '/test/doc1.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk1a'}) 
		 SET c.chunk_index = 0, c.text = 'chunk 1a text content', c.start_offset = 0, c.end_offset = 100, 
		     c.parent_file_id = 'file1', c.parent_file_path = '/test/doc1.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = true, c.has_prev = false 
		 MERGE (f)-[:HAS_CHUNK {index: 0}]->(c)`,
		`MATCH (f:File {path: '/test/doc1.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk1b'}) 
		 SET c.chunk_index = 1, c.text = 'chunk 1b text content', c.start_offset = 100, c.end_offset = 200, 
		     c.parent_file_id = 'file1', c.parent_file_path = '/test/doc1.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = false, c.has_prev = true 
		 MERGE (f)-[:HAS_CHUNK {index: 1}]->(c)`,
		// File 2 chunks
		`MATCH (f:File {path: '/test/doc2.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk2a'}) 
		 SET c.chunk_index = 0, c.text = 'chunk 2a text content', c.start_offset = 0, c.end_offset = 100, 
		     c.parent_file_id = 'file2', c.parent_file_path = '/test/doc2.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = true, c.has_prev = false 
		 MERGE (f)-[:HAS_CHUNK {index: 0}]->(c)`,
		`MATCH (f:File {path: '/test/doc2.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk2b'}) 
		 SET c.chunk_index = 1, c.text = 'chunk 2b text content', c.start_offset = 100, c.end_offset = 200, 
		     c.parent_file_id = 'file2', c.parent_file_path = '/test/doc2.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = false, c.has_prev = true 
		 MERGE (f)-[:HAS_CHUNK {index: 1}]->(c)`,
		// File 3 chunks
		`MATCH (f:File {path: '/test/doc3.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk3a'}) 
		 SET c.chunk_index = 0, c.text = 'chunk 3a text content', c.start_offset = 0, c.end_offset = 100, 
		     c.parent_file_id = 'file3', c.parent_file_path = '/test/doc3.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = true, c.has_prev = false 
		 MERGE (f)-[:HAS_CHUNK {index: 0}]->(c)`,
		`MATCH (f:File {path: '/test/doc3.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk3b'}) 
		 SET c.chunk_index = 1, c.text = 'chunk 3b text content', c.start_offset = 100, c.end_offset = 200, 
		     c.parent_file_id = 'file3', c.parent_file_path = '/test/doc3.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = false, c.has_prev = true 
		 MERGE (f)-[:HAS_CHUNK {index: 1}]->(c)`,
		// File 4 chunks
		`MATCH (f:File {path: '/test/doc4.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk4a'}) 
		 SET c.chunk_index = 0, c.text = 'chunk 4a text content', c.start_offset = 0, c.end_offset = 100, 
		     c.parent_file_id = 'file4', c.parent_file_path = '/test/doc4.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = true, c.has_prev = false 
		 MERGE (f)-[:HAS_CHUNK {index: 0}]->(c)`,
		`MATCH (f:File {path: '/test/doc4.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk4b'}) 
		 SET c.chunk_index = 1, c.text = 'chunk 4b text content', c.start_offset = 100, c.end_offset = 200, 
		     c.parent_file_id = 'file4', c.parent_file_path = '/test/doc4.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = false, c.has_prev = true 
		 MERGE (f)-[:HAS_CHUNK {index: 1}]->(c)`,
		// File 5 chunks
		`MATCH (f:File {path: '/test/doc5.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk5a'}) 
		 SET c.chunk_index = 0, c.text = 'chunk 5a text content', c.start_offset = 0, c.end_offset = 100, 
		     c.parent_file_id = 'file5', c.parent_file_path = '/test/doc5.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = true, c.has_prev = false 
		 MERGE (f)-[:HAS_CHUNK {index: 0}]->(c)`,
		`MATCH (f:File {path: '/test/doc5.md'}) 
		 MERGE (c:FileChunk:Node {id: 'chunk5b'}) 
		 SET c.chunk_index = 1, c.text = 'chunk 5b text content', c.start_offset = 100, c.end_offset = 200, 
		     c.parent_file_id = 'file5', c.parent_file_path = '/test/doc5.md', c.type = 'file_chunk', 
		     c.total_chunks = 2, c.has_next = false, c.has_prev = true 
		 MERGE (f)-[:HAS_CHUNK {index: 1}]->(c)`,
	}
	for i, q := range chunkQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Chunk query %d failed", i)
	}

	// Flush to BadgerDB
	err = async.Flush()
	require.NoError(t, err)

	// ==========================================================================
	// Step 3: Verify chunks were created with relationships
	// ==========================================================================
	allNodes, err := async.AllNodes()
	require.NoError(t, err)

	// Build maps for lookup
	fileNodes := make(map[string]*storage.Node)
	chunkNodes := make(map[string]*storage.Node)
	for _, n := range allNodes {
		if hasLabel(n.Labels, "File") {
			if path, ok := n.Properties["path"].(string); ok {
				fileNodes[path] = n
			}
		}
		if hasLabel(n.Labels, "FileChunk") {
			if id, ok := n.Properties["id"].(string); ok {
				chunkNodes[id] = n
			}
		}
	}

	t.Logf("Found %d File nodes and %d FileChunk nodes", len(fileNodes), len(chunkNodes))
	require.Equal(t, 10, len(fileNodes), "Should have 10 File nodes")
	require.Equal(t, 10, len(chunkNodes), "Should have 10 FileChunk nodes")

	// ==========================================================================
	// Step 4: Set embeddings on Files and Chunks manually
	// - 3 Files with embeddings (file1, file2, file3)
	// - 6 Chunks with embeddings (chunk1a, chunk1b, chunk2a, chunk2b, chunk3a, chunk3b)
	// Total embeddings expected: 3 + 6 = 9
	// ==========================================================================
	filesWithEmbeddings := []string{"file1", "file2", "file3"}
	chunksWithEmbeddings := []string{"chunk1a", "chunk1b", "chunk2a", "chunk2b", "chunk3a", "chunk3b"}

	// Set embeddings on files
	// Mimir stores the actual embedding ARRAY as a property, not just a marker!
	// From FileIndexer.ts: SET f.embedding = $embedding, f.embedding_dimensions = ...
	for _, filePath := range []string{"/test/doc1.md", "/test/doc2.md", "/test/doc3.md"} {
		node := fileNodes[filePath]
		if node == nil {
			t.Logf("Warning: file node not found for %s", filePath)
			continue
		}
		// Store embedding as []interface{} property (like Mimir/Neo4j does)
		embeddingArray := []interface{}{0.1, 0.2, 0.3, 0.4}
		node.Embedding = []float32{0.1, 0.2, 0.3, 0.4} // Also native field
		node.Properties["embedding"] = embeddingArray  // Property like Mimir!
		node.Properties["embedding_dimensions"] = 4
		node.Properties["embedding_model"] = "test-model"
		node.Properties["has_embedding"] = true
		err := async.UpdateNode(node)
		require.NoError(t, err)
	}

	// Set embeddings on chunks - they already have embedding property from Cypher MERGE
	// But let's update them to ensure embedding property is set correctly
	for _, chunkID := range chunksWithEmbeddings {
		node := chunkNodes[chunkID]
		if node == nil {
			continue
		}
		// Store embedding as []interface{} property (like Mimir does)
		embeddingArray := []interface{}{0.5, 0.6, 0.7, 0.8}
		node.Embedding = []float32{0.5, 0.6, 0.7, 0.8} // Also native field
		node.Properties["embedding"] = embeddingArray  // Property like Mimir!
		node.Properties["embedding_dimensions"] = 4
		node.Properties["embedding_model"] = "test-model"
		node.Properties["has_embedding"] = true
		err := async.UpdateNode(node)
		require.NoError(t, err)
	}

	// Flush embedding updates
	err = async.Flush()
	require.NoError(t, err)

	t.Logf("Set embeddings: %d files, %d chunks", len(filesWithEmbeddings), len(chunksWithEmbeddings))

	// ==========================================================================
	// Step 5: Run EXACT Mimir stats query and verify counts
	// Expected:
	// - totalFiles: 10
	// - totalChunks: 10 (5 files × 2 chunks each)
	// - totalEmbeddings: 12 (Mimir query behavior: 3 files × 2 chunks = 6 file counts + 6 chunk counts)
	//   NOTE: The Mimir query counts file embeddings ONCE PER CHUNK due to OPTIONAL MATCH
	//         So 3 files with embeddings, each with 2 chunks = 6 fileHasEmbedding counts
	//         Plus 6 chunks with embeddings = 6 chunkHasEmbedding counts = 12 total
	// ==========================================================================
	t.Run("verify totalFiles, totalChunks, and totalEmbeddings", func(t *testing.T) {
		query := `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c,
			  CASE WHEN c IS NOT NULL AND c.embedding IS NOT NULL THEN 1 ELSE 0 END as chunkHasEmbedding,
			  CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END as fileHasEmbedding
			WITH 
			  COUNT(DISTINCT f) as totalFiles,
			  COUNT(DISTINCT c) as totalChunks,
			  SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) as totalEmbeddings,
			  COLLECT(DISTINCT f.extension) as extensions
			RETURN 
			  totalFiles,
			  totalChunks,
			  totalEmbeddings,
			  extensions
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Stats query should succeed")
		require.Len(t, result.Rows, 1, "Should return 1 row")

		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row: %v", result.Rows[0])

		var totalFiles, totalChunks, totalEmbeddings int64
		for i, col := range result.Columns {
			val := result.Rows[0][i]
			switch col {
			case "totalFiles":
				totalFiles = exactTestToInt64(val)
			case "totalChunks":
				totalChunks = exactTestToInt64(val)
			case "totalEmbeddings":
				totalEmbeddings = exactTestToInt64(val)
			}
		}

		assert.Equal(t, int64(10), totalFiles, "Should have 10 total files")
		assert.Equal(t, int64(10), totalChunks, "Should have 10 chunks (5 files × 2 chunks)")
		assert.Equal(t, int64(12), totalEmbeddings, "Should have 12 embeddings (3 files × 2 chunks each + 6 chunk embeddings)")
	})

	t.Run("verify extension counts", func(t *testing.T) {
		query := `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			WITH f.extension as ext, COUNT(f) as count
			RETURN ext, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		byExtension := make(map[string]int64)
		for _, row := range result.Rows {
			ext := fmt.Sprintf("%v", row[0])
			count := exactTestToInt64(row[1])
			byExtension[ext] = count
		}

		t.Logf("byExtension: %v", byExtension)
		assert.Equal(t, int64(8), byExtension[".md"], "Should have 8 .md files")
		assert.Equal(t, int64(1), byExtension[".ts"], "Should have 1 .ts file")
		assert.Equal(t, int64(1), byExtension[".js"], "Should have 1 .js file")
	})

	t.Run("verify byType counts", func(t *testing.T) {
		query := `
			MATCH (f:File)
			WITH f, [label IN labels(f) WHERE label <> 'File'] as filteredLabels
			UNWIND filteredLabels as label
			WITH label, COUNT(f) as count
			RETURN label as type, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		byType := make(map[string]int64)
		for _, row := range result.Rows {
			typ := fmt.Sprintf("%v", row[0])
			count := exactTestToInt64(row[1])
			byType[typ] = count
		}

		t.Logf("byType: %v", byType)
		assert.Equal(t, int64(10), byType["Node"], "Should have 10 Node labels")
		assert.Equal(t, int64(0), byType["File"], "File should be filtered out")
	})

	// ==========================================================================
	// Step 6: Verify embeddings persisted to BadgerDB
	// ==========================================================================
	t.Run("verify embeddings persisted in BadgerDB", func(t *testing.T) {
		err = async.Flush()
		require.NoError(t, err)

		var filesWithEmbed, chunksWithEmbed int
		badger.IterateNodes(func(n *storage.Node) bool {
			if len(n.Embedding) > 0 {
				if hasLabel(n.Labels, "File") {
					filesWithEmbed++
				}
				if hasLabel(n.Labels, "FileChunk") {
					chunksWithEmbed++
				}
			}
			return true
		})

		assert.Equal(t, 3, filesWithEmbed, "BadgerDB should have 3 File nodes with embeddings")
		assert.Equal(t, 6, chunksWithEmbed, "BadgerDB should have 6 FileChunk nodes with embeddings")
	})
}

// hasLabel checks if a label is in the labels slice
func hasLabel(labels []string, target string) bool {
	for _, l := range labels {
		if l == target {
			return true
		}
	}
	return false
}

func exactTestToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case nil:
		return 0
	default:
		return 0
	}
}
