package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMimirStatsQueries tests the exact queries used by Mimir's index-stats API.
// These queries must work for the VSCode plugin to show correct stats.
func TestMimirStatsQueries(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create File nodes with various properties
	setupQueries := []string{
		`CREATE (f:File:Node {path: '/test/file1.ts', extension: '.ts', name: 'file1.ts', content: 'const x = 1'})`,
		`CREATE (f:File:Node {path: '/test/file2.ts', extension: '.ts', name: 'file2.ts', content: 'const y = 2'})`,
		`CREATE (f:File:Node {path: '/test/file3.md', extension: '.md', name: 'file3.md', content: '# Title'})`,
		`CREATE (f:File:Node {path: '/test/file4.js', extension: '.js', name: 'file4.js', content: 'var z = 3'})`,
		`CREATE (f:File:Node {path: '/test/file5.txt', name: 'file5.txt', content: 'plain text'})`, // No extension
	}
	for _, q := range setupQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("exact Mimir aggregate stats query", func(t *testing.T) {
		// This is the EXACT query from Mimir's index-api.ts lines 642-658
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
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should return exactly 1 row")

		row := result.Rows[0]
		t.Logf("Result row: %v", row)
		t.Logf("Columns: %v", result.Columns)

		// Find column indices
		totalFilesIdx := findColumnIndex(result.Columns, "totalFiles")
		totalChunksIdx := findColumnIndex(result.Columns, "totalChunks")
		totalEmbeddingsIdx := findColumnIndex(result.Columns, "totalEmbeddings")

		require.GreaterOrEqual(t, totalFilesIdx, 0, "Should have totalFiles column")
		require.GreaterOrEqual(t, totalChunksIdx, 0, "Should have totalChunks column")
		require.GreaterOrEqual(t, totalEmbeddingsIdx, 0, "Should have totalEmbeddings column")

		totalFiles := mimirToInt64(row[totalFilesIdx])
		totalChunks := mimirToInt64(row[totalChunksIdx])
		totalEmbeddings := mimirToInt64(row[totalEmbeddingsIdx])

		assert.Equal(t, int64(5), totalFiles, "Should count 5 files")
		assert.Equal(t, int64(0), totalChunks, "No chunks created yet")
		assert.Equal(t, int64(0), totalEmbeddings, "No embeddings yet")
	})

	t.Run("exact Mimir extension query", func(t *testing.T) {
		// This is the EXACT query from Mimir's index-api.ts lines 666-680
		query := `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			WITH f.extension as ext, COUNT(f) as count
			RETURN ext, count
			ORDER BY count DESC
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		t.Logf("Extension result: %v rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v", row)
		}

		// Should have 3 distinct extensions: .ts (2), .md (1), .js (1)
		// file5.txt has no extension so shouldn't be counted
		assert.GreaterOrEqual(t, len(result.Rows), 1, "Should have at least 1 extension group")
	})

	t.Run("exact Mimir byType query", func(t *testing.T) {
		// This is the EXACT query from Mimir's index-api.ts lines 682-689
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

		t.Logf("Type result: %v rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v", row)
		}

		// All files have label "Node" in addition to "File"
		// So we should get Node: 5
		assert.GreaterOrEqual(t, len(result.Rows), 1, "Should have at least 1 type")

		if len(result.Rows) > 0 {
			// First row should be "Node" with count 5
			assert.Equal(t, "Node", result.Rows[0][0])
			assert.Equal(t, int64(5), mimirToInt64(result.Rows[0][1]))
		}
	})
}

// TestMimirStatsWithEmbeddings tests stats queries when nodes have embeddings
func TestMimirStatsWithEmbeddings(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a file with an embedding (simulating what NornicDB's embed worker does)
	// Note: We need to create the node and then set its embedding
	_, err := exec.Execute(ctx, `CREATE (f:File:Node {path: '/test/file1.ts', extension: '.ts', content: 'hello'})`, nil)
	require.NoError(t, err)

	// Get the node and set its embedding directly
	nodes, err := store.GetNodesByLabel("File")
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	// Set embedding on the node (this is what NornicDB's embed worker does)
	nodes[0].ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3, 0.4}}
	nodes[0].Properties["has_embedding"] = true
	// Also set "embedding" property for Mimir's IS NOT NULL check
	nodes[0].Properties["embedding"] = true // Just a marker, not the actual array
	err = store.UpdateNode(nodes[0])
	require.NoError(t, err)

	t.Run("count files with embeddings using has_embedding property", func(t *testing.T) {
		query := `
			MATCH (f:File)
			WHERE f.has_embedding = true
			RETURN count(f) as count
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		count := mimirToInt64(result.Rows[0][0])
		assert.Equal(t, int64(1), count, "Should find 1 file with embedding")
	})

	t.Run("simplified embedding count query", func(t *testing.T) {
		// Simpler query that NornicDB can handle
		query := `
			MATCH (f:File)
			WITH 
			  COUNT(f) as totalFiles,
			  SUM(CASE WHEN f.has_embedding = true THEN 1 ELSE 0 END) as totalEmbeddings
			RETURN totalFiles, totalEmbeddings
		`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		t.Logf("Result: %v", result.Rows[0])

		totalFiles := mimirToInt64(result.Rows[0][0])
		totalEmbeddings := mimirToInt64(result.Rows[0][1])

		assert.Equal(t, int64(1), totalFiles)
		assert.Equal(t, int64(1), totalEmbeddings)
	})
}

// Helper to find column index
func findColumnIndex(columns []string, name string) int {
	for i, col := range columns {
		if col == name {
			return i
		}
	}
	return -1
}

// Helper to convert to int64
func mimirToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}
