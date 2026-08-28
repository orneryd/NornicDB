// Unit tests for the VSCode Code Intelligence stats query.
// This tests the complex compound query pattern used by the index-stats endpoint.

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupFileIndexTestData creates test data simulating indexed files with chunks
func setupFileIndexTestData(t *testing.T, store storage.Engine) {
	ctx := context.Background()
	exec := NewStorageExecutor(store)

	// Create File nodes
	_, err := exec.Execute(ctx, `CREATE (f:File {id: 'file1', path: '/test/file1.ts', extension: '.ts', embedding: [0.1, 0.2]})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (f:File {id: 'file2', path: '/test/file2.ts', extension: '.ts'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (f:File {id: 'file3', path: '/test/file3.md', extension: '.md', embedding: [0.3, 0.4]})`, nil)
	require.NoError(t, err)

	// Create FileChunk nodes
	_, err = exec.Execute(ctx, `CREATE (c:FileChunk {id: 'chunk1', text: 'chunk 1 content', embedding: [0.1, 0.2]})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (c:FileChunk {id: 'chunk2', text: 'chunk 2 content', embedding: [0.3, 0.4]})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (c:FileChunk {id: 'chunk3', text: 'chunk 3 content'})`, nil) // No embedding
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (c:FileChunk {id: 'chunk4', text: 'chunk 4 content', embedding: [0.5, 0.6]})`, nil)
	require.NoError(t, err)

	// Create HAS_CHUNK relationships
	// file1 -> chunk1, chunk2
	_, err = exec.Execute(ctx, `
		MATCH (f:File {id: 'file1'}), (c:FileChunk {id: 'chunk1'})
		CREATE (f)-[:HAS_CHUNK]->(c)
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (f:File {id: 'file1'}), (c:FileChunk {id: 'chunk2'})
		CREATE (f)-[:HAS_CHUNK]->(c)
	`, nil)
	require.NoError(t, err)

	// file2 -> chunk3
	_, err = exec.Execute(ctx, `
		MATCH (f:File {id: 'file2'}), (c:FileChunk {id: 'chunk3'})
		CREATE (f)-[:HAS_CHUNK]->(c)
	`, nil)
	require.NoError(t, err)

	// file3 has no chunks (orphan file)
}

// TestMatchOptionalMatchBasic tests basic MATCH ... OPTIONAL MATCH pattern
func TestMatchOptionalMatchBasic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupFileIndexTestData(t, store)

	t.Run("MATCH OPTIONAL MATCH with count", func(t *testing.T) {
		// This is a simplified version of the stats query
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			RETURN count(DISTINCT f) as fileCount, count(c) as chunkCount
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// We have 3 files
		fileCount, ok := result.Rows[0][0].(int64)
		require.True(t, ok, "fileCount should be int64, got %T", result.Rows[0][0])
		assert.Equal(t, int64(3), fileCount)

		// We have 3 chunks connected (chunk1, chunk2, chunk3) - chunk4 is orphan
		chunkCount, ok := result.Rows[0][1].(int64)
		require.True(t, ok, "chunkCount should be int64, got %T", result.Rows[0][1])
		assert.Equal(t, int64(3), chunkCount)
	})
}

func TestMatchOptionalMatchWithAggregationWhereFiltersAfterWith(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:T {name:'T1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:T {name:'T2'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:C {name:'C1', met:false})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:C {name:'C2', met:true})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (t:T {name:'T1'}),(c:C {name:'C1'}) CREATE (t)-[:BLOCKED_BY]->(c)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (t:T {name:'T2'}),(c:C {name:'C2'}) CREATE (t)-[:BLOCKED_BY]->(c)`, nil)
	require.NoError(t, err)

	joined, err := exec.Execute(ctx, `
		MATCH (t:T)
		OPTIONAL MATCH (t)-[:BLOCKED_BY]->(c:C) WHERE c.met = false
		RETURN t.name AS name, c.name AS blocker
	`, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"T1", "C1"}, {"T2", nil}}, joined.Rows)

	counts, err := exec.Execute(ctx, `
		MATCH (t:T)
		OPTIONAL MATCH (t)-[:BLOCKED_BY]->(c:C) WHERE c.met = false
		WITH t, count(c) AS n
		RETURN t.name AS name, n
	`, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"T1", int64(1)}, {"T2", int64(0)}}, counts.Rows)

	result, err := exec.Execute(ctx, `
		MATCH (t:T)
		OPTIONAL MATCH (t)-[:BLOCKED_BY]->(c:C) WHERE c.met = false
		WITH t, count(c) AS n WHERE n = 0
		RETURN t.name AS name
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "T2", result.Rows[0][0])
}

func TestReportedWhereAndCreateRegressionCluster(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, query := range []string{
		`CREATE (:T {name:'T1'})`,
		`CREATE (:T {name:'T2'})`,
		`CREATE (:C {name:'C1', met:false})`,
		`CREATE (:C {name:'C2', met:true})`,
		`MATCH (t:T {name:'T1'}),(c:C {name:'C1'}) CREATE (t)-[:BLOCKED_BY]->(c)`,
		`MATCH (t:T {name:'T2'}),(c:C {name:'C2'}) CREATE (t)-[:BLOCKED_BY]->(c)`,
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	simpleWithWhere, err := exec.Execute(ctx, `
		MATCH (t:T)
		WITH t WHERE t.name = 'T2'
		RETURN t.name AS name
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, simpleWithWhere.Columns)
	require.Equal(t, [][]interface{}{{"T2"}}, simpleWithWhere.Rows)

	patternPredicate, err := exec.Execute(ctx, `
		MATCH (t:T)
		WHERE NOT (t)-[:BLOCKED_BY]->(:C {met:false})
		RETURN t.name AS name
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, patternPredicate.Columns)
	require.Equal(t, [][]interface{}{{"T2"}}, patternPredicate.Rows)

	notBooleanProperty, err := exec.Execute(ctx, `
		MATCH (t:T)-[:BLOCKED_BY]->(c:C)
		WHERE NOT c.met
		RETURN t.name AS name
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, notBooleanProperty.Columns)
	require.Equal(t, [][]interface{}{{"T1"}}, notBooleanProperty.Rows)

	createTwoRelationships, err := exec.Execute(ctx, `
		CREATE (a:T {name:'x'})-[:R]->(b:C {name:'y'})
		CREATE (c:T {name:'z'})-[:R]->(d:C {name:'w'})
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, createTwoRelationships.Stats)
	require.Equal(t, 4, createTwoRelationships.Stats.NodesCreated)
	require.Equal(t, 2, createTwoRelationships.Stats.RelationshipsCreated)
}

func TestOptionalRelatedMatchesWhereBindsTargetVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	pattern := exec.parseOptionalRelPattern(ctx, `(t)-[:BLOCKED_BY]->(c:C)`)
	require.Equal(t, "c", pattern.targetVar)

	related := optionalRelResult{node: &storage.Node{ID: "c1", Labels: []string{"C"}, Properties: map[string]interface{}{"met": false}}}
	assert.True(t, exec.optionalRelatedMatchesWhere(ctx, related, pattern, "c.met = false"))

	related.node.Properties["met"] = true
	assert.False(t, exec.optionalRelatedMatchesWhere(ctx, related, pattern, "c.met = false"))
}

func TestGroupedWithAggregationCountSkipsNullOptionalTarget(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t1 := &storage.Node{ID: "t1", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "T1"}}
	t2 := &storage.Node{ID: "t2", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "T2"}}
	c1 := &storage.Node{ID: "c1", Labels: []string{"C"}, Properties: map[string]interface{}{"name": "C1"}}
	rows := []joinedRow{
		{initialNode: t1, relatedNode: c1},
		{initialNode: t2, relatedNode: nil},
	}
	require.Equal(t, "c", extractFuncInner("count(c)"))
	require.True(t, isAggregateExpression("count(c)"))
	require.True(t, isAggregateFuncName("count(c)", "count"))

	result, err := exec.processGroupedWithAggregation(
		ctx,
		rows,
		"t",
		"c",
		"",
		[]returnItem{{expr: "t"}, {expr: "count(c)", alias: "n"}},
		[]returnItem{{expr: "t.name", alias: "name"}, {expr: "n"}},
		"",
	)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"T1", int64(1)}, {"T2", int64(0)}}, result.Rows)
}

// TestMatchOptionalMatchWithCase tests OPTIONAL MATCH with CASE WHEN
func TestMatchOptionalMatchWithCase(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupFileIndexTestData(t, store)

	t.Run("CASE WHEN with null check", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c, 
				CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END as hasChunk
			RETURN count(DISTINCT f) as files, sum(hasChunk) as chunkConnections
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		files := result.Rows[0][0]
		assert.Equal(t, int64(3), files)

		// 3 chunk connections (file1->chunk1, file1->chunk2, file2->chunk3)
		chunkConnections := result.Rows[0][1]
		assert.Equal(t, float64(3), chunkConnections)
	})
}

// TestMatchOptionalMatchWithMultipleWith tests multiple WITH clauses
func TestMatchOptionalMatchWithMultipleWith(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupFileIndexTestData(t, store)

	t.Run("multiple WITH clauses", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c,
				CASE WHEN c IS NOT NULL AND c.embedding IS NOT NULL THEN 1 ELSE 0 END as chunkHasEmbedding,
				CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END as fileHasEmbedding
			WITH 
				COUNT(DISTINCT f) as totalFiles,
				COUNT(DISTINCT c) as totalChunks,
				SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) as totalEmbeddings
			RETURN totalFiles, totalChunks, totalEmbeddings
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// 3 files
		totalFiles := result.Rows[0][0]
		assert.Equal(t, int64(3), totalFiles, "Expected 3 files")

		// 3 chunks connected via relationships
		totalChunks := result.Rows[0][1]
		assert.Equal(t, int64(3), totalChunks, "Expected 3 chunks")

		// Embeddings: file1 has embedding (1), file3 has embedding (1),
		// chunk1 has embedding (1), chunk2 has embedding (1), chunk3 no embedding (0)
		// Total = 2 file embeddings + 2 chunk embeddings = 4
		// But we only count chunks connected via HAS_CHUNK, so chunk4 doesn't count
		totalEmbeddings := result.Rows[0][2]
		assert.NotNil(t, totalEmbeddings, "totalEmbeddings should not be nil")
	})
}

// TestVSCodeStatsQuery tests the exact query used by VSCode extension
func TestVSCodeStatsQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupFileIndexTestData(t, store)

	t.Run("exact VSCode stats query", func(t *testing.T) {
		// This is the exact query from index-api.ts
		result, err := exec.Execute(ctx, `
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
		`, nil)
		require.NoError(t, err, "Query should execute without error")
		require.Len(t, result.Rows, 1, "Should return exactly 1 row")

		// Verify columns
		assert.Contains(t, result.Columns, "totalFiles")
		assert.Contains(t, result.Columns, "totalChunks")
		assert.Contains(t, result.Columns, "totalEmbeddings")
		assert.Contains(t, result.Columns, "extensions")

		// Verify values are not null
		assert.NotNil(t, result.Rows[0][0], "totalFiles should not be nil")
		assert.NotNil(t, result.Rows[0][1], "totalChunks should not be nil")

		// Verify correct counts
		totalFiles := result.Rows[0][0]
		assert.Equal(t, int64(3), totalFiles, "Should have 3 files")

		totalChunks := result.Rows[0][1]
		assert.Equal(t, int64(3), totalChunks, "Should have 3 chunks connected")
	})
}

// TestCountDistinct tests COUNT(DISTINCT) aggregation
func TestCountDistinct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with duplicate values
	_, err := exec.Execute(ctx, `CREATE (n:Item {type: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {type: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {type: 'B'})`, nil)
	require.NoError(t, err)

	t.Run("COUNT DISTINCT nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN COUNT(DISTINCT n) as distinctNodes, COUNT(n) as totalNodes
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Both should be 3 (3 distinct nodes)
		distinctNodes := result.Rows[0][0]
		assert.Equal(t, int64(3), distinctNodes)

		totalNodes := result.Rows[0][1]
		assert.Equal(t, int64(3), totalNodes)
	})

	t.Run("COUNT DISTINCT property", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN COUNT(DISTINCT n.type) as distinctTypes
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Should be 2 distinct types (A, B)
		distinctTypes := result.Rows[0][0]
		assert.Equal(t, int64(2), distinctTypes)
	})
}

func TestBug_TraversalCountDistinctNode(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, query := range []string{
		"CREATE (:Entity {id: 'a'})",
		"CREATE (:Entity {id: 'b'})",
		"CREATE (:Entity {id: 'c'})",
		"MATCH (a:Entity {id: 'a'}), (b:Entity {id: 'b'}) CREATE (a)-[:LINK]->(b)",
		"MATCH (a:Entity {id: 'a'}), (c:Entity {id: 'c'}) CREATE (a)-[:LINK]->(c)",
		"MATCH (b:Entity {id: 'b'}), (c:Entity {id: 'c'}) CREATE (b)-[:LINK]->(c)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (source)-[:LINK]->() RETURN count(DISTINCT source), count(source), count(source.missing)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3), int64(0)}}, result.Rows)
}

func TestBug_TraversalRepeatedEndpointVariableRequiresSelfLoop(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, query := range []string{
		"CREATE (:Entity {id: 'a'})",
		"CREATE (:Entity {id: 'b'})",
		"MATCH (a:Entity {id: 'a'}), (b:Entity {id: 'b'}) CREATE (a)-[:LINK]->(b)",
		"MATCH (a:Entity {id: 'a'}) CREATE (a)-[:LINK]->(a)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (node)-[:LINK]->(node) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	grouped, err := exec.Execute(ctx, "MATCH (node)-[:LINK]->(node) RETURN node.id, count(*)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", int64(1)}}, grouped.Rows)
}

// TestCollectDistinct tests COLLECT(DISTINCT) aggregation
func TestCollectDistinct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Item {ext: '.ts'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {ext: '.ts'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {ext: '.md'})`, nil)
	require.NoError(t, err)

	t.Run("COLLECT DISTINCT property", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN COLLECT(DISTINCT n.ext) as extensions
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		extensions, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok, "Should return array, got %T", result.Rows[0][0])
		assert.Len(t, extensions, 2, "Should have 2 distinct extensions")
	})
}

// TestCaseWhenInStatsQuery tests CASE WHEN expression evaluation for stats queries
func TestCaseWhenInStatsQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Item {value: 10, name: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {value: null, name: 'B'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {name: 'C'})`, nil) // No value property
	require.NoError(t, err)

	t.Run("CASE WHEN IS NOT NULL", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WITH n, CASE WHEN n.value IS NOT NULL THEN 1 ELSE 0 END as hasValue
			RETURN sum(hasValue) as countWithValue
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Only 'A' has non-null value
		// Neo4j: SUM of integers returns int64
		countWithValue := result.Rows[0][0]
		assert.Equal(t, int64(1), countWithValue)
	})

	t.Run("CASE WHEN with AND condition", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WITH n, CASE WHEN n.value IS NOT NULL AND n.value > 5 THEN 1 ELSE 0 END as hasLargeValue
			RETURN sum(hasLargeValue) as countLargeValue
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Neo4j: SUM of integers returns int64
		countLargeValue := result.Rows[0][0]
		assert.Equal(t, int64(1), countLargeValue)
	})
}

// TestSumWithArithmetic tests SUM() + SUM() arithmetic
func TestSumWithArithmetic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Item {a: 1, b: 2})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {a: 3, b: 4})`, nil)
	require.NoError(t, err)

	t.Run("SUM + SUM arithmetic", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN SUM(n.a) + SUM(n.b) as total
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// SUM(a) = 1+3 = 4, SUM(b) = 2+4 = 6, total = 10
		total := result.Rows[0][0]
		assert.Equal(t, float64(10), total)
	})
}

// TestOptionalMatchWithNoMatches tests OPTIONAL MATCH when no relationships exist
func TestOptionalMatchWithNoMatches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create file with no chunks
	_, err := exec.Execute(ctx, `CREATE (f:File {id: 'lonely', path: '/test/lonely.ts'})`, nil)
	require.NoError(t, err)

	t.Run("OPTIONAL MATCH returns null for unmatched", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			RETURN f.id as fileId, c as chunk
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, "lonely", result.Rows[0][0])
		assert.Nil(t, result.Rows[0][1], "chunk should be null when no match")
	})

	t.Run("COUNT handles null from OPTIONAL MATCH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			RETURN COUNT(f) as files, COUNT(c) as chunks
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, int64(1), result.Rows[0][0], "Should count 1 file")
		assert.Equal(t, int64(0), result.Rows[0][1], "Should count 0 chunks (null)")
	})
}
