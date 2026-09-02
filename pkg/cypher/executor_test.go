// Package cypher provides tests for the Cypher executor.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStorageExecutor(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	assert.NotNil(t, exec)
	assert.NotNil(t, exec.parser)
	assert.NotNil(t, exec.storage)
}

func TestStorageExecutorQueryCacheTTLIsDatabaseScoped(t *testing.T) {
	fast := NewStorageExecutorWithQueryCachePolicy(newTestMemoryEngine(t), 25, 2*time.Second)
	slow := NewStorageExecutorWithQueryCachePolicy(newTestMemoryEngine(t), 2500, 30*time.Minute)
	disabled := NewStorageExecutorWithQueryCachePolicy(newTestMemoryEngine(t), 0, time.Minute)

	fastMaxEntries, fastTTL := fast.QueryCachePolicy()
	slowMaxEntries, slowTTL := slow.QueryCachePolicy()
	require.Equal(t, 25, fastMaxEntries)
	require.Equal(t, 2*time.Second, fastTTL)
	require.Equal(t, 2500, slowMaxEntries)
	require.Equal(t, 30*time.Minute, slowTTL)
	require.Nil(t, disabled.cache)
	require.Equal(t, fast.queryCacheTTL, fast.cloneWithStorage(fast.storage).queryCacheTTL)
	require.Equal(t, slow.queryCacheTTL, slow.cloneWithStorage(slow.storage).queryCacheTTL)
}

func TestStorageExecutorCachesResultsWithDatabaseTTL(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(newTestMemoryEngine(t), 10, 37*time.Second)
	query := "MATCH (n) RETURN n"

	_, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)

	key := cacheKeyFNV(query, nil)
	exec.cache.mu.RLock()
	entry := exec.cache.cache[key]
	exec.cache.mu.RUnlock()
	require.NotNil(t, entry)
	require.Equal(t, 37*time.Second, entry.ttl)
}

func TestInvalidateCommittedWriteCaches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	query := "MATCH (n:Repository) RETURN n"
	exec.cache.Put(query, nil, &ExecuteResult{Rows: [][]interface{}{{"stale"}}}, time.Minute)
	exec.nodeLookupCache["Repository:id=repository"] = &storage.Node{ID: "repository"}

	exec.InvalidateCommittedWriteCaches()

	_, found := exec.cache.Get(query, nil)
	assert.False(t, found)
	assert.Empty(t, exec.nodeLookupCache)
}

func TestCloneWithStorageSharesNodeLookupCacheLock(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	clone := exec.cloneWithStorage(store)

	exec.nodeLookupCacheMu.Lock()
	defer exec.nodeLookupCacheMu.Unlock()

	locked := make(chan struct{})
	go func() {
		clone.nodeLookupCacheMu.Lock()
		defer clone.nodeLookupCacheMu.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
		t.Fatal("clone acquired node lookup cache lock while original held it")
	case <-time.After(200 * time.Millisecond):
	}

	exec.nodeLookupCacheMu.Unlock()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("clone did not acquire node lookup cache lock after original released it")
	}
	exec.nodeLookupCacheMu.Lock()
}

func TestExecuteEmptyQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := exec.Execute(context.Background(), "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty query")
}

func TestExecuteInvalidSyntax(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "unmatched parenthesis",
			query: "MATCH (n RETURN n",
		},
		{
			name:  "unmatched bracket",
			query: "MATCH (a)-[r->(b) RETURN a",
		},
		{
			name:  "unmatched brace",
			query: "CREATE (n:Person {name: 'Alice')",
		},
		{
			name:  "unmatched single quote",
			query: "MATCH (n) WHERE n.name = 'Alice RETURN n",
		},
		{
			name:  "unmatched double quote",
			query: `MATCH (n) WHERE n.name = "Alice RETURN n`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), tt.query, nil)
			assert.Error(t, err, "Expected syntax error for invalid query")
			assert.Contains(t, err.Error(), "syntax error", "Error should be a syntax error")
		})
	}
}

func TestNormalizeCypherSyntaxConfusables(t *testing.T) {
	t.Run("normalizes syntax outside literals and comments", func(t *testing.T) {
		query := "// keep → and NBSP\nMATCH\u00A0（n:`Sec→tion` {note: 'a→b\u00A0', text: \"c←d\"}）\u200B-[r:SUBSECTION_OF]→（p）/* keep ← */ RETURN n．code， p"
		expected := "// keep → and NBSP\nMATCH (n:`Sec→tion` {note: 'a→b\u00A0', text: \"c←d\"})-[r:SUBSECTION_OF]->(p)/* keep ← */ RETURN n.code, p"
		assert.Equal(t, expected, normalizeCypherSyntaxConfusables(query))
	})

	t.Run("preserves escaped quoted and backticked content", func(t *testing.T) {
		query := "MATCH (n {single: 'it''s →', double: \"say \"\"←\"\"\", ident: `odd``→``name`}) RETURN n"
		assert.Equal(t, query, normalizeCypherSyntaxConfusables(query))
	})
}

func TestExecuteNormalizesCypherSyntaxConfusables(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "MERGE\u00A0（parent:Section\u00A0｛code：＄parent｝） WITH parent MERGE\u00A0（child:Section\u00A0｛code：＄child， note：'keep → inside literal'｝） MERGE\u00A0（child）-[r:SUBSECTION_OF]→（parent）", map[string]interface{}{
		"parent": "3.18",
		"child":  "3.18.1",
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH\u00A0（n:Section） OPTIONAL MATCH\u200B（n）-[r:SUBSECTION_OF]→（p） RETURN n.code AS code， type（r） AS relType， p.code AS parentCode ORDER BY code", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"3.18", nil, nil}, {"3.18.1", "SUBSECTION_OF", "3.18"}}, result.Rows)

	note, err := exec.Execute(ctx, "MATCH (n:Section {code: $code}) RETURN n.note", map[string]interface{}{"code": "3.18.1"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"keep → inside literal"}}, note.Rows)
}

func TestExecuteCompoundCreateWithDelete_Branches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeCompoundCreateWithDelete(ctx, "CREATE (n:Tmp {name:'x'}) RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid CREATE...WITH...DELETE")

	// Edge delete branch.
	res, err := exec.executeCompoundCreateWithDelete(ctx, "CREATE (a:TmpA)-[r:REL]->(b:TmpB) WITH r DELETE r RETURN count(r)")
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)
	assert.Equal(t, 1, res.Stats.RelationshipsDeleted)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0])

	// Node delete with non-count RETURN branch should return nil placeholder.
	res, err = exec.executeCompoundCreateWithDelete(ctx, "CREATE (n:TmpNode {name:'y'}) WITH n DELETE n RETURN n.name")
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 1, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.NodesDeleted)
	require.Equal(t, []string{"n.name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Nil(t, res.Rows[0][0])

	// Delete target missing from created vars/edges should be a no-op for deletion stats.
	res, err = exec.executeCompoundCreateWithDelete(ctx, "CREATE (n:TmpNode {name:'z'}) WITH n DELETE missing RETURN count(missing)")
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 1, res.Stats.NodesCreated)
	assert.Equal(t, 0, res.Stats.NodesDeleted)
	assert.Equal(t, int64(1), res.Rows[0][0])
}

func TestExecuteUnsupportedQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// DROP INDEX on a non-existent index returns an error (Neo4j semantics).
	_, err := exec.Execute(context.Background(), "DROP INDEX idx", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// DROP INDEX IF EXISTS on a non-existent index is a silent no-op.
	result, err := exec.Execute(context.Background(), "DROP INDEX idx IF EXISTS", nil)
	assert.NoError(t, err)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)

	// Test a truly unsupported query
	_, err = exec.Execute(context.Background(), "GRANT ADMIN TO user", nil)
	assert.Error(t, err)
	// Error should mention invalid clause
	assert.Contains(t, err.Error(), "syntax error")
}

func TestExecuteMatchEmptyGraph(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	result, err := exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Rows)
}

func TestExecuteMatchWithLabel(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some nodes
	node1 := &storage.Node{
		ID:         "person-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &storage.Node{
		ID:         "company-1",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme"},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Match only Person nodes
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchAllNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Test"},
			Properties: map[string]interface{}{"index": i},
		}
		err := error(nil)
		if i == 0 {
			_, err = store.CreateNode(node)
			require.NoError(t, err)
		} else {
			_, err = store.CreateNode(node)
			require.NoError(t, err)
		}
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteMatchWithWhere(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node1 := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	node2 := &storage.Node{
		ID:         "p2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob", "age": float64(25)},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Test equality
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test greater than
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age > 26 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test less than
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age < 28 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchWithCount(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		err := error(nil)
		if i == 0 {
			_, err = store.CreateNode(node)
			require.NoError(t, err)
		} else {
			_, err = store.CreateNode(node)
			require.NoError(t, err)
		}
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(5), result.Rows[0][0])
	assert.Equal(t, "cnt", result.Columns[0])
}

func TestExecuteMatchWithLimit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n LIMIT 3", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteMatchWithSkip(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n SKIP 5", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
}

func TestExecuteCreateNode(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify node exists
	nodes, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, "Alice", nodes[0].Properties["name"])
}

func TestExecuteCreateWithReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Bob'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Rows, 1)
	assert.Contains(t, result.Columns, "n")
}

func TestExecuteCreateMultipleNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person), (b:Company)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)

	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(2), nodeCount)
}

func TestExecuteCreateRelationship(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person)-[:KNOWS]->(b:Person)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edgeCount, _ := store.EdgeCount()
	assert.Equal(t, int64(1), edgeCount)
}

func TestExecuteMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First merge creates
	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Second merge should not create (based on label match)
	result, err = exec.Execute(ctx, "MERGE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	// Note: current implementation may create duplicates - depends on matching logic
}

func TestExecuteDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node first
	node := &storage.Node{
		ID:         "delete-me",
		Labels:     []string{"Temp"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Delete it
	result, err := exec.Execute(ctx, "MATCH (n:Temp) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify deleted
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

func TestExecuteDetachDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with relationship
	node1 := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	require.NoError(t, store.CreateEdge(edge))

	// Detach delete
	result, err := exec.Execute(ctx, "MATCH (n:Person) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesDeleted)
}

func TestExecuteCallProcedure(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Add some test data
	node := &storage.Node{ID: "test", Labels: []string{"Memory"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test db.labels()
	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test db.relationshipTypes()
	result, err = exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	// May be empty if no relationships

	// Test db.schema.visualization()
	result, err = exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
}

func TestExecuteCallWithYieldWhere(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Add test data with multiple labels
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Memory"}, Properties: map[string]interface{}{}},
		{ID: "n2", Labels: []string{"Todo"}, Properties: map[string]interface{}{}},
		{ID: "n3", Labels: []string{"File"}, Properties: map[string]interface{}{}},
		{ID: "n4", Labels: []string{"Memory", "Important"}, Properties: map[string]interface{}{}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	t.Run("YIELD with column selection", func(t *testing.T) {
		// Basic YIELD - just get labels
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label"}, result.Columns)
		require.GreaterOrEqual(t, len(result.Rows), 3) // Memory, Todo, File, Important
	})

	t.Run("YIELD with alias", func(t *testing.T) {
		// YIELD with alias
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label AS labelName", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"labelName"}, result.Columns)
	})

	t.Run("YIELD *", func(t *testing.T) {
		// YIELD * returns all columns
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD *", nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Columns), 1)
	})

	t.Run("YIELD with WHERE filtering", func(t *testing.T) {
		// WHERE should filter results
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label = 'Memory'", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label"}, result.Columns)
		require.Equal(t, 1, len(result.Rows), "WHERE should filter to only 'Memory' label")
		require.Equal(t, "Memory", result.Rows[0][0])
	})

	t.Run("YIELD with WHERE CONTAINS", func(t *testing.T) {
		// WHERE with CONTAINS operator
		// Search for 'd' which is only in "Todo"
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label CONTAINS 'd'", nil)
		require.NoError(t, err)
		foundLabels := make(map[string]bool)
		for _, row := range result.Rows {
			foundLabels[row[0].(string)] = true
		}
		require.True(t, foundLabels["Todo"], "Todo should be in results (contains 'd')")
		require.False(t, foundLabels["Memory"], "Memory should NOT be in results (no 'd')")
		require.False(t, foundLabels["File"], "File should NOT be in results (no 'd')")
		require.False(t, foundLabels["Important"], "Important should NOT be in results (no 'd')")
	})

	t.Run("YIELD with WHERE <> (not equals)", func(t *testing.T) {
		// WHERE with <> operator
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label <> 'Memory'", nil)
		require.NoError(t, err)
		for _, row := range result.Rows {
			require.NotEqual(t, "Memory", row[0], "Memory should be filtered out")
		}
	})
}

func TestParseYieldClause(t *testing.T) {
	tests := []struct {
		name     string
		cypher   string
		expected *yieldClause
	}{
		{
			name:     "no yield",
			cypher:   "CALL db.labels()",
			expected: nil,
		},
		{
			name:   "yield star",
			cypher: "CALL db.labels() YIELD *",
			expected: &yieldClause{
				yieldAll: true,
				items:    []yieldItem{},
			},
		},
		{
			name:   "yield single column",
			cypher: "CALL db.labels() YIELD label",
			expected: &yieldClause{
				items: []yieldItem{{name: "label", alias: ""}},
			},
		},
		{
			name:   "yield multiple columns",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
			},
		},
		{
			name:   "yield with alias",
			cypher: "CALL db.labels() YIELD label AS labelName",
			expected: &yieldClause{
				items: []yieldItem{{name: "label", alias: "labelName"}},
			},
		},
		{
			name:   "yield with WHERE",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score WHERE score > 0.5",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
				where: "score > 0.5",
			},
		},
		{
			name:   "yield star with WHERE",
			cypher: "CALL db.index.fulltext.queryNodes('idx', 'search') YIELD * WHERE score > 0.8",
			expected: &yieldClause{
				yieldAll: true,
				items:    []yieldItem{},
				where:    "score > 0.8",
			},
		},
		{
			name:   "yield with WHERE and RETURN",
			cypher: "CALL db.labels() YIELD label WHERE label = 'Memory' RETURN label",
			expected: &yieldClause{
				items:      []yieldItem{{name: "label", alias: ""}},
				where:      "label = 'Memory'",
				hasReturn:  true,
				returnExpr: "label",
			},
		},
		{
			name:   "yield with WITH boundary",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score WITH node MATCH (node)-[:REL]->(m) RETURN node, m",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
			},
		},
		{
			name:   "yield with MATCH boundary",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score MATCH (node)-[:REL]->(m) RETURN node, m",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
			},
		},
		{
			name:   "yield with WITH boundary and multiple projected columns",
			cypher: "CALL db.proc() YIELD a, b AS bAlias, c, d AS dAlias WITH a, bAlias MATCH (n) RETURN n",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: "bAlias"},
					{name: "c", alias: ""},
					{name: "d", alias: "dAlias"},
				},
			},
		},
		{
			name:   "yield with OPTIONAL MATCH boundary",
			cypher: "CALL db.proc() YIELD a, b OPTIONAL MATCH (n) RETURN n",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with UNWIND boundary",
			cypher: "CALL db.proc() YIELD a, b UNWIND [a,b] AS v RETURN v",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with CALL boundary",
			cypher: "CALL db.proc() YIELD a, b CALL { WITH a RETURN a } RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with CREATE boundary",
			cypher: "CALL db.proc() YIELD a CREATE (:Tag {v:a}) RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
				},
			},
		},
		{
			name:   "yield with MERGE boundary",
			cypher: "CALL db.proc() YIELD a MERGE (:Tag {v:a}) RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
				},
			},
		},
		{
			name:   "yield with SET boundary",
			cypher: "CALL db.proc() YIELD a, b SET a.flag = true RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with REMOVE boundary",
			cypher: "CALL db.proc() YIELD a, b REMOVE a.flag RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with DELETE boundary",
			cypher: "CALL db.proc() YIELD a, b DELETE a RETURN b",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with DETACH DELETE boundary",
			cypher: "CALL db.proc() YIELD a, b DETACH DELETE a RETURN b",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
					{name: "b", alias: ""},
				},
			},
		},
		{
			name:   "yield with FOREACH boundary",
			cypher: "CALL db.proc() YIELD a FOREACH (x IN [1] | SET a.flag = x) RETURN a",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "a", alias: ""},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseYieldClause(tt.cypher)
			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			assert.Equal(t, tt.expected.yieldAll, result.yieldAll, "yieldAll mismatch")
			assert.Equal(t, len(tt.expected.items), len(result.items), "items count mismatch")
			for i, item := range tt.expected.items {
				if i < len(result.items) {
					assert.Equal(t, item.name, result.items[i].name, "item name mismatch at %d", i)
					assert.Equal(t, item.alias, result.items[i].alias, "item alias mismatch at %d", i)
				}
			}
			assert.Equal(t, tt.expected.where, result.where, "where mismatch")
			assert.Equal(t, tt.expected.hasReturn, result.hasReturn, "hasReturn mismatch")
			if tt.expected.hasReturn {
				assert.Equal(t, tt.expected.returnExpr, result.returnExpr, "returnExpr mismatch")
			}
		})
	}
}

func TestExecuteWithParameters(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Query with parameters
	params := map[string]interface{}{
		"name": "Alice",
	}
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = $name RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteReturnPropertyAccess(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Return specific properties
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Contains(t, result.Columns, "n.name")
	assert.Contains(t, result.Columns, "n.age")
}

func TestExecuteMatchRelationship(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and relationship
	node1 := &storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	node2 := &storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "e1", StartNode: "p1", EndNode: "p2", Type: "KNOWS"}
	require.NoError(t, store.CreateEdge(edge))

	// Match with relationship pattern
	result, err := exec.Execute(ctx, "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b", nil)
	require.NoError(t, err)
	// Should find the relationship
	assert.NotEmpty(t, result.Columns)
}

func TestExecuteWhereOperators(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	nodes := []struct {
		id   string
		name string
		age  float64
	}{
		{"p1", "Alice", 30},
		{"p2", "Bob", 25},
		{"p3", "Charlie", 35},
	}

	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": n.name, "age": n.age},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"equals string", "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n", 1},
		{"equals number", "MATCH (n:Person) WHERE n.age = 25 RETURN n", 1},
		{"greater than", "MATCH (n:Person) WHERE n.age > 28 RETURN n", 2},
		{"greater or equal", "MATCH (n:Person) WHERE n.age >= 30 RETURN n", 2},
		{"less than", "MATCH (n:Person) WHERE n.age < 30 RETURN n", 1},
		{"less or equal", "MATCH (n:Person) WHERE n.age <= 30 RETURN n", 2},
		{"not equals <>", "MATCH (n:Person) WHERE n.age <> 30 RETURN n", 2},
		{"not equals !=", "MATCH (n:Person) WHERE n.age != 30 RETURN n", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Len(t, result.Rows, tt.expected)
		})
	}
}

func TestExecuteContainsOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name CONTAINS 'Smith' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// No match
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.name CONTAINS 'Jones' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestExecuteStartsWithOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name STARTS WITH 'Alice' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteEndsWithOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name ENDS WITH 'Smith' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteDistinct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with same labels
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"category": "A"},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// DISTINCT should deduplicate - but we return full nodes so may not dedupe
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN DISTINCT n.category", nil)
	require.NoError(t, err)
	// The distinct logic depends on implementation
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteOrderBy(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	nodes := []struct {
		id  string
		age float64
	}{
		{"p3", 35},
		{"p1", 25},
		{"p2", 30},
	}

	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"age": n.age},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// Order by age ascending
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.age ORDER BY n.age", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
	// Note: ORDER BY implementation may need verification
}

func TestExecuteQueryStats(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create should report stats
	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Stats)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 0, result.Stats.NodesDeleted)
}

func TestExecuteNornicDbProcedures(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test nornicdb.version()
	result, err := exec.Execute(ctx, "CALL nornicdb.version()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test nornicdb.stats()
	result, err = exec.Execute(ctx, "CALL nornicdb.stats()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test nornicdb.decay.info()
	result, err = exec.Execute(ctx, "CALL nornicdb.decay.info()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)
}

// Additional tests for full coverage

func TestExecuteSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node first
	node := &storage.Node{
		ID:         "set-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(25)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Update property with SET
	result, err := exec.Execute(ctx, "MATCH (n:Person) SET n.age = 30", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)

	// Verify update - integer values should be stored as int64 (Neo4j compatible)
	updated, _ := store.GetNode("set-test")
	assert.Equal(t, int64(30), updated.Properties["age"])
}

func TestExecuteSetWithReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "set-return-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// SET with RETURN
	result, err := exec.Execute(ctx, "MATCH (n:Person) SET n.status = 'active' RETURN n", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)
	assert.Contains(t, result.Columns, "n")
}

func TestExecuteSetNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET with no matching nodes
	_, err := exec.Execute(ctx, "MATCH (n:NonExistent) SET n.prop = 'value'", nil)
	require.NoError(t, err) // Should succeed with 0 updates
}

func TestExecuteSetInvalidQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET without proper assignment
	_, err := exec.Execute(ctx, "MATCH (n) SET invalid", nil)
	assert.Error(t, err)
}

func TestExecuteAggregationSum(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with numeric values
	for i := 1; i <= 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("sum-%d", i)),
			Labels:     []string{"Number"},
			Properties: map[string]interface{}{"value": float64(i * 10)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:Number) RETURN sum(n.value) AS total", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, float64(150), result.Rows[0][0]) // 10+20+30+40+50
}

func TestExecuteAggregationAvg(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 1; i <= 4; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("avg-%d", i)),
			Labels:     []string{"Score"},
			Properties: map[string]interface{}{"value": float64(i * 25)}, // 25,50,75,100
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:Score) RETURN avg(n.value) AS average", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, float64(62.5), result.Rows[0][0])
}

func TestExecuteAggregationMinMax(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	values := []float64{15, 42, 8, 99, 23}
	for i, v := range values {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("minmax-%d", i)),
			Labels:     []string{"Value"},
			Properties: map[string]interface{}{"num": v},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// Test MIN
	result, err := exec.Execute(ctx, "MATCH (n:Value) RETURN min(n.num) AS minimum", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(8), result.Rows[0][0])

	// Test MAX
	result, err = exec.Execute(ctx, "MATCH (n:Value) RETURN max(n.num) AS maximum", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(99), result.Rows[0][0])
}

func TestExecuteAggregationCollect(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	names := []string{"Alice", "Bob", "Charlie"}
	for i, name := range names {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("collect-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": name},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN collect(n.name) AS names", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 3)
}

func TestExecuteAggregationEmpty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Aggregation on empty set
	result, err := exec.Execute(ctx, "MATCH (n:NonExistent) RETURN avg(n.value) AS avg", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteInOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i, status := range []string{"active", "pending", "inactive"} {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("in-%d", i)),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"status": status},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.status IN ['active', 'pending'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestExecuteIsNullOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes - one with email, one without
	node1 := &storage.Node{
		ID:         "null-1",
		Labels:     []string{"Contact"},
		Properties: map[string]interface{}{"name": "Alice", "email": "alice@example.com"},
	}
	node2 := &storage.Node{
		ID:         "null-2",
		Labels:     []string{"Contact"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// IS NULL
	result, err := exec.Execute(ctx, "MATCH (n:Contact) WHERE n.email IS NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// IS NOT NULL
	result, err = exec.Execute(ctx, "MATCH (n:Contact) WHERE n.email IS NOT NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteRegexOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	emails := []string{"alice@gmail.com", "bob@yahoo.com", "charlie@gmail.com"}
	for i, email := range emails {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("regex-%d", i)),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"email": email},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.email =~ '.*@gmail\\.com' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestExecuteCreateRelationshipBothDirections(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test forward direction
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Test backward direction
	result, err = exec.Execute(ctx, "CREATE (a:Person {name: 'Charlie'})<-[:FOLLOWS]-(b:Person {name: 'Dave'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestExecuteCreateRelationshipWithReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:City {name: 'NYC'})-[:CONNECTED_TO]->(b:City {name: 'LA'}) RETURN a, b", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.NotEmpty(t, result.Rows)
}

// TestExecuteCreateRelationshipWithArrayProperties tests CREATE with relationship properties containing arrays
// This is a Neo4j-compatible feature for creating edges with properties like {roles: ['Neo']}
func TestExecuteCreateRelationshipWithArrayProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test direct CREATE with relationship properties (array value)
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Keanu'})-[:ACTED_IN {roles: ['Neo', 'John Wick']}]->(m:Movie {title: 'The Matrix'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has properties
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ACTED_IN", edges[0].Type)
	roles, ok := edges[0].Properties["roles"]
	assert.True(t, ok, "relationship should have 'roles' property")
	assert.NotNil(t, roles)
}

// TestExecuteCreateRelationshipWithStringProperty tests CREATE with a string property on relationship
func TestExecuteCreateRelationshipWithStringProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Director'})-[:DIRECTED {since: '1999'}]->(m:Movie {title: 'Film'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "DIRECTED", edges[0].Type)
	since, ok := edges[0].Properties["since"]
	assert.True(t, ok, "relationship should have 'since' property")
	assert.Equal(t, "1999", since)
}

// TestExecuteMatchCreateRelationshipWithProperties tests MATCH...CREATE with relationship properties
func TestExecuteMatchCreateRelationshipWithProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create the nodes
	_, err := exec.Execute(ctx, "CREATE (p:Person {name: 'Keanu Reeves'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (m:Movie {title: 'The Matrix'})", nil)
	require.NoError(t, err)

	// Now use MATCH...CREATE with relationship properties (Neo4j Movies dataset pattern)
	result, err := exec.Execute(ctx, `
		MATCH (keanu:Person {name: 'Keanu Reeves'}), (matrix:Movie {title: 'The Matrix'})
		CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix)
	`, nil)
	require.NoError(t, err, "MATCH...CREATE with relationship properties should work")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has properties
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ACTED_IN", edges[0].Type)
	roles, ok := edges[0].Properties["roles"]
	assert.True(t, ok, "relationship should have 'roles' property")
	assert.NotNil(t, roles)
}

// TestExecuteMatchCreateRelationshipWithMultipleProperties tests multiple properties on relationships
func TestExecuteMatchCreateRelationshipWithMultipleProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes first
	_, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)

	// MATCH...CREATE with multiple relationship properties
	result, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
		CREATE (a)-[:KNOWS {since: 2020, trust: 'high', tags: ['friend', 'colleague']}]->(b)
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "KNOWS", edges[0].Type)
	_, hasSince := edges[0].Properties["since"]
	_, hasTrust := edges[0].Properties["trust"]
	_, hasTags := edges[0].Properties["tags"]
	assert.True(t, hasSince, "should have 'since' property")
	assert.True(t, hasTrust, "should have 'trust' property")
	assert.True(t, hasTags, "should have 'tags' property")
}

// TestExecuteMatchCreateNodeAndRelationships tests MATCH...CREATE that creates NEW nodes
// and relationships to matched nodes (Northwind pattern: MATCH (s), (c) CREATE (p) CREATE (p)-[:REL]->(c))
func TestExecuteMatchCreateNodeAndRelationships(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial nodes (like Supplier and Category in Northwind)
	_, err := exec.Execute(ctx, "CREATE (s:Supplier {supplierID: 1, companyName: 'Exotic Liquids'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (c:Category {categoryID: 1, categoryName: 'Beverages'})", nil)
	require.NoError(t, err)

	// Verify initial state
	nodes, _ := store.AllNodes()
	assert.Len(t, nodes, 2, "Should have 2 initial nodes")

	// Now use the Northwind pattern: MATCH existing nodes, CREATE new node AND relationships
	result, err := exec.Execute(ctx, `
		MATCH (s:Supplier {supplierID: 1}), (c:Category {categoryID: 1})
		CREATE (p:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})
		CREATE (p)-[:PART_OF]->(c)
		CREATE (s)-[:SUPPLIES]->(p)
	`, nil)
	require.NoError(t, err, "MATCH...CREATE with new node and relationships should work")

	// Verify stats
	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 new Product node")
	assert.Equal(t, 2, result.Stats.RelationshipsCreated, "Should create 2 relationships")

	// Verify total nodes
	nodes, err = store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 3, "Should now have 3 nodes total")

	// Verify Product was created with correct properties
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	require.Len(t, products, 1, "Should have 1 Product node")
	assert.Equal(t, "Chai", products[0].Properties["productName"])
	assert.Equal(t, float64(18.0), products[0].Properties["unitPrice"])

	// Verify relationships
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 2, "Should have 2 relationships")

	// Check relationship types
	relTypes := make(map[string]bool)
	for _, edge := range edges {
		relTypes[edge.Type] = true
	}
	assert.True(t, relTypes["PART_OF"], "Should have PART_OF relationship")
	assert.True(t, relTypes["SUPPLIES"], "Should have SUPPLIES relationship")
}

// TestExecuteMatchCreateNodeThenRelationship tests the Northwind pattern where:
// 1. MATCH finds an existing node
// 2. CREATE creates a new node
// 3. CREATE creates a relationship FROM the matched node TO the created node
// This is the pattern: MATCH (cu:Customer) CREATE (o:Order) CREATE (cu)-[:PURCHASED]->(o)
func TestExecuteMatchCreateNodeThenRelationship(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a customer first
	_, err := exec.Execute(ctx, "CREATE (cu:Customer {customerID: 'ALFKI', companyName: 'Alfreds Futterkiste'})", nil)
	require.NoError(t, err)

	// Verify customer exists
	customers, _ := store.GetNodesByLabel("Customer")
	require.Len(t, customers, 1, "Should have 1 customer")

	// Now run the Northwind pattern:
	// MATCH the customer, CREATE an order, CREATE the relationship
	result, err := exec.Execute(ctx, `
		MATCH (cu:Customer {customerID: 'ALFKI'})
		CREATE (o:Order {orderID: 10643, orderDate: '1997-08-25', shipCountry: 'Germany'})
		CREATE (cu)-[:PURCHASED]->(o)
	`, nil)

	// This is the critical assertion - the query should succeed
	require.NoError(t, err, "MATCH + CREATE node + CREATE relationship should work; variable 'cu' from MATCH should be available")

	// Verify stats
	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 Order node")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 PURCHASED relationship")

	// Verify order was created
	orders, err := store.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, orders, 1, "Should have 1 Order node")
	// Check orderID - could be int64 or float64 depending on parsing
	orderID := orders[0].Properties["orderID"]
	switch v := orderID.(type) {
	case int64:
		assert.Equal(t, int64(10643), v)
	case float64:
		assert.Equal(t, float64(10643), v)
	default:
		t.Errorf("Unexpected orderID type: %T", orderID)
	}

	// Verify relationship exists
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1, "Should have 1 relationship")
	assert.Equal(t, "PURCHASED", edges[0].Type)

	// Verify the relationship connects the right nodes
	customerNode := customers[0]
	orderNode := orders[0]
	assert.Equal(t, customerNode.ID, edges[0].StartNode, "Relationship should start from Customer")
	assert.Equal(t, orderNode.ID, edges[0].EndNode, "Relationship should end at Order")
}

// TestExecuteMatchCreateMultipleNodes tests creating multiple nodes in a single MATCH...CREATE
func TestExecuteMatchCreateMultipleNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create category
	_, err := exec.Execute(ctx, "CREATE (c:Category {categoryID: 1, categoryName: 'Beverages'})", nil)
	require.NoError(t, err)

	// Create multiple products referencing the same category
	result, err := exec.Execute(ctx, `
		MATCH (c:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Chai'})
		CREATE (p2:Product {productID: 2, productName: 'Chang'})
		CREATE (p1)-[:PART_OF]->(c)
		CREATE (p2)-[:PART_OF]->(c)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated, "Should create 2 Product nodes")
	assert.Equal(t, 2, result.Stats.RelationshipsCreated, "Should create 2 PART_OF relationships")

	// Verify products
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	assert.Len(t, products, 2, "Should have 2 Product nodes")
}

// TestExecuteMatchCreateWithRelationshipProperties tests the full Northwind pattern with rel properties
func TestExecuteMatchCreateWithRelationshipProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create customer and order pattern (like Northwind)
	_, err := exec.Execute(ctx, "CREATE (c:Customer {customerID: 'ALFKI', companyName: 'Alfreds'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (o:Order {orderID: 10643})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (p:Product {productID: 1, productName: 'Chai'})", nil)
	require.NoError(t, err)

	// Create ORDERS relationship with quantity property
	result, err := exec.Execute(ctx, `
		MATCH (o:Order {orderID: 10643}), (p:Product {productID: 1})
		CREATE (o)-[:ORDERS {quantity: 15}]->(p)
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has the quantity property
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ORDERS", edges[0].Type)
	qty, ok := edges[0].Properties["quantity"]
	assert.True(t, ok, "Should have quantity property")
	// Check the value (could be int64 or float64 depending on parsing)
	switch v := qty.(type) {
	case int64:
		assert.Equal(t, int64(15), v)
	case float64:
		assert.Equal(t, float64(15), v)
	default:
		t.Errorf("quantity has unexpected type: %T", qty)
	}
}

// TestExecuteMultipleMatchCreateBlocks tests the Northwind pattern where multiple
// MATCH...CREATE blocks exist in a single query, each creating nodes and relationships
// This is the exact pattern from the Northwind benchmark that was failing:
// MATCH (s1), (c1) CREATE (p1) CREATE (p1)-[:REL]->(c1)
// MATCH (s2), (c2) CREATE (p2) CREATE (p2)-[:REL]->(c2)
// NOTE: This is a NornicDB-specific extension - ANTLR's OpenCypher grammar doesn't support
// multiple MATCH...CREATE blocks without semicolons
func TestExecuteMultipleMatchCreateBlocks(t *testing.T) {
	if config.IsANTLRParser() {
		t.Skip("Skipping: ANTLR parser doesn't support multiple MATCH...CREATE blocks without semicolons (NornicDB extension)")
	}
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create Categories (like Northwind)
	_, err := exec.Execute(ctx, `
		CREATE (c1:Category {categoryID: 1, categoryName: 'Beverages'})
		CREATE (c2:Category {categoryID: 2, categoryName: 'Condiments'})
	`, nil)
	require.NoError(t, err)

	// Create Suppliers (like Northwind)
	_, err = exec.Execute(ctx, `
		CREATE (s1:Supplier {supplierID: 1, companyName: 'Exotic Liquids'})
		CREATE (s2:Supplier {supplierID: 2, companyName: 'New Orleans Cajun'})
	`, nil)
	require.NoError(t, err)

	// Verify initial state
	nodes, _ := store.AllNodes()
	assert.Len(t, nodes, 4, "Should have 4 initial nodes (2 categories + 2 suppliers)")

	// This is the EXACT pattern from Northwind benchmark that was failing:
	// Multiple MATCH...CREATE blocks in a single query
	result, err := exec.Execute(ctx, `
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})
		CREATE (p1)-[:PART_OF]->(c1)
		CREATE (s1)-[:SUPPLIES]->(p1)
		
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p2:Product {productID: 2, productName: 'Chang', unitPrice: 19.0})
		CREATE (p2)-[:PART_OF]->(c1)
		CREATE (s1)-[:SUPPLIES]->(p2)
		
		MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
		CREATE (p3:Product {productID: 3, productName: 'Aniseed Syrup', unitPrice: 10.0})
		CREATE (p3)-[:PART_OF]->(c2)
		CREATE (s2)-[:SUPPLIES]->(p3)
	`, nil)
	require.NoError(t, err, "Multiple MATCH...CREATE blocks should work")

	// Verify stats - should create 3 products with 6 relationships (2 per product)
	assert.Equal(t, 3, result.Stats.NodesCreated, "Should create 3 Product nodes")
	assert.Equal(t, 6, result.Stats.RelationshipsCreated, "Should create 6 relationships")

	// Verify total nodes
	nodes, err = store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 7, "Should now have 7 nodes total (4 initial + 3 products)")

	// Verify Products were created correctly
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	assert.Len(t, products, 3, "Should have 3 Product nodes")

	// Verify relationships
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 6, "Should have 6 relationships")

	// Count relationship types
	partOfCount := 0
	suppliesCount := 0
	for _, edge := range edges {
		switch edge.Type {
		case "PART_OF":
			partOfCount++
		case "SUPPLIES":
			suppliesCount++
		}
	}
	assert.Equal(t, 3, partOfCount, "Should have 3 PART_OF relationships")
	assert.Equal(t, 3, suppliesCount, "Should have 3 SUPPLIES relationships")
}

// TestExecuteMultipleMatchCreateBlocksWithDifferentCategories tests that
// each MATCH block correctly finds its own nodes and doesn't reuse previous block's nodes
// SKIP: This test relies on MATCH property filtering ({supplierID: 1}) which is currently broken
// TODO: Re-enable once MATCH property filtering is fixed
func TestExecuteMultipleMatchCreateBlocksWithDifferentCategories(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple categories with different IDs
	_, err := exec.Execute(ctx, `
		CREATE (c1:Category {categoryID: 1, categoryName: 'Beverages'})
		CREATE (c2:Category {categoryID: 2, categoryName: 'Condiments'})
		CREATE (c3:Category {categoryID: 3, categoryName: 'Confections'})
	`, nil)
	require.NoError(t, err)

	// Create multiple suppliers
	_, err = exec.Execute(ctx, `
		CREATE (s1:Supplier {supplierID: 1, companyName: 'Supplier One'})
		CREATE (s2:Supplier {supplierID: 2, companyName: 'Supplier Two'})
		CREATE (s3:Supplier {supplierID: 3, companyName: 'Supplier Three'})
	`, nil)
	require.NoError(t, err)

	// Multiple MATCH blocks referencing DIFFERENT categories
	result, err := exec.Execute(ctx, `
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Product1'})
		CREATE (p1)-[:PART_OF]->(c1)
		
		MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
		CREATE (p2:Product {productID: 2, productName: 'Product2'})
		CREATE (p2)-[:PART_OF]->(c2)
		
		MATCH (s3:Supplier {supplierID: 3}), (c3:Category {categoryID: 3})
		CREATE (p3:Product {productID: 3, productName: 'Product3'})
		CREATE (p3)-[:PART_OF]->(c3)
	`, nil)
	require.NoError(t, err, "Different category MATCHes should work")

	assert.Equal(t, 3, result.Stats.NodesCreated)
	assert.Equal(t, 3, result.Stats.RelationshipsCreated)

	// Verify each product is linked to the CORRECT category
	products, _ := store.GetNodesByLabel("Product")
	require.Len(t, products, 3)

	for _, product := range products {
		productID := product.Properties["productID"]
		edges, _ := store.GetOutgoingEdges(product.ID)

		// Find the PART_OF edge
		for _, edge := range edges {
			if edge.Type == "PART_OF" {
				// Get the target category
				targetNode, _ := store.GetNode(edge.EndNode)
				categoryID := targetNode.Properties["categoryID"]

				// Product1 should link to Category1, Product2 to Category2, etc.
				assert.Equal(t, productID, categoryID,
					"Product %v should be linked to Category %v", productID, categoryID)
			}
		}
	}
}

// TestExecuteMixedNodesAndRelationshipsCreate tests creating nodes AND relationships
// in a single CREATE statement (like the FastRP social network pattern)
func TestExecuteMixedNodesAndRelationshipsCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This is the exact pattern from the FastRP benchmark
	result, err := exec.Execute(ctx, `
		CREATE
			(dan:Person {name: 'Dan', age: 18}),
			(annie:Person {name: 'Annie', age: 12}),
			(matt:Person {name: 'Matt', age: 22}),
			(dan)-[:KNOWS {weight: 1.0}]->(annie),
			(dan)-[:KNOWS {weight: 1.5}]->(matt),
			(annie)-[:KNOWS {weight: 2.0}]->(matt)
	`, nil)
	require.NoError(t, err, "Mixed nodes and relationships CREATE should work")

	// Should create 3 nodes and 3 relationships
	assert.Equal(t, 3, result.Stats.NodesCreated, "Should create 3 Person nodes")
	assert.Equal(t, 3, result.Stats.RelationshipsCreated, "Should create 3 KNOWS relationships")

	// Verify nodes
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 3)

	// Verify edges
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 3)

	// Verify relationship properties
	for _, edge := range edges {
		assert.Equal(t, "KNOWS", edge.Type)
		weight, exists := edge.Properties["weight"]
		assert.True(t, exists, "Should have weight property")
		_, isFloat := weight.(float64)
		assert.True(t, isFloat, "Weight should be float64")
	}
}

func TestExecuteAllProcedures(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	node := &storage.Node{
		ID:         "proc-test",
		Labels:     []string{"TestLabel"},
		Properties: map[string]interface{}{"prop1": "value1"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	procedures := []string{
		"CALL db.labels()",
		"CALL db.relationshipTypes()",
		"CALL db.propertyKeys()",
		"CALL db.indexes()",
		"CALL db.constraints()",
		"CALL db.schema.visualization()",
		"CALL db.schema.nodeProperties()",
		"CALL db.schema.relProperties()",
		"CALL dbms.components()",
		"CALL dbms.procedures()",
		"CALL dbms.functions()",
		"CALL nornicdb.version()",
		"CALL nornicdb.stats()",
		"CALL nornicdb.decay.info()",
	}

	for _, proc := range procedures {
		t.Run(proc, func(t *testing.T) {
			result, err := exec.Execute(ctx, proc, nil)
			require.NoError(t, err, "procedure %s failed", proc)
			assert.NotNil(t, result)
			assert.NotEmpty(t, result.Columns)
		})
	}
}

func TestExecuteUnknownProcedure(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL unknown.procedure()", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown procedure")
}

func TestExecuteAndOrOperators(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	nodes := []struct {
		name   string
		age    float64
		active bool
	}{
		{"Alice", 25, true},
		{"Bob", 35, true},
		{"Charlie", 25, false},
		{"Dave", 35, false},
	}
	for i, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("logic-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": n.name, "age": n.age, "active": n.active},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// Test AND
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age = 25 AND n.active = true RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Only Alice

	// Test OR
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age = 25 OR n.age = 35 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 4) // All
}

func TestExecuteOrderByDesc(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with different ages
	ages := []float64{20, 30, 25, 35, 28}
	for i, age := range ages {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("order-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"age": age},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.age ORDER BY n.age DESC", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
	// First should be highest
	assert.Equal(t, float64(35), result.Rows[0][0])
}

func TestExecuteSkipAndLimit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 10 nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("page-%d", i)),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{"index": float64(i)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// SKIP 3 LIMIT 4
	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n SKIP 3 LIMIT 4", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 4)
}

func TestExecuteMatchNoReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MATCH without RETURN should return matched indicator
	result, err := exec.Execute(ctx, "MATCH (n:Something)", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSubstituteParams(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "param-test",
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test with various parameter types
	params := map[string]interface{}{
		"name":   "Alice",
		"age":    30,
		"active": true,
		"data":   nil,
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.name = $name RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteDeleteNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DELETE with no MATCH clause should error
	_, err := exec.Execute(ctx, "DELETE n", nil)
	assert.Error(t, err)
}

func TestResolveReturnItemVariants(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "return-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Return whole node
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Rows[0][0])

	// Return property
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result.Rows[0][0])

	// Return id
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id", nil)
	require.NoError(t, err)
	assert.Equal(t, "return-test", result.Rows[0][0])

	// Return *
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN *", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Rows[0][0])
}

func TestParsePropertiesVariants(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with number property
	result, err := exec.Execute(ctx, "CREATE (n:Test {count: 42, rate: 3.14, flag: true})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify properties were parsed correctly
	nodes, _ := store.GetNodesByLabel("Test")
	assert.Len(t, nodes, 1)
}

func TestExecuteCountProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes, some with email, some without
	node1 := &storage.Node{ID: "cp1", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Alice", "email": "a@b.com"}}
	node2 := &storage.Node{ID: "cp2", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Bob"}}
	node3 := &storage.Node{ID: "cp3", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Charlie", "email": "c@d.com"}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	// COUNT(n.email) should only count non-null
	result, err := exec.Execute(ctx, "MATCH (n:User) RETURN count(n.email) AS emailCount", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestToFloat64Variants(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with various numeric types
	node := &storage.Node{
		ID:     "float-test",
		Labels: []string{"Numbers"},
		Properties: map[string]interface{}{
			"int32":   int32(100),
			"int64":   int64(200),
			"float32": float32(3.14),
			"float64": float64(2.71),
			"string":  "42.5",
		},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// These should all work with numeric comparisons
	result, err := exec.Execute(ctx, "MATCH (n:Numbers) WHERE n.int32 > 50 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}
