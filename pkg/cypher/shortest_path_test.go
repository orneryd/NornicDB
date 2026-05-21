package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShortestPathCypher(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph: A -> B -> C
	//                     A -> D -> C
	// Create nodes first
	exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (b:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (c:Person {name: 'Carol'})`, nil)
	exec.Execute(ctx, `CREATE (d:Person {name: 'Dave'})`, nil)

	// Create relationships using MATCH...CREATE
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)`, nil)
	exec.Execute(ctx, `MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(d)`, nil)
	exec.Execute(ctx, `MATCH (d:Person {name: 'Dave'}), (c:Person {name: 'Carol'}) CREATE (d)-[:KNOWS]->(c)`, nil)

	t.Run("BasicShortestPath", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected at least one path")
		}

		// Should find a path of length 2 (Alice -> Bob -> Carol or Alice -> Dave -> Carol)
		path := result.Rows[0][0].(map[string]interface{})
		length := path["length"].(int64)
		if length != int64(2) {
			t.Errorf("Expected path length 2, got %d", length)
		}
	})

	t.Run("ShortestPathWithLength", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN length(p) AS pathLength
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected result")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})

	t.Run("AllShortestPaths", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = allShortestPaths((start)-[:KNOWS*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should find 2 paths: Alice -> Bob -> Carol AND Alice -> Dave -> Carol
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 shortest paths, got %d", len(result.Rows))
		}

		// Both should be length 2
		for i, row := range result.Rows {
			path := row[0].(map[string]interface{})
			length := path["length"].(int64)
			if length != int64(2) {
				t.Errorf("Path %d: Expected length 2, got %d", i, length)
			}
		}
	})

	t.Run("ShortestPathNodesAndRels", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN nodes(p) AS nodeList, relationships(p) AS relList
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected result")
		}

		nodes := result.Rows[0][0].([]interface{})
		rels := result.Rows[0][1].([]interface{})

		// Path length 2 means 3 nodes and 2 relationships
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(nodes))
		}
		if len(rels) != 2 {
			t.Errorf("Expected 2 relationships, got %d", len(rels))
		}
	})
}

func TestShortestPathBidirectional(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create bidirectional graph
	exec.Execute(ctx, `CREATE (a:Node {id: 1})`, nil)
	exec.Execute(ctx, `CREATE (b:Node {id: 2})`, nil)
	exec.Execute(ctx, `CREATE (c:Node {id: 3})`, nil)
	exec.Execute(ctx, `MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:REL]->(b)`, nil)
	exec.Execute(ctx, `MATCH (b:Node {id: 2}), (c:Node {id: 3}) CREATE (b)-[:REL]->(c)`, nil)

	t.Run("BidirectionalPath", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 3})
			MATCH p = shortestPath((start)-[:REL*]-(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected path")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})
}

func TestShortestPathNoPath(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create disconnected nodes
	exec.Execute(ctx, "CREATE (a:Node {id: 1}), (b:Node {id: 2})", nil)

	t.Run("NoPathExists", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 2})
			MATCH p = shortestPath((start)-[:REL*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should return empty result when no path exists
		if len(result.Rows) != 0 {
			t.Errorf("Expected no results, got %d", len(result.Rows))
		}
	})
}

func TestShortestPathWithMaxHops(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create chain: A -> B -> C -> D -> E
	exec.Execute(ctx, `CREATE (n1:Node {id: 1})`, nil)
	exec.Execute(ctx, `CREATE (n2:Node {id: 2})`, nil)
	exec.Execute(ctx, `CREATE (n3:Node {id: 3})`, nil)
	exec.Execute(ctx, `CREATE (n4:Node {id: 4})`, nil)
	exec.Execute(ctx, `CREATE (n5:Node {id: 5})`, nil)
	exec.Execute(ctx, `MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)`, nil)
	exec.Execute(ctx, `MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)`, nil)
	exec.Execute(ctx, `MATCH (n3:Node {id: 3}), (n4:Node {id: 4}) CREATE (n3)-[:NEXT]->(n4)`, nil)
	exec.Execute(ctx, `MATCH (n4:Node {id: 4}), (n5:Node {id: 5}) CREATE (n4)-[:NEXT]->(n5)`, nil)

	t.Run("WithinMaxHops", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 3})
			MATCH p = shortestPath((start)-[:NEXT*..5]->(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected path")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2 (1->2->3), got %d", length)
		}
	})

	t.Run("BeyondMaxHops", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 5})
			MATCH p = shortestPath((start)-[:NEXT*..2]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Path requires 4 hops, max is 2, so no result
		if len(result.Rows) != 0 {
			t.Errorf("Expected no result (path too long), got %d rows", len(result.Rows))
		}
	})
}

func TestShortestPathDirectional(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create directed path: A -> B <- C
	exec.Execute(ctx, `CREATE (a:Node {id: 'A'})`, nil)
	exec.Execute(ctx, `CREATE (b:Node {id: 'B'})`, nil)
	exec.Execute(ctx, `CREATE (c:Node {id: 'C'})`, nil)
	exec.Execute(ctx, `MATCH (a:Node {id: 'A'}), (b:Node {id: 'B'}) CREATE (a)-[:TO]->(b)`, nil)
	exec.Execute(ctx, `MATCH (c:Node {id: 'C'}), (b:Node {id: 'B'}) CREATE (c)-[:TO]->(b)`, nil)

	t.Run("OutgoingOnly", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 'A'}), (end:Node {id: 'C'})
			MATCH p = shortestPath((start)-[:TO*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// No outgoing path from A to C
		if len(result.Rows) != 0 {
			t.Errorf("Expected no path (wrong direction), got %d", len(result.Rows))
		}
	})

	t.Run("Undirected", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 'A'}), (end:Node {id: 'C'})
			MATCH p = shortestPath((start)-[:TO*]-(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Path exists via B when undirected: A-B-C
		if len(result.Rows) == 0 {
			t.Fatal("Expected path when undirected")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2 (A-B-C), got %d", length)
		}
	})
}

func TestShortestPathQueryParsingAndExecutionHelpers(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (a:Node {id: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Node {id: 'B'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Node {id: 'C'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:Node {id: 'A'}), (b:Node {id: 'B'}) CREATE (a)-[:TO]->(b)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (b:Node {id: 'B'}), (c:Node {id: 'C'}) CREATE (b)-[:TO]->(c)`, nil)
	require.NoError(t, err)

	_, err = exec.parseShortestPathQuery(ctx, "MATCH (n) RETURN n")
	require.Error(t, err)
	_, err = exec.parseShortestPathQuery(ctx, "MATCH p = shortestPath((a)-[:TO*]-(b) RETURN p")
	require.Error(t, err)

	parsed, err := exec.parseShortestPathQuery(ctx, `
		MATCH (start:Node {id: 'A'}), (end:Node {id: 'C'})
		MATCH p = shortestPath((start)-[:TO*..4]->(end))
		WHERE length(p) > 0
		RETURN p
	`)
	require.NoError(t, err)
	assert.Equal(t, "p", parsed.pathVariable)
	assert.Equal(t, 4, parsed.maxHops)
	assert.Contains(t, parsed.whereClause, "length(p) > 0")
	assert.Equal(t, "p", parsed.returnClause)
	require.NotNil(t, parsed.startVarBinding)
	require.NotNil(t, parsed.endVarBinding)

	pathResult, err := exec.executeShortestPathQuery(context.Background(), parsed)
	require.NoError(t, err)
	require.NotEmpty(t, pathResult.Rows)

	direct := &ShortestPathQuery{
		pathVariable: "p",
		startNode:    nodePatternInfo{labels: []string{"Node"}, properties: map[string]interface{}{"id": "A"}},
		endNode:      nodePatternInfo{labels: []string{"Node"}, properties: map[string]interface{}{"id": "C"}},
		relTypes:     []string{"TO"},
		direction:    "outgoing",
		maxHops:      5,
	}
	noReturnResult, err := exec.executeShortestPathQuery(context.Background(), direct)
	require.NoError(t, err)
	require.Equal(t, []string{"p"}, noReturnResult.Columns)
	require.NotEmpty(t, noReturnResult.Rows)

	invalidExpr := &ShortestPathQuery{
		pathVariable: "p",
		startNode:    nodePatternInfo{labels: []string{"Node"}, properties: map[string]interface{}{"id": "A"}},
		endNode:      nodePatternInfo{labels: []string{"Node"}, properties: map[string]interface{}{"id": "C"}},
		relTypes:     []string{"TO"},
		direction:    "outgoing",
		maxHops:      5,
		returnClause: "unknownExpr",
	}
	invalidExprRes, err := exec.executeShortestPathQuery(context.Background(), invalidExpr)
	require.NoError(t, err)
	require.NotEmpty(t, invalidExprRes.Rows)
	assert.Nil(t, invalidExprRes.Rows[0][0])

	assert.True(t, isShortestPathQuery("MATCH p = shortestPath((a)-[*]-(b)) RETURN p"))
	assert.True(t, isShortestPathQuery("MATCH p = allShortestPaths((a)-[*]-(b)) RETURN p"))
	assert.False(t, isShortestPathQuery("MATCH (n) RETURN n"))
}

func TestShortestPathSingleMatchRegression(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (a:Person {name:'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Person {name:'Bob'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)`, nil)
	require.NoError(t, err)

	query := "MATCH p = shortestPath((a:Person {name:'Alice'})-[*..3]->(b:Person {name:'Bob'})) RETURN p LIMIT 1"

	assert.Equal(t, "", extractPreviousMatchClause(query, strings.Index(query, "shortestPath")))

	parsed, err := exec.parseShortestPathQuery(ctx, query)
	require.NoError(t, err)
	assert.Equal(t, "p", parsed.pathVariable)
	assert.Equal(t, 3, parsed.maxHops)
	assert.Nil(t, parsed.startVarBinding)
	assert.Nil(t, parsed.endVarBinding)

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	path, ok := result.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, int64(1), path["length"])
}

func TestFindNodeByPattern_HelperBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:         "x1",
		Labels:     []string{"Other"},
		Properties: map[string]interface{}{"name": "X"},
	})
	require.NoError(t, err)

	byLabel := exec.findNodeByPattern(nodePatternInfo{
		labels:     []string{"Person"},
		properties: map[string]interface{}{"name": "Alice"},
	})
	require.NotNil(t, byLabel)
	assert.Equal(t, storage.NodeID("p1"), byLabel.ID)

	allNodesPath := exec.findNodeByPattern(nodePatternInfo{
		properties: map[string]interface{}{"name": "X"},
	})
	require.NotNil(t, allNodesPath)
	assert.Equal(t, storage.NodeID("x1"), allNodesPath.ID)

	notFound := exec.findNodeByPattern(nodePatternInfo{
		labels:     []string{"Person"},
		properties: map[string]interface{}{"name": "Missing"},
	})
	assert.Nil(t, notFound)
}
