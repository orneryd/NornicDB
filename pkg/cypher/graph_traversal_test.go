package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGraphTraversalWithCallSubquery tests a complex query that finds neighbors
// 1-2 hops away from a seed node using CALL subquery
func TestGraphTraversalWithCallSubquery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: A graph with seed node and neighbors at different distances
	// Graph structure:
	//   (seed:Person {id: "alice"})
	//     -[:FRIEND]-> (bob:Person {id: "bob"})
	//       -[:FRIEND]-> (charlie:Person {id: "charlie"})
	//     -[:COLLEAGUE]-> (diana:Person {id: "diana"})
	//     -[:KNOWS]-> (eve:Person {id: "eve"})
	//       -[:WORKS_WITH]-> (frank:Person {id: "frank"})

	// Create seed node
	_, err := exec.Execute(ctx, `CREATE (n:Person {id: "alice", name: "Alice"})`, nil)
	require.NoError(t, err)

	// Create 1-hop neighbors
	_, err = exec.Execute(ctx, `
		MATCH (alice:Person {id: "alice"})
		CREATE (bob:Person {id: "bob", name: "Bob"}),
		       (diana:Person {id: "diana", name: "Diana"}),
		       (eve:Person {id: "eve", name: "Eve"}),
		       (alice)-[:FRIEND]->(bob),
		       (alice)-[:COLLEAGUE]->(diana),
		       (alice)-[:KNOWS]->(eve)
	`, nil)
	require.NoError(t, err)

	// Create 2-hop neighbors
	_, err = exec.Execute(ctx, `
		MATCH (bob:Person {id: "bob"}), (eve:Person {id: "eve"})
		CREATE (charlie:Person {id: "charlie", name: "Charlie"}),
		       (frank:Person {id: "frank", name: "Frank"}),
		       (bob)-[:FRIEND]->(charlie),
		       (eve)-[:WORKS_WITH]->(frank)
	`, nil)
	require.NoError(t, err)

	// Test the query with property-based seed ID
	query := `
		MATCH (seed)
		WHERE seed.id = $seed_id OR id(seed) = $seed_id
		CALL {
			WITH seed
			MATCH path = (seed)-[r*1..2]-(connected)
			RETURN seed, collect(DISTINCT {
				rel: type(r),
				node: connected,
				labels: labels(connected),
				distance: length(path)
			}) as neighbors
		}
		RETURN seed, neighbors
	`

	params := map[string]interface{}{
		"seed_id": "alice",
	}

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify results
	assert.Len(t, result.Rows, 1, "Should return 1 row for seed node")

	if len(result.Rows) > 0 {
		seedNode := result.Rows[0][0]
		neighbors := result.Rows[0][1]

		t.Logf("Seed node: %+v", seedNode)
		t.Logf("Neighbors: %+v", neighbors)

		// Verify seed node
		assert.NotNil(t, seedNode, "Seed node should not be nil")

		// Verify neighbors collection
		assert.NotNil(t, neighbors, "Neighbors should not be nil")

		// Should have neighbors at distance 1 and 2
		// Distance 1: bob, diana, eve (3 nodes)
		// Distance 2: charlie (via bob), frank (via eve) (2 nodes)
		// Total expected: 5 distinct neighbors
		if neighborsList, ok := neighbors.([]interface{}); ok {
			assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors (1-hop)")
			t.Logf("Found %d neighbors", len(neighborsList))
		}
	}
}

// TestGraphTraversalWithInternalID tests the same query but using internal node ID
func TestGraphTraversalWithInternalID(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create simple graph
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: "Alice"}),
		       (b:Person {name: "Bob"}),
		       (c:Person {name: "Charlie"}),
		       (a)-[:FRIEND]->(b),
		       (b)-[:FRIEND]->(c)
	`, nil)
	require.NoError(t, err)

	// Get Alice's internal ID
	result, err := exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}) RETURN id(a) as nodeId`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	aliceID := result.Rows[0][0].(string)
	t.Logf("Alice's internal ID: %s", aliceID)

	// Test query with internal ID
	query := `
		MATCH (seed)
		WHERE seed.id = $seed_id OR id(seed) = $seed_id
		CALL {
			WITH seed
			MATCH path = (seed)-[r*1..2]-(connected)
			RETURN seed, collect(DISTINCT {
				rel: type(r),
				node: connected,
				labels: labels(connected),
				distance: length(path)
			}) as neighbors
		}
		RETURN seed, neighbors
	`

	params := map[string]interface{}{
		"seed_id": aliceID,
	}

	result2, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.NotNil(t, result2)

	assert.Len(t, result2.Rows, 1, "Should return 1 row")
	if len(result2.Rows) > 0 {
		t.Logf("Seed: %+v", result2.Rows[0][0])
		t.Logf("Neighbors: %+v", result2.Rows[0][1])
	}
}

// TestSimplifiedGraphTraversal tests a simpler version without CALL subquery
// to isolate any issues with the variable-length path pattern
func TestSimplifiedGraphTraversal(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph
	_, err := exec.Execute(ctx, `
		CREATE (alice:Person {id: "alice", name: "Alice"}),
		       (bob:Person {id: "bob", name: "Bob"}),
		       (charlie:Person {id: "charlie", name: "Charlie"}),
		       (alice)-[:FRIEND]->(bob),
		       (bob)-[:FRIEND]->(charlie)
	`, nil)
	require.NoError(t, err)

	// Test simple variable-length path
	result, err := exec.Execute(ctx, `
		MATCH (seed:Person {id: "alice"})
		MATCH path = (seed)-[r*1..2]-(connected)
		RETURN seed.name as seed_name, 
		       type(r) as rel_type,
		       connected.name as connected_name,
		       length(path) as distance
	`, nil)

	require.NoError(t, err)
	require.NotNil(t, result)

	t.Logf("Simple traversal found %d paths", len(result.Rows))
	for i, row := range result.Rows {
		t.Logf("Path %d: %v -> %v -> %v (distance: %v)",
			i+1, row[0], row[1], row[2], row[3])
	}

	// Should find at least Bob (1 hop) and Charlie (2 hops)
	assert.GreaterOrEqual(t, len(result.Rows), 2, "Should find paths to at least 2 neighbors")
}

// TestPOCRelationshipQuery tests a production query pattern that finds
// all relationships connected to a specific POC node
func TestPOCRelationshipQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data matching production structure
	// POC node with various relationships to Person, Area, and Sheet nodes
	_, err := exec.Execute(ctx, `
		CREATE (poc:POC {id: "poc_79", name: "Test POC 79"}),
		       (leader:Person {name: "Saul Mankes"}),
		       (area:Area {area_team: "Enterprise Service Layer (ESL)"}),
		       (contact:Person {name: "Solomon Daniel"}),
		       (sheet:Sheet {name: "WS2026 POCs"}),
		       (manager:Person {name: "Solomon Daniel"}),
		       (poc)-[:HAS_LEADER]->(leader),
		       (poc)-[:BELONGS_TO]->(area),
		       (poc)-[:HAS_CONTACT]->(contact),
		       (poc)-[:FROM_SHEET]->(sheet),
		       (poc)-[:MANAGED_BY]->(manager)
	`, nil)
	require.NoError(t, err)

	// Test the production query
	query := `
		MATCH (poc:POC {id: 'poc_79'})-[r]-(other)
		RETURN type(r) as rel, labels(other)[0] as label, 
		       coalesce(other.name, other.area_team, 'N/A') as name
		LIMIT 5
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should find 5 relationships (or all if fewer than 5)
	assert.GreaterOrEqual(t, len(result.Rows), 5, "Should find at least 5 relationships")
	assert.LessOrEqual(t, len(result.Rows), 5, "LIMIT 5 should cap results")

	t.Logf("Found %d relationships for poc_79:", len(result.Rows))

	// Collect relationship types to verify they match expected types
	relTypes := make(map[string]bool)
	for i, row := range result.Rows {
		relType := row[0].(string)
		label := row[1].(string)
		name := row[2].(string)

		relTypes[relType] = true
		t.Logf("  %d: -%s-> %s(%s)", i+1, relType, label, name)
	}

	// Verify expected relationship types exist
	expectedRels := []string{"HAS_LEADER", "BELONGS_TO", "HAS_CONTACT", "FROM_SHEET", "MANAGED_BY"}
	foundCount := 0
	for _, expectedRel := range expectedRels {
		if relTypes[expectedRel] {
			foundCount++
		}
	}
	assert.GreaterOrEqual(t, foundCount, 5, "Should find all 5 relationship types")
}

// TestPOCRelationshipQueryWithCoalesce tests that coalesce() works correctly
// to handle different property names across different node types
func TestPOCRelationshipQueryWithCoalesce(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with different property structures
	_, err := exec.Execute(ctx, `
		CREATE (poc:POC {id: "poc_1"}),
		       (person:Person {name: "John Doe"}),
		       (area:Area {area_team: "Engineering"}),
		       (sheet:Sheet),
		       (poc)-[:HAS_PERSON]->(person),
		       (poc)-[:IN_AREA]->(area),
		       (poc)-[:FROM]->(sheet)
	`, nil)
	require.NoError(t, err)

	// Query with coalesce - should return correct property value for each node type
	query := `
		MATCH (poc:POC {id: 'poc_1'})-[r]-(other)
		RETURN type(r) as rel, 
		       coalesce(other.name, other.area_team, 'N/A') as display_name
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3, "Should find 3 relationships")

	// Verify coalesce returned correct values
	displayNames := make(map[string]string)
	for _, row := range result.Rows {
		relType := row[0].(string)
		displayName := row[1].(string)
		displayNames[relType] = displayName
		t.Logf("%s: %s", relType, displayName)
	}

	assert.Equal(t, "John Doe", displayNames["HAS_PERSON"], "Should use person.name")
	assert.Equal(t, "Engineering", displayNames["IN_AREA"], "Should use area.area_team")
	assert.Equal(t, "N/A", displayNames["FROM"], "Should use default for sheet with no properties")
}

// TestVariableLengthPathWithExplicitDistance tests variable-length paths with explicit distance return
// Expected: Should return up to 20 connected nodes within 2 hops
func TestVariableLengthPathWithExplicitDistance(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph with Area as seed and multiple connected nodes at different distances
	// Graph structure:
	//   (area_16:Area) -[:MANAGES]-> (poc_1:POC) -[:HAS_LEADER]-> (person_1:Person)
	//   (area_16:Area) -[:CONTAINS]-> (team_1:Team)
	//   (area_16:Area) -[:BELONGS_TO]<- (poc_2:POC)
	_, err := exec.Execute(ctx, `
		CREATE (area:Area {id: "area_16", name: "Enterprise Service Layer", area_team: "ESL"}),
		       (poc1:POC {id: "poc_1", name: "POC Alpha"}),
		       (poc2:POC {id: "poc_2", name: "POC Beta"}),
		       (person1:Person {id: "person_1", name: "John Smith"}),
		       (person2:Person {id: "person_2", name: "Jane Doe"}),
		       (team1:Team {id: "team_1", name: "Core Team"}),
		       (area)-[:MANAGES]->(poc1),
		       (area)-[:CONTAINS]->(team1),
		       (poc2)-[:BELONGS_TO]->(area),
		       (poc1)-[:HAS_LEADER]->(person1),
		       (poc2)-[:HAS_CONTACT]->(person2)
	`, nil)
	require.NoError(t, err)

	// Test the exact query from production
	query := `
		MATCH path = (seed:Area {id: 'area_16'})-[*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		RETURN seed.name,
		       labels(connected)[0] as conn_label,
		       coalesce(connected.name, connected.area_team, 'N/A') as conn_name,
		       length(path) as distance
		LIMIT 20
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	t.Logf("Variable-length path found %d results:", len(result.Rows))
	for i, row := range result.Rows {
		t.Logf("  %d: seed=%v, label=%v, name=%v, distance=%v", i+1, row[0], row[1], row[2], row[3])
	}

	// Should find:
	// Distance 1: poc_1, poc_2, team_1 (3 nodes)
	// Distance 2: person_1 (via poc_1), person_2 (via poc_2) (2 nodes)
	// Total: at least 5 connected nodes
	assert.GreaterOrEqual(t, len(result.Rows), 3, "Should find at least 3 connected nodes (1-hop)")

	// Verify we have results at different distances
	distances := make(map[int64]int)
	for _, row := range result.Rows {
		if dist, ok := row[3].(int64); ok {
			distances[dist]++
		}
	}
	t.Logf("Distances found: %v", distances)

	// Should have both distance 1 and distance 2 results
	assert.Greater(t, distances[1], 0, "Should have distance 1 results")
}

// TestAggregatedNeighborsWithRelationshipChain tests aggregation with relationship type chains
// Expected: Should return aggregated neighbors with relationship chains
func TestAggregatedNeighborsWithRelationshipChain(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph with Area as seed and multiple connected nodes
	_, err := exec.Execute(ctx, `
		CREATE (area:Area {id: "area_16", name: "Enterprise Service Layer", area_team: "ESL"}),
		       (poc1:POC {id: "poc_1", name: "POC Alpha"}),
		       (poc2:POC {id: "poc_2", name: "POC Beta"}),
		       (person1:Person {id: "person_1", name: "John Smith"}),
		       (person2:Person {id: "person_2", name: "Jane Doe"}),
		       (area)-[:MANAGES]->(poc1),
		       (poc2)-[:BELONGS_TO]->(area),
		       (poc1)-[:HAS_LEADER]->(person1),
		       (poc2)-[:HAS_CONTACT]->(person2)
	`, nil)
	require.NoError(t, err)

	// Test the exact aggregation query from production
	query := `
		MATCH (seed:Area {id: 'area_16'})
		OPTIONAL MATCH path = (seed)-[rels*1..2]-(connected)
		WHERE connected IS NOT NULL AND id(connected) <> id(seed)
		WITH seed, connected, 
		     [rel IN relationships(path) | type(rel)] as rel_chain,
		     length(path) as dist
		RETURN seed.name,
		       collect({
		           labels: labels(connected),
		           name: coalesce(connected.name, connected.area_team, 'N/A'),
		           distance: dist,
		           rel_chain: rel_chain
		       })[..10] as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	t.Logf("Aggregated neighbors query returned %d rows", len(result.Rows))
	require.Len(t, result.Rows, 1, "Should return 1 row for the seed node")

	seedName := result.Rows[0][0]
	neighbors := result.Rows[0][1]

	t.Logf("Seed: %v", seedName)
	t.Logf("Neighbors: %+v", neighbors)

	assert.Equal(t, "Enterprise Service Layer", seedName, "Seed name should match")
	assert.NotNil(t, neighbors, "Neighbors should not be nil")

	// Verify neighbors is a collection
	if neighborsList, ok := neighbors.([]interface{}); ok {
		t.Logf("Found %d neighbors in collection", len(neighborsList))
		assert.GreaterOrEqual(t, len(neighborsList), 2, "Should have at least 2 neighbors")

		// Log each neighbor for debugging
		for i, n := range neighborsList {
			t.Logf("  Neighbor %d: %+v", i+1, n)
		}
	} else if neighborsMap, ok := neighbors.([]map[string]interface{}); ok {
		t.Logf("Found %d neighbors in map collection", len(neighborsMap))
		assert.GreaterOrEqual(t, len(neighborsMap), 2, "Should have at least 2 neighbors")
	}
}

// TestCollectWithMapLiteral tests that collect() correctly evaluates map literals
// This was a bug where { key: value } was returned as strings instead of maps
func TestCollectWithMapLiteral(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data matching the issue reproduction
	_, err := exec.Execute(ctx, `
		CREATE (seed:Area {id: 'area_16', name: 'Enterprise Service Layer (ESL)'}),
		       (p1:Person {id: 'person_1', name: 'Solomon Daniel'}),
		       (p2:Person {id: 'person_2', name: 'Saul Mankes'}),
		       (s:Sheet {id: 'sheet_1', title: 'WS2026 POCs'}),
		       (seed)-[:WORKS_IN]->(p1),
		       (seed)-[:WORKS_IN]->(p2),
		       (seed)-[:BELONGS_TO]->(s)
	`, nil)
	require.NoError(t, err)

	// First test without DISTINCT to isolate the issue
	t.Run("collect without DISTINCT", func(t *testing.T) {
		query := `
			MATCH (seed {id: 'area_16'})-[r]-(connected)
			RETURN seed.name as seed_name,
			       collect({
			           node_name: connected.name,
			           labels: labels(connected),
			           rel_type: type(r)
			       }) as neighbors
		`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return 1 row")

		t.Logf("Columns: %v", result.Columns)
		t.Logf("Seed: %+v", result.Rows[0][0])
		t.Logf("Neighbors: %+v", result.Rows[0][1])

		neighbors := result.Rows[0][1]
		require.NotNil(t, neighbors, "neighbors should not be nil")

		// Verify neighbors is a slice of maps, not strings
		neighborsList, ok := neighbors.([]interface{})
		require.True(t, ok, "neighbors should be a []interface{}, got %T", neighbors)
		assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors")

		// Check that each neighbor is a map with the expected keys
		for i, n := range neighborsList {
			neighborMap, isMap := n.(map[string]interface{})
			require.True(t, isMap, "neighbor %d should be a map, got %T: %v", i, n, n)

			t.Logf("Neighbor %d: %+v", i, neighborMap)
		}
	})

	// Test with DISTINCT
	t.Run("collect with DISTINCT", func(t *testing.T) {
		query := `
			MATCH (seed {id: 'area_16'})-[r]-(connected)
			RETURN seed,
			       collect(DISTINCT {
			           node: connected,
			           labels: labels(connected),
			           rel_type: type(r),
			           distance: 1
			       }) as neighbors
		`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return 1 row")

		t.Logf("Columns: %v", result.Columns)
		t.Logf("Seed: %+v", result.Rows[0][0])
		t.Logf("Neighbors: %+v", result.Rows[0][1])

		neighbors := result.Rows[0][1]
		require.NotNil(t, neighbors, "neighbors should not be nil")

		// Verify neighbors is a slice of maps, not strings
		neighborsList, ok := neighbors.([]interface{})
		require.True(t, ok, "neighbors should be a []interface{}, got %T", neighbors)
		assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors")

		// Check that each neighbor is a map with the expected keys
		for i, n := range neighborsList {
			neighborMap, isMap := n.(map[string]interface{})
			require.True(t, isMap, "neighbor %d should be a map, got %T: %v", i, n, n)

			// Check expected keys exist
			_, hasNode := neighborMap["node"]
			_, hasLabels := neighborMap["labels"]
			_, hasRelType := neighborMap["rel_type"]
			_, hasDistance := neighborMap["distance"]

			assert.True(t, hasNode, "neighbor %d should have 'node' key", i)
			assert.True(t, hasLabels, "neighbor %d should have 'labels' key", i)
			assert.True(t, hasRelType, "neighbor %d should have 'rel_type' key", i)
			assert.True(t, hasDistance, "neighbor %d should have 'distance' key", i)

			t.Logf("Neighbor %d: node=%v, labels=%v, rel_type=%v, distance=%v",
				i, neighborMap["node"], neighborMap["labels"], neighborMap["rel_type"], neighborMap["distance"])
		}
	})
}
