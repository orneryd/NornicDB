package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupAreaGraphData creates a test graph for the failing query tests.
// Graph structure:
//
//	(area:Area {id: 'area_16', name: 'Enterprise Service Layer'})
//	  -[:MANAGES]-> (poc1:POC {id: 'poc_1', name: 'POC Alpha'})
//	    -[:HAS_LEADER]-> (person1:Person {id: 'person_1', name: 'John Smith'})
//	  -[:CONTAINS]-> (team1:Team {id: 'team_1', name: 'Core Team'})
//	  <-[:BELONGS_TO]- (poc2:POC {id: 'poc_2', name: 'POC Beta'})
//	    -[:HAS_CONTACT]-> (person2:Person {id: 'person_2', name: 'Jane Doe'})
func setupAreaGraphData(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	_, err := exec.Execute(ctx, `
		CREATE (area:Area {id: 'area_16', name: 'Enterprise Service Layer', area_team: 'ESL'}),
		       (poc1:POC {id: 'poc_1', name: 'POC Alpha'}),
		       (poc2:POC {id: 'poc_2', name: 'POC Beta'}),
		       (person1:Person {id: 'person_1', name: 'John Smith'}),
		       (person2:Person {id: 'person_2', name: 'Jane Doe'}),
		       (team1:Team {id: 'team_1', name: 'Core Team'}),
		       (area)-[:MANAGES]->(poc1),
		       (area)-[:CONTAINS]->(team1),
		       (poc2)-[:BELONGS_TO]->(area),
		       (poc1)-[:HAS_LEADER]->(person1),
		       (poc2)-[:HAS_CONTACT]->(person2)
	`, nil)
	require.NoError(t, err)
}

// ============================================================================
// FAIL #1: CALL subquery with variable-length path
// ============================================================================
// Issue: CALL subquery not properly aggregating before outer RETURN
// Expected: List of neighbor maps with node, labels, distance
// Actual: Returns no results, driver warning "Expected a single record, but found multiple"

func TestFailingQuery_CallSubqueryWithVariableLengthPath(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// EXACT FAILING QUERY - DO NOT MODIFY
	query := `
		MATCH (seed {id: 'area_16'})
		CALL {
			WITH seed
			MATCH path = (seed)-[*1..2]-(connected)
			WHERE id(connected) <> id(seed)
			RETURN connected, length(path) as dist
		}
		RETURN collect(DISTINCT {
			node: connected,
			labels: labels(connected),
			distance: dist
		}) as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "Query should execute without error")
	require.NotNil(t, result, "Result should not be nil")

	// Should return exactly 1 row (aggregated result)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row with aggregated neighbors")

	// The single row should have 'neighbors' column
	require.Len(t, result.Columns, 1, "Should have 1 column: neighbors")
	assert.Equal(t, "neighbors", result.Columns[0], "Column should be 'neighbors'")

	// Get the neighbors collection
	neighbors := result.Rows[0][0]
	require.NotNil(t, neighbors, "Neighbors should not be nil")

	neighborsList, ok := neighbors.([]interface{})
	require.True(t, ok, "Neighbors should be a list, got %T", neighbors)

	// Should have at least 3 distinct neighbors:
	// Distance 1: poc1, team1, poc2 (3 nodes)
	// Distance 2: person1 (via poc1), person2 (via poc2) (2 nodes)
	assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors")

	t.Logf("Found %d neighbors", len(neighborsList))

	// Verify each neighbor is a map with expected keys
	for i, n := range neighborsList {
		neighborMap, isMap := n.(map[string]interface{})
		require.True(t, isMap, "Neighbor %d should be a map, got %T: %v", i, n, n)

		// Check required keys exist
		_, hasNode := neighborMap["node"]
		_, hasLabels := neighborMap["labels"]
		_, hasDistance := neighborMap["distance"]

		assert.True(t, hasNode, "Neighbor %d should have 'node' key", i)
		assert.True(t, hasLabels, "Neighbor %d should have 'labels' key", i)
		assert.True(t, hasDistance, "Neighbor %d should have 'distance' key", i)

		// Distance should be 1 or 2
		if dist, ok := neighborMap["distance"].(int64); ok {
			assert.True(t, dist == 1 || dist == 2, "Distance should be 1 or 2, got %d", dist)
		}

		t.Logf("  Neighbor %d: labels=%v, distance=%v", i, neighborMap["labels"], neighborMap["distance"])
	}
}

// ============================================================================
// FAIL #2: Variable-length with WITH clause aggregation
// ============================================================================
// Issue: WITH clause before aggregation breaks map literal evaluation
// Expected: Single row with seed_name and list of neighbor maps
// Actual: Returns strings instead of maps, `seed_name` is literal "seed.name"

func TestFailingQuery_VariableLengthWithClauseAggregation(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// EXACT FAILING QUERY - DO NOT MODIFY
	query := `
		MATCH path = (seed {id: 'area_16'})-[*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		WITH seed, connected, length(path) as dist
		RETURN seed.name as seed_name,
		       collect(DISTINCT {
		           node: connected,
		           labels: labels(connected),
		           distance: dist
		       }) as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "Query should execute without error")
	require.NotNil(t, result, "Result should not be nil")

	// Should return exactly 1 row (aggregated result)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row")

	// Should have 2 columns: seed_name and neighbors
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "seed_name", result.Columns[0], "First column should be 'seed_name'")
	assert.Equal(t, "neighbors", result.Columns[1], "Second column should be 'neighbors'")

	// Verify seed_name is the actual value, not the literal string "seed.name"
	seedName := result.Rows[0][0]
	require.NotNil(t, seedName, "seed_name should not be nil")
	assert.Equal(t, "Enterprise Service Layer", seedName, "seed_name should be the actual Area name")
	assert.NotEqual(t, "seed.name", seedName, "seed_name should NOT be the literal string 'seed.name'")

	// Get the neighbors collection
	neighbors := result.Rows[0][1]
	require.NotNil(t, neighbors, "Neighbors should not be nil")

	neighborsList, ok := neighbors.([]interface{})
	require.True(t, ok, "Neighbors should be a list, got %T", neighbors)

	// Should have at least 3 distinct neighbors
	assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors")

	t.Logf("seed_name: %v", seedName)
	t.Logf("Found %d neighbors", len(neighborsList))

	// Verify each neighbor is a map (not a string!)
	for i, n := range neighborsList {
		neighborMap, isMap := n.(map[string]interface{})
		require.True(t, isMap, "Neighbor %d should be a map, got %T: %v", i, n, n)

		// Check required keys exist
		_, hasNode := neighborMap["node"]
		_, hasLabels := neighborMap["labels"]
		_, hasDistance := neighborMap["distance"]

		assert.True(t, hasNode, "Neighbor %d should have 'node' key", i)
		assert.True(t, hasLabels, "Neighbor %d should have 'labels' key", i)
		assert.True(t, hasDistance, "Neighbor %d should have 'distance' key", i)

		// Distance should be populated (not nil)
		assert.NotNil(t, neighborMap["distance"], "Neighbor %d distance should not be nil", i)

		t.Logf("  Neighbor %d: labels=%v, distance=%v", i, neighborMap["labels"], neighborMap["distance"])
	}
}

// ============================================================================
// FAIL #3: Relationship chain extraction (partial fail)
// ============================================================================
// Issue: `relationships(path)` returns empty array when used inside map literal
// Expected: Maps with `rel_chain` populated as `['MANAGES', 'HAS_LEADER']` etc
// Actual: Maps are created correctly but `rel_chain` is always empty `[]`

func TestFailingQuery_RelationshipChainExtraction(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// EXACT FAILING QUERY - DO NOT MODIFY
	query := `
		MATCH path = (seed {id: 'area_16'})-[rels*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		RETURN collect(DISTINCT {
			node: connected,
			labels: labels(connected),
			distance: length(path),
			rel_chain: [r IN relationships(path) | type(r)]
		}) as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "Query should execute without error")
	require.NotNil(t, result, "Result should not be nil")

	// Should return exactly 1 row (aggregated result)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row")

	// Get the neighbors collection
	neighbors := result.Rows[0][0]
	require.NotNil(t, neighbors, "Neighbors should not be nil")

	neighborsList, ok := neighbors.([]interface{})
	require.True(t, ok, "Neighbors should be a list, got %T", neighbors)

	// Should have at least 3 distinct neighbors
	assert.GreaterOrEqual(t, len(neighborsList), 3, "Should have at least 3 neighbors")

	t.Logf("Found %d neighbors", len(neighborsList))

	// Track whether we found at least one neighbor with a populated rel_chain
	foundPopulatedRelChain := false

	// Verify each neighbor has a populated rel_chain
	for i, n := range neighborsList {
		neighborMap, isMap := n.(map[string]interface{})
		require.True(t, isMap, "Neighbor %d should be a map, got %T: %v", i, n, n)

		// Check rel_chain exists and is not empty
		relChain, hasRelChain := neighborMap["rel_chain"]
		assert.True(t, hasRelChain, "Neighbor %d should have 'rel_chain' key", i)

		if relChain != nil {
			if relChainList, ok := relChain.([]interface{}); ok {
				// rel_chain should NOT be empty
				if len(relChainList) > 0 {
					foundPopulatedRelChain = true
					// Each element should be a relationship type string
					for j, relType := range relChainList {
						relTypeStr, isStr := relType.(string)
						assert.True(t, isStr, "rel_chain[%d] should be a string, got %T", j, relType)
						assert.NotEmpty(t, relTypeStr, "rel_chain[%d] should not be empty", j)
					}
				}
				t.Logf("  Neighbor %d: labels=%v, distance=%v, rel_chain=%v",
					i, neighborMap["labels"], neighborMap["distance"], relChainList)
			}
		}
	}

	// At least one neighbor should have a populated rel_chain
	assert.True(t, foundPopulatedRelChain,
		"At least one neighbor should have a non-empty rel_chain (expected relationship types like ['MANAGES', 'HAS_LEADER'])")
}

// ============================================================================
// Additional test to verify relationships(path) works in simpler context
// ============================================================================

func TestRelationshipsPathFunction_Basic(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Simple setup: A -[:REL1]-> B -[:REL2]-> C
	_, err := exec.Execute(ctx, `
		CREATE (a:Node {id: 'a', name: 'A'}),
		       (b:Node {id: 'b', name: 'B'}),
		       (c:Node {id: 'c', name: 'C'}),
		       (a)-[:REL1]->(b),
		       (b)-[:REL2]->(c)
	`, nil)
	require.NoError(t, err)

	// Test relationships(path) returning the relationship list
	result, err := exec.Execute(ctx, `
		MATCH path = (a:Node {id: 'a'})-[*1..2]->(c)
		RETURN [r IN relationships(path) | type(r)] as rel_types
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should find at least one path
	require.GreaterOrEqual(t, len(result.Rows), 1, "Should find at least one path")

	// Check that rel_types is populated
	for i, row := range result.Rows {
		relTypes := row[0]
		require.NotNil(t, relTypes, "rel_types should not be nil")

		relTypesList, ok := relTypes.([]interface{})
		require.True(t, ok, "rel_types should be a list, got %T", relTypes)

		// Should have at least one relationship type
		assert.GreaterOrEqual(t, len(relTypesList), 1, "Should have at least one relationship type")

		t.Logf("Path %d: rel_types=%v", i, relTypesList)
	}
}

// ============================================================================
// Additional test to verify length(path) works in collect with map literal
// ============================================================================

func TestLengthPathInCollectMapLiteral(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Simple setup: A -[:REL1]-> B -[:REL2]-> C
	_, err := exec.Execute(ctx, `
		CREATE (a:Node {id: 'a', name: 'A'}),
		       (b:Node {id: 'b', name: 'B'}),
		       (c:Node {id: 'c', name: 'C'}),
		       (a)-[:REL1]->(b),
		       (b)-[:REL2]->(c)
	`, nil)
	require.NoError(t, err)

	// Test length(path) inside collect with map literal
	result, err := exec.Execute(ctx, `
		MATCH path = (a:Node {id: 'a'})-[*1..2]-(connected)
		WHERE id(connected) <> id(a)
		RETURN collect({
			name: connected.name,
			distance: length(path)
		}) as results
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1, "Should return 1 aggregated row")

	results := result.Rows[0][0]
	require.NotNil(t, results)

	resultsList, ok := results.([]interface{})
	require.True(t, ok, "results should be a list")
	require.GreaterOrEqual(t, len(resultsList), 1, "Should have at least 1 result")

	// Check that each result has a non-nil, non-zero distance
	foundNonZeroDistance := false
	for i, r := range resultsList {
		resultMap, isMap := r.(map[string]interface{})
		require.True(t, isMap, "Result %d should be a map", i)

		distance := resultMap["distance"]
		assert.NotNil(t, distance, "Result %d distance should not be nil", i)

		// Distance should be 1 or 2, not 0
		if dist, ok := distance.(int64); ok && dist > 0 {
			foundNonZeroDistance = true
		}

		t.Logf("Result %d: name=%v, distance=%v", i, resultMap["name"], distance)
	}

	// At least one result should have a non-zero distance
	assert.True(t, foundNonZeroDistance,
		"At least one result should have a non-zero distance (expected 1 or 2)")
}

// TestFailingQuery_WithPathLimitCollectMapLiteral tests:
// MATCH path = (seed)-[*1..2]-(connected)
// WITH path, connected LIMIT 10
// RETURN collect({...}) as neighbors
//
// This pattern was failing because after WITH, the path context was lost
// and map literal expressions were returned as literal strings.
func TestFailingQuery_WithPathLimitCollectMapLiteral(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// EXACT FAILING QUERY - DO NOT MODIFY
	query := `
		MATCH path = (seed {id: 'area_16'})-[rels*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		WITH path, connected
		LIMIT 10
		RETURN collect({
			node: connected,
			labels: labels(connected),
			distance: length(path),
			rel_chain: [r IN relationships(path) | type(r)]
		}) as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should return exactly 1 row (aggregated)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row with aggregated neighbors")

	// Get the neighbors list
	neighbors := result.Rows[0][0]
	require.NotNil(t, neighbors, "neighbors should not be nil")

	neighborsList, ok := neighbors.([]interface{})
	require.True(t, ok, "neighbors should be a list, got %T", neighbors)
	require.GreaterOrEqual(t, len(neighborsList), 1, "Should have at least 1 neighbor")

	t.Logf("Found %d neighbors", len(neighborsList))

	// Verify each neighbor is a properly evaluated map (not literal strings)
	foundValidLabels := false
	foundNonZeroDistance := false
	foundNonEmptyRelChain := false

	for i, n := range neighborsList {
		neighborMap, isMap := n.(map[string]interface{})
		require.True(t, isMap, "Neighbor %d should be a map, got %T: %v", i, n, n)

		// labels should be a list, NOT the literal string "labels(connected)"
		labels := neighborMap["labels"]
		labelsStr, isString := labels.(string)
		require.False(t, isString && labelsStr == "labels(connected)",
			"labels should NOT be the literal string 'labels(connected)', got: %v", labels)
		// Accept both []interface{} and []string
		switch lbls := labels.(type) {
		case []interface{}:
			if len(lbls) > 0 {
				foundValidLabels = true
			}
		case []string:
			if len(lbls) > 0 {
				foundValidLabels = true
			}
		}

		// distance should be a number, NOT the literal string "length(path)"
		distance := neighborMap["distance"]
		distStr, isString := distance.(string)
		require.False(t, isString && distStr == "length(path)",
			"distance should NOT be the literal string 'length(path)', got: %v", distance)
		// Accept int64, int, or float64
		switch d := distance.(type) {
		case int64:
			if d > 0 {
				foundNonZeroDistance = true
			}
		case int:
			if d > 0 {
				foundNonZeroDistance = true
			}
		case float64:
			if d > 0 {
				foundNonZeroDistance = true
			}
		}

		// rel_chain should be a list, NOT the literal string "[r IN relationships(path) | type(r)]"
		relChain := neighborMap["rel_chain"]
		relChainStr, isString := relChain.(string)
		require.False(t, isString && strings.Contains(relChainStr, "relationships"),
			"rel_chain should NOT be a literal string, got: %v", relChain)
		if relList, ok := relChain.([]interface{}); ok && len(relList) > 0 {
			foundNonEmptyRelChain = true
		}

		if i < 3 {
			t.Logf("  Neighbor %d: labels=%v, distance=%v, rel_chain=%v",
				i, labels, distance, relChain)
		}
	}

	assert.True(t, foundValidLabels, "At least one neighbor should have valid labels")
	assert.True(t, foundNonZeroDistance, "At least one neighbor should have non-zero distance")
	assert.True(t, foundNonEmptyRelChain, "At least one neighbor should have non-empty rel_chain")
}

// TestFailingQuery_WithSeedConnectedDistLimit tests the exact query pattern that was hanging:
// MATCH path = (seed)-[*1..2]-(connected)
// WITH seed, connected, length(path) as dist LIMIT 20
// RETURN seed.name, collect(DISTINCT {...})
func TestFailingQuery_WithSeedConnectedDistLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// EXACT HANGING QUERY - DO NOT MODIFY
	query := `
		MATCH path = (seed {id: 'area_16'})-[*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		WITH seed, connected, length(path) as dist
		LIMIT 20
		RETURN seed.name as seed_name,
		       collect(DISTINCT {
		           node: connected,
		           labels: labels(connected),
		           distance: dist
		       }) as neighbors
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "Query should execute without error")
	require.NotNil(t, result, "Result should not be nil")

	// Should return exactly 1 row (aggregated result)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row")

	// Should have 2 columns: seed_name and neighbors
	require.Len(t, result.Columns, 2, "Should have 2 columns")

	// Verify seed_name is the actual value
	seedName := result.Rows[0][0]
	require.NotNil(t, seedName, "seed_name should not be nil")
	assert.NotEqual(t, "seed.name", seedName, "seed_name should NOT be literal string")
	t.Logf("seed_name: %v", seedName)

	// Get the neighbors collection
	neighbors := result.Rows[0][1]
	require.NotNil(t, neighbors, "Neighbors should not be nil")

	neighborsList, ok := neighbors.([]interface{})
	require.True(t, ok, "Neighbors should be a list, got %T", neighbors)
	require.GreaterOrEqual(t, len(neighborsList), 1, "Should have at least 1 neighbor")

	t.Logf("Found %d neighbors", len(neighborsList))

	// Verify neighbors are maps with correct structure
	for i, n := range neighborsList {
		neighborMap, isMap := n.(map[string]interface{})
		require.True(t, isMap, "Neighbor %d should be a map, got %T: %v", i, n, n)

		// Verify distance is a number, not literal string
		dist := neighborMap["distance"]
		_, isString := dist.(string)
		require.False(t, isString, "distance should be a number, not string: %v", dist)

		if i < 3 {
			t.Logf("  Neighbor %d: labels=%v, distance=%v", i, neighborMap["labels"], dist)
		}
	}
}

// ============================================================================
// TEST: size() function with aggregation
// ============================================================================
// Issue: size(nodes) returning literal string instead of count
// Expected: size() should return an integer count

func TestSizeFunctionWithAggregation(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAreaGraphData(t, exec, ctx)

	// Query that uses size() on collected nodes
	query := `
		MATCH (seed {id: 'area_16'})-[*1..2]-(connected)
		WHERE id(connected) <> id(seed)
		WITH seed, collect(DISTINCT connected) as nodes
		RETURN seed.name as seed_name, size(nodes) as neighbor_count
	`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1, "Should return exactly 1 row")
	require.Len(t, result.Columns, 2, "Should have 2 columns: seed_name, neighbor_count")

	seedName := result.Rows[0][0]
	neighborCount := result.Rows[0][1]

	t.Logf("seed_name: %v, neighbor_count: %v (type: %T)", seedName, neighborCount, neighborCount)

	// Verify seed_name is not literal string
	assert.NotEqual(t, "seed.name", seedName, "seed_name should NOT be literal string")
	assert.Equal(t, "Enterprise Service Layer", seedName, "seed_name should be the actual name")

	// Verify neighbor_count is a number, not literal string "size(nodes)"
	_, isString := neighborCount.(string)
	require.False(t, isString, "neighbor_count should be a number, not string: %v", neighborCount)

	// Should be int64
	count, ok := neighborCount.(int64)
	require.True(t, ok, "neighbor_count should be int64, got %T", neighborCount)
	require.GreaterOrEqual(t, count, int64(1), "Should have at least 1 neighbor")
	t.Logf("Verified neighbor_count: %d", count)
}
