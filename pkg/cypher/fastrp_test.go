package cypher

import (
	"context"
	"fmt"
	"math"
	mathrand "math/rand"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

func setupFastRPTestStorage(t *testing.T) storage.Engine {
	t.Helper()
	// Use in-memory Badger for tests to avoid disk I/O and Windows OOM issues
	baseEngine, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	t.Cleanup(func() { _ = baseEngine.Close() })
	// Wrap with NamespacedEngine to ensure prefixed IDs
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	return engine
}

func createSocialNetwork(t *testing.T, engine storage.Engine) {
	t.Helper()

	// Create Person nodes with age property
	people := []struct {
		id   string
		name string
		age  int
	}{
		{"p1", "Dan", 18},
		{"p2", "Annie", 12},
		{"p3", "Matt", 22},
		{"p4", "Jeff", 51},
		{"p5", "Brie", 45},
		{"p6", "Elsa", 65},
		{"p7", "John", 64},
	}

	for _, p := range people {
		node := &storage.Node{
			ID:     storage.NodeID(p.id),
			Labels: []string{"Person"},
			Properties: map[string]any{
				"name": p.name,
				"age":  p.age,
			},
		}
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node %s: %v", p.id, err)
		}
	}

	// Create KNOWS relationships with weights
	edges := []struct {
		id       string
		from, to string
		weight   float64
	}{
		{"e1", "p1", "p2", 1.0}, // Dan -> Annie
		{"e2", "p1", "p3", 1.0}, // Dan -> Matt
		{"e3", "p2", "p3", 1.0}, // Annie -> Matt
		{"e4", "p2", "p4", 1.0}, // Annie -> Jeff
		{"e5", "p2", "p5", 1.0}, // Annie -> Brie
		{"e6", "p3", "p5", 3.5}, // Matt -> Brie (strong connection)
		{"e7", "p5", "p6", 1.0}, // Brie -> Elsa
		{"e8", "p5", "p4", 2.0}, // Brie -> Jeff
		{"e9", "p7", "p4", 1.0}, // John -> Jeff
	}

	for _, e := range edges {
		edge := &storage.Edge{
			ID:        storage.EdgeID(e.id),
			StartNode: storage.NodeID(e.from),
			EndNode:   storage.NodeID(e.to),
			Type:      "KNOWS",
			Properties: map[string]any{
				"weight": e.weight,
			},
		}
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("Failed to create edge %s->%s: %v", e.from, e.to, err)
		}
	}
}

func TestInitializeEmbeddingChunkCapsOversizedDimensions(t *testing.T) {
	const oversizedDim = maxFastRPEmbeddingDimension * 2
	embeddings := make([][]float64, 2)
	rng := mathrand.New(mathrand.NewSource(42))

	initializeEmbeddingChunk(embeddings, 0, len(embeddings), oversizedDim, rng)

	for i, embedding := range embeddings {
		if len(embedding) != maxFastRPEmbeddingDimension {
			t.Fatalf("embedding %d length = %d, want %d", i, len(embedding), maxFastRPEmbeddingDimension)
		}
	}
}

// createLargeGraph creates a graph with many nodes for stress testing
func createLargeGraph(t *testing.T, engine storage.Engine, numNodes, avgDegree int) {
	t.Helper()

	// Create nodes
	for i := 0; i < numNodes; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("n%d", i)),
			Labels: []string{"Node"},
			Properties: map[string]any{
				"index": i,
				"value": float64(i) / float64(numNodes),
			},
		}
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node n%d: %v", i, err)
		}
	}

	// Create edges (random-ish but deterministic)
	edgeCount := 0
	for i := 0; i < numNodes; i++ {
		for j := 0; j < avgDegree; j++ {
			target := (i + j*7 + 1) % numNodes // Deterministic "random" target
			if target == i {
				continue
			}
			edge := &storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("e%d", edgeCount)),
				StartNode: storage.NodeID(fmt.Sprintf("n%d", i)),
				EndNode:   storage.NodeID(fmt.Sprintf("n%d", target)),
				Type:      "CONNECTED",
				Properties: map[string]any{
					"weight": 1.0 + float64(j)*0.1,
				},
			}
			if err := engine.CreateEdge(edge); err != nil {
				t.Fatalf("Failed to create edge: %v", err)
			}
			edgeCount++
		}
	}
	t.Logf("Created graph with %d nodes and %d edges", numNodes, edgeCount)
}

// ============================================================================
// GDS Version Tests
// ============================================================================

func TestGdsVersion(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL gds.version() YIELD gdsVersion RETURN gdsVersion", nil)
	if err != nil {
		t.Fatalf("gds.version() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	version, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string version, got %T", result.Rows[0][0])
	}
	if !strings.Contains(version, "nornicdb") {
		t.Errorf("Expected version to contain 'nornicdb', got: %s", version)
	}

	t.Logf("✓ gds.version() returned: %s", version)
}

// ============================================================================
// Graph Projection Tests
// ============================================================================

func TestGdsGraphProject(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a graph projection
	result, err := exec.Execute(ctx, "CALL gds.graph.project('test-graph', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("gds.graph.project() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// GDS's columns: nodeProjection, relationshipProjection, graphName,
	// nodeCount, relationshipCount, projectMillis.
	require.Equal(t, []string{"nodeProjection", "relationshipProjection", "graphName", "nodeCount", "relationshipCount", "projectMillis"}, result.Columns)
	graphName, ok := result.Rows[0][2].(string)
	if !ok {
		t.Fatalf("Expected string graphName, got %T", result.Rows[0][2])
	}
	nodeCount, ok := result.Rows[0][3].(int64)
	if !ok {
		t.Fatalf("Expected INTEGER nodeCount, got %T", result.Rows[0][3])
	}
	relCount, ok := result.Rows[0][4].(int64)
	if !ok {
		t.Fatalf("Expected INTEGER relCount, got %T", result.Rows[0][4])
	}

	if graphName != "test-graph" {
		t.Errorf("Expected graph name 'test-graph', got: %s", graphName)
	}

	if nodeCount != 7 {
		t.Errorf("Expected 7 nodes, got: %d", nodeCount)
	}

	if relCount < 1 {
		t.Errorf("Expected at least 1 relationship, got: %d", relCount)
	}

	t.Logf("✓ gds.graph.project() created: %s with %d nodes, %d relationships", graphName, nodeCount, relCount)

	// Clean up
	_, _ = exec.Execute(ctx, "CALL gds.graph.drop('test-graph')", nil)
}

func TestGdsGraphList(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create two projections
	_, err := exec.Execute(ctx, "CALL gds.graph.project('graph1', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph1: %v", err)
	}

	_, err = exec.Execute(ctx, "CALL gds.graph.project('graph2', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph2: %v", err)
	}

	// List graphs
	result, err := exec.Execute(ctx, "CALL gds.graph.list() YIELD graphName RETURN graphName", nil)
	if err != nil {
		t.Fatalf("gds.graph.list() failed: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 graphs, got: %d", len(result.Rows))
	}

	graphNames := make(map[string]bool)
	for _, row := range result.Rows {
		if name, ok := row[0].(string); ok {
			graphNames[name] = true
		}
	}

	if !graphNames["graph1"] || !graphNames["graph2"] {
		t.Errorf("Expected graph1 and graph2 in list, got: %v", graphNames)
	}

	t.Logf("✓ gds.graph.list() returned %d graphs", len(result.Rows))

	// Clean up
	_, _ = exec.Execute(ctx, "CALL gds.graph.drop('graph1')", nil)
	_, _ = exec.Execute(ctx, "CALL gds.graph.drop('graph2')", nil)
}

func TestGdsGraphDrop(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('to-drop', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}

	// Drop it
	result, err := exec.Execute(ctx, "CALL gds.graph.drop('to-drop')", nil)
	if err != nil {
		t.Fatalf("gds.graph.drop() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// Verify it's gone - try to drop again should error
	_, err = exec.Execute(ctx, "CALL gds.graph.drop('to-drop')", nil)
	if err == nil {
		t.Error("Expected error when dropping already-dropped graph")
	}

	t.Log("✓ gds.graph.drop() successfully removed the graph")
}

func TestGdsGraphDropNonExistent(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Try to drop non-existent graph
	_, err := exec.Execute(ctx, "CALL gds.graph.drop('nonexistent')", nil)
	if err == nil {
		t.Error("Expected error when dropping non-existent graph")
	}

	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected 'does not exist' in error, got: %v", err)
	}

	t.Logf("✓ gds.graph.drop() correctly errors for non-existent graph")
}

// ============================================================================
// FastRP Core Functionality Tests
// ============================================================================

func TestGdsFastRPStream(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection first
	_, err := exec.Execute(ctx, "CALL gds.graph.project('fastrp-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('fastrp-test')", nil)
	}()

	// Run FastRP
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('fastrp-test', {
			embeddingDimension: 8,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("gds.fastRP.stream() failed: %v", err)
	}

	if len(result.Rows) != 7 {
		t.Errorf("Expected 7 embeddings (one per node), got: %d", len(result.Rows))
	}

	// Verify embedding dimensions and normalization
	for _, row := range result.Rows {
		nodeID, ok := row[0].(string)
		if !ok {
			t.Errorf("Expected string nodeId, got %T", row[0])
			continue
		}
		embedding, ok := row[1].([]float64)
		if !ok {
			t.Errorf("Expected []float64 embedding, got %T", row[1])
			continue
		}

		if len(embedding) != 8 {
			t.Errorf("Node %s: expected embedding dimension 8, got %d", nodeID, len(embedding))
		}

		// Verify embedding is normalized (L2 norm should be ~1.0)
		var sumSq float64
		for _, v := range embedding {
			sumSq += v * v
		}
		norm := math.Sqrt(sumSq)
		if norm > 2.0+1e-9 || math.IsNaN(norm) {
			t.Errorf("Node %s: embedding out of range (GDS sums at most two normalised iterations), L2 norm = %f", nodeID, norm)
		}
	}

	t.Logf("✓ gds.fastRP.stream() generated %d normalized embeddings with dimension 8", len(result.Rows))
}

func TestGdsFastRPStreamDifferentDimensions(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('dim-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('dim-test')", nil)
	}()

	testCases := []int{4, 16, 32, 64, 128, 256}

	for _, dim := range testCases {
		t.Run(fmt.Sprintf("Dimension_%d", dim), func(t *testing.T) {
			query := fmt.Sprintf(`
				CALL gds.fastRP.stream('dim-test', {
					embeddingDimension: %d,
					randomSeed: 42
				})
				YIELD nodeId, embedding
				RETURN nodeId, embedding
			`, dim)

			result, err := exec.Execute(ctx, query, nil)
			if err != nil {
				t.Fatalf("Dimension %d failed: %v", dim, err)
			}

			if len(result.Rows) != 7 {
				t.Errorf("Expected 7 rows, got %d", len(result.Rows))
			}

			for _, row := range result.Rows {
				embedding, ok := row[1].([]float64)
				if !ok {
					t.Errorf("Expected []float64, got %T", row[1])
					continue
				}
				if len(embedding) != dim {
					t.Errorf("Expected dimension %d, got %d", dim, len(embedding))
				}
				var sumSq float64
				for _, v := range embedding {
					sumSq += v * v
				}
				if norm := math.Sqrt(sumSq); norm > 2.0+1e-9 || math.IsNaN(norm) {
					t.Errorf("Embedding out of range (GDS sums at most two normalised iterations), L2 norm = %f", norm)
				}
			}
		})
	}
}

func TestGdsFastRPWithWeights(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('weighted-test', 'Person', {KNOWS: {properties: {weight: {property: 'weight', defaultValue: 1.0}}}})", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('weighted-test')", nil)
	}()

	// Run FastRP with relationship weights
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('weighted-test', {
			embeddingDimension: 16,
			randomSeed: 42,
			relationshipWeightProperty: 'weight'
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("gds.fastRP.stream() with weights failed: %v", err)
	}

	if len(result.Rows) != 7 {
		t.Errorf("Expected 7 embeddings, got: %d", len(result.Rows))
	}

	// All embeddings should be valid
	for _, row := range result.Rows {
		embedding, ok := row[1].([]float64)
		if !ok {
			t.Errorf("Expected []float64, got %T", row[1])
			continue
		}
		if len(embedding) != 16 {
			t.Errorf("Expected dimension 16, got %d", len(embedding))
		}
	}

	t.Logf("✓ gds.fastRP.stream() with relationshipWeightProperty works")
}

func TestGdsFastRPStats(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('stats-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('stats-test')", nil)
	}()

	// Run FastRP stats
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stats('stats-test', {
			embeddingDimension: 32,
			randomSeed: 42
		})
		YIELD nodeCount
		RETURN nodeCount
	`, nil)
	if err != nil {
		t.Fatalf("gds.fastRP.stats() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	nodeCount, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected INTEGER nodeCount, got %T", result.Rows[0][0])
	}
	if nodeCount != 7 {
		t.Errorf("Expected nodeCount 7, got: %d", nodeCount)
	}

	t.Logf("✓ gds.fastRP.stats() returned nodeCount: %d", nodeCount)
}

func TestGdsFastRPNoGraph(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Try FastRP without creating a graph projection first
	_, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('nonexistent', {
			embeddingDimension: 8
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)

	if err == nil {
		t.Error("Expected error when running FastRP on non-existent graph")
	}

	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected 'does not exist' error, got: %v", err)
	}

	t.Logf("✓ FastRP correctly errors for non-existent graph")
}

func TestGdsFastRPStats_ErrorBranches(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Missing graph argument.
	_, err := exec.Execute(ctx, "CALL gds.fastRP.stats()", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "requires at least 1 arguments")

	// Non-existent projected graph for stats.
	_, err = exec.Execute(ctx, "CALL gds.fastRP.stats('missing-graph', {embeddingDimension: 4})", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
}

// ============================================================================
// Embedding Quality and Correctness Tests
// ============================================================================

func TestFastRPEmbeddingDeterminism(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('determinism-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('determinism-test')", nil)
	}()

	query := `
		CALL gds.fastRP.stream('determinism-test', {
			embeddingDimension: 8,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`

	// Run FastRP twice with same seed
	result1, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("First FastRP run failed: %v", err)
	}

	result2, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Second FastRP run failed: %v", err)
	}

	// Build maps for comparison
	embeddings1 := make(map[string][]float64)
	for _, row := range result1.Rows {
		nodeID := row[0].(string)
		embedding := row[1].([]float64)
		embeddings1[nodeID] = embedding
	}

	embeddings2 := make(map[string][]float64)
	for _, row := range result2.Rows {
		nodeID := row[0].(string)
		embedding := row[1].([]float64)
		embeddings2[nodeID] = embedding
	}

	// Compare embeddings
	for nodeID, emb1 := range embeddings1 {
		emb2, ok := embeddings2[nodeID]
		if !ok {
			t.Errorf("Node %s missing from second run", nodeID)
			continue
		}

		for i := range emb1 {
			if math.Abs(emb1[i]-emb2[i]) > 1e-10 {
				t.Errorf("Node %s: embedding mismatch at index %d: %f vs %f", nodeID, i, emb1[i], emb2[i])
			}
		}
	}

	t.Log("✓ FastRP produces deterministic results with same random seed")
}

func TestFastRPDifferentSeedsProduceDifferentEmbeddings(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('seed-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('seed-test')", nil)
	}()

	// Run with seed 42
	result1, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('seed-test', {
			embeddingDimension: 8,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("First run failed: %v", err)
	}

	// Run with seed 123
	result2, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('seed-test', {
			embeddingDimension: 8,
			randomSeed: 123
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("Second run failed: %v", err)
	}

	// Build maps
	embeddings1 := make(map[string][]float64)
	for _, row := range result1.Rows {
		embeddings1[row[0].(string)] = row[1].([]float64)
	}

	embeddings2 := make(map[string][]float64)
	for _, row := range result2.Rows {
		embeddings2[row[0].(string)] = row[1].([]float64)
	}

	// At least some embeddings should be different
	differentCount := 0
	for nodeID, emb1 := range embeddings1 {
		emb2 := embeddings2[nodeID]
		for i := range emb1 {
			if math.Abs(emb1[i]-emb2[i]) > 1e-6 {
				differentCount++
				break
			}
		}
	}

	if differentCount == 0 {
		t.Error("Expected different embeddings with different seeds, but all were identical")
	}

	t.Logf("✓ Different seeds produce different embeddings (%d/%d nodes differ)", differentCount, len(embeddings1))
}

func TestFastRPConnectedNodesSimilarity(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('similarity-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('similarity-test')", nil)
	}()

	// Run FastRP with higher dimension for better quality
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('similarity-test', {
			embeddingDimension: 64,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP failed: %v", err)
	}

	// Build embedding map
	embeddings := make(map[string][]float64)
	for _, row := range result.Rows {
		embeddings[row[0].(string)] = row[1].([]float64)
	}

	// Log similarities for analysis
	// Dan (p1) is connected to Annie (p2) and Matt (p3)
	// John (p7) is only connected to Jeff (p4)
	simDanAnnie := vector.CosineSimilarityFloat64(embeddings["p1"], embeddings["p2"])
	simDanMatt := vector.CosineSimilarityFloat64(embeddings["p1"], embeddings["p3"])
	simDanJohn := vector.CosineSimilarityFloat64(embeddings["p1"], embeddings["p7"])

	t.Logf("Similarity Dan-Annie (connected): %.4f", simDanAnnie)
	t.Logf("Similarity Dan-Matt (connected): %.4f", simDanMatt)
	t.Logf("Similarity Dan-John (not connected): %.4f", simDanJohn)

	// Verify embeddings are valid (normalized)
	for nodeID, emb := range embeddings {
		var sumSq float64
		for _, v := range emb {
			sumSq += v * v
		}
		if norm := math.Sqrt(sumSq); norm > 2.0+1e-9 || math.IsNaN(norm) {
			t.Errorf("Node %s: embedding out of range (GDS sums at most two normalised iterations), norm=%f", nodeID, norm)
		}
	}

	t.Log("✓ FastRP embedding similarity analysis complete")
}

func TestFastRPEmptyGraph(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	// Don't create any nodes - empty graph
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection of empty graph
	_, err := exec.Execute(ctx, "CALL gds.graph.project('empty-test', '*', '*')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('empty-test')", nil)
	}()

	// Run FastRP on empty graph
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('empty-test', {
			embeddingDimension: 8,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP on empty graph failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 embeddings for empty graph, got %d", len(result.Rows))
	}

	t.Log("✓ FastRP handles empty graph correctly")
}

func TestFastRPIsolatedNodes(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	// Create nodes with no edges
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("isolated%d", i)),
			Labels: []string{"Isolated"},
			Properties: map[string]any{
				"index": i,
			},
		}
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	_, err := exec.Execute(ctx, "CALL gds.graph.project('isolated-test', '*', '*')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('isolated-test')", nil)
	}()

	// Run FastRP
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('isolated-test', {
			embeddingDimension: 8,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP failed: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 embeddings, got %d", len(result.Rows))
	}

	// All isolated nodes should still have valid normalized embeddings
	for _, row := range result.Rows {
		embedding := row[1].([]float64)
		var sumSq float64
		for _, v := range embedding {
			sumSq += v * v
		}
		if norm := math.Sqrt(sumSq); norm > 2.0+1e-9 || math.IsNaN(norm) {
			t.Errorf("Isolated node embedding out of range (GDS sums at most two normalised iterations), norm=%f", norm)
		}
	}

	t.Log("✓ FastRP handles isolated nodes correctly")
}

// ============================================================================
// Scale Tests (Correctness at Scale)
// ============================================================================

func TestFastRPMediumGraph(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping medium graph test in short mode")
	}

	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	// Create a medium-sized graph: 500 nodes, avg degree 5
	createLargeGraph(t, engine, 500, 5)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	start := time.Now()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('medium-test', '*', '*')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	projectTime := time.Since(start)
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('medium-test')", nil)
	}()

	// Run FastRP
	start = time.Now()
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('medium-test', {
			embeddingDimension: 64,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP failed: %v", err)
	}
	fastRPTime := time.Since(start)

	if len(result.Rows) != 500 {
		t.Errorf("Expected 500 embeddings, got %d", len(result.Rows))
	}

	// Verify all embeddings are valid
	invalidCount := 0
	for _, row := range result.Rows {
		embedding := row[1].([]float64)
		if len(embedding) != 64 {
			invalidCount++
			continue
		}
		var sumSq float64
		for _, v := range embedding {
			sumSq += v * v
		}
		if norm := math.Sqrt(sumSq); norm > 2.0+1e-9 || math.IsNaN(norm) {
			invalidCount++
		}
	}

	if invalidCount > 0 {
		t.Errorf("%d embeddings were invalid", invalidCount)
	}

	t.Logf("✓ Medium graph (500 nodes): project=%v, fastRP=%v", projectTime, fastRPTime)
}

func TestFastRPLargeGraph(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large graph test in short mode")
	}

	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	// Create a larger graph: 2000 nodes, avg degree 8
	createLargeGraph(t, engine, 2000, 8)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create projection
	start := time.Now()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('large-test', '*', '*')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	projectTime := time.Since(start)
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('large-test')", nil)
	}()

	// Run FastRP
	start = time.Now()
	result, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('large-test', {
			embeddingDimension: 128,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP failed: %v", err)
	}
	fastRPTime := time.Since(start)

	if len(result.Rows) != 2000 {
		t.Errorf("Expected 2000 embeddings, got %d", len(result.Rows))
	}

	// Sample check: verify first 100 embeddings
	invalidCount := 0
	for i := 0; i < 100 && i < len(result.Rows); i++ {
		embedding := result.Rows[i][1].([]float64)
		if len(embedding) != 128 {
			invalidCount++
			continue
		}
		var sumSq float64
		for _, v := range embedding {
			sumSq += v * v
		}
		if norm := math.Sqrt(sumSq); norm > 2.0+1e-9 || math.IsNaN(norm) {
			invalidCount++
		}
	}

	if invalidCount > 0 {
		t.Errorf("%d of first 100 embeddings were invalid", invalidCount)
	}

	t.Logf("✓ Large graph (2000 nodes): project=%v, fastRP=%v", projectTime, fastRPTime)
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestGdsIntegrationWithLinkPrediction(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// First, check that link prediction still works
	result, err := exec.Execute(ctx, `
		CALL gds.linkPrediction.adamicAdar.stream({
			sourceNode: 'p1',
			topK: 5
		})
		YIELD node1, node2, score
		RETURN node1, node2, score
	`, nil)
	if err != nil {
		t.Fatalf("Link prediction failed: %v", err)
	}

	t.Logf("✓ Link prediction returned %d suggestions", len(result.Rows))

	// Now test FastRP
	_, err = exec.Execute(ctx, "CALL gds.graph.project('integration-test', 'Person', 'KNOWS')", nil)
	if err != nil {
		t.Fatalf("Failed to create graph: %v", err)
	}
	defer func() {
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('integration-test')", nil)
	}()

	fastRPResult, err := exec.Execute(ctx, `
		CALL gds.fastRP.stream('integration-test', {
			embeddingDimension: 16,
			randomSeed: 42
		})
		YIELD nodeId, embedding
		RETURN nodeId, embedding
	`, nil)
	if err != nil {
		t.Fatalf("FastRP failed: %v", err)
	}

	if len(fastRPResult.Rows) != 7 {
		t.Errorf("Expected 7 embeddings, got %d", len(fastRPResult.Rows))
	}

	t.Logf("✓ FastRP returned %d embeddings", len(fastRPResult.Rows))
	t.Log("✓ GDS procedures (link prediction + FastRP) work together")
}

func TestMultipleGraphProjections(t *testing.T) {
	engine := setupFastRPTestStorage(t)
	defer engine.Close()

	createSocialNetwork(t, engine)

	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create multiple projections
	graphNames := []string{"graph-a", "graph-b", "graph-c"}
	for _, name := range graphNames {
		_, err := exec.Execute(ctx, fmt.Sprintf("CALL gds.graph.project('%s', 'Person', 'KNOWS')", name), nil)
		if err != nil {
			t.Fatalf("Failed to create %s: %v", name, err)
		}
	}

	// Run FastRP on each
	for _, name := range graphNames {
		result, err := exec.Execute(ctx, fmt.Sprintf(`
			CALL gds.fastRP.stream('%s', {
				embeddingDimension: 8,
				randomSeed: 42
			})
			YIELD nodeId, embedding
			RETURN nodeId, embedding
		`, name), nil)
		if err != nil {
			t.Errorf("FastRP on %s failed: %v", name, err)
			continue
		}
		if len(result.Rows) != 7 {
			t.Errorf("Expected 7 embeddings from %s, got %d", name, len(result.Rows))
		}
	}

	// Clean up
	for _, name := range graphNames {
		_, _ = exec.Execute(ctx, fmt.Sprintf("CALL gds.graph.drop('%s')", name), nil)
	}

	t.Log("✓ Multiple graph projections work correctly")
}

func TestGenerateFastRPEmbeddings_Branches(t *testing.T) {
	empty, err := generateFastRPEmbeddings(&GraphProjection{}, FastRPConfig{EmbeddingDimension: 4, IterationWeights: []float64{1}})
	require.NoError(t, err)
	require.Empty(t, empty)

	proj := &GraphProjection{
		NodeIDs:          []string{"n1", "n2", "n3"},
		Labels:           []string{"P"},
		NodeLabels:       map[string][]string{"n1": {"P"}, "n2": {"P"}, "n3": {"P"}},
		NodeProperties:   map[string]map[string]interface{}{"n1": {"age": 20}, "n2": {"age": 40}, "n3": {"age": 60}},
		NodePropertyKeys: map[string][]string{"P": {"age"}},
		Types:            []string{"K"},
		Relationships: []ProjectedRelationship{
			{ID: "e1", Source: "n1", Target: "n2", Type: "K", Properties: map[string]float64{"weight": 2}},
			{ID: "e1", Source: "n2", Target: "n1", Type: "K", Properties: map[string]float64{"weight": 2}},
		},
		RelationshipPropertyKeys: map[string][]string{"K": {"weight"}},
	}
	cfg := FastRPConfig{
		EmbeddingDimension:         8,
		IterationWeights:           []float64{0.0, 1.0},
		PropertyRatio:              0.25,
		FeatureProperties:          []string{"age"},
		RelationshipWeightProperty: "weight",
		NormalizationStrength:      0.5,
		NodeSelfInfluence:          0.5,
		RandomSeed:                 7,
	}
	out, err := generateFastRPEmbeddings(proj, cfg)
	require.NoError(t, err)
	require.Len(t, out, 3)
	for _, embedding := range out {
		require.Len(t, embedding.vector, 8)
	}
	require.Equal(t, []string{"n1", "n2", "n3"}, []string{out[0].nodeID, out[1].nodeID, out[2].nodeID})

	// Without self-influence, a node nothing propagates to is zero, as in GDS.
	cfg.NodeSelfInfluence = 0
	out, err = generateFastRPEmbeddings(proj, cfg)
	require.NoError(t, err)
	require.Equal(t, make([]float64, 8), out[2].vector)
	require.NotEqual(t, make([]float64, 8), out[0].vector)
}

func TestCallGdsGraphProject_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (p:Person {name:'p'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (u:User {name:'u'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (m:Memory {name:'m'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (p:Person {name:'p'}), (u:User {name:'u'}) CREATE (p)-[:KNOWS]->(u)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (u:User {name:'u'}), (m:Memory {name:'m'}) CREATE (u)-[:RELATES_TO]->(m)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (m:Memory {name:'m'}), (p:Person {name:'p'}) CREATE (m)-[:REFERENCES]->(p)", nil)
	require.NoError(t, err)

	// Missing graph name: the procedure's failure.
	_, err = exec.callGdsGraphProject(nil)
	require.ErrorContains(t, err, "argument graphName is null")

	// A name text listing several labels / types (NornicDB's kept form).
	counts := func(res *ExecuteResult) []any { return res.Rows[0][2:5] }
	res, err := exec.callGdsGraphProject([]interface{}{"g_parse", "Person:User:Memory", "KNOWS|RELATES_TO|REFERENCES"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, []any{"g_parse", int64(3), int64(3)}, counts(res))

	// Lists, maps (an entry's label / type overrides its key) and '*'.
	res, err = exec.callGdsGraphProject([]interface{}{"g_list", []interface{}{"Person", "User"}, map[string]interface{}{"K": map[string]interface{}{"type": "KNOWS"}}})
	require.NoError(t, err)
	assert.Equal(t, []any{"g_list", int64(2), int64(1)}, counts(res))
	res, err = exec.callGdsGraphProject([]interface{}{"g_default", "*", "*"})
	require.NoError(t, err)
	assert.Equal(t, []any{"g_default", int64(3), int64(3)}, counts(res))
	_, err = exec.callGdsGraphProject([]interface{}{"g_bad", int64(1), "*"})
	require.ErrorContains(t, err, "Cannot construct a node projection out of a java.lang.Long")
	_, err = exec.callGdsGraphProject([]interface{}{"g_bad", []interface{}{"Person", int64(1)}, "*"})
	require.ErrorContains(t, err, "Cannot construct a node projection out of a java.lang.Long")

	// Projections bound earlier in the statement.
	res, err = exec.Execute(ctx, "WITH ['Person', 'User'] AS labels, 'KNOWS' AS type CALL gds.graph.project('g_bound', labels, type) YIELD graphName, nodeCount, relationshipCount RETURN graphName, nodeCount, relationshipCount", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{"g_bound", int64(2), int64(1)}}, res.Rows)
}

func TestFastRPConfiguration(t *testing.T) {
	config, err := fastRPConfiguration(map[string]interface{}{"embeddingDimension": int64(4)})
	require.NoError(t, err)
	assert.Equal(t, 4, config.EmbeddingDimension)
	assert.Equal(t, int64(42), config.RandomSeed)
	assert.Equal(t, []float64{0, 1, 1}, config.IterationWeights)
	config, err = fastRPConfiguration(map[string]interface{}{
		"EMBEDDINGDIMENSION": int64(8), "randomSeed": int64(-3), "propertyRatio": 0.5, "featureProperties": []interface{}{"age"},
		"relationshipWeightProperty": "weight", "concurrency": int64(2), "jobId": "j", "logProgress": false, "sudo": false,
		"nodeLabels": "*", "relationshipTypes": []interface{}{"K"},
	})
	require.NoError(t, err)
	assert.Equal(t, 8, config.EmbeddingDimension)
	assert.Equal(t, int64(-3), config.RandomSeed)
	assert.Nil(t, config.NodeLabels)
	assert.Equal(t, []string{"K"}, config.RelationshipTypes)
	assert.Equal(t, "j", config.Configuration["jobId"])
}
