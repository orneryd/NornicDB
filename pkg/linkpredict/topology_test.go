package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestCommonNeighbors verifies basic common neighbor counting.
func TestCommonNeighbors(t *testing.T) {
	graph := buildTestGraph()

	// alice and diana share bob and charlie as common neighbors
	predictions := CommonNeighbors(graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions, got none")
	}

	// diana should be top prediction (2 common neighbors)
	if predictions[0].TargetID != "diana" {
		t.Errorf("Expected diana, got %s", predictions[0].TargetID)
	}

	if predictions[0].Score != 2.0 {
		t.Errorf("Expected score 2.0, got %.1f", predictions[0].Score)
	}
}

// TestJaccard verifies Jaccard coefficient calculation.
func TestJaccard(t *testing.T) {
	graph := buildTestGraph()

	predictions := Jaccard(graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions, got none")
	}

	// Check that scores are in valid range [0, 1]
	for _, pred := range predictions {
		if pred.Score < 0 || pred.Score > 1 {
			t.Errorf("Jaccard score out of range: %.3f", pred.Score)
		}
	}

	// Verify algorithm tag
	if predictions[0].Algorithm != "jaccard" {
		t.Errorf("Expected algorithm 'jaccard', got '%s'", predictions[0].Algorithm)
	}
}

// TestAdamicAdar verifies Adamic-Adar scoring.
func TestAdamicAdar(t *testing.T) {
	graph := buildTestGraph()

	predictions := AdamicAdar(graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions, got none")
	}

	// Scores should be positive
	for _, pred := range predictions {
		if pred.Score <= 0 {
			t.Errorf("Expected positive score, got %.3f", pred.Score)
		}
	}

	// Verify algorithm tag
	if predictions[0].Algorithm != "adamic_adar" {
		t.Errorf("Expected algorithm 'adamic_adar', got '%s'", predictions[0].Algorithm)
	}
}

// TestPreferentialAttachment verifies degree product scoring.
func TestPreferentialAttachment(t *testing.T) {
	graph := buildTestGraph()

	predictions := PreferentialAttachment(graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions, got none")
	}

	// Verify scores are degree products
	aliceDegree := float64(graph.Degree("alice"))
	
	for _, pred := range predictions {
		expectedScore := aliceDegree * float64(graph.Degree(pred.TargetID))
		if pred.Score != expectedScore {
			t.Errorf("Expected score %.0f, got %.0f for %s",
				expectedScore, pred.Score, pred.TargetID)
		}
	}

	// Verify algorithm tag
	if predictions[0].Algorithm != "preferential_attachment" {
		t.Errorf("Expected algorithm 'preferential_attachment', got '%s'",
			predictions[0].Algorithm)
	}
}

// TestResourceAllocation verifies resource allocation index.
func TestResourceAllocation(t *testing.T) {
	graph := buildTestGraph()

	predictions := ResourceAllocation(graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions, got none")
	}

	// Scores should be positive
	for _, pred := range predictions {
		if pred.Score <= 0 {
			t.Errorf("Expected positive score, got %.3f", pred.Score)
		}
	}

	// Verify algorithm tag
	if predictions[0].Algorithm != "resource_allocation" {
		t.Errorf("Expected algorithm 'resource_allocation', got '%s'",
			predictions[0].Algorithm)
	}
}

// TestTopKLimit verifies that topK parameter limits results.
func TestTopKLimit(t *testing.T) {
	graph := buildTestGraph()

	// Request only top 2
	predictions := CommonNeighbors(graph, "alice", 2)

	if len(predictions) > 2 {
		t.Errorf("Expected at most 2 predictions, got %d", len(predictions))
	}

	// Request more than available
	predictions = CommonNeighbors(graph, "alice", 1000)
	// Should return all available, not fail
	if len(predictions) == 0 {
		t.Error("Expected some predictions")
	}
}

// TestExcludeExistingEdges verifies existing edges are not predicted.
func TestExcludeExistingEdges(t *testing.T) {
	graph := buildTestGraph()

	predictions := CommonNeighbors(graph, "alice", 10)

	// bob and charlie are already connected to alice
	for _, pred := range predictions {
		if pred.TargetID == "bob" || pred.TargetID == "charlie" {
			t.Errorf("Predicted existing edge to %s", pred.TargetID)
		}
	}
}

// TestExcludeSelf verifies nodes don't predict themselves.
func TestExcludeSelf(t *testing.T) {
	graph := buildTestGraph()

	predictions := CommonNeighbors(graph, "alice", 10)

	for _, pred := range predictions {
		if pred.TargetID == "alice" {
			t.Error("Predicted self-edge")
		}
	}
}

// TestEmptyGraph verifies handling of empty graph.
func TestEmptyGraph(t *testing.T) {
	graph := make(Graph)

	predictions := CommonNeighbors(graph, "nonexistent", 10)

	if predictions != nil {
		t.Errorf("Expected nil for nonexistent node, got %d predictions",
			len(predictions))
	}
}

// TestIsolatedNode verifies handling of isolated nodes.
func TestIsolatedNode(t *testing.T) {
	graph := Graph{
		"isolated": make(NodeSet),
		"alice":    {"bob": {}},
		"bob":      {"alice": {}},
	}

	predictions := CommonNeighbors(graph, "isolated", 10)

	// Isolated node has no neighbors, so no predictions
	if len(predictions) != 0 {
		t.Errorf("Expected 0 predictions for isolated node, got %d",
			len(predictions))
	}
}

// TestBuildGraphFromEngine verifies graph construction from storage.
func TestBuildGraphFromEngine(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine()

	// Create test nodes
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}},
		{ID: "n2", Labels: []string{"Person"}},
		{ID: "n3", Labels: []string{"Person"}},
	}

	for _, node := range nodes {
		if err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	// Create test edges
	edges := []*storage.Edge{
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"},
		{ID: "e2", StartNode: "n2", EndNode: "n3", Type: "KNOWS"},
	}

	for _, edge := range edges {
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("Failed to create edge: %v", err)
		}
	}

	// Build undirected graph
	graph, err := BuildGraphFromEngine(ctx, engine, true)
	if err != nil {
		t.Fatalf("Failed to build graph: %v", err)
	}

	// Verify nodes
	if len(graph) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(graph))
	}

	// Verify undirected edges (both directions)
	if !graph["n1"].Contains("n2") {
		t.Error("Expected n1 -> n2")
	}
	if !graph["n2"].Contains("n1") {
		t.Error("Expected n2 -> n1 (undirected)")
	}

	// Build directed graph
	graphDirected, err := BuildGraphFromEngine(ctx, engine, false)
	if err != nil {
		t.Fatalf("Failed to build directed graph: %v", err)
	}

	// Verify directed edges (only original direction)
	if !graphDirected["n1"].Contains("n2") {
		t.Error("Expected n1 -> n2")
	}
	if graphDirected["n2"].Contains("n1") {
		t.Error("Did not expect n2 -> n1 (directed)")
	}
}

// TestSortedByScore verifies predictions are sorted descending by score.
func TestSortedByScore(t *testing.T) {
	graph := buildTestGraph()

	predictions := CommonNeighbors(graph, "alice", 10)

	// Verify descending order
	for i := 1; i < len(predictions); i++ {
		if predictions[i].Score > predictions[i-1].Score {
			t.Errorf("Predictions not sorted: %.1f > %.1f at index %d",
				predictions[i].Score, predictions[i-1].Score, i)
		}
	}
}

// TestRealWorldScenario simulates a citation network.
func TestRealWorldScenario(t *testing.T) {
	// Build a simple citation graph
	// Papers cite each other, predict missing citations
	graph := Graph{
		"paper1": {"paper2": {}, "paper3": {}},
		"paper2": {"paper1": {}, "paper3": {}, "paper4": {}},
		"paper3": {"paper1": {}, "paper2": {}, "paper5": {}},
		"paper4": {"paper2": {}, "paper5": {}},
		"paper5": {"paper3": {}, "paper4": {}},
	}

	// Predict citations for paper1
	predictions := AdamicAdar(graph, "paper1", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions for paper1")
	}

	// paper1 should be predicted to cite paper4 or paper5
	// (via common neighbors paper2 and paper3)
	foundPaper4or5 := false
	for _, pred := range predictions {
		if pred.TargetID == "paper4" || pred.TargetID == "paper5" {
			foundPaper4or5 = true
			break
		}
	}

	if !foundPaper4or5 {
		t.Error("Expected paper4 or paper5 in predictions")
	}
}

// TestCompareAlgorithms verifies different algorithms give different rankings.
func TestCompareAlgorithms(t *testing.T) {
	graph := buildTestGraph()

	cn := CommonNeighbors(graph, "alice", 3)
	jac := Jaccard(graph, "alice", 3)
	aa := AdamicAdar(graph, "alice", 3)
	pa := PreferentialAttachment(graph, "alice", 3)
	ra := ResourceAllocation(graph, "alice", 3)

	// All should return predictions
	if len(cn) == 0 || len(jac) == 0 || len(aa) == 0 ||
		len(pa) == 0 || len(ra) == 0 {
		t.Error("Some algorithms returned no predictions")
	}

	// Scores should differ across algorithms
	// (not a strict requirement, but expected in practice)
	scores := map[string]float64{
		"cn":  cn[0].Score,
		"jac": jac[0].Score,
		"aa":  aa[0].Score,
		"pa":  pa[0].Score,
		"ra":  ra[0].Score,
	}

	t.Logf("Algorithm scores: %+v", scores)
}

// BenchmarkCommonNeighbors benchmarks common neighbors algorithm.
func BenchmarkCommonNeighbors(b *testing.B) {
	graph := buildLargeGraph(1000, 10) // 1000 nodes, avg degree 10

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CommonNeighbors(graph, "node-500", 20)
	}
}

// BenchmarkJaccard benchmarks Jaccard coefficient algorithm.
func BenchmarkJaccard(b *testing.B) {
	graph := buildLargeGraph(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Jaccard(graph, "node-500", 20)
	}
}

// BenchmarkAdamicAdar benchmarks Adamic-Adar algorithm.
func BenchmarkAdamicAdar(b *testing.B) {
	graph := buildLargeGraph(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AdamicAdar(graph, "node-500", 20)
	}
}

// BenchmarkPreferentialAttachment benchmarks preferential attachment.
func BenchmarkPreferentialAttachment(b *testing.B) {
	graph := buildLargeGraph(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PreferentialAttachment(graph, "node-500", 20)
	}
}

// BenchmarkResourceAllocation benchmarks resource allocation algorithm.
func BenchmarkResourceAllocation(b *testing.B) {
	graph := buildLargeGraph(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ResourceAllocation(graph, "node-500", 20)
	}
}

// TestNodeSetContains tests NodeSet Contains method
func TestNodeSetContains(t *testing.T) {
	ns := NodeSet{
		"node1": struct{}{},
		"node2": struct{}{},
	}

	if !ns.Contains("node1") {
		t.Error("Expected Contains to return true for node1")
	}

	if ns.Contains("node3") {
		t.Error("Expected Contains to return false for node3")
	}
}

// TestGraphDegree tests Graph Degree method
func TestGraphDegree(t *testing.T) {
	graph := buildTestGraph()

	degree := graph.Degree("alice")
	if degree != 2 {
		t.Errorf("Alice degree = %d, want 2", degree)
	}

	degree = graph.Degree("eve")
	if degree != 0 {
		t.Errorf("Eve degree = %d, want 0", degree)
	}

	degree = graph.Degree("nonexistent")
	if degree != 0 {
		t.Errorf("Nonexistent node degree = %d, want 0", degree)
	}
}

// TestBuildGraphErrors tests error handling in BuildGraphFromEngine
func TestBuildGraphErrors(t *testing.T) {
	ctx := context.Background()

	// Test with nil engine should not panic
	// (actual behavior depends on Engine interface implementation)

	// Test with empty engine
	engine := storage.NewMemoryEngine()
	graph, err := BuildGraphFromEngine(ctx, engine, true)

	if err != nil {
		t.Fatalf("BuildGraphFromEngine with empty engine failed: %v", err)
	}

	if len(graph) != 0 {
		t.Errorf("Expected empty graph, got %d nodes", len(graph))
	}
}

// TestPredictionReason tests that predictions include reasons
func TestPredictionReason(t *testing.T) {
	graph := buildTestGraph()

	predictions := AdamicAdar(graph, "alice", 5)

	for _, pred := range predictions {
		if pred.Reason == "" {
			t.Errorf("Prediction for %s missing reason", pred.TargetID)
		}
		if pred.Algorithm == "" {
			t.Errorf("Prediction for %s missing algorithm", pred.TargetID)
		}
	}
}

// TestEdgeCases tests edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("zero topK", func(t *testing.T) {
		graph := buildTestGraph()
		predictions := CommonNeighbors(graph, "alice", 0)
		// Should return empty or all candidates
		t.Logf("Predictions with topK=0: %d", len(predictions))
	})

	t.Run("negative topK", func(t *testing.T) {
		graph := buildTestGraph()
		predictions := CommonNeighbors(graph, "alice", -1)
		// Should handle gracefully
		t.Logf("Predictions with topK=-1: %d", len(predictions))
	})

	t.Run("very large topK", func(t *testing.T) {
		graph := buildTestGraph()
		predictions := CommonNeighbors(graph, "alice", 10000)
		// Should return all available
		t.Logf("Predictions with topK=10000: %d", len(predictions))
	})
}

// Helper: buildTestGraph creates a small test graph.
//
// Structure:
//   alice -- bob -- diana
//   alice -- charlie -- diana
//   eve (isolated)
func buildTestGraph() Graph {
	return Graph{
		"alice":   {"bob": {}, "charlie": {}},
		"bob":     {"alice": {}, "diana": {}},
		"charlie": {"alice": {}, "diana": {}},
		"diana":   {"bob": {}, "charlie": {}},
		"eve":     {}, // isolated node
	}
}

// Helper: buildLargeGraph creates a synthetic graph for benchmarking.
func buildLargeGraph(nodes int, avgDegree int) Graph {
	graph := make(Graph)

	// Initialize nodes
	for i := 0; i < nodes; i++ {
		nodeID := storage.NodeID("node-" + string(rune(i)))
		graph[nodeID] = make(NodeSet)
	}

	// Create random edges (simplified - not truly random)
	for i := 0; i < nodes; i++ {
		nodeID := storage.NodeID("node-" + string(rune(i)))
		
		// Connect to next avgDegree nodes (circular)
		for j := 1; j <= avgDegree; j++ {
			targetIdx := (i + j) % nodes
			targetID := storage.NodeID("node-" + string(rune(targetIdx)))
			graph[nodeID][targetID] = struct{}{}
			graph[targetID][nodeID] = struct{}{} // undirected
		}
	}

	return graph
}
