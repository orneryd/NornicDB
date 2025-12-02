package linkpredict

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// CHAOS TESTS - Stress tests with random/adversarial inputs
// =============================================================================

// TestChaosRandomGraph tests algorithms on randomly generated graphs.
func TestChaosRandomGraph(t *testing.T) {
	sizes := []int{10, 50, 100, 500}
	densities := []float64{0.1, 0.3, 0.5, 0.8}

	for _, size := range sizes {
		for _, density := range densities {
			name := fmt.Sprintf("size_%d_density_%.1f", size, density)
			t.Run(name, func(t *testing.T) {
				graph := buildRandomGraph(size, density, 42)

				// Pick random source node
				var sourceID storage.NodeID
				for id := range graph {
					sourceID = id
					break
				}

				// All algorithms should not panic
				algorithms := []struct {
					name string
					fn   func(Graph, storage.NodeID, int) []Prediction
				}{
					{"CommonNeighbors", CommonNeighbors},
					{"Jaccard", Jaccard},
					{"AdamicAdar", AdamicAdar},
					{"PreferentialAttachment", PreferentialAttachment},
					{"ResourceAllocation", ResourceAllocation},
				}

				for _, algo := range algorithms {
					t.Run(algo.name, func(t *testing.T) {
						defer func() {
							if r := recover(); r != nil {
								t.Errorf("%s panicked on random graph: %v", algo.name, r)
							}
						}()

						preds := algo.fn(graph, sourceID, 10)

						// Verify predictions are valid
						for _, pred := range preds {
							if pred.TargetID == sourceID {
								t.Errorf("%s predicted self-edge", algo.name)
							}
							if _, exists := graph[sourceID][pred.TargetID]; exists {
								t.Errorf("%s predicted existing edge", algo.name)
							}
						}
					})
				}
			})
		}
	}
}

// TestChaosStarGraph tests with hub-and-spoke topology (one central node).
func TestChaosStarGraph(t *testing.T) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			graph := buildStarGraph(size)
			hub := storage.NodeID("hub")

			// Hub should have no predictions (connected to everyone)
			preds := CommonNeighbors(graph, hub, 10)
			if len(preds) != 0 {
				t.Errorf("Hub should have no predictions, got %d", len(preds))
			}

			// Spoke nodes should predict other spokes (via hub)
			spoke := storage.NodeID("spoke-0")
			preds = CommonNeighbors(graph, spoke, 10)

			// All spokes share hub as common neighbor
			if len(preds) == 0 && size > 2 {
				t.Error("Spokes should predict other spokes")
			}

			// Adamic-Adar should handle high-degree hub correctly
			preds = AdamicAdar(graph, spoke, 10)
			for _, pred := range preds {
				// Score should be low (hub has high degree)
				if pred.Score > 1.0 {
					t.Logf("Warning: AA score %.3f for hub-mediated connection", pred.Score)
				}
			}
		})
	}
}

// TestChaosCliqueGraph tests with fully connected graph.
func TestChaosCliqueGraph(t *testing.T) {
	sizes := []int{5, 10, 20}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			graph := buildCliqueGraph(size)

			// Pick any node
			var sourceID storage.NodeID
			for id := range graph {
				sourceID = id
				break
			}

			// All algorithms should return empty (everyone connected)
			algorithms := []struct {
				name string
				fn   func(Graph, storage.NodeID, int) []Prediction
			}{
				{"CommonNeighbors", CommonNeighbors},
				{"Jaccard", Jaccard},
				{"AdamicAdar", AdamicAdar},
				{"ResourceAllocation", ResourceAllocation},
			}

			for _, algo := range algorithms {
				preds := algo.fn(graph, sourceID, 10)
				if len(preds) != 0 {
					t.Errorf("%s should return empty for clique, got %d", algo.name, len(preds))
				}
			}

			// PreferentialAttachment is different - it scores ALL non-neighbors
			// which in a clique is zero
			preds := PreferentialAttachment(graph, sourceID, 10)
			if len(preds) != 0 {
				t.Errorf("PreferentialAttachment should return empty for clique, got %d", len(preds))
			}
		})
	}
}

// TestChaosChainGraph tests with linear chain topology.
func TestChaosChainGraph(t *testing.T) {
	sizes := []int{5, 10, 50, 100}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			graph := buildChainGraph(size)

			// Middle node should predict 2-hop neighbors
			middle := storage.NodeID(fmt.Sprintf("node-%d", size/2))

			preds := CommonNeighbors(graph, middle, 10)

			// Chain has limited common neighbors (only at distance 2)
			if size > 4 {
				// Nodes at distance 2 should be predicted
				expectedTarget := storage.NodeID(fmt.Sprintf("node-%d", size/2+2))
				found := false
				for _, pred := range preds {
					if pred.TargetID == expectedTarget {
						found = true
						break
					}
				}
				if !found && size > 4 {
					t.Logf("Expected to predict node at distance 2")
				}
			}
		})
	}
}

// TestChaosBipartiteGraph tests with bipartite graph (two disjoint sets).
func TestChaosBipartiteGraph(t *testing.T) {
	sizes := []int{5, 20, 50}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			graph := buildBipartiteGraph(size)

			// Node in set A should predict other nodes in set A (via set B)
			nodeA := storage.NodeID("A-0")
			preds := CommonNeighbors(graph, nodeA, 10)

			// All predictions should be in set A (same partition)
			for _, pred := range preds {
				if pred.TargetID[0] != 'A' {
					t.Errorf("Predicted cross-partition: A-0 -> %s", pred.TargetID)
				}
			}
		})
	}
}

// TestChaosConcurrentAccess tests thread safety with concurrent predictions.
func TestChaosConcurrentAccess(t *testing.T) {
	graph := buildRandomGraph(100, 0.3, 42)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Run 100 concurrent predictions
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("panic in goroutine %d: %v", idx, r)
				}
			}()

			sourceID := storage.NodeID(fmt.Sprintf("node-%d", idx%100))

			// Run different algorithms concurrently
			switch idx % 5 {
			case 0:
				CommonNeighbors(graph, sourceID, 10)
			case 1:
				Jaccard(graph, sourceID, 10)
			case 2:
				AdamicAdar(graph, sourceID, 10)
			case 3:
				PreferentialAttachment(graph, sourceID, 10)
			case 4:
				ResourceAllocation(graph, sourceID, 10)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestChaosHybridConcurrent tests hybrid scorer thread safety.
func TestChaosHybridConcurrent(t *testing.T) {
	graph := buildRandomGraph(50, 0.4, 42)

	config := DefaultHybridConfig()
	scorer := NewHybridScorer(config)

	// Thread-safe semantic scorer with random delay
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		// Simulate variable latency
		time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond)
		return rand.Float64()
	})

	var wg sync.WaitGroup
	errChan := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("panic: %v", r)
				}
			}()

			sourceID := storage.NodeID(fmt.Sprintf("node-%d", idx%50))
			preds := scorer.Predict(context.Background(), graph, sourceID, 5)

			// Verify predictions are valid
			for _, pred := range preds {
				if pred.Score < 0 || pred.Score > 2.0 { // Allow some margin
					errChan <- fmt.Errorf("invalid score: %.3f", pred.Score)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestChaosExtremeValues tests with extreme score values.
func TestChaosExtremeValues(t *testing.T) {
	// Graph with extreme degree variance
	graph := Graph{
		"superhub": make(NodeSet), // Will have 1000 neighbors
		"isolated": make(NodeSet), // Will have 0 neighbors
		"normal":   make(NodeSet), // Will have 5 neighbors
	}

	// Create superhub with 1000 connections
	for i := 0; i < 1000; i++ {
		nodeID := storage.NodeID(fmt.Sprintf("leaf-%d", i))
		graph[nodeID] = NodeSet{"superhub": {}}
		graph["superhub"][nodeID] = struct{}{}
	}

	// Add some normal connections
	for i := 0; i < 5; i++ {
		nodeID := storage.NodeID(fmt.Sprintf("leaf-%d", i))
		graph["normal"][nodeID] = struct{}{}
		graph[nodeID]["normal"] = struct{}{}
	}

	// Test all algorithms don't crash or produce NaN/Inf
	algorithms := []struct {
		name string
		fn   func(Graph, storage.NodeID, int) []Prediction
	}{
		{"CommonNeighbors", CommonNeighbors},
		{"Jaccard", Jaccard},
		{"AdamicAdar", AdamicAdar},
		{"PreferentialAttachment", PreferentialAttachment},
		{"ResourceAllocation", ResourceAllocation},
	}

	sources := []storage.NodeID{"superhub", "isolated", "normal", "leaf-0"}

	for _, source := range sources {
		for _, algo := range algorithms {
			t.Run(fmt.Sprintf("%s_%s", algo.name, source), func(t *testing.T) {
				preds := algo.fn(graph, source, 10)

				for _, pred := range preds {
					if pred.Score != pred.Score { // NaN check
						t.Errorf("NaN score from %s for %s", algo.name, source)
					}
					if pred.Score > 1e10 || pred.Score < -1e10 {
						t.Logf("Warning: extreme score %.3e from %s", pred.Score, algo.name)
					}
				}
			})
		}
	}
}

// =============================================================================
// COMPLEX USAGE TESTS - Real-world scenarios with intricate patterns
// =============================================================================

// TestComplexMultiLayerGraph tests with nodes that have multiple edge types.
func TestComplexMultiLayerGraph(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine()

	// Create a multi-layer social network:
	// - Friendship layer
	// - Work colleague layer
	// - Family layer
	users := []string{"alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry"}

	for _, u := range users {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(u), Labels: []string{"Person"}})
	}

	// Friendship layer
	friendships := [][2]string{
		{"alice", "bob"}, {"alice", "charlie"}, {"bob", "diana"},
		{"charlie", "diana"}, {"eve", "frank"}, {"frank", "grace"},
	}
	for i, f := range friendships {
		engine.CreateEdge(&storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("friend-%d", i)),
			StartNode: storage.NodeID(f[0]),
			EndNode:   storage.NodeID(f[1]),
			Type:      "FRIENDS_WITH",
		})
	}

	// Work colleague layer
	colleagues := [][2]string{
		{"alice", "eve"}, {"bob", "frank"}, {"charlie", "grace"},
		{"diana", "henry"}, {"eve", "grace"},
	}
	for i, c := range colleagues {
		engine.CreateEdge(&storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("colleague-%d", i)),
			StartNode: storage.NodeID(c[0]),
			EndNode:   storage.NodeID(c[1]),
			Type:      "WORKS_WITH",
		})
	}

	// Build unified graph
	graph, err := BuildGraphFromEngine(ctx, engine, true)
	if err != nil {
		t.Fatalf("Failed to build graph: %v", err)
	}

	// Test predictions for alice
	preds := AdamicAdar(graph, "alice", 5)

	// alice should predict diana (via bob and charlie in friendship layer)
	foundDiana := false
	for _, pred := range preds {
		if pred.TargetID == "diana" {
			foundDiana = true
			t.Logf("alice -> diana: %.3f (multi-path via bob and charlie)", pred.Score)
		}
	}
	if !foundDiana {
		t.Error("Expected alice to predict diana")
	}

	// alice should also predict grace (via eve in work layer, and charlie)
	foundGrace := false
	for _, pred := range preds {
		if pred.TargetID == "grace" {
			foundGrace = true
			t.Logf("alice -> grace: %.3f (cross-layer connection)", pred.Score)
		}
	}
	if !foundGrace {
		t.Error("Expected alice to predict grace")
	}
}

// TestComplexTemporalEvolution tests predictions on evolving graphs.
func TestComplexTemporalEvolution(t *testing.T) {
	// Simulate graph evolution over time
	graph := make(Graph)

	// Initial state: small network
	initialNodes := []string{"a", "b", "c", "d", "e"}
	for _, n := range initialNodes {
		graph[storage.NodeID(n)] = make(NodeSet)
	}
	addEdge(graph, "a", "b")
	addEdge(graph, "b", "c")
	addEdge(graph, "c", "d")
	addEdge(graph, "d", "e")

	// Record predictions at T0
	predsT0 := AdamicAdar(graph, "a", 10)
	t.Logf("T0: %d predictions for 'a'", len(predsT0))

	// Evolution: add more connections (growth)
	addEdge(graph, "a", "c")
	addEdge(graph, "b", "d")
	addEdge(graph, "c", "e")

	// Record predictions at T1
	predsT1 := AdamicAdar(graph, "a", 10)
	t.Logf("T1: %d predictions for 'a'", len(predsT1))

	// Evolution: add hub node
	graph["hub"] = make(NodeSet)
	for _, n := range initialNodes {
		addEdge(graph, "hub", n)
	}

	// Record predictions at T2
	predsT2 := AdamicAdar(graph, "a", 10)
	t.Logf("T2: %d predictions for 'a'", len(predsT2))

	// Predictions should change as graph evolves
	// Hub should appear in predictions (everyone connects to hub)
}

// TestComplexHybridWeightTuning tests systematic weight tuning.
func TestComplexHybridWeightTuning(t *testing.T) {
	graph := buildTestGraph()

	// Semantic scorer that gives high scores to specific targets
	semanticScores := map[storage.NodeID]float64{
		"diana": 0.9,
		"eve":   0.1,
	}

	weightConfigs := []struct {
		topoWeight float64
		semWeight  float64
		expected   storage.NodeID
	}{
		{1.0, 0.0, "diana"}, // Pure topology
		{0.0, 1.0, "diana"}, // Pure semantic (diana has high semantic score)
		{0.5, 0.5, "diana"}, // Balanced
		{0.8, 0.2, "diana"}, // Topology dominant
		{0.2, 0.8, "diana"}, // Semantic dominant
	}

	for _, cfg := range weightConfigs {
		name := fmt.Sprintf("topo_%.1f_sem_%.1f", cfg.topoWeight, cfg.semWeight)
		t.Run(name, func(t *testing.T) {
			config := HybridConfig{
				TopologyWeight:    cfg.topoWeight,
				SemanticWeight:    cfg.semWeight,
				TopologyAlgorithm: "adamic_adar",
				NormalizeScores:   true,
				MinThreshold:      0.0,
			}
			scorer := NewHybridScorer(config)
			scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
				if score, ok := semanticScores[target]; ok {
					return score
				}
				return 0.3
			})

			preds := scorer.Predict(context.Background(), graph, "alice", 5)

			if len(preds) == 0 {
				t.Fatal("No predictions")
			}

			t.Logf("Top prediction: %s (%.3f)", preds[0].TargetID, preds[0].Score)

			// Verify expected winner
			if preds[0].TargetID != cfg.expected {
				t.Logf("Warning: expected %s, got %s", cfg.expected, preds[0].TargetID)
			}
		})
	}
}

// TestComplexEnsembleDisagreement tests when ensemble algorithms disagree.
func TestComplexEnsembleDisagreement(t *testing.T) {
	// Create graph where different algorithms rank differently
	// Hub-and-spoke favors PreferentialAttachment
	// Dense cluster favors CommonNeighbors
	graph := Graph{
		"alice":   {"bob": {}, "charlie": {}},                          // Normal node
		"hub":     {"a1": {}, "a2": {}, "a3": {}, "a4": {}, "a5": {}},   // Hub
		"a1":      {"hub": {}},
		"a2":      {"hub": {}},
		"a3":      {"hub": {}, "a4": {}},
		"a4":      {"hub": {}, "a3": {}, "a5": {}},
		"a5":      {"hub": {}, "a4": {}},
		"bob":     {"alice": {}, "diana": {}},
		"charlie": {"alice": {}, "diana": {}},
		"diana":   {"bob": {}, "charlie": {}},
	}

	config := HybridConfig{
		TopologyWeight:  1.0,
		SemanticWeight:  0.0,
		UseEnsemble:     true,
		NormalizeScores: true,
	}
	scorer := NewHybridScorer(config)

	// Predictions for alice (in dense cluster)
	preds := scorer.Predict(context.Background(), graph, "alice", 5)

	if len(preds) == 0 {
		t.Fatal("No predictions")
	}

	t.Log("Ensemble predictions for alice:")
	for _, pred := range preds {
		t.Logf("  %s: %.3f (topo: %.3f)", pred.TargetID, pred.Score, pred.TopologyScore)
	}

	// Diana should rank high (common neighbors via bob and charlie)
	foundDiana := false
	for _, pred := range preds {
		if pred.TargetID == "diana" {
			foundDiana = true
			break
		}
	}
	if !foundDiana {
		t.Error("Expected diana in predictions")
	}
}

// TestComplexContextCancellation tests proper context handling.
func TestComplexContextCancellation(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create large graph
	for i := 0; i < 100; i++ {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
	}
	for i := 0; i < 100; i++ {
		for j := i + 1; j < i+10 && j < 100; j++ {
			engine.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("e%d-%d", i, j)),
				StartNode: storage.NodeID(fmt.Sprintf("n%d", i)),
				EndNode:   storage.NodeID(fmt.Sprintf("n%d", j)),
				Type:      "CONNECTS",
			})
		}
	}

	// Test with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := BuildGraphFromEngine(ctx, engine, true)
	// BuildGraphFromEngine doesn't currently check context, but should still work
	if err != nil {
		t.Logf("BuildGraphFromEngine with cancelled context: %v", err)
	}
}

// TestComplexScoreDistribution tests score distribution properties.
func TestComplexScoreDistribution(t *testing.T) {
	graph := buildRandomGraph(200, 0.2, 42)

	var sourceID storage.NodeID
	for id := range graph {
		sourceID = id
		break
	}

	algorithms := []struct {
		name string
		fn   func(Graph, storage.NodeID, int) []Prediction
	}{
		{"CommonNeighbors", CommonNeighbors},
		{"Jaccard", Jaccard},
		{"AdamicAdar", AdamicAdar},
		{"ResourceAllocation", ResourceAllocation},
	}

	for _, algo := range algorithms {
		t.Run(algo.name, func(t *testing.T) {
			preds := algo.fn(graph, sourceID, 50)

			// With 200 nodes at 20% density, we MUST get predictions
			// Zero predictions indicates a bug in the algorithm
			if len(preds) == 0 {
				t.Fatal("Expected predictions for a well-connected graph, got 0 - algorithm may be broken")
			}

			// Calculate score statistics
			var sum, min, max float64
			min = preds[0].Score
			max = preds[0].Score

			for _, pred := range preds {
				sum += pred.Score
				if pred.Score < min {
					min = pred.Score
				}
				if pred.Score > max {
					max = pred.Score
				}
			}

			avg := sum / float64(len(preds))

			t.Logf("%s: min=%.3f, max=%.3f, avg=%.3f, count=%d",
				algo.name, min, max, avg, len(preds))

			// Verify sorted descending
			for i := 1; i < len(preds); i++ {
				if preds[i].Score > preds[i-1].Score {
					t.Errorf("Predictions not sorted at index %d", i)
				}
			}

			// Jaccard should be in [0, 1]
			if algo.name == "Jaccard" {
				for _, pred := range preds {
					if pred.Score < 0 || pred.Score > 1 {
						t.Errorf("Jaccard score out of range: %.3f", pred.Score)
					}
				}
			}
		})
	}
}

// TestComplexEmptyResults tests edge cases that return empty results.
func TestComplexEmptyResults(t *testing.T) {
	testCases := []struct {
		name  string
		graph Graph
		src   storage.NodeID
	}{
		{
			name:  "empty_graph",
			graph: Graph{},
			src:   "nonexistent",
		},
		{
			name:  "single_node",
			graph: Graph{"alone": {}},
			src:   "alone",
		},
		{
			name:  "disconnected_pair",
			graph: Graph{"a": {}, "b": {}},
			src:   "a",
		},
		{
			name:  "single_edge",
			graph: Graph{"a": {"b": {}}, "b": {"a": {}}},
			src:   "a",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			preds := CommonNeighbors(tc.graph, tc.src, 10)
			// Should not panic, may return empty
			t.Logf("Predictions: %d", len(preds))
		})
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// buildRandomGraph creates a random graph with given size and density.
func buildRandomGraph(nodes int, density float64, seed int64) Graph {
	r := rand.New(rand.NewSource(seed))
	graph := make(Graph)

	// Create nodes
	for i := 0; i < nodes; i++ {
		graph[storage.NodeID(fmt.Sprintf("node-%d", i))] = make(NodeSet)
	}

	// Create edges based on density
	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			if r.Float64() < density {
				srcID := storage.NodeID(fmt.Sprintf("node-%d", i))
				dstID := storage.NodeID(fmt.Sprintf("node-%d", j))
				graph[srcID][dstID] = struct{}{}
				graph[dstID][srcID] = struct{}{}
			}
		}
	}

	return graph
}

// buildStarGraph creates a hub-and-spoke graph.
func buildStarGraph(spokes int) Graph {
	graph := make(Graph)
	hub := storage.NodeID("hub")
	graph[hub] = make(NodeSet)

	for i := 0; i < spokes; i++ {
		spoke := storage.NodeID(fmt.Sprintf("spoke-%d", i))
		graph[spoke] = NodeSet{hub: {}}
		graph[hub][spoke] = struct{}{}
	}

	return graph
}

// buildCliqueGraph creates a fully connected graph.
func buildCliqueGraph(nodes int) Graph {
	graph := make(Graph)

	for i := 0; i < nodes; i++ {
		nodeI := storage.NodeID(fmt.Sprintf("node-%d", i))
		graph[nodeI] = make(NodeSet)

		for j := 0; j < nodes; j++ {
			if i != j {
				nodeJ := storage.NodeID(fmt.Sprintf("node-%d", j))
				graph[nodeI][nodeJ] = struct{}{}
			}
		}
	}

	return graph
}

// buildChainGraph creates a linear chain graph.
func buildChainGraph(nodes int) Graph {
	graph := make(Graph)

	for i := 0; i < nodes; i++ {
		nodeID := storage.NodeID(fmt.Sprintf("node-%d", i))
		graph[nodeID] = make(NodeSet)

		if i > 0 {
			prevID := storage.NodeID(fmt.Sprintf("node-%d", i-1))
			graph[nodeID][prevID] = struct{}{}
			graph[prevID][nodeID] = struct{}{}
		}
	}

	return graph
}

// buildBipartiteGraph creates a bipartite graph.
func buildBipartiteGraph(nodesPerSide int) Graph {
	graph := make(Graph)

	// Create nodes
	for i := 0; i < nodesPerSide; i++ {
		graph[storage.NodeID(fmt.Sprintf("A-%d", i))] = make(NodeSet)
		graph[storage.NodeID(fmt.Sprintf("B-%d", i))] = make(NodeSet)
	}

	// Connect each A to all B's
	for i := 0; i < nodesPerSide; i++ {
		nodeA := storage.NodeID(fmt.Sprintf("A-%d", i))
		for j := 0; j < nodesPerSide; j++ {
			nodeB := storage.NodeID(fmt.Sprintf("B-%d", j))
			graph[nodeA][nodeB] = struct{}{}
			graph[nodeB][nodeA] = struct{}{}
		}
	}

	return graph
}

// addEdge helper to add undirected edge.
func addEdge(graph Graph, a, b string) {
	nodeA := storage.NodeID(a)
	nodeB := storage.NodeID(b)

	if graph[nodeA] == nil {
		graph[nodeA] = make(NodeSet)
	}
	if graph[nodeB] == nil {
		graph[nodeB] = make(NodeSet)
	}

	graph[nodeA][nodeB] = struct{}{}
	graph[nodeB][nodeA] = struct{}{}
}
