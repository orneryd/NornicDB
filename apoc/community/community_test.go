package community

import (
	"math"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// createTestGraph creates a simple test graph with two clear communities.
// Community 1: nodes 1-4 (clique)
// Community 2: nodes 5-8 (clique)
// One edge connecting the communities (4-5)
func createTestGraph() ([]*Node, []*Relationship) {
	nodes := []*Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}},
		{ID: 4, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Dave"}},
		{ID: 5, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Eve"}},
		{ID: 6, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Frank"}},
		{ID: 7, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Grace"}},
		{ID: 8, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Henry"}},
	}

	// Create two cliques connected by one edge
	rels := []*Relationship{
		// Community 1 (clique 1-2-3-4)
		{ID: 1, Type: "KNOWS", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
		{ID: 2, Type: "KNOWS", StartNode: 1, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 3, Type: "KNOWS", StartNode: 1, EndNode: 4, Properties: map[string]interface{}{}},
		{ID: 4, Type: "KNOWS", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 5, Type: "KNOWS", StartNode: 2, EndNode: 4, Properties: map[string]interface{}{}},
		{ID: 6, Type: "KNOWS", StartNode: 3, EndNode: 4, Properties: map[string]interface{}{}},
		// Community 2 (clique 5-6-7-8)
		{ID: 7, Type: "KNOWS", StartNode: 5, EndNode: 6, Properties: map[string]interface{}{}},
		{ID: 8, Type: "KNOWS", StartNode: 5, EndNode: 7, Properties: map[string]interface{}{}},
		{ID: 9, Type: "KNOWS", StartNode: 5, EndNode: 8, Properties: map[string]interface{}{}},
		{ID: 10, Type: "KNOWS", StartNode: 6, EndNode: 7, Properties: map[string]interface{}{}},
		{ID: 11, Type: "KNOWS", StartNode: 6, EndNode: 8, Properties: map[string]interface{}{}},
		{ID: 12, Type: "KNOWS", StartNode: 7, EndNode: 8, Properties: map[string]interface{}{}},
		// Bridge between communities
		{ID: 13, Type: "KNOWS", StartNode: 4, EndNode: 5, Properties: map[string]interface{}{}},
	}

	return nodes, rels
}

// createTriangleGraph creates a graph with known triangle structure.
func createTriangleGraph() ([]*Node, []*Relationship) {
	nodes := []*Node{
		{ID: 1, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 2, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 3, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 4, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
	}

	// Triangle: 1-2-3-1, and 4 connected only to 1
	rels := []*Relationship{
		{ID: 1, Type: "CONNECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
		{ID: 2, Type: "CONNECTED", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 3, Type: "CONNECTED", StartNode: 1, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 4, Type: "CONNECTED", StartNode: 1, EndNode: 4, Properties: map[string]interface{}{}},
	}

	return nodes, rels
}

// createDisconnectedGraph creates a graph with multiple components.
func createDisconnectedGraph() ([]*Node, []*Relationship) {
	nodes := []*Node{
		{ID: 1, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 2, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 3, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 4, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 5, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
	}

	// Component 1: 1-2-3, Component 2: 4-5
	rels := []*Relationship{
		{ID: 1, Type: "CONNECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
		{ID: 2, Type: "CONNECTED", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 3, Type: "CONNECTED", StartNode: 4, EndNode: 5, Properties: map[string]interface{}{}},
	}

	return nodes, rels
}

// createWeightedGraph creates a graph with weighted edges.
func createWeightedGraph() ([]*Node, []*Relationship) {
	nodes := []*Node{
		{ID: 1, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 2, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 3, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 4, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
	}

	rels := []*Relationship{
		{ID: 1, Type: "CONNECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{"weight": 5.0}},
		{ID: 2, Type: "CONNECTED", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{"weight": 1.0}},
		{ID: 3, Type: "CONNECTED", StartNode: 3, EndNode: 4, Properties: map[string]interface{}{"weight": 5.0}},
		{ID: 4, Type: "CONNECTED", StartNode: 1, EndNode: 4, Properties: map[string]interface{}{"weight": 0.1}},
	}

	return nodes, rels
}

func TestLouvain(t *testing.T) {
	nodes, rels := createTestGraph()
	results := Louvain(nodes, rels, DefaultLouvainConfig())

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}

	// Check that we get at least 2 communities (the two cliques)
	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) < 2 {
		t.Errorf("Expected at least 2 communities, got %d", len(communities))
	}
}

func TestLouvainEmptyGraph(t *testing.T) {
	results := Louvain([]*Node{}, []*Relationship{}, DefaultLouvainConfig())
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty graph, got %d", len(results))
	}
}

func TestLabelPropagation(t *testing.T) {
	nodes, rels := createTestGraph()
	results := LabelPropagation(nodes, rels, 10)

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}

	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) == 0 {
		t.Error("Expected at least 1 community")
	}
}

func TestLabelPropagationEmptyGraph(t *testing.T) {
	results := LabelPropagation([]*Node{}, []*Relationship{}, 10)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty graph, got %d", len(results))
	}
}

func TestModularity(t *testing.T) {
	nodes, rels := createTestGraph()

	// Perfect community assignment
	communityMap := map[int64]int64{
		1: 0, 2: 0, 3: 0, 4: 0, // Community 1
		5: 1, 6: 1, 7: 1, 8: 1, // Community 2
	}

	modularity := Modularity(nodes, rels, communityMap)

	// With two clear communities, modularity should be positive
	if modularity <= 0 {
		t.Errorf("Expected positive modularity for well-separated communities, got %f", modularity)
	}

	// Should be less than 1
	if modularity > 1 {
		t.Errorf("Modularity should be <= 1, got %f", modularity)
	}
}

func TestModularityEmptyGraph(t *testing.T) {
	modularity := Modularity([]*Node{}, []*Relationship{}, map[int64]int64{})
	if modularity != 0 {
		t.Errorf("Expected 0 modularity for empty graph, got %f", modularity)
	}
}

func TestTriangleCount(t *testing.T) {
	nodes, rels := createTriangleGraph()
	results := TriangleCount(nodes, rels)

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	triangleCounts := make(map[int64]int)
	for _, r := range results {
		triangleCounts[r.Node.ID] = r.Triangles
	}

	// Nodes 1, 2, 3 form a triangle (each should count 1)
	// Node 4 is not in any triangle
	if triangleCounts[1] != 1 {
		t.Errorf("Node 1 should have 1 triangle, got %d", triangleCounts[1])
	}
	if triangleCounts[2] != 1 {
		t.Errorf("Node 2 should have 1 triangle, got %d", triangleCounts[2])
	}
	if triangleCounts[3] != 1 {
		t.Errorf("Node 3 should have 1 triangle, got %d", triangleCounts[3])
	}
	if triangleCounts[4] != 0 {
		t.Errorf("Node 4 should have 0 triangles, got %d", triangleCounts[4])
	}
}

func TestTotalTriangles(t *testing.T) {
	nodes, rels := createTriangleGraph()
	total := TotalTriangles(nodes, rels)

	// There's exactly 1 triangle in this graph
	if total != 1 {
		t.Errorf("Expected 1 triangle, got %d", total)
	}
}

func TestClusteringCoefficient(t *testing.T) {
	nodes, rels := createTriangleGraph()
	results := ClusteringCoefficient(nodes, rels)

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	for _, r := range results {
		if r.Coefficient < 0 || r.Coefficient > 1 {
			t.Errorf("Coefficient should be 0-1, got %f for node %d", r.Coefficient, r.Node.ID)
		}
	}
}

func TestAverageClusteringCoefficient(t *testing.T) {
	nodes, rels := createTriangleGraph()
	avg := AverageClusteringCoefficient(nodes, rels)

	if avg < 0 || avg > 1 {
		t.Errorf("Average clustering coefficient should be 0-1, got %f", avg)
	}
}

func TestConnectedComponents(t *testing.T) {
	nodes, rels := createDisconnectedGraph()
	results := ConnectedComponents(nodes, rels)

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	components := make(map[int64][]int64)
	for _, r := range results {
		components[r.ComponentID] = append(components[r.ComponentID], r.Node.ID)
	}

	if len(components) != 2 {
		t.Errorf("Expected 2 components, got %d", len(components))
	}
}

func TestNumComponents(t *testing.T) {
	nodes, rels := createDisconnectedGraph()
	numComponents := NumComponents(nodes, rels)

	if numComponents != 2 {
		t.Errorf("Expected 2 components, got %d", numComponents)
	}
}

func TestStronglyConnectedComponents(t *testing.T) {
	nodes := []*Node{
		{ID: 1, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 2, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 3, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 4, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
	}

	// Cycle: 1->2->3->1, and 4 with no incoming edges
	rels := []*Relationship{
		{ID: 1, Type: "DIRECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
		{ID: 2, Type: "DIRECTED", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 3, Type: "DIRECTED", StartNode: 3, EndNode: 1, Properties: map[string]interface{}{}},
		{ID: 4, Type: "DIRECTED", StartNode: 1, EndNode: 4, Properties: map[string]interface{}{}},
	}

	results := StronglyConnectedComponents(nodes, rels)

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	components := make(map[int64][]int64)
	for _, r := range results {
		components[r.ComponentID] = append(components[r.ComponentID], r.Node.ID)
	}

	if len(components) != 2 {
		t.Errorf("Expected 2 strongly connected components, got %d", len(components))
	}
}

func TestWeaklyConnectedComponents(t *testing.T) {
	nodes, rels := createDisconnectedGraph()
	results := WeaklyConnectedComponents(nodes, rels)

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	components := make(map[int64]bool)
	for _, r := range results {
		components[r.ComponentID] = true
	}

	if len(components) != 2 {
		t.Errorf("Expected 2 weakly connected components, got %d", len(components))
	}
}

func TestKCore(t *testing.T) {
	nodes, rels := createTestGraph()

	// 2-core should include nodes from the cliques
	result := KCore(nodes, rels, 2)

	if len(result) == 0 {
		t.Error("2-core should not be empty for this graph")
	}

	// 4-core should be empty (no node has degree >= 4 within the k-core)
	result4 := KCore(nodes, rels, 5)
	if len(result4) != 0 {
		t.Errorf("Expected 0 nodes in 5-core, got %d", len(result4))
	}
}

func TestKCoreEmptyGraph(t *testing.T) {
	result := KCore([]*Node{}, []*Relationship{}, 1)
	if len(result) != 0 {
		t.Errorf("Expected 0 nodes for empty graph, got %d", len(result))
	}
}

func TestCoreNumber(t *testing.T) {
	nodes, rels := createTriangleGraph()
	results := CoreNumber(nodes, rels)

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	for _, r := range results {
		coreNum := r["coreNumber"].(int)
		node := r["node"].(*storage.Node)
		if coreNum < 0 {
			t.Errorf("Node %d should have non-negative core number, got %d", node.ID, coreNum)
		}
	}
}

func TestConductance(t *testing.T) {
	nodes, rels := createTestGraph()
	community1 := []*Node{nodes[0], nodes[1], nodes[2], nodes[3]}

	conductance := Conductance(nodes, rels, community1)

	if conductance < 0 || conductance > 1 {
		t.Errorf("Conductance should be 0-1, got %f", conductance)
	}

	// With well-separated communities, conductance should be low
	if conductance > 0.5 {
		t.Errorf("Expected low conductance for well-separated community, got %f", conductance)
	}
}

func TestDensity(t *testing.T) {
	// Create a complete graph of 4 nodes
	nodes := []*Node{
		{ID: 1, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 2, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 3, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
		{ID: 4, Labels: []string{"Node"}, Properties: map[string]interface{}{}},
	}

	// Complete graph: 6 edges
	rels := []*Relationship{
		{ID: 1, Type: "CONNECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
		{ID: 2, Type: "CONNECTED", StartNode: 1, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 3, Type: "CONNECTED", StartNode: 1, EndNode: 4, Properties: map[string]interface{}{}},
		{ID: 4, Type: "CONNECTED", StartNode: 2, EndNode: 3, Properties: map[string]interface{}{}},
		{ID: 5, Type: "CONNECTED", StartNode: 2, EndNode: 4, Properties: map[string]interface{}{}},
		{ID: 6, Type: "CONNECTED", StartNode: 3, EndNode: 4, Properties: map[string]interface{}{}},
	}

	density := Density(nodes, rels)

	// Complete graph should have density 1.0
	if math.Abs(density-1.0) > 0.001 {
		t.Errorf("Complete graph should have density 1.0, got %f", density)
	}

	// Sparse graph
	sparseRels := []*Relationship{
		{ID: 1, Type: "CONNECTED", StartNode: 1, EndNode: 2, Properties: map[string]interface{}{}},
	}

	sparseDensity := Density(nodes, sparseRels)
	expectedDensity := 1.0 / 6.0
	if math.Abs(sparseDensity-expectedDensity) > 0.001 {
		t.Errorf("Sparse graph density should be ~0.167, got %f", sparseDensity)
	}
}

func TestFastGreedy(t *testing.T) {
	nodes, rels := createTestGraph()
	results := FastGreedy(nodes, rels)

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}

	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) == 0 {
		t.Error("Expected at least 1 community")
	}
}

func TestSpinGlass(t *testing.T) {
	nodes, rels := createTestGraph()
	results := SpinGlass(nodes, rels, 10, 1.0)

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}

	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) == 0 {
		t.Error("Expected at least 1 community")
	}
}

func TestWalkTrap(t *testing.T) {
	nodes, rels := createTestGraph()
	results := WalkTrap(nodes, rels, 4)

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}

	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) == 0 {
		t.Error("Expected at least 1 community")
	}
}

func TestInfoMap(t *testing.T) {
	nodes, rels := createTestGraph()
	results := InfoMap(nodes, rels, 10)

	if len(results) != 8 {
		t.Errorf("Expected 8 results, got %d", len(results))
	}
}

func TestGetRelWeight(t *testing.T) {
	// Test with no properties
	rel1 := &Relationship{ID: 1, Type: "TEST", Properties: nil}
	if getRelWeight(rel1) != 1.0 {
		t.Error("Expected default weight 1.0 for nil properties")
	}

	// Test with float64 weight
	rel2 := &Relationship{ID: 2, Type: "TEST", Properties: map[string]interface{}{"weight": 5.5}}
	if getRelWeight(rel2) != 5.5 {
		t.Errorf("Expected weight 5.5, got %f", getRelWeight(rel2))
	}

	// Test with int64 weight
	rel3 := &Relationship{ID: 3, Type: "TEST", Properties: map[string]interface{}{"weight": int64(3)}}
	if getRelWeight(rel3) != 3.0 {
		t.Errorf("Expected weight 3.0, got %f", getRelWeight(rel3))
	}

	// Test with int weight
	rel4 := &Relationship{ID: 4, Type: "TEST", Properties: map[string]interface{}{"weight": 7}}
	if getRelWeight(rel4) != 7.0 {
		t.Errorf("Expected weight 7.0, got %f", getRelWeight(rel4))
	}

	// Test with no weight property
	rel5 := &Relationship{ID: 5, Type: "TEST", Properties: map[string]interface{}{"other": "value"}}
	if getRelWeight(rel5) != 1.0 {
		t.Error("Expected default weight 1.0 for missing weight property")
	}
}

func TestLouvainWithWeights(t *testing.T) {
	nodes, rels := createWeightedGraph()
	results := Louvain(nodes, rels, DefaultLouvainConfig())

	if len(results) != 4 {
		t.Errorf("Expected 4 results, got %d", len(results))
	}

	communities := make(map[int64]bool)
	for _, r := range results {
		communities[r.CommunityID] = true
	}

	if len(communities) == 0 {
		t.Error("Expected at least 1 community")
	}
}

func BenchmarkLouvain(b *testing.B) {
	nodes, rels := createTestGraph()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Louvain(nodes, rels, DefaultLouvainConfig())
	}
}

func BenchmarkLabelPropagation(b *testing.B) {
	nodes, rels := createTestGraph()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LabelPropagation(nodes, rels, 10)
	}
}

func BenchmarkTriangleCount(b *testing.B) {
	nodes, rels := createTestGraph()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TriangleCount(nodes, rels)
	}
}

func BenchmarkConnectedComponents(b *testing.B) {
	nodes, rels := createTestGraph()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ConnectedComponents(nodes, rels)
	}
}
