package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestLinkPredictionConfigFromArguments: the configuration map is the
// call's evaluated MAP argument, alone or after a graph name; sourceNode is
// a node, its id or an INTEGER id.
func TestLinkPredictionConfigFromArguments(t *testing.T) {
	node := &storage.Node{ID: "node-123"}
	for _, tc := range []struct {
		name     string
		args     []interface{}
		wantNode string
		wantTopK int
		wantErr  bool
	}{
		{"map alone", []interface{}{map[string]interface{}{"sourceNode": "node-123", "topK": int64(10)}}, "node-123", 10, false},
		{"after a graph name", []interface{}{"g", map[string]interface{}{"sourceNode": "node-456", "topK": int64(5)}}, "node-456", 5, false},
		{"default topK", []interface{}{map[string]interface{}{"sourceNode": "node-789"}}, "node-789", 10, false},
		{"a node", []interface{}{map[string]interface{}{"sourceNode": node, "topK": 3.0}}, "node-123", 3, false},
		{"an integer id", []interface{}{map[string]interface{}{"sourceNode": int64(42)}}, "42", 10, false},
		{"missing sourceNode", []interface{}{map[string]interface{}{"topK": int64(10)}}, "", 0, true},
		{"no config", nil, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := linkPredictionConfigFromArguments(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(config.SourceNode) != tc.wantNode || config.TopK != tc.wantTopK {
				t.Fatalf("got source %q topK %d, want %q %d", config.SourceNode, config.TopK, tc.wantNode, tc.wantTopK)
			}
		})
	}

	config, err := linkPredictionConfigFromArguments([]interface{}{map[string]interface{}{
		"sourceNode": "seed", "topK": "bad", "algorithm": "jaccard", "topologyWeight": 0.7, "semanticWeight": 0.3, "minThreshold": 0.2,
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.TopK != 10 || config.Algorithm != "jaccard" || config.TopologyWeight != 0.7 || config.SemanticWeight != 0.3 || config.MinThreshold != 0.2 {
		t.Fatalf("config read incorrectly: %+v", config)
	}
}

// TestGdsLinkPredictionAdamicAdar tests Adamic-Adar procedure
func TestGdsLinkPredictionAdamicAdar(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	ctx := context.Background()

	cypher := []interface{}{map[string]interface{}{"sourceNode": "alice", "topK": int64(5)}}
	result, err := executor.callGdsLinkPredictionAdamicAdar(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionAdamicAdar() error = %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Check columns
	expectedCols := []string{"node1", "node2", "score"}
	if len(result.Columns) != len(expectedCols) {
		t.Errorf("Columns = %v, want %v", result.Columns, expectedCols)
	}

	// Should have predictions
	if len(result.Rows) == 0 {
		t.Error("Expected predictions, got none")
	}

	// Check result format
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != 3 {
			t.Errorf("Row length = %d, want 3", len(row))
		}

		// node1 should be alice
		if row[0] != "alice" {
			t.Errorf("node1 = %v, want alice", row[0])
		}

		// score should be float64
		if _, ok := row[2].(float64); !ok {
			t.Errorf("score type = %T, want float64", row[2])
		}
	}
}

// TestGdsLinkPredictionCommonNeighbors tests Common Neighbors procedure
func TestGdsLinkPredictionCommonNeighbors(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	ctx := context.Background()

	cypher := []interface{}{map[string]interface{}{"sourceNode": "alice", "topK": int64(5)}}
	result, err := executor.callGdsLinkPredictionCommonNeighbors(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionCommonNeighbors() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionResourceAllocation tests Resource Allocation procedure
func TestGdsLinkPredictionResourceAllocation(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	ctx := context.Background()
	cypher := []interface{}{map[string]interface{}{"sourceNode": "alice", "topK": int64(5)}}
	result, err := executor.callGdsLinkPredictionResourceAllocation(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionResourceAllocation() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionPreferentialAttachment tests Preferential Attachment procedure
func TestGdsLinkPredictionPreferentialAttachment(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	ctx := context.Background()
	cypher := []interface{}{map[string]interface{}{"sourceNode": "alice", "topK": int64(5)}}
	result, err := executor.callGdsLinkPredictionPreferentialAttachment(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionPreferentialAttachment() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionJaccard tests Jaccard procedure
func TestGdsLinkPredictionJaccard(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	ctx := context.Background()
	cypher := []interface{}{map[string]interface{}{"sourceNode": "alice", "topK": int64(5)}}
	result, err := executor.callGdsLinkPredictionJaccard(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionJaccard() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionPredict tests hybrid prediction procedure
func TestGdsLinkPredictionPredict(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	setupTestGraph(t, engine)

	// Add embeddings
	context.Background()
	for _, nodeID := range []storage.NodeID{"alice", "bob", "charlie", "diana"} {
		node, _ := engine.GetNode(nodeID)
		if node != nil {
			node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3, 0.4}}
			engine.UpdateNode(node)
		}
	}

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := []interface{}{map[string]interface{}{
		"sourceNode": "alice", "topK": int64(5), "algorithm": "adamic_adar", "topologyWeight": 0.6, "semanticWeight": 0.4,
	}}

	ctx := context.Background()
	result, err := executor.callGdsLinkPredictionPredict(ctx, cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionPredict() error = %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Should have extended columns for hybrid
	expectedCols := []string{"node1", "node2", "score", "topology_score", "semantic_score", "reason"}
	if len(result.Columns) != len(expectedCols) {
		t.Errorf("Columns = %v, want %v", result.Columns, expectedCols)
	}

	// Should have predictions
	if len(result.Rows) == 0 {
		t.Error("Expected hybrid predictions, got none")
	}

	// Check hybrid result format
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != 6 {
			t.Errorf("Row length = %d, want 6", len(row))
		}
	}
}

// TestFormatLinkPredictionResults tests result formatting
func TestFormatLinkPredictionResults(t *testing.T) {
	executor := &StorageExecutor{}

	// Create test predictions using linkpredict.Prediction type
	predictions := []linkpredict.Prediction{
		{
			TargetID:  "node1",
			Score:     0.9,
			Algorithm: "adamic_adar",
			Reason:    "test reason 1",
		},
		{
			TargetID:  "node2",
			Score:     0.7,
			Algorithm: "adamic_adar",
			Reason:    "test reason 2",
		},
	}

	result := executor.formatLinkPredictionResults(predictions, "source")

	if result == nil {
		t.Error("formatLinkPredictionResults returned nil")
	}

	if len(result.Columns) != 3 {
		t.Errorf("Columns length = %d, want 3", len(result.Columns))
	}

	expectedCols := []string{"node1", "node2", "score"}
	for i, col := range expectedCols {
		if i < len(result.Columns) && result.Columns[i] != col {
			t.Errorf("Column[%d] = %s, want %s", i, result.Columns[i], col)
		}
	}

	// Check we have rows
	if len(result.Rows) != 2 {
		t.Errorf("Rows length = %d, want 2", len(result.Rows))
	}

	// Check first row
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != 3 {
			t.Errorf("Row 0 length = %d, want 3", len(row))
		}
		if row[0] != "source" {
			t.Errorf("Row 0 node1 = %v, want 'source'", row[0])
		}
		if row[1] != "node1" {
			t.Errorf("Row 0 node2 = %v, want 'node1'", row[1])
		}
		if score, ok := row[2].(float64); !ok || score != 0.9 {
			t.Errorf("Row 0 score = %v, want 0.9", row[2])
		}
	}
}

// Helper: setupTestGraph creates test data for procedures
func setupTestGraph(t *testing.T, engine storage.Engine) {
	nodes := []*storage.Node{
		{ID: "alice", Labels: []string{"Person"}},
		{ID: "bob", Labels: []string{"Person"}},
		{ID: "charlie", Labels: []string{"Person"}},
		{ID: "diana", Labels: []string{"Person"}},
	}

	for _, node := range nodes {
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	edges := []*storage.Edge{
		{ID: "e1", StartNode: "alice", EndNode: "bob", Type: "KNOWS"},
		{ID: "e2", StartNode: "alice", EndNode: "charlie", Type: "KNOWS"},
		{ID: "e3", StartNode: "bob", EndNode: "diana", Type: "KNOWS"},
		{ID: "e4", StartNode: "charlie", EndNode: "diana", Type: "KNOWS"},
	}

	for _, edge := range edges {
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("Failed to create edge: %v", err)
		}
	}
}
