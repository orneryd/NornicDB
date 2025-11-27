package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestParseLinkPredictionConfig tests configuration parsing
func TestParseLinkPredictionConfig(t *testing.T) {
	executor := &StorageExecutor{
		storage: storage.NewMemoryEngine(),
	}

	tests := []struct {
		name     string
		cypher   string
		wantErr  bool
		wantNode string
		wantTopK int
	}{
		{
			name:     "basic config",
			cypher:   `CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'node-123', topK: 10})`,
			wantErr:  false,
			wantNode: "node-123",
			wantTopK: 10,
		},
		{
			name:     "without braces",
			cypher:   `CALL gds.linkPrediction.adamicAdar.stream(sourceNode: 'node-456', topK: 5)`,
			wantErr:  false,
			wantNode: "node-456",
			wantTopK: 5,
		},
		{
			name:     "default topK",
			cypher:   `CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'node-789'})`,
			wantErr:  false,
			wantNode: "node-789",
			wantTopK: 10, // default
		},
		{
			name:    "missing sourceNode",
			cypher:  `CALL gds.linkPrediction.adamicAdar.stream({topK: 10})`,
			wantErr: true,
		},
		{
			name:    "invalid syntax",
			cypher:  `CALL gds.linkPrediction.adamicAdar.stream(`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := executor.parseLinkPredictionConfig(tt.cypher)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseLinkPredictionConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if string(config.SourceNode) != tt.wantNode {
					t.Errorf("SourceNode = %v, want %v", config.SourceNode, tt.wantNode)
				}
				if config.TopK != tt.wantTopK {
					t.Errorf("TopK = %v, want %v", config.TopK, tt.wantTopK)
				}
			}
		})
	}
}

// TestGdsLinkPredictionAdamicAdar tests Adamic-Adar procedure
func TestGdsLinkPredictionAdamicAdar(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'alice', topK: 5})`
	result, err := executor.callGdsLinkPredictionAdamicAdar(cypher)

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
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'alice', topK: 5})`
	result, err := executor.callGdsLinkPredictionCommonNeighbors(cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionCommonNeighbors() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionResourceAllocation tests Resource Allocation procedure
func TestGdsLinkPredictionResourceAllocation(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.resourceAllocation.stream({sourceNode: 'alice', topK: 5})`
	result, err := executor.callGdsLinkPredictionResourceAllocation(cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionResourceAllocation() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionPreferentialAttachment tests Preferential Attachment procedure
func TestGdsLinkPredictionPreferentialAttachment(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.preferentialAttachment.stream({sourceNode: 'alice', topK: 5})`
	result, err := executor.callGdsLinkPredictionPreferentialAttachment(cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionPreferentialAttachment() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionJaccard tests Jaccard procedure
func TestGdsLinkPredictionJaccard(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.jaccard.stream({sourceNode: 'alice', topK: 5})`
	result, err := executor.callGdsLinkPredictionJaccard(cypher)

	if err != nil {
		t.Fatalf("callGdsLinkPredictionJaccard() error = %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		t.Error("Expected predictions")
	}
}

// TestGdsLinkPredictionPredict tests hybrid prediction procedure
func TestGdsLinkPredictionPredict(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	// Add embeddings
	context.Background()
	for _, nodeID := range []storage.NodeID{"alice", "bob", "charlie", "diana"} {
		node, _ := engine.GetNode(nodeID)
		if node != nil {
			node.Embedding = []float32{0.1, 0.2, 0.3, 0.4}
			engine.UpdateNode(node)
		}
	}

	executor := &StorageExecutor{
		storage: engine,
	}

	cypher := `CALL gds.linkPrediction.predict.stream({
		sourceNode: 'alice',
		topK: 5,
		algorithm: 'adamic_adar',
		topologyWeight: 0.6,
		semanticWeight: 0.4
	})`

	result, err := executor.callGdsLinkPredictionPredict(cypher)

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
		if err := engine.CreateNode(node); err != nil {
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
