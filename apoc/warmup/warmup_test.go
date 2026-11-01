package warmup

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// setupTestStorage creates a mock storage with test data
func setupTestStorage() (*Functions, storage.Storage) {
	mockStore := storage.NewInMemoryStorage()

	// Add test nodes using CreateNode
	mockStore.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Alice", "age": 30}) // ID 1
	mockStore.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Bob", "age": 25})   // ID 2
	mockStore.CreateNode([]string{"Company"}, map[string]interface{}{"name": "Acme"})            // ID 3

	// Add test relationships using CreateRelationship
	mockStore.CreateRelationship(1, 3, "WORKS_AT", map[string]interface{}{"since": 2020})
	mockStore.CreateRelationship(2, 3, "WORKS_AT", map[string]interface{}{"since": 2021})
	mockStore.CreateRelationship(1, 2, "KNOWS", map[string]interface{}{})

	// Bind one instance to this backend; the warmup cache belongs to it too.
	return New(mockStore), mockStore
}

func TestRun(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	result := functions.Run(context.Background())

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != 3 {
		t.Errorf("Expected nodesLoaded=3, got %v", result["nodesLoaded"])
	}

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != 3 {
		t.Errorf("Expected relationshipsLoaded=3, got %v", result["relationshipsLoaded"])
	}

	// Check timing
	if _, ok := result["timeTaken"]; !ok {
		t.Error("Result should include timeTaken")
	}

	functions.Clear()
}

func TestRunWithParams(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	// Test with label filter
	params := map[string]interface{}{
		"labels": []string{"Person"},
	}

	result := functions.RunWithParams(context.Background(), params)

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != 2 {
		t.Errorf("Expected nodesLoaded=2 (only Person), got %v", result["nodesLoaded"])
	}

	functions.Clear()

	// Test with relationship type filter
	params = map[string]interface{}{
		"types": []string{"WORKS_AT"},
	}

	result = functions.RunWithParams(context.Background(), params)

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != 2 {
		t.Errorf("Expected relationshipsLoaded=2 (only WORKS_AT), got %v", result["relationshipsLoaded"])
	}

	functions.Clear()
}

func TestNodes(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	result := functions.Nodes(context.Background(), []string{"Person"})

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != 2 {
		t.Errorf("Expected nodesLoaded=2, got %v", result["nodesLoaded"])
	}

	functions.Clear()
}

func TestRelationships(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	result := functions.Relationships(context.Background(), []string{"KNOWS"})

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != 1 {
		t.Errorf("Expected relationshipsLoaded=1, got %v", result["relationshipsLoaded"])
	}

	functions.Clear()
}

func TestIndexes(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	result := functions.Indexes(context.Background())

	if result == nil {
		t.Error("Indexes() returned nil")
	}

	indexesLoaded, ok := result["indexesLoaded"].(int)
	if !ok || indexesLoaded < 1 {
		t.Errorf("Expected indexesLoaded >= 1, got %v", result["indexesLoaded"])
	}

	functions.Clear()
}

func TestProperties(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	result := functions.Properties(context.Background(), []string{"name", "age"})

	if result == nil {
		t.Error("Properties() returned nil")
	}

	propertiesLoaded, ok := result["propertiesLoaded"].(int)
	if !ok || propertiesLoaded < 1 {
		t.Errorf("Expected propertiesLoaded >= 1, got %v", result["propertiesLoaded"])
	}

	functions.Clear()
}

func TestStats(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	// Run warmup first
	functions.Run(context.Background())

	stats := functions.Stats()

	if stats["nodesCached"] != 3 {
		t.Errorf("Expected nodesCached=3, got %v", stats["nodesCached"])
	}

	if stats["relationshipsCached"] != 3 {
		t.Errorf("Expected relationshipsCached=3, got %v", stats["relationshipsCached"])
	}

	// Test cache hits
	functions.GetCachedNode(1)
	functions.GetCachedNode(1) // second hit

	stats = functions.Stats()
	if stats["cacheHits"].(int64) < 2 {
		t.Errorf("Expected cacheHits >= 2, got %v", stats["cacheHits"])
	}

	functions.Clear()
}

func TestClear(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Run(context.Background())

	// Should have cached items
	stats := functions.Stats()
	if stats["itemsCached"].(int) == 0 {
		t.Error("Should have cached items before Clear()")
	}

	// Clear
	result := functions.Clear()
	if result["cleared"] != true {
		t.Error("Clear() should return cleared=true")
	}

	// Now should be empty
	stats = functions.Stats()
	if stats["itemsCached"].(int) != 0 {
		t.Errorf("Should have 0 items after Clear(), got %v", stats["itemsCached"])
	}
}

func TestStatus(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	// Before warmup
	status := functions.Status()
	if status["running"] != false {
		t.Error("Should not be running initially")
	}

	// Run warmup
	functions.Run(context.Background())

	// After warmup
	status = functions.Status()
	if status["lastRun"] == nil {
		t.Error("Should have lastRun after Run()")
	}

	functions.Clear()
}

func TestProgress(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	// Before warmup - 0%
	progress := functions.Progress(context.Background())
	pct, ok := progress["percentage"].(float64)
	if !ok || pct != 0 {
		t.Errorf("Expected 0%% progress before warmup, got %v", progress["percentage"])
	}

	// Run warmup
	functions.Run(context.Background())

	// After warmup - 100%
	progress = functions.Progress(context.Background())
	pct, ok = progress["percentage"].(float64)
	if !ok || pct != 100 {
		t.Errorf("Expected 100%% progress after warmup, got %v", progress["percentage"])
	}

	functions.Clear()
}

func TestSchedule(t *testing.T) {
	functions, _ := setupTestStorage()
	result := functions.Schedule("0 4 * * *")
	if result == nil {
		t.Error("Schedule() returned nil")
	}

	if result["scheduled"] != true {
		t.Error("Schedule() should return scheduled=true")
	}

	if result["cron"] != "0 4 * * *" {
		t.Error("Schedule() should return the cron expression")
	}
}

func TestOptimize(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	// Before warmup - should recommend running warmup
	result := functions.Optimize()
	if result["optimized"] != true {
		t.Error("Optimize() should return optimized=true")
	}

	recommendations := result["recommendations"].([]string)
	if len(recommendations) == 0 {
		t.Error("Optimize() should provide recommendations")
	}

	// After warmup
	functions.Run(context.Background())
	result = functions.Optimize()
	if result["optimized"] != true {
		t.Error("Optimize() should return optimized=true after warmup")
	}

	functions.Clear()
}

func TestCache(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()

	queries := []string{"MATCH (n:Person) RETURN n", "MATCH (n:Company) RETURN n"}
	result := functions.Cache(queries)

	if result == nil {
		t.Error("Cache() returned nil")
	}

	queriesWarmed, ok := result["queriesWarmed"].(int)
	if !ok || queriesWarmed != 2 {
		t.Errorf("Expected queriesWarmed=2, got %v", result["queriesWarmed"])
	}

	functions.Clear()
}

func TestGetCachedNode(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()
	functions.Run(context.Background())

	// Test hit
	node, found := functions.GetCachedNode(1)
	if !found {
		t.Error("Should find cached node 1")
	}
	if node == nil || node.ID != 1 {
		t.Error("Should return correct node")
	}

	// Test miss
	_, found = functions.GetCachedNode(999)
	if found {
		t.Error("Should not find non-existent node")
	}

	functions.Clear()
}

func TestGetCachedRelationship(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()
	functions.Run(context.Background())

	// Test hit - relationship IDs are auto-generated starting at 1
	rel, found := functions.GetCachedRelationship(1)
	if !found {
		t.Error("Should find cached relationship 1")
	}
	if rel == nil || rel.ID != 1 {
		t.Error("Should return correct relationship")
	}

	// Test miss
	_, found = functions.GetCachedRelationship(999)
	if found {
		t.Error("Should not find non-existent relationship")
	}

	functions.Clear()
}

func TestGetNodesByLabel(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()
	functions.Run(context.Background())

	nodes, found := functions.GetNodesByLabel("Person")
	if !found {
		t.Error("Should find nodes by label 'Person'")
	}
	if len(nodes) != 2 {
		t.Errorf("Expected 2 Person nodes, got %d", len(nodes))
	}

	// Test non-existent label
	_, found = functions.GetNodesByLabel("NonExistent")
	if found {
		t.Error("Should not find non-existent label")
	}

	functions.Clear()
}

func TestGetRelationshipsByType(t *testing.T) {
	functions, _ := setupTestStorage()
	functions.Clear()
	functions.Run(context.Background())

	rels, found := functions.GetRelationshipsByType("WORKS_AT")
	if !found {
		t.Error("Should find relationships by type 'WORKS_AT'")
	}
	if len(rels) != 2 {
		t.Errorf("Expected 2 WORKS_AT relationships, got %d", len(rels))
	}

	// Test non-existent type
	_, found = functions.GetRelationshipsByType("NonExistent")
	if found {
		t.Error("Should not find non-existent type")
	}

	functions.Clear()
}

func TestSubgraph(t *testing.T) {
	functions, backend := setupTestStorage()
	functions.Clear()

	// Get a node from storage to use as start node
	nodes, _ := backend.AllNodes(context.Background())

	result := functions.Subgraph(nodes[0], 2)

	if result == nil {
		t.Error("Subgraph() returned nil")
	}

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded < 1 {
		t.Errorf("Expected nodesLoaded >= 1, got %v", result["nodesLoaded"])
	}

	functions.Clear()
}

func TestPath(t *testing.T) {
	functions, backend := setupTestStorage()
	functions.Clear()

	nodes, _ := backend.AllNodes(context.Background())
	rels, _ := backend.AllRelationships(context.Background())

	result := functions.Path(nodes, rels)

	if result == nil {
		t.Error("Path() returned nil")
	}

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != len(nodes) {
		t.Errorf("Expected nodesLoaded=%d, got %v", len(nodes), result["nodesLoaded"])
	}

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != len(rels) {
		t.Errorf("Expected relationshipsLoaded=%d, got %v", len(rels), result["relationshipsLoaded"])
	}

	functions.Clear()
}
