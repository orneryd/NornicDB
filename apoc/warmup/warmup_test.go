package warmup

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// setupTestStorage creates a mock storage with test data
func setupTestStorage() {
	mockStore := storage.NewInMemoryStorage()

	// Add test nodes using CreateNode
	mockStore.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Alice", "age": 30}) // ID 1
	mockStore.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Bob", "age": 25})   // ID 2
	mockStore.CreateNode([]string{"Company"}, map[string]interface{}{"name": "Acme"})            // ID 3

	// Add test relationships using CreateRelationship
	mockStore.CreateRelationship(1, 3, "WORKS_AT", map[string]interface{}{"since": 2020})
	mockStore.CreateRelationship(2, 3, "WORKS_AT", map[string]interface{}{"since": 2021})
	mockStore.CreateRelationship(1, 2, "KNOWS", map[string]interface{}{})

	// Set the global storage
	Storage = mockStore
}

func TestRun(t *testing.T) {
	setupTestStorage()
	Clear()

	result := Run()

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

	Clear()
}

func TestRunWithParams(t *testing.T) {
	setupTestStorage()
	Clear()

	// Test with label filter
	params := map[string]interface{}{
		"labels": []string{"Person"},
	}

	result := RunWithParams(params)

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != 2 {
		t.Errorf("Expected nodesLoaded=2 (only Person), got %v", result["nodesLoaded"])
	}

	Clear()

	// Test with relationship type filter
	params = map[string]interface{}{
		"types": []string{"WORKS_AT"},
	}

	result = RunWithParams(params)

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != 2 {
		t.Errorf("Expected relationshipsLoaded=2 (only WORKS_AT), got %v", result["relationshipsLoaded"])
	}

	Clear()
}

func TestNodes(t *testing.T) {
	setupTestStorage()
	Clear()

	result := Nodes([]string{"Person"})

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded != 2 {
		t.Errorf("Expected nodesLoaded=2, got %v", result["nodesLoaded"])
	}

	Clear()
}

func TestRelationships(t *testing.T) {
	setupTestStorage()
	Clear()

	result := Relationships([]string{"KNOWS"})

	relsLoaded, ok := result["relationshipsLoaded"].(int)
	if !ok || relsLoaded != 1 {
		t.Errorf("Expected relationshipsLoaded=1, got %v", result["relationshipsLoaded"])
	}

	Clear()
}

func TestIndexes(t *testing.T) {
	setupTestStorage()
	Clear()

	result := Indexes()

	if result == nil {
		t.Error("Indexes() returned nil")
	}

	indexesLoaded, ok := result["indexesLoaded"].(int)
	if !ok || indexesLoaded < 1 {
		t.Errorf("Expected indexesLoaded >= 1, got %v", result["indexesLoaded"])
	}

	Clear()
}

func TestProperties(t *testing.T) {
	setupTestStorage()
	Clear()

	result := Properties([]string{"name", "age"})

	if result == nil {
		t.Error("Properties() returned nil")
	}

	propertiesLoaded, ok := result["propertiesLoaded"].(int)
	if !ok || propertiesLoaded < 1 {
		t.Errorf("Expected propertiesLoaded >= 1, got %v", result["propertiesLoaded"])
	}

	Clear()
}

func TestStats(t *testing.T) {
	setupTestStorage()
	Clear()

	// Run warmup first
	Run()

	stats := Stats()

	if stats["nodesCached"] != 3 {
		t.Errorf("Expected nodesCached=3, got %v", stats["nodesCached"])
	}

	if stats["relationshipsCached"] != 3 {
		t.Errorf("Expected relationshipsCached=3, got %v", stats["relationshipsCached"])
	}

	// Test cache hits
	GetCachedNode(1)
	GetCachedNode(1) // second hit

	stats = Stats()
	if stats["cacheHits"].(int64) < 2 {
		t.Errorf("Expected cacheHits >= 2, got %v", stats["cacheHits"])
	}

	Clear()
}

func TestClear(t *testing.T) {
	setupTestStorage()
	Run()

	// Should have cached items
	stats := Stats()
	if stats["itemsCached"].(int) == 0 {
		t.Error("Should have cached items before Clear()")
	}

	// Clear
	result := Clear()
	if result["cleared"] != true {
		t.Error("Clear() should return cleared=true")
	}

	// Now should be empty
	stats = Stats()
	if stats["itemsCached"].(int) != 0 {
		t.Errorf("Should have 0 items after Clear(), got %v", stats["itemsCached"])
	}
}

func TestStatus(t *testing.T) {
	setupTestStorage()
	Clear()

	// Before warmup
	status := Status()
	if status["running"] != false {
		t.Error("Should not be running initially")
	}

	// Run warmup
	Run()

	// After warmup
	status = Status()
	if status["lastRun"] == nil {
		t.Error("Should have lastRun after Run()")
	}

	Clear()
}

func TestProgress(t *testing.T) {
	setupTestStorage()
	Clear()

	// Before warmup - 0%
	progress := Progress()
	pct, ok := progress["percentage"].(float64)
	if !ok || pct != 0 {
		t.Errorf("Expected 0%% progress before warmup, got %v", progress["percentage"])
	}

	// Run warmup
	Run()

	// After warmup - 100%
	progress = Progress()
	pct, ok = progress["percentage"].(float64)
	if !ok || pct != 100 {
		t.Errorf("Expected 100%% progress after warmup, got %v", progress["percentage"])
	}

	Clear()
}

func TestSchedule(t *testing.T) {
	result := Schedule("0 4 * * *")
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
	setupTestStorage()
	Clear()

	// Before warmup - should recommend running warmup
	result := Optimize()
	if result["optimized"] != true {
		t.Error("Optimize() should return optimized=true")
	}

	recommendations := result["recommendations"].([]string)
	if len(recommendations) == 0 {
		t.Error("Optimize() should provide recommendations")
	}

	// After warmup
	Run()
	result = Optimize()
	if result["optimized"] != true {
		t.Error("Optimize() should return optimized=true after warmup")
	}

	Clear()
}

func TestCache(t *testing.T) {
	setupTestStorage()
	Clear()

	queries := []string{"MATCH (n:Person) RETURN n", "MATCH (n:Company) RETURN n"}
	result := Cache(queries)

	if result == nil {
		t.Error("Cache() returned nil")
	}

	queriesWarmed, ok := result["queriesWarmed"].(int)
	if !ok || queriesWarmed != 2 {
		t.Errorf("Expected queriesWarmed=2, got %v", result["queriesWarmed"])
	}

	Clear()
}

func TestGetCachedNode(t *testing.T) {
	setupTestStorage()
	Clear()
	Run()

	// Test hit
	node, found := GetCachedNode(1)
	if !found {
		t.Error("Should find cached node 1")
	}
	if node == nil || node.ID != 1 {
		t.Error("Should return correct node")
	}

	// Test miss
	_, found = GetCachedNode(999)
	if found {
		t.Error("Should not find non-existent node")
	}

	Clear()
}

func TestGetCachedRelationship(t *testing.T) {
	setupTestStorage()
	Clear()
	Run()

	// Test hit - relationship IDs are auto-generated starting at 1
	rel, found := GetCachedRelationship(1)
	if !found {
		t.Error("Should find cached relationship 1")
	}
	if rel == nil || rel.ID != 1 {
		t.Error("Should return correct relationship")
	}

	// Test miss
	_, found = GetCachedRelationship(999)
	if found {
		t.Error("Should not find non-existent relationship")
	}

	Clear()
}

func TestGetNodesByLabel(t *testing.T) {
	setupTestStorage()
	Clear()
	Run()

	nodes, found := GetNodesByLabel("Person")
	if !found {
		t.Error("Should find nodes by label 'Person'")
	}
	if len(nodes) != 2 {
		t.Errorf("Expected 2 Person nodes, got %d", len(nodes))
	}

	// Test non-existent label
	_, found = GetNodesByLabel("NonExistent")
	if found {
		t.Error("Should not find non-existent label")
	}

	Clear()
}

func TestGetRelationshipsByType(t *testing.T) {
	setupTestStorage()
	Clear()
	Run()

	rels, found := GetRelationshipsByType("WORKS_AT")
	if !found {
		t.Error("Should find relationships by type 'WORKS_AT'")
	}
	if len(rels) != 2 {
		t.Errorf("Expected 2 WORKS_AT relationships, got %d", len(rels))
	}

	// Test non-existent type
	_, found = GetRelationshipsByType("NonExistent")
	if found {
		t.Error("Should not find non-existent type")
	}

	Clear()
}

func TestSubgraph(t *testing.T) {
	setupTestStorage()
	Clear()

	// Get a node from storage to use as start node
	nodes, _ := Storage.AllNodes()

	result := Subgraph(nodes[0], 2)

	if result == nil {
		t.Error("Subgraph() returned nil")
	}

	nodesLoaded, ok := result["nodesLoaded"].(int)
	if !ok || nodesLoaded < 1 {
		t.Errorf("Expected nodesLoaded >= 1, got %v", result["nodesLoaded"])
	}

	Clear()
}

func TestPath(t *testing.T) {
	setupTestStorage()
	Clear()

	nodes, _ := Storage.AllNodes()
	rels, _ := Storage.AllRelationships()

	result := Path(nodes, rels)

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

	Clear()
}
