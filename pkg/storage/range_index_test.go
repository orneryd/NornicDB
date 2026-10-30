package storage

import (
	"fmt"
	"testing"
)

func TestRangeIndex_BasicOperations(t *testing.T) {
	sm := NewSchemaManager()

	// Create range index
	err := sm.AddRangeIndex("idx_person_age", "Person", "age")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Insert some values
	testData := []struct {
		nodeID NodeID
		age    int
	}{
		{"person-1", 25},
		{"person-2", 30},
		{"person-3", 35},
		{"person-4", 20},
		{"person-5", 40},
	}

	for _, td := range testData {
		err := sm.RangeIndexInsert("idx_person_age", td.nodeID, td.age)
		if err != nil {
			t.Fatalf("RangeIndexInsert failed for %s: %v", td.nodeID, err)
		}
	}

	// Test range query: age >= 25 AND age <= 35
	results, err := sm.RangeQuery("idx_person_age", 25, 35, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results (ages 25, 30, 35), got %d: %v", len(results), results)
	}

	// Verify expected nodes are in results
	expectedNodes := map[NodeID]bool{
		"person-1": true, // age 25
		"person-2": true, // age 30
		"person-3": true, // age 35
	}
	for _, nodeID := range results {
		if !expectedNodes[nodeID] {
			t.Errorf("Unexpected node in results: %s", nodeID)
		}
	}
}

func TestRangeIndex_ExclusiveBounds(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_score", "Test", "score")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Insert values 1-5
	for i := 1; i <= 5; i++ {
		err := sm.RangeIndexInsert("idx_score", NodeID("n-"+string(rune('0'+i))), i)
		if err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
	}

	// Test exclusive bounds: score > 2 AND score < 5
	results, err := sm.RangeQuery("idx_score", 2, 5, false, false)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results (scores 3, 4), got %d: %v", len(results), results)
	}
}

func TestRangeIndex_UnboundedQueries(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_price", "Product", "price")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Insert some prices
	prices := []float64{9.99, 19.99, 29.99, 49.99, 99.99}
	for i, price := range prices {
		err := sm.RangeIndexInsert("idx_price", NodeID("product-"+string(rune('1'+i))), price)
		if err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
	}

	// Test: price >= 30 (no upper bound)
	results, err := sm.RangeQuery("idx_price", 30.0, nil, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results (49.99, 99.99), got %d", len(results))
	}

	// Test: price < 25 (no lower bound)
	results, err = sm.RangeQuery("idx_price", nil, 25.0, true, false)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results (9.99, 19.99), got %d", len(results))
	}
}

func TestRangeIndex_Delete(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_rating", "Review", "rating")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Insert ratings 1-5
	for i := 1; i <= 5; i++ {
		err := sm.RangeIndexInsert("idx_rating", NodeID("review-"+string(rune('0'+i))), i)
		if err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
	}

	// Delete rating 3
	err = sm.RangeIndexDelete("idx_rating", "review-3")
	if err != nil {
		t.Fatalf("RangeIndexDelete failed: %v", err)
	}

	// Query all (should be 4 results now)
	results, err := sm.RangeQuery("idx_rating", 1, 5, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	if len(results) != 4 {
		t.Errorf("Expected 4 results after delete, got %d", len(results))
	}

	// Verify review-3 is not in results
	for _, nodeID := range results {
		if nodeID == "review-3" {
			t.Errorf("Deleted node review-3 should not be in results")
		}
	}
}

func TestRangeIndex_EmptyIndex(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_empty", "Empty", "value")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Query empty index
	results, err := sm.RangeQuery("idx_empty", 0, 100, true, true)
	if err != nil {
		t.Fatalf("RangeQuery on empty index failed: %v", err)
	}

	if results != nil && len(results) != 0 {
		t.Errorf("Expected empty results from empty index, got %v", results)
	}
}

func TestRangeIndex_NonExistent(t *testing.T) {
	sm := NewSchemaManager()

	// Try operations on non-existent index
	_, err := sm.RangeQuery("non_existent", 0, 10, true, true)
	if err == nil {
		t.Errorf("Expected error for non-existent index, got nil")
	}

	err = sm.RangeIndexInsert("non_existent", "node-1", 5)
	if err == nil {
		t.Errorf("Expected error for insert to non-existent index, got nil")
	}

	err = sm.RangeIndexDelete("non_existent", "node-1")
	if err == nil {
		t.Errorf("Expected error for delete from non-existent index, got nil")
	}
}

func TestRangeIndex_FloatValues(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_temperature", "Sensor", "temperature")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Insert float values
	temps := []float64{-10.5, 0.0, 15.3, 22.7, 37.2, 100.0}
	for i, temp := range temps {
		err := sm.RangeIndexInsert("idx_temperature", NodeID("sensor-"+string(rune('A'+i))), temp)
		if err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
	}

	// Query: temperature >= 15.0 AND temperature <= 38.0
	results, err := sm.RangeQuery("idx_temperature", 15.0, 38.0, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	// Should get 15.3, 22.7, 37.2 = 3 results
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

func TestRangeIndex_ReinsertUpdatesValue(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddRangeIndex("idx_reinsert", "Node", "value")
	if err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	node := NodeID("n1")

	if err := sm.RangeIndexInsert("idx_reinsert", node, 10); err != nil {
		t.Fatalf("RangeIndexInsert(10) failed: %v", err)
	}
	if err := sm.RangeIndexInsert("idx_reinsert", node, 20); err != nil {
		t.Fatalf("RangeIndexInsert(20) failed: %v", err)
	}

	// Full range should include exactly one row for the node.
	all, err := sm.RangeQuery("idx_reinsert", nil, nil, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}
	if len(all) != 1 || all[0] != node {
		t.Fatalf("Expected 1 result [n1], got %v", all)
	}

	// Old range should be empty.
	oldRange, err := sm.RangeQuery("idx_reinsert", 0, 15, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}
	if len(oldRange) != 0 {
		t.Fatalf("Expected 0 results for old range, got %v", oldRange)
	}

	// New range should match.
	newRange, err := sm.RangeQuery("idx_reinsert", 15, 25, true, true)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}
	if len(newRange) != 1 || newRange[0] != node {
		t.Fatalf("Expected 1 result [n1] for new range, got %v", newRange)
	}
}

func BenchmarkRangeIndex_Insert(b *testing.B) {
	sm := NewSchemaManager()
	sm.AddRangeIndex("bench_idx", "Node", "value")

	// Keep index size bounded and realistic:
	// update the same N node IDs repeatedly instead of continuously growing.
	const numNodes = 10000
	ids := make([]NodeID, numNodes)
	for i := 0; i < numNodes; i++ {
		ids[i] = NodeID(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.RangeIndexInsert("bench_idx", ids[i%numNodes], i%numNodes)
	}
}

func BenchmarkRangeIndex_Query(b *testing.B) {
	sm := NewSchemaManager()
	sm.AddRangeIndex("bench_idx", "Node", "value")

	// Pre-populate with 10K entries
	for i := 0; i < 10000; i++ {
		sm.RangeIndexInsert("bench_idx", NodeID("node-"+string(rune(i))), i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.RangeQuery("bench_idx", 2500, 7500, true, true)
	}
}
