package storage

import (
	"testing"
)

func TestPropertyIndex_BasicLookup(t *testing.T) {
	sm := NewSchemaManager()

	// Create property index
	err := sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"})
	if err != nil {
		t.Fatalf("AddPropertyIndex failed: %v", err)
	}

	// Insert some values
	testData := []struct {
		nodeID NodeID
		name   string
	}{
		{"person-1", "Alice"},
		{"person-2", "Bob"},
		{"person-3", "Alice"}, // Duplicate name
		{"person-4", "Charlie"},
	}

	for _, td := range testData {
		err := sm.PropertyIndexInsert("Person", "name", td.nodeID, td.name)
		if err != nil {
			t.Fatalf("PropertyIndexInsert failed for %s: %v", td.nodeID, err)
		}
	}

	// Lookup "Alice" - should return 2 nodes
	results := sm.PropertyIndexLookup("Person", "name", "Alice")
	if len(results) != 2 {
		t.Errorf("Expected 2 results for 'Alice', got %d: %v", len(results), results)
	}

	// Lookup "Bob" - should return 1 node
	results = sm.PropertyIndexLookup("Person", "name", "Bob")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for 'Bob', got %d", len(results))
	}
	if results[0] != "person-2" {
		t.Errorf("Expected person-2, got %s", results[0])
	}

	// Lookup non-existent value
	results = sm.PropertyIndexLookup("Person", "name", "Dave")
	if results != nil && len(results) != 0 {
		t.Errorf("Expected 0 results for 'Dave', got %d", len(results))
	}
}

func TestPropertyIndex_Delete(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddPropertyIndex("idx_user_email", "User", []string{"email"})
	if err != nil {
		t.Fatalf("AddPropertyIndex failed: %v", err)
	}

	// Insert values
	sm.PropertyIndexInsert("User", "email", "user-1", "alice@example.com")
	sm.PropertyIndexInsert("User", "email", "user-2", "bob@example.com")

	// Verify both exist
	if len(sm.PropertyIndexLookup("User", "email", "alice@example.com")) != 1 {
		t.Error("alice@example.com should exist")
	}
	if len(sm.PropertyIndexLookup("User", "email", "bob@example.com")) != 1 {
		t.Error("bob@example.com should exist")
	}

	// Delete one
	err = sm.PropertyIndexDelete("User", "email", "user-1", "alice@example.com")
	if err != nil {
		t.Fatalf("PropertyIndexDelete failed: %v", err)
	}

	// Verify deletion
	results := sm.PropertyIndexLookup("User", "email", "alice@example.com")
	if results != nil && len(results) != 0 {
		t.Errorf("alice@example.com should be deleted, got %v", results)
	}

	// bob should still exist
	if len(sm.PropertyIndexLookup("User", "email", "bob@example.com")) != 1 {
		t.Error("bob@example.com should still exist")
	}
}

func TestPropertyIndex_NonExistentIndex(t *testing.T) {
	sm := NewSchemaManager()

	// Lookup on non-existent index returns nil
	results := sm.PropertyIndexLookup("NonExistent", "prop", "value")
	if results != nil {
		t.Errorf("Expected nil for non-existent index, got %v", results)
	}

	// Insert on non-existent index returns error
	err := sm.PropertyIndexInsert("NonExistent", "prop", "node-1", "value")
	if err == nil {
		t.Error("Expected error for insert to non-existent index")
	}

	// Delete on non-existent index is a no-op (no error)
	err = sm.PropertyIndexDelete("NonExistent", "prop", "node-1", "value")
	if err != nil {
		t.Errorf("Expected no error for delete from non-existent index, got %v", err)
	}
}

func TestPropertyIndex_NumericValues(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.AddPropertyIndex("idx_person_age", "Person", []string{"age"})
	if err != nil {
		t.Fatalf("AddPropertyIndex failed: %v", err)
	}

	// Insert numeric values
	sm.PropertyIndexInsert("Person", "age", "person-1", 25)
	sm.PropertyIndexInsert("Person", "age", "person-2", 30)
	sm.PropertyIndexInsert("Person", "age", "person-3", 25) // Same age

	// Lookup by integer
	results := sm.PropertyIndexLookup("Person", "age", 25)
	if len(results) != 2 {
		t.Errorf("Expected 2 results for age 25, got %d", len(results))
	}

	results = sm.PropertyIndexLookup("Person", "age", 30)
	if len(results) != 1 {
		t.Errorf("Expected 1 result for age 30, got %d", len(results))
	}
}

func TestGetPropertyIndex(t *testing.T) {
	sm := NewSchemaManager()

	// Non-existent index
	_, exists := sm.GetPropertyIndex("Person", "name")
	if exists {
		t.Error("Expected index to not exist")
	}

	// Create index
	err := sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"})
	if err != nil {
		t.Fatalf("AddPropertyIndex failed: %v", err)
	}

	// Now it should exist
	idx, exists := sm.GetPropertyIndex("Person", "name")
	if !exists {
		t.Error("Expected index to exist")
	}
	if idx.Name != "idx_person_name" {
		t.Errorf("Expected name idx_person_name, got %s", idx.Name)
	}
}

func BenchmarkPropertyIndex_Lookup(b *testing.B) {
	sm := NewSchemaManager()
	sm.AddPropertyIndex("bench_idx", "Node", []string{"value"})

	// Pre-populate with 10K entries
	for i := 0; i < 10000; i++ {
		sm.PropertyIndexInsert("Node", "value", NodeID("node-"+string(rune(i))), i%100) // 100 distinct values
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.PropertyIndexLookup("Node", "value", i%100)
	}
}
