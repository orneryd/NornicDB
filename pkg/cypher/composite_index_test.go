package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestCompositeIndex tests composite (multi-property) index creation
func TestCompositeIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index with name
	_, err := exec.Execute(ctx, "CREATE INDEX person_name_age FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Verify index was created (via schema)
	schema := store.GetSchema()
	indexes := schema.GetIndexes()
	
	// Should have the composite index
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)
			
			if name == "person_name_age" && label == "Person" {
				if len(props) == 2 && props[0] == "firstName" && props[1] == "lastName" {
					found = true
					break
				}
			}
		}
	}
	
	if !found {
		t.Error("Composite index not found in schema")
	}
}

// TestCompositeIndexUnnamed tests composite index without explicit name
func TestCompositeIndexUnnamed(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index without name
	_, err := exec.Execute(ctx, "CREATE INDEX FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create unnamed composite index: %v", err)
	}

	// Verify index was created with auto-generated name
	schema := store.GetSchema()
	indexes := schema.GetIndexes()
	
	// Should have the composite index (name will be auto-generated)
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)
			
			if label == "Person" && len(props) == 2 {
				if props[0] == "firstName" && props[1] == "lastName" {
					found = true
					// Check auto-generated name (should be lowercase)
					if name != "index_person_firstname_lastname" {
						t.Errorf("Unexpected auto-generated name: %s", name)
					}
					break
				}
			}
		}
	}
	
	if !found {
		t.Error("Unnamed composite index not found in schema")
	}
}

// TestCompositeIndexThreeProperties tests composite index with three properties
func TestCompositeIndexThreeProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index with three properties
	_, err := exec.Execute(ctx, "CREATE INDEX address_idx FOR (a:Address) ON (a.city, a.state, a.zipCode)", nil)
	if err != nil {
		t.Fatalf("Failed to create 3-property composite index: %v", err)
	}

	// Verify index
	schema := store.GetSchema()
	indexes := schema.GetIndexes()
	
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)
			
			if name == "address_idx" && label == "Address" {
				if len(props) == 3 {
					if props[0] == "city" && props[1] == "state" && props[2] == "zipCode" {
						found = true
						break
					}
				}
			}
		}
	}
	
	if !found {
		t.Error("3-property composite index not found in schema")
	}
}

// TestCompositeIndexWithSpaces tests parsing with various spacing
func TestCompositeIndexWithSpaces(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{"minimal_spaces", "CREATE INDEX test1 FOR (n:Node) ON (n.a,n.b)"},
		{"extra_spaces", "CREATE INDEX test2 FOR (n:Node) ON (n.a , n.b)"},
		{"lots_of_spaces", "CREATE INDEX test3 FOR (n:Node) ON ( n.a , n.b , n.c )"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				t.Errorf("Failed to create index with spacing variation: %v", err)
			}
		})
	}
}

// TestCompositeIndexIfNotExists tests IF NOT EXISTS clause with composite indexes
func TestCompositeIndexIfNotExists(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index
	_, err := exec.Execute(ctx, "CREATE INDEX person_idx IF NOT EXISTS FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Create again with IF NOT EXISTS - should not error
	_, err = exec.Execute(ctx, "CREATE INDEX person_idx IF NOT EXISTS FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Errorf("IF NOT EXISTS should not error on duplicate: %v", err)
	}
}

// TestSinglePropertyIndexStillWorks tests that single-property indexes still work
func TestSinglePropertyIndexStillWorks(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create single-property index
	_, err := exec.Execute(ctx, "CREATE INDEX name_idx FOR (p:Person) ON (p.name)", nil)
	if err != nil {
		t.Fatalf("Failed to create single-property index: %v", err)
	}

	// Verify
	schema := store.GetSchema()
	indexes := schema.GetIndexes()
	
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)
			
			if name == "name_idx" && label == "Person" {
				if len(props) == 1 && props[0] == "name" {
					found = true
					break
				}
			}
		}
	}
	
	if !found {
		t.Error("Single-property index not found in schema")
	}
}

// TestCompositeIndexQueryOptimization tests that composite indexes can be used in queries
func TestCompositeIndexQueryOptimization(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index
	_, err := exec.Execute(ctx, "CREATE INDEX person_name_idx FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Create test data
	_, err = exec.Execute(ctx, `
		CREATE (p1:Person {firstName: 'John', lastName: 'Doe', age: 30}),
		       (p2:Person {firstName: 'Jane', lastName: 'Doe', age: 25}),
		       (p3:Person {firstName: 'John', lastName: 'Smith', age: 35})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Query using both properties (should benefit from composite index)
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE p.firstName = 'John' AND p.lastName = 'Doe'
		RETURN p.firstName, p.lastName, p.age
	`, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should find exactly one match
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	
	if result.Rows[0][0] != "John" || result.Rows[0][1] != "Doe" || result.Rows[0][2] != int64(30) {
		t.Errorf("Unexpected result: %v", result.Rows[0])
	}
}

// TestParseIndexProperties tests the parseIndexProperties helper function
func TestParseIndexProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"single", "n.name", []string{"name"}},
		{"two_props", "n.firstName, n.lastName", []string{"firstName", "lastName"}},
		{"three_props", "n.a, n.b, n.c", []string{"a", "b", "c"}},
		{"with_spaces", "n.a , n.b , n.c", []string{"a", "b", "c"}},
		{"no_spaces", "n.a,n.b,n.c", []string{"a", "b", "c"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := exec.parseIndexProperties(tc.input)
			
			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d properties, got %d", len(tc.expected), len(result))
				return
			}
			
			for i, prop := range result {
				if prop != tc.expected[i] {
					t.Errorf("Property %d: expected %s, got %s", i, tc.expected[i], prop)
				}
			}
		})
	}
}
