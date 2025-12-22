// Test for node/relationship functions
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestNodeRelationshipFunctions(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: nodes and relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})
		CREATE (b:Person {name: 'Bob', age: 25})
		CREATE (a)-[r:KNOWS {since: 2020}]->(b)
	`, nil)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	t.Run("elementId function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person {name: 'Alice'})
			RETURN elementId(n) AS id
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		if str, ok := result.Rows[0][0].(string); ok {
			if str == "" {
				t.Error("elementId() returned empty string")
			}
			t.Logf("elementId: %s", str)
		} else {
			t.Errorf("elementId() returned %T, want string", result.Rows[0][0])
		}
	})

	t.Run("startNode and endNode functions", func(t *testing.T) {
		// Test that functions return node maps
		result, err := exec.Execute(ctx, `
			MATCH (a)-[r:KNOWS]->(b)
			RETURN startNode(r) AS start, endNode(r) AS end
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}

		startNode, ok1 := result.Rows[0][0].(*storage.Node)
		endNode, ok2 := result.Rows[0][1].(*storage.Node)

		if !ok1 || !ok2 {
			t.Fatalf("startNode/endNode should return *storage.Node, got %T and %T", result.Rows[0][0], result.Rows[0][1])
		}

		// Check properties exist
		if startNode.Properties["name"] != "Alice" {
			t.Errorf("startNode().Properties['name'] = %v, want 'Alice'", startNode.Properties["name"])
		}
		if endNode.Properties["name"] != "Bob" {
			t.Errorf("endNode().Properties['name'] = %v, want 'Bob'", endNode.Properties["name"])
		}
	})

	t.Run("id function on node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person {name: 'Alice'})
			RETURN id(n) AS id
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		if result.Rows[0][0] == nil || result.Rows[0][0] == "" {
			t.Error("id() returned empty value")
		}
	})

	t.Run("id function on relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH ()-[r:KNOWS]->()
			RETURN id(r) AS id
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		if result.Rows[0][0] == nil || result.Rows[0][0] == "" {
			t.Error("id() on relationship returned empty value")
		}
	})

	t.Run("labels function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person {name: 'Alice'})
			RETURN labels(n) AS labels
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		
		labels, ok := result.Rows[0][0].([]interface{})
		if !ok {
			t.Fatalf("labels() returned %T, want []interface{}", result.Rows[0][0])
		}
		
		if len(labels) == 0 {
			t.Error("labels() returned empty list")
		}
		if labels[0] != "Person" {
			t.Errorf("labels()[0] = %v, want 'Person'", labels[0])
		}
	})

	t.Run("type function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH ()-[r:KNOWS]->()
			RETURN type(r) AS type
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		
		if result.Rows[0][0] != "KNOWS" {
			t.Errorf("type() = %v, want 'KNOWS'", result.Rows[0][0])
		}
	})

	t.Run("keys function on node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person {name: 'Alice'})
			RETURN keys(n) AS keys
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		
		keys, ok := result.Rows[0][0].([]interface{})
		if !ok {
			t.Fatalf("keys() returned %T, want []interface{}", result.Rows[0][0])
		}
		
		if len(keys) == 0 {
			t.Error("keys() returned empty list")
		}
		// Should have 'name' and 'age' keys
		hasName := false
		hasAge := false
		for _, k := range keys {
			if k == "name" {
				hasName = true
			}
			if k == "age" {
				hasAge = true
			}
		}
		if !hasName || !hasAge {
			t.Errorf("keys() = %v, want keys including 'name' and 'age'", keys)
		}
	})

	t.Run("properties function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person {name: 'Alice'})
			RETURN properties(n) AS props
		`, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) == 0 {
			t.Fatal("No rows returned")
		}
		
		props, ok := result.Rows[0][0].(map[string]interface{})
		if !ok {
			t.Fatalf("properties() returned %T, want map[string]interface{}", result.Rows[0][0])
		}
		
		if props["name"] != "Alice" {
			t.Errorf("properties()['name'] = %v, want 'Alice'", props["name"])
		}
		if props["age"] != int64(30) {
			t.Errorf("properties()['age'] = %v, want 30", props["age"])
		}
	})
}
