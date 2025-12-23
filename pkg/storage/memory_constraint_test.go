// Tests for constraint enforcement in storage operations.
package storage

import (
	"strings"
	"testing"
)

func TestBulkCreateNodesConstraintEnforcement(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")

	// Add unique constraint
	err := engine.GetSchema().AddUniqueConstraint("unique_email", "User", "email")
	if err != nil {
		t.Fatalf("Failed to add constraint: %v", err)
	}

	t.Run("bulk create enforces unique constraint", func(t *testing.T) {
		// Create first node
		node1 := &Node{
			ID:         "user-1",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"email": "alice@example.com", "name": "Alice"},
		}
		_, err := engine.CreateNode(node1)
		if err != nil {
			t.Fatalf("Failed to create first node: %v", err)
		}

		// Try bulk create with duplicate email
		nodes := []*Node{
			{
				ID:         "user-2",
				Labels:     []string{"User"},
				Properties: map[string]interface{}{"email": "bob@example.com", "name": "Bob"},
			},
			{
				ID:         "user-3",
				Labels:     []string{"User"},
				Properties: map[string]interface{}{"email": "alice@example.com", "name": "Alice Clone"}, // duplicate!
			},
		}

		err = engine.BulkCreateNodes(nodes)
		if err == nil {
			t.Fatal("Expected constraint violation error, got nil")
		}

		if !strings.Contains(err.Error(), "constraint violation") {
			t.Errorf("Expected constraint violation error, got: %v", err)
		}

		// Verify no nodes were created (atomic - all or nothing)
		_, err = engine.GetNode("user-2")
		if err == nil {
			t.Error("Node user-2 should not exist after failed bulk create")
		}
	})

	t.Run("bulk create registers unique values", func(t *testing.T) {
		// Create a new engine for this test
		base2 := NewMemoryEngine()
		defer base2.Close()
		engine2 := NewNamespacedEngine(base2, "test")

		err := engine2.GetSchema().AddUniqueConstraint("unique_email", "User", "email")
		if err != nil {
			t.Fatalf("Failed to add constraint: %v", err)
		}

		// Bulk create some nodes
		nodes := []*Node{
			{
				ID:         "user-1",
				Labels:     []string{"User"},
				Properties: map[string]interface{}{"email": "a@test.com"},
			},
			{
				ID:         "user-2",
				Labels:     []string{"User"},
				Properties: map[string]interface{}{"email": "b@test.com"},
			},
		}

		err = engine2.BulkCreateNodes(nodes)
		if err != nil {
			t.Fatalf("Bulk create should succeed: %v", err)
		}

		// Now try to create a node with duplicate email
		node := &Node{
			ID:         "user-3",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"email": "a@test.com"},
		}

		_, err = engine2.CreateNode(node)
		if err == nil {
			t.Fatal("Expected constraint violation error when creating duplicate after bulk create")
		}

		if !strings.Contains(err.Error(), "constraint violation") {
			t.Errorf("Expected constraint violation error, got: %v", err)
		}
	})

	t.Run("bulk create with no constraints succeeds", func(t *testing.T) {
		base3 := NewMemoryEngine()
		defer base3.Close()
		engine3 := NewNamespacedEngine(base3, "test")

		// No constraints - bulk create should work with any data
		nodes := []*Node{
			{
				ID:         "node-1",
				Labels:     []string{"Test"},
				Properties: map[string]interface{}{"value": "a"},
			},
			{
				ID:         "node-2",
				Labels:     []string{"Test"},
				Properties: map[string]interface{}{"value": "a"}, // same value is OK
			},
		}

		err := engine3.BulkCreateNodes(nodes)
		if err != nil {
			t.Fatalf("Bulk create without constraints should succeed: %v", err)
		}
	})
}
