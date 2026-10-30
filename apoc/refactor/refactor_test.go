package refactor

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

func TestRenameLabel(t *testing.T) {
	// Reset storage
	Storage = storage.NewInMemoryStorage()

	// Create nodes with OldLabel
	node1, _ := Storage.CreateNode([]string{"OldLabel", "Other"}, map[string]interface{}{"name": "A"})
	node2, _ := Storage.CreateNode([]string{"OldLabel"}, map[string]interface{}{"name": "B"})
	Storage.CreateNode([]string{"Different"}, map[string]interface{}{"name": "C"})

	// Rename OldLabel -> NewLabel
	count := RenameLabel("OldLabel", "NewLabel")

	if count != 2 {
		t.Errorf("RenameLabel returned %d, expected 2", count)
	}

	// Verify labels were renamed
	updated1, _ := Storage.GetNode(node1.ID)
	if !hasLabel(updated1, "NewLabel") {
		t.Error("Node 1 should have NewLabel")
	}
	if hasLabel(updated1, "OldLabel") {
		t.Error("Node 1 should not have OldLabel")
	}
	if !hasLabel(updated1, "Other") {
		t.Error("Node 1 should still have Other label")
	}

	updated2, _ := Storage.GetNode(node2.ID)
	if !hasLabel(updated2, "NewLabel") {
		t.Error("Node 2 should have NewLabel")
	}
}

func TestRenameType(t *testing.T) {
	// Reset storage
	Storage = storage.NewInMemoryStorage()

	// Create nodes and relationships
	node1, _ := Storage.CreateNode([]string{"Person"}, map[string]interface{}{"name": "A"})
	node2, _ := Storage.CreateNode([]string{"Person"}, map[string]interface{}{"name": "B"})
	node3, _ := Storage.CreateNode([]string{"Person"}, map[string]interface{}{"name": "C"})

	rel1, _ := Storage.CreateRelationship(node1.ID, node2.ID, "OLD_TYPE", nil)
	rel2, _ := Storage.CreateRelationship(node2.ID, node3.ID, "OLD_TYPE", nil)
	Storage.CreateRelationship(node1.ID, node3.ID, "DIFFERENT", nil)

	// Rename OLD_TYPE -> NEW_TYPE
	count := RenameType("OLD_TYPE", "NEW_TYPE")

	if count != 2 {
		t.Errorf("RenameType returned %d, expected 2", count)
	}

	// Verify types were renamed
	updated1, _ := Storage.GetRelationship(rel1.ID)
	if updated1.Type != "NEW_TYPE" {
		t.Errorf("Rel 1 type = %s, expected NEW_TYPE", updated1.Type)
	}

	updated2, _ := Storage.GetRelationship(rel2.ID)
	if updated2.Type != "NEW_TYPE" {
		t.Errorf("Rel 2 type = %s, expected NEW_TYPE", updated2.Type)
	}
}

func TestRenameProperty(t *testing.T) {
	// Reset storage
	Storage = storage.NewInMemoryStorage()

	// Create nodes with oldProp
	node1, _ := Storage.CreateNode([]string{"Person"}, map[string]interface{}{"oldProp": "value1", "other": "x"})
	node2, _ := Storage.CreateNode([]string{"Person"}, map[string]interface{}{"oldProp": "value2"})
	Storage.CreateNode([]string{"Person"}, map[string]interface{}{"different": "y"})

	// Rename oldProp -> newProp
	count := RenameProperty("oldProp", "newProp")

	if count != 2 {
		t.Errorf("RenameProperty returned %d, expected 2", count)
	}

	// Verify properties were renamed
	updated1, _ := Storage.GetNode(node1.ID)
	if _, exists := updated1.Properties["oldProp"]; exists {
		t.Error("Node 1 should not have oldProp")
	}
	if val, exists := updated1.Properties["newProp"]; !exists || val != "value1" {
		t.Errorf("Node 1 newProp = %v, expected value1", val)
	}
	if val, exists := updated1.Properties["other"]; !exists || val != "x" {
		t.Error("Node 1 should still have 'other' property")
	}

	updated2, _ := Storage.GetNode(node2.ID)
	if val, exists := updated2.Properties["newProp"]; !exists || val != "value2" {
		t.Errorf("Node 2 newProp = %v, expected value2", val)
	}
}

func TestRenameRelProperty(t *testing.T) {
	// Reset storage
	Storage = storage.NewInMemoryStorage()

	// Create nodes and relationships with oldProp
	node1, _ := Storage.CreateNode([]string{"Person"}, nil)
	node2, _ := Storage.CreateNode([]string{"Person"}, nil)
	node3, _ := Storage.CreateNode([]string{"Person"}, nil)

	rel1, _ := Storage.CreateRelationship(node1.ID, node2.ID, "KNOWS", map[string]interface{}{"oldProp": "val1"})
	rel2, _ := Storage.CreateRelationship(node2.ID, node3.ID, "KNOWS", map[string]interface{}{"oldProp": "val2", "other": "x"})
	Storage.CreateRelationship(node1.ID, node3.ID, "KNOWS", map[string]interface{}{"different": "y"})

	// Rename oldProp -> newProp
	count := RenameRelProperty("oldProp", "newProp")

	if count != 2 {
		t.Errorf("RenameRelProperty returned %d, expected 2", count)
	}

	// Verify properties were renamed
	updated1, _ := Storage.GetRelationship(rel1.ID)
	if _, exists := updated1.Properties["oldProp"]; exists {
		t.Error("Rel 1 should not have oldProp")
	}
	if val, exists := updated1.Properties["newProp"]; !exists || val != "val1" {
		t.Errorf("Rel 1 newProp = %v, expected val1", val)
	}

	updated2, _ := Storage.GetRelationship(rel2.ID)
	if val, exists := updated2.Properties["newProp"]; !exists || val != "val2" {
		t.Errorf("Rel 2 newProp = %v, expected val2", val)
	}
	if val, exists := updated2.Properties["other"]; !exists || val != "x" {
		t.Error("Rel 2 should still have 'other' property")
	}
}

func hasLabel(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}
