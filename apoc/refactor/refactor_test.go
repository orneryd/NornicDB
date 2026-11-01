package refactor

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// TestRenameLabel binds one instance to a fresh backend; the helpers it calls
// answer from that instance's backend, never from a package-level default.
func TestRenameLabel(t *testing.T) {
	// Reset storage
	backend := storage.NewInMemoryStorage()
	functions := New(backend)

	// Create nodes with OldLabel
	node1, _ := backend.CreateNode([]string{"OldLabel", "Other"}, map[string]interface{}{"name": "A"})
	node2, _ := backend.CreateNode([]string{"OldLabel"}, map[string]interface{}{"name": "B"})
	backend.CreateNode([]string{"Different"}, map[string]interface{}{"name": "C"})

	// Rename OldLabel -> NewLabel
	count := functions.RenameLabel(context.Background(), "OldLabel", "NewLabel")

	if count != 2 {
		t.Errorf("RenameLabel returned %d, expected 2", count)
	}

	// Verify labels were renamed
	updated1, _ := backend.GetNode(node1.ID)
	if !hasLabel(updated1, "NewLabel") {
		t.Error("Node 1 should have NewLabel")
	}
	if hasLabel(updated1, "OldLabel") {
		t.Error("Node 1 should not have OldLabel")
	}
	if !hasLabel(updated1, "Other") {
		t.Error("Node 1 should still have Other label")
	}

	updated2, _ := backend.GetNode(node2.ID)
	if !hasLabel(updated2, "NewLabel") {
		t.Error("Node 2 should have NewLabel")
	}
}

func TestRenameType(t *testing.T) {
	// Reset storage
	backend := storage.NewInMemoryStorage()
	functions := New(backend)

	// Create nodes and relationships
	node1, _ := backend.CreateNode([]string{"Person"}, map[string]interface{}{"name": "A"})
	node2, _ := backend.CreateNode([]string{"Person"}, map[string]interface{}{"name": "B"})
	node3, _ := backend.CreateNode([]string{"Person"}, map[string]interface{}{"name": "C"})

	rel1, _ := backend.CreateRelationship(node1.ID, node2.ID, "OLD_TYPE", nil)
	rel2, _ := backend.CreateRelationship(node2.ID, node3.ID, "OLD_TYPE", nil)
	backend.CreateRelationship(node1.ID, node3.ID, "DIFFERENT", nil)

	// Rename OLD_TYPE -> NEW_TYPE
	count := functions.RenameType(context.Background(), "OLD_TYPE", "NEW_TYPE")

	if count != 2 {
		t.Errorf("RenameType returned %d, expected 2", count)
	}

	// Verify types were renamed
	updated1, _ := backend.GetRelationship(rel1.ID)
	if updated1.Type != "NEW_TYPE" {
		t.Errorf("Rel 1 type = %s, expected NEW_TYPE", updated1.Type)
	}

	updated2, _ := backend.GetRelationship(rel2.ID)
	if updated2.Type != "NEW_TYPE" {
		t.Errorf("Rel 2 type = %s, expected NEW_TYPE", updated2.Type)
	}
}

// renamedRecord names one record whose properties the shared rename kernel
// asserts after the rename ran: the old key must be gone, the new key must hold
// wantNew and every property in keep must still hold its original value.
type renamedRecord struct {
	record  string
	load    func() map[string]interface{}
	wantNew interface{}
	keep    map[string]interface{}
}

// renamePropertyCase is one property-rename scenario. seed creates that case's
// own node or relationship fixtures on a fresh backend and returns the rename
// invocation together with the records to reload and assert once it has run.
type renamePropertyCase struct {
	rename string
	oldKey string
	newKey string
	seed   func(backend *storage.InMemoryStorage) (func() int, []renamedRecord)
}

// runRenamePropertyCase drives one scenario through the shared property-rename
// kernel: the case's own fixtures, one rename call, the renamed count and then
// the old-key-gone / new-value / surviving-property assertions per record.
func runRenamePropertyCase(t *testing.T, testCase renamePropertyCase) {
	t.Helper()
	backend := storage.NewInMemoryStorage()
	rename, records := testCase.seed(backend)

	count := rename()
	if count != 2 {
		t.Errorf("%s returned %d, expected 2", testCase.rename, count)
	}

	for _, record := range records {
		properties := record.load()
		if _, exists := properties[testCase.oldKey]; exists {
			t.Errorf("%s should not have %s", record.record, testCase.oldKey)
		}
		if val, exists := properties[testCase.newKey]; !exists || val != record.wantNew {
			t.Errorf("%s %s = %v, expected %v", record.record, testCase.newKey, val, record.wantNew)
		}
		for key, want := range record.keep {
			if val, exists := properties[key]; !exists || val != want {
				t.Errorf("%s should still have %s = %v, got %v (exists %t)", record.record, key, want, val, exists)
			}
		}
	}
}

// TestRenamePropertyKernel runs the node and relationship property renames
// through that one kernel. Each subtest keeps its original test name, its
// original fixtures and its original row-level assertions.
func TestRenamePropertyKernel(t *testing.T) {
	t.Run("TestRenameProperty", func(t *testing.T) {
		runRenamePropertyCase(t, renamePropertyCase{
			rename: "RenameProperty",
			oldKey: "oldProp",
			newKey: "newProp",
			seed: func(backend *storage.InMemoryStorage) (func() int, []renamedRecord) {
				functions := New(backend)

				// Create nodes with oldProp
				node1, _ := backend.CreateNode([]string{"Person"}, map[string]interface{}{"oldProp": "value1", "other": "x"})
				node2, _ := backend.CreateNode([]string{"Person"}, map[string]interface{}{"oldProp": "value2"})
				backend.CreateNode([]string{"Person"}, map[string]interface{}{"different": "y"})

				return func() int {
						return functions.RenameProperty(context.Background(), "oldProp", "newProp")
					}, []renamedRecord{
						{
							record: "Node 1",
							load: func() map[string]interface{} {
								updated, _ := backend.GetNode(node1.ID)
								return updated.Properties
							},
							wantNew: "value1",
							keep:    map[string]interface{}{"other": "x"},
						},
						{
							record: "Node 2",
							load: func() map[string]interface{} {
								updated, _ := backend.GetNode(node2.ID)
								return updated.Properties
							},
							wantNew: "value2",
						},
					}
			},
		})
	})

	t.Run("TestRenameRelProperty", func(t *testing.T) {
		runRenamePropertyCase(t, renamePropertyCase{
			rename: "RenameRelProperty",
			oldKey: "oldProp",
			newKey: "newProp",
			seed: func(backend *storage.InMemoryStorage) (func() int, []renamedRecord) {
				functions := New(backend)

				// Create nodes and relationships with oldProp
				node1, _ := backend.CreateNode([]string{"Person"}, nil)
				node2, _ := backend.CreateNode([]string{"Person"}, nil)
				node3, _ := backend.CreateNode([]string{"Person"}, nil)

				rel1, _ := backend.CreateRelationship(node1.ID, node2.ID, "KNOWS", map[string]interface{}{"oldProp": "val1"})
				rel2, _ := backend.CreateRelationship(node2.ID, node3.ID, "KNOWS", map[string]interface{}{"oldProp": "val2", "other": "x"})
				backend.CreateRelationship(node1.ID, node3.ID, "KNOWS", map[string]interface{}{"different": "y"})

				return func() int {
						return functions.RenameRelProperty("oldProp", "newProp")
					}, []renamedRecord{
						{
							record: "Rel 1",
							load: func() map[string]interface{} {
								updated, _ := backend.GetRelationship(rel1.ID)
								return updated.Properties
							},
							wantNew: "val1",
						},
						{
							record: "Rel 2",
							load: func() map[string]interface{} {
								updated, _ := backend.GetRelationship(rel2.ID)
								return updated.Properties
							},
							wantNew: "val2",
							keep:    map[string]interface{}{"other": "x"},
						},
					}
			},
		})
	})
}

func hasLabel(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}
