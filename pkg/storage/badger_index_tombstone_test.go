package storage

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestIndexTombstone_WriteAndProbe(t *testing.T) {
	eng := newTestEngine(t)

	origKey := labelIndexKey("person", 1)
	if err := eng.SetIndexTombstones([][]byte{origKey}, true); err != nil {
		t.Fatal(err)
	}

	eng.withView(func(txn *badger.Txn) error {
		if !hasIndexTombstone(txn, origKey) {
			t.Error("expected tombstone to exist")
		}
		if hasIndexTombstone(txn, labelIndexKey("person", 2)) {
			t.Error("tombstone should not exist for different node")
		}
		return nil
	})
}

func TestIndexTombstone_Delete(t *testing.T) {
	eng := newTestEngine(t)

	origKey := labelIndexKey("person", 1)
	if err := eng.SetIndexTombstones([][]byte{origKey}, true); err != nil {
		t.Fatal(err)
	}
	if err := eng.SetIndexTombstones([][]byte{origKey}, false); err != nil {
		t.Fatal(err)
	}

	eng.withView(func(txn *badger.Txn) error {
		if hasIndexTombstone(txn, origKey) {
			t.Error("tombstone should be deleted")
		}
		return nil
	})
}

// tombstoneLabelIndexEntry tombstones the label-index entry of a node that the
// case has already created. Every tombstone visibility case shares this write;
// the caller supplies its label and identity and keeps its own seed population,
// RevealAll toggle and read assertions.
func tombstoneLabelIndexEntry(t *testing.T, eng *BadgerEngine, label string, id NodeID) {
	t.Helper()

	tombstoneKey := eng.labelIndexKeyStringLookup(label, id)
	if tombstoneKey == nil {
		t.Fatalf("missing numID for %s", id)
	}
	if err := eng.SetIndexTombstones([][]byte{tombstoneKey}, true); err != nil {
		t.Fatal(err)
	}
}

func TestIndexTombstone_LabelIndexSkipsTombstoned(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node1 := &Node{
		ID:         "nornic:visible",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &Node{
		ID:         "nornic:hidden",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	if _, err := eng.CreateNode(node1); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.CreateNode(node2); err != nil {
		t.Fatal(err)
	}

	tombstoneLabelIndexEntry(t, eng, "Person", node2.ID)

	nodes, err := eng.GetNodesByLabel("Person")
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range nodes {
		if string(n.ID) == "nornic:hidden" {
			t.Error("tombstoned node should be skipped in label index scan")
		}
	}
}

func TestIndexTombstone_RevealAllBypassesTombstone(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		ID:         "nornic:revealed",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Charlie"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	tombstoneLabelIndexEntry(t, eng, "Person", node.ID)

	eng.SetRevealAll(true)
	defer eng.SetRevealAll(false)

	nodes, err := eng.GetNodesByLabel("Person")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, n := range nodes {
		if string(n.ID) == "nornic:revealed" {
			found = true
		}
	}
	if !found {
		t.Error("revealAll should bypass tombstone")
	}
}

func TestIndexTombstone_EmptyWriteNoError(t *testing.T) {
	eng := newTestEngine(t)
	if err := eng.SetIndexTombstones(nil, true); err != nil {
		t.Fatal(err)
	}
	if err := eng.SetIndexTombstones(nil, false); err != nil {
		t.Fatal(err)
	}
}

func TestIndexTombstone_ForEachNodeIDSkipsTombstoned(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	for _, id := range []string{"nornic:a", "nornic:b"} {
		node := &Node{
			ID:         NodeID(id),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{"name": id},
		}
		if _, err := eng.CreateNode(node); err != nil {
			t.Fatal(err)
		}
	}

	tombstoneLabelIndexEntry(t, eng, "Item", "nornic:b")

	var visited []string
	eng.ForEachNodeIDByLabel("Item", func(id NodeID) bool {
		visited = append(visited, string(id))
		return true
	})

	for _, v := range visited {
		if v == "nornic:b" {
			t.Error("tombstoned node should be skipped in ForEachNodeIDByLabel")
		}
	}
}
