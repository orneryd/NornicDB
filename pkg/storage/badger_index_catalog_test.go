package storage

import (
	"testing"
)

func TestIndexCatalog_PutAndGet(t *testing.T) {
	eng := newTestEngine(t)

	cat := &IndexEntryCatalog{
		TargetID:    "nornic:node1",
		TargetScope: "NODE",
		IndexKeys: [][]byte{
			labelIndexKey("person", "nornic:node1"),
			labelIndexKey("user", "nornic:node1"),
		},
	}

	if err := eng.PutIndexEntryCatalog("nornic:node1", cat); err != nil {
		t.Fatal(err)
	}

	got, err := eng.GetIndexEntryCatalog("nornic:node1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected catalog, got nil")
	}
	if got.TargetID != "nornic:node1" {
		t.Errorf("expected targetId=nornic:node1, got %s", got.TargetID)
	}
	if len(got.IndexKeys) != 2 {
		t.Errorf("expected 2 index keys, got %d", len(got.IndexKeys))
	}
}

func TestIndexCatalog_GetMissing(t *testing.T) {
	eng := newTestEngine(t)

	got, err := eng.GetIndexEntryCatalog("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for missing catalog")
	}
}

func TestIndexCatalog_Delete(t *testing.T) {
	eng := newTestEngine(t)

	cat := &IndexEntryCatalog{
		TargetID:    "nornic:node1",
		TargetScope: "NODE",
		IndexKeys:   [][]byte{labelIndexKey("person", "nornic:node1")},
	}
	if err := eng.PutIndexEntryCatalog("nornic:node1", cat); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeleteIndexEntryCatalog("nornic:node1"); err != nil {
		t.Fatal(err)
	}
	got, err := eng.GetIndexEntryCatalog("nornic:node1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestIndexCatalog_DeindexedFlag(t *testing.T) {
	eng := newTestEngine(t)

	cat := &IndexEntryCatalog{
		TargetID:    "nornic:node1",
		TargetScope: "NODE",
		IndexKeys:   [][]byte{labelIndexKey("person", "nornic:node1")},
		Deindexed:   true,
	}
	if err := eng.PutIndexEntryCatalog("nornic:node1", cat); err != nil {
		t.Fatal(err)
	}

	got, err := eng.GetIndexEntryCatalog("nornic:node1")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Deindexed {
		t.Error("expected Deindexed=true")
	}
}

func TestIndexCatalog_CollectNodeIndexKeys(t *testing.T) {
	keys := collectNodeIndexKeys("nornic:n1", []string{"Person", "User"})
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	for _, k := range keys {
		if k[0] != prefixLabelIndex {
			t.Errorf("expected prefix 0x%02x, got 0x%02x", prefixLabelIndex, k[0])
		}
	}
}

func TestIndexCatalog_CollectEdgeIndexKeys(t *testing.T) {
	keys := collectEdgeIndexKeys("nornic:e1", "nornic:a", "nornic:b", "KNOWS")
	if len(keys) != 4 {
		t.Fatalf("expected 4 keys, got %d", len(keys))
	}
	prefixes := map[byte]bool{
		prefixOutgoingIndex:    false,
		prefixIncomingIndex:    false,
		prefixEdgeTypeIndex:    false,
		prefixEdgeBetweenIndex: false,
	}
	for _, k := range keys {
		prefixes[k[0]] = true
	}
	for p, found := range prefixes {
		if !found {
			t.Errorf("missing prefix 0x%02x in edge index keys", p)
		}
	}
	for _, k := range keys {
		if k[0] == prefixEdgeBetweenHead {
			t.Fatal("edge head key must not be cataloged per-edge because it is shared across same-type siblings")
		}
	}
}

func TestIndexCatalog_WrittenOnCreateNode(t *testing.T) {
	eng := newTestEngine(t)

	node := &Node{
		ID:         "nornic:test_cat",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	cat, err := eng.GetIndexEntryCatalog("nornic:test_cat")
	if err != nil {
		t.Fatal(err)
	}
	if cat == nil {
		t.Fatal("expected catalog to be written on CreateNode")
	}
	if cat.TargetScope != "NODE" {
		t.Errorf("expected scope=NODE, got %s", cat.TargetScope)
	}
	if len(cat.IndexKeys) != 1 {
		t.Errorf("expected 1 index key, got %d", len(cat.IndexKeys))
	}
}

func TestIndexCatalog_DeletedOnDeleteNode(t *testing.T) {
	eng := newTestEngine(t)

	node := &Node{
		ID:         "nornic:del_cat",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	if err := eng.DeleteNode("nornic:del_cat"); err != nil {
		t.Fatal(err)
	}

	cat, err := eng.GetIndexEntryCatalog("nornic:del_cat")
	if err != nil {
		t.Fatal(err)
	}
	if cat != nil {
		t.Error("expected catalog to be deleted on DeleteNode")
	}
}
