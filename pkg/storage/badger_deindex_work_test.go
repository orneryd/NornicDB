package storage

import "testing"

func TestDeindexWork_PutAndGet(t *testing.T) {
	eng := newTestEngine(t)

	item := &DeindexWorkItem{
		WorkItemID:  "deindex:nornic:node1",
		TargetID:    "nornic:node1",
		TargetScope: "NODE",
		EnqueuedAt:  1000000,
		Status:      "pending",
	}

	if err := eng.PutDeindexWorkItem(item); err != nil {
		t.Fatal(err)
	}

	got, err := eng.GetDeindexWorkItem("deindex:nornic:node1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected work item, got nil")
	}
	if got.TargetID != "nornic:node1" {
		t.Errorf("expected targetId=nornic:node1, got %s", got.TargetID)
	}
	if got.Status != "pending" {
		t.Errorf("expected status=pending, got %s", got.Status)
	}
}

func TestDeindexWork_GetMissing(t *testing.T) {
	eng := newTestEngine(t)
	got, err := eng.GetDeindexWorkItem("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for missing work item")
	}
}

func TestDeindexWork_Delete(t *testing.T) {
	eng := newTestEngine(t)

	item := &DeindexWorkItem{
		WorkItemID: "deindex:nornic:node1",
		TargetID:   "nornic:node1",
		Status:     "pending",
	}
	if err := eng.PutDeindexWorkItem(item); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeleteDeindexWorkItem("deindex:nornic:node1"); err != nil {
		t.Fatal(err)
	}
	got, err := eng.GetDeindexWorkItem("deindex:nornic:node1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestDeindexWork_ScanPending(t *testing.T) {
	eng := newTestEngine(t)

	items := []*DeindexWorkItem{
		{WorkItemID: "w1", TargetID: "nornic:a", Status: "pending"},
		{WorkItemID: "w2", TargetID: "nornic:b", Status: "completed"},
		{WorkItemID: "w3", TargetID: "nornic:c", Status: "pending"},
	}
	for _, item := range items {
		if err := eng.PutDeindexWorkItem(item); err != nil {
			t.Fatal(err)
		}
	}

	pending, err := eng.ScanPendingDeindexWorkItems()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending items, got %d", len(pending))
	}

	ids := map[string]bool{}
	for _, p := range pending {
		ids[p.WorkItemID] = true
	}
	if !ids["w1"] || !ids["w3"] {
		t.Error("expected w1 and w3 in pending items")
	}
}
