package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeindexCleanup_ProcessesPendingItems(t *testing.T) {
	eng := newTestEngine(t)

	node := &Node{
		ID:         "nornic:cleanup_node",
		Labels:     []string{"Thing"},
		Properties: map[string]interface{}{"name": "test"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	item := &DeindexWorkItem{
		WorkItemID:  "deindex:nornic:cleanup_node",
		TargetID:    "nornic:cleanup_node",
		TargetScope: "NODE",
		Status:      "pending",
	}
	if err := eng.PutDeindexWorkItem(item); err != nil {
		t.Fatal(err)
	}

	job := NewDeindexCleanupJob(eng, 0)
	n, err := job.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 deindexed, got %d", n)
	}

	cat, err := eng.GetIndexEntryCatalog("nornic:cleanup_node")
	if err != nil {
		t.Fatal(err)
	}
	if cat == nil {
		t.Fatal("expected catalog to still exist")
	}
	if !cat.Deindexed {
		t.Error("expected catalog to be marked Deindexed")
	}

	remaining, err := eng.ScanPendingDeindexWorkItems()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Errorf("expected 0 pending items after cleanup, got %d", len(remaining))
	}
}

func TestDeindexCleanup_Idempotent(t *testing.T) {
	eng := newTestEngine(t)

	node := &Node{
		ID:         "nornic:idem_node",
		Labels:     []string{"Thing"},
		Properties: map[string]interface{}{"name": "test"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	item := &DeindexWorkItem{
		WorkItemID:  "deindex:nornic:idem_node",
		TargetID:    "nornic:idem_node",
		TargetScope: "NODE",
		Status:      "pending",
	}
	if err := eng.PutDeindexWorkItem(item); err != nil {
		t.Fatal(err)
	}

	job := NewDeindexCleanupJob(eng, 0)

	n1, err := job.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 1 {
		t.Errorf("first run: expected 1, got %d", n1)
	}

	n2, err := job.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Errorf("second run: expected 0, got %d", n2)
	}
}

func TestDeindexCleanup_NoCatalog(t *testing.T) {
	eng := newTestEngine(t)

	item := &DeindexWorkItem{
		WorkItemID:  "deindex:nornic:no_catalog",
		TargetID:    "nornic:no_catalog",
		TargetScope: "NODE",
		Status:      "pending",
	}
	if err := eng.PutDeindexWorkItem(item); err != nil {
		t.Fatal(err)
	}

	job := NewDeindexCleanupJob(eng, 0)
	n, err := job.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 (no-op deindex), got %d", n)
	}

	got, err := eng.GetDeindexWorkItem("deindex:nornic:no_catalog")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("work item should be deleted even without catalog")
	}
}

func TestDeindexCleanup_EmptyQueue(t *testing.T) {
	eng := newTestEngine(t)
	job := NewDeindexCleanupJob(eng, 0)
	n, err := job.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0 on empty queue, got %d", n)
	}
}

func TestDeindexCleanupJob_DefaultIntervalAndStartStop(t *testing.T) {
	eng := newTestEngine(t)

	// Default-interval branch: interval ≤ 0 → 24h.
	j := NewDeindexCleanupJob(eng, 0)
	require.Equal(t, 24*time.Hour, j.interval)

	// Explicit interval honored.
	j2 := NewDeindexCleanupJob(eng, 50*time.Millisecond)
	j2.Start(context.Background())
	// Second Start while running is a no-op (cancel != nil branch).
	j2.Start(context.Background())
	// Stop must wait for the goroutine to exit; second Stop is a no-op.
	j2.Stop()
	j2.Stop()
}

func TestDeindexCleanupJob_RunOnce_CanceledContext(t *testing.T) {
	eng := newTestEngine(t)

	for i := 0; i < 3; i++ {
		require.NoError(t, eng.PutDeindexWorkItem(&DeindexWorkItem{
			WorkItemID:  "wi-cancel-" + string(rune('a'+i)),
			TargetID:    "nornic:cancel-" + string(rune('a'+i)),
			TargetScope: "NODE",
			Status:      "pending",
		}))
	}

	j := NewDeindexCleanupJob(eng, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := j.RunOnce(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
