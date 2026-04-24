package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

func newTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func TestAccessMeta_PutAndGet(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "node-1",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed: knowledgepolicy.AccessMetaFixedFields{
			AccessCount:    42,
			LastAccessedAt: 1000000,
		},
		LastMutatedAt: 2000000,
		MutationCount: 5,
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatalf("PutAccessMeta: %v", err)
	}

	got, err := engine.GetAccessMeta("node-1")
	if err != nil {
		t.Fatalf("GetAccessMeta: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}
	if got.TargetID != "node-1" {
		t.Errorf("TargetID: expected node-1, got %s", got.TargetID)
	}
	if got.Fixed.AccessCount != 42 {
		t.Errorf("AccessCount: expected 42, got %d", got.Fixed.AccessCount)
	}
	if got.LastMutatedAt != 2000000 {
		t.Errorf("LastMutatedAt: expected 2000000, got %d", got.LastMutatedAt)
	}
}

func TestAccessMeta_GetNotFound(t *testing.T) {
	engine := newTestEngine(t)

	got, err := engine.GetAccessMeta("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Error("expected nil for missing key")
	}
}

func TestAccessMeta_Delete(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "node-1",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed:       knowledgepolicy.AccessMetaFixedFields{AccessCount: 10},
	}
	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatal(err)
	}
	if err := engine.DeleteAccessMeta("node-1"); err != nil {
		t.Fatal(err)
	}

	got, err := engine.GetAccessMeta("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestAccessMeta_Overwrite(t *testing.T) {
	engine := newTestEngine(t)

	entry1 := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 1},
	}
	entry2 := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 99},
	}

	if err := engine.PutAccessMeta("node-1", entry1); err != nil {
		t.Fatal(err)
	}
	if err := engine.PutAccessMeta("node-1", entry2); err != nil {
		t.Fatal(err)
	}

	got, err := engine.GetAccessMeta("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Fixed.AccessCount != 99 {
		t.Errorf("expected overwritten count 99, got %d", got.Fixed.AccessCount)
	}
}

func TestAccessMeta_Scan(t *testing.T) {
	engine := newTestEngine(t)

	for _, id := range []string{"a", "b", "c"} {
		entry := &knowledgepolicy.AccessMetaEntry{
			TargetID: id,
			Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 1},
		}
		if err := engine.PutAccessMeta(id, entry); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := engine.ScanAccessMeta()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestAccessMeta_WithKalmanFilters(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "node-1",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed:       knowledgepolicy.AccessMetaFixedFields{AccessCount: 10},
		KalmanFilters: map[string]*knowledgepolicy.KalmanPropertyState{
			"confidence": {
				FilteredValue: 0.75,
				Filter: knowledgepolicy.KalmanFilterState{
					X:            0.75,
					P:            1.5,
					Q:            0.001,
					R:            50.0,
					K:            0.03,
					E:            1.0,
					LastX:        0.72,
					Observations: 20,
				},
			},
		},
		Overflow: map[string]interface{}{
			"customField": int64(42),
		},
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatal(err)
	}

	got, err := engine.GetAccessMeta("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected entry")
	}

	ks, ok := got.KalmanFilters["confidence"]
	if !ok {
		t.Fatal("expected KalmanFilters[confidence]")
	}
	if ks.FilteredValue != 0.75 {
		t.Errorf("FilteredValue: expected 0.75, got %f", ks.FilteredValue)
	}
	if ks.Filter.X != 0.75 {
		t.Errorf("Filter.X: expected 0.75, got %f", ks.Filter.X)
	}
	if ks.Filter.Observations != 20 {
		t.Errorf("Observations: expected 20, got %d", ks.Filter.Observations)
	}
}

func TestAccessMeta_WithVarianceTracker(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		KalmanFilters: map[string]*knowledgepolicy.KalmanPropertyState{
			"metric": {
				FilteredValue: 5.0,
				Filter:        knowledgepolicy.KalmanFilterState{X: 5.0, P: 1.0, Q: 0.01, R: 1.0},
				Variance: &knowledgepolicy.VarianceTrackerState{
					Window:    []float64{4.9, 5.1, 5.0, 4.8},
					WindowIdx: 3,
					Mean:      4.95,
					Variance:  0.0125,
				},
			},
		},
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatal(err)
	}

	got, err := engine.GetAccessMeta("node-1")
	if err != nil {
		t.Fatal(err)
	}

	ks := got.KalmanFilters["metric"]
	if ks.Variance == nil {
		t.Fatal("expected VarianceTracker")
	}
	if len(ks.Variance.Window) != 4 {
		t.Errorf("expected window length 4, got %d", len(ks.Variance.Window))
	}
	if ks.Variance.WindowIdx != 3 {
		t.Errorf("expected WindowIdx 3, got %d", ks.Variance.WindowIdx)
	}
}
