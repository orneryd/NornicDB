package search

import "testing"

func TestFilterDecayedCandidates_NilFilter(t *testing.T) {
	s := &Service{}
	input := []indexResult{
		{ID: "a", Score: 0.9},
		{ID: "b", Score: 0.8},
	}
	out := s.filterDecayedCandidates(input)
	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d", len(out))
	}
}

func TestFilterDecayedCandidates_SuppressSome(t *testing.T) {
	s := &Service{}
	s.nodeDecayFilter = func(nodeID string) bool {
		return nodeID == "decayed"
	}

	input := []indexResult{
		{ID: "keep1", Score: 0.9},
		{ID: "decayed", Score: 0.85},
		{ID: "keep2", Score: 0.7},
	}
	out := s.filterDecayedCandidates(input)
	if len(out) != 2 {
		t.Fatalf("expected 2 results, got %d", len(out))
	}
	if out[0].ID != "keep1" || out[1].ID != "keep2" {
		t.Errorf("unexpected result IDs: %v, %v", out[0].ID, out[1].ID)
	}
}

func TestFilterDecayedCandidates_AllSuppressed(t *testing.T) {
	s := &Service{}
	s.nodeDecayFilter = func(nodeID string) bool {
		return true
	}

	input := []indexResult{
		{ID: "a", Score: 0.9},
		{ID: "b", Score: 0.8},
	}
	out := s.filterDecayedCandidates(input)
	if len(out) != 0 {
		t.Errorf("expected 0 results, got %d", len(out))
	}
}

func TestSetNodeDecayFilter(t *testing.T) {
	s := &Service{}
	if s.nodeDecayFilter != nil {
		t.Error("expected nil filter initially")
	}

	called := false
	s.SetNodeDecayFilter(func(nodeID string) bool {
		called = true
		return false
	})

	input := []indexResult{{ID: "test", Score: 0.5}}
	s.filterDecayedCandidates(input)
	if !called {
		t.Error("expected filter function to be called")
	}
}
