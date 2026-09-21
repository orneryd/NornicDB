package tck

import "testing"

func TestCompareResultsUsesRowMultisets(t *testing.T) {
	expected := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}, {int64(2)}, {int64(1)}}}
	actual := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}, {int64(1)}, {int64(2)}}}
	if err := CompareResults(actual, expected, false, false); err != nil {
		t.Fatalf("CompareResults() error = %v", err)
	}

	missingDuplicate := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}, {int64(2)}}}
	if err := CompareResults(missingDuplicate, expected, false, false); err == nil {
		t.Fatal("CompareResults() accepted a missing duplicate row")
	}
}

func TestCompareResultsRetainsDeclaredOrder(t *testing.T) {
	expected := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}, {int64(2)}}}
	actual := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(2)}, {int64(1)}}}
	if err := CompareResults(actual, expected, true, false); err == nil {
		t.Fatal("CompareResults() ignored row order")
	}
}

func TestCompareResultsDistinguishesIntegersAndFloats(t *testing.T) {
	expected := QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}
	actual := QueryResult{Columns: []string{"value"}, Rows: [][]any{{float64(1)}}}
	if err := CompareResults(actual, expected, true, false); err == nil {
		t.Fatal("CompareResults() treated an integer and float as equal")
	}
}

func TestCompareResultsCanIgnoreNestedListOrder(t *testing.T) {
	expected := QueryResult{Columns: []string{"value"}, Rows: [][]any{{[]any{int64(1), []any{"a", "b"}}}}}
	actual := QueryResult{Columns: []string{"value"}, Rows: [][]any{{[]any{[]any{"b", "a"}, int64(1)}}}}
	if err := CompareResults(actual, expected, true, true); err != nil {
		t.Fatalf("CompareResults() error = %v", err)
	}
}

func TestObserveSideEffectsCountsPropertyReplacementAsRemovalAndAddition(t *testing.T) {
	before := GraphSnapshot{Nodes: []NodeValue{{
		Identity: "node-a", Labels: []string{"Shared"}, Properties: map[string]any{"name": "old"},
	}}}
	after := GraphSnapshot{Nodes: []NodeValue{{
		Identity: "node-a", Labels: []string{"Shared"}, Properties: map[string]any{"name": "new"},
	}}}
	got, err := ObserveSideEffects(before, after)
	if err != nil {
		t.Fatalf("ObserveSideEffects() error = %v", err)
	}
	if got.AddedProperties != 1 || got.RemovedProperties != 1 {
		t.Fatalf("property effects = %+v", got)
	}
}

func TestObserveSideEffectsUsesDistinctGraphLabels(t *testing.T) {
	before := GraphSnapshot{Nodes: []NodeValue{{Identity: "node-a", Labels: []string{"Shared"}}}}
	after := GraphSnapshot{Nodes: []NodeValue{
		{Identity: "node-a", Labels: []string{"Shared"}},
		{Identity: "node-b", Labels: []string{"Shared"}},
	}}
	got, err := ObserveSideEffects(before, after)
	if err != nil {
		t.Fatalf("ObserveSideEffects() error = %v", err)
	}
	if got.AddedNodes != 1 || got.AddedLabels != 0 {
		t.Fatalf("effects = %+v", got)
	}
}

func TestObserveSideEffectsTreatsCreateThenDeleteAsNoChange(t *testing.T) {
	snapshot := GraphSnapshot{Nodes: []NodeValue{{Identity: "node-a"}}}
	got, err := ObserveSideEffects(snapshot, snapshot)
	if err != nil {
		t.Fatalf("ObserveSideEffects() error = %v", err)
	}
	if got != (SideEffects{}) {
		t.Fatalf("effects = %+v, want none", got)
	}
}

func TestObserveSideEffectsRejectsUnstableIdentities(t *testing.T) {
	_, err := ObserveSideEffects(GraphSnapshot{}, GraphSnapshot{Nodes: []NodeValue{{}}})
	if err == nil {
		t.Fatal("ObserveSideEffects() accepted an empty identity")
	}
}
