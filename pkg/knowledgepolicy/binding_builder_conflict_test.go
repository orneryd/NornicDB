package knowledgepolicy

import (
	"strings"
	"testing"
)

func TestBuildBindingTable_SameKeySameOrder_Error(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p1": {Name: "p1", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"p2": {Name: "p2", HalfLifeSeconds: 7200, Function: DecayFunctionLinear, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b1": {Name: "b1", TargetLabels: []string{"User"}, ProfileRef: "p1", Order: 0},
		"b2": {Name: "b2", TargetLabels: []string{"User"}, ProfileRef: "p2", Order: 0},
	}

	_, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err == nil {
		t.Fatal("expected conflict error for same label set and same Order")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Errorf("expected conflict in error message, got: %v", err)
	}
}

func TestBuildBindingTable_SameKeyDifferentOrder_LowerWins(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"fast": {Name: "fast", HalfLifeSeconds: 1800, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"slow": {Name: "slow", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b_high": {Name: "b_high", TargetLabels: []string{"User"}, ProfileRef: "slow", Order: 10},
		"b_low":  {Name: "b_low", TargetLabels: []string{"User"}, ProfileRef: "fast", Order: 1},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cb := bt.LookupNode("User")
	if cb.HalfLifeNanos != 1800*1e9 {
		t.Errorf("expected lower-Order binding (fast, 1800s), got halflife %d", cb.HalfLifeNanos)
	}
}

func TestBuildBindingTable_MultiLabelSameKeySameOrder_Error(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p1": {Name: "p1", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"p2": {Name: "p2", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b1": {Name: "b1", TargetLabels: []string{"Recipe", "Tested"}, ProfileRef: "p1", Order: 0},
		"b2": {Name: "b2", TargetLabels: []string{"Tested", "Recipe"}, ProfileRef: "p2", Order: 0},
	}

	_, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err == nil {
		t.Fatal("expected conflict error for same sorted label set and same Order")
	}
}

func TestBuildBindingTable_PropertyRuleMissingProfileRef(t *testing.T) {
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			TargetLabels: []string{"X"},
			NoDecay:      true,
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "field", ProfileRef: "ghost"},
			},
		},
	}

	_, err := BuildBindingTable(nil, bindings, nil, nil)
	if err == nil {
		t.Fatal("expected error for missing property rule profile ref")
	}
}

func TestBuildBindingTable_EdgeConflictNotDetected(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p1": {Name: "p1", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"p2": {Name: "p2", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b1": {Name: "b1", IsEdge: true, TargetEdgeType: "KNOWS", ProfileRef: "p1"},
		"b2": {Name: "b2", IsEdge: true, TargetEdgeType: "KNOWS", ProfileRef: "p2"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatalf("edge bindings should overwrite without conflict error: %v", err)
	}
	cb := bt.LookupEdge("KNOWS")
	if cb == nil {
		t.Fatal("expected edge binding")
	}
}
