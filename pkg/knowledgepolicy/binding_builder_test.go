package knowledgepolicy

import (
	"math"
	"testing"
)

func ptrFloat64(v float64) *float64 { return &v }

func TestBuildBindingTable_Empty(t *testing.T) {
	bt, err := BuildBindingTable(nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bt.NodeCount() != 0 || bt.EdgeCount() != 0 {
		t.Fatal("expected empty table")
	}
}

func TestBuildBindingTable_SingleNodeBinding(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"fast": {
			Name:                "fast",
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			Function:            DecayFunctionExponential,
			ScoreFrom:           ScoreFromCreated,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"b1": {
			Name:         "b1",
			TargetLabels: []string{"User"},
			ProfileRef:   "fast",
		},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bt.NodeCount() != 1 {
		t.Fatalf("expected 1 node binding, got %d", bt.NodeCount())
	}

	cb := bt.LookupNode("User")
	if cb == nil {
		t.Fatal("expected binding for User")
	}
	if cb.Function != DecayFunctionExponential {
		t.Errorf("expected exponential, got %s", cb.Function)
	}
	if cb.HalfLifeNanos != 3600*1e9 {
		t.Errorf("expected halflife %d, got %d", int64(3600*1e9), cb.HalfLifeNanos)
	}
	if cb.ScoreFrom != ScoreFromCreated {
		t.Errorf("expected CREATED, got %s", cb.ScoreFrom)
	}
	if cb.NoDecay {
		t.Error("expected NoDecay=false")
	}
}

func TestBuildBindingTable_ThresholdAgeNanos_Exponential(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, Function: DecayFunctionExponential},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	expected := int64(-float64(3600*1e9) * math.Log(0.10) / ln2)
	if cb.ThresholdAgeNanos != expected {
		t.Errorf("exponential threshold: expected %d, got %d", expected, cb.ThresholdAgeNanos)
	}
}

func TestBuildBindingTable_ThresholdAgeNanos_Linear(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, Function: DecayFunctionLinear},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	expected := int64((1.0 - 0.10) * float64(3600*1e9) * 2.0)
	if cb.ThresholdAgeNanos != expected {
		t.Errorf("linear threshold: expected %d, got %d", expected, cb.ThresholdAgeNanos)
	}
}

func TestBuildBindingTable_ThresholdAgeNanos_Step(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, Function: DecayFunctionStep},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	if cb.ThresholdAgeNanos != 3600*1e9 {
		t.Errorf("step threshold: expected %d, got %d", int64(3600*1e9), cb.ThresholdAgeNanos)
	}
}

func TestBuildBindingTable_ThresholdAgeNanos_None(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, Function: DecayFunctionNone},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	if cb.ThresholdAgeNanos != math.MaxInt64 {
		t.Errorf("none threshold: expected MaxInt64, got %d", cb.ThresholdAgeNanos)
	}
}

func TestBuildBindingTable_NoDecayBinding(t *testing.T) {
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Permanent"}, NoDecay: true},
	}

	bt, err := BuildBindingTable(nil, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("Permanent")
	if cb == nil {
		t.Fatal("expected binding")
	}
	if !cb.NoDecay {
		t.Error("expected NoDecay=true")
	}
}

func TestBuildBindingTable_WildcardNode(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.05},
	}
	bindings := map[string]*DecayProfileBinding{
		"w": {Name: "w", IsWildcard: true, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bt.HasWildNode() {
		t.Fatal("expected wildcard node")
	}
	cb := bt.LookupNode("AnyLabel")
	if cb == nil {
		t.Fatal("wildcard should match any label")
	}
	if cb.HalfLifeNanos != 7200*1e9 {
		t.Errorf("unexpected halflife: %d", cb.HalfLifeNanos)
	}
}

func TestBuildBindingTable_WildcardEdge(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 1800, Function: DecayFunctionLinear, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"w": {Name: "w", IsWildcard: true, IsEdge: true, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bt.HasWildEdge() {
		t.Fatal("expected wildcard edge")
	}
	cb := bt.LookupEdge("KNOWS")
	if cb == nil {
		t.Fatal("wildcard should match any edge")
	}
}

func TestBuildBindingTable_EdgeBinding(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 600, Function: DecayFunctionStep, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", IsEdge: true, TargetEdgeType: "KNOWS", ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupEdge("KNOWS")
	if cb == nil {
		t.Fatal("expected edge binding")
	}
	if cb.Function != DecayFunctionStep {
		t.Errorf("expected step, got %s", cb.Function)
	}
}

func TestBuildBindingTable_MultiLabelKey(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Recipe", "Tested"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("Recipe\x00Tested")
	if cb == nil {
		t.Fatal("expected multi-label binding")
	}
}

func TestBuildBindingTable_BindingOverridesThreshold(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p", VisibilityThreshold: ptrFloat64(0.25)},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	if cb.VisibilityThreshold != 0.25 {
		t.Errorf("expected threshold 0.25, got %f", cb.VisibilityThreshold)
	}
}

func TestBuildBindingTable_PropertyRuleExpansion(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10, ScoreFloor: 0.05},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			TargetLabels: []string{"User"},
			ProfileRef:   "p",
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "name", NoDecay: true},
				{PropertyPath: "bio", HalfLifeSeconds: 1800},
				{PropertyPath: "score", ScoreFloor: 0.20},
			},
		},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("User")
	if len(cb.CompiledPropertyRules) != 3 {
		t.Fatalf("expected 3 property rules, got %d", len(cb.CompiledPropertyRules))
	}

	nameRule := cb.CompiledPropertyRules["name"]
	if !nameRule.NoDecay {
		t.Error("name rule should be NoDecay")
	}

	bioRule := cb.CompiledPropertyRules["bio"]
	if bioRule.HalfLifeNanos != 1800*1e9 {
		t.Errorf("bio halflife: expected %d, got %d", int64(1800*1e9), bioRule.HalfLifeNanos)
	}

	scoreRule := cb.CompiledPropertyRules["score"]
	if scoreRule.DecayFloor != 0.20 {
		t.Errorf("score floor: expected 0.20, got %f", scoreRule.DecayFloor)
	}
}

func TestBuildBindingTable_MissingProfileRef(t *testing.T) {
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "nonexistent"},
	}

	_, err := BuildBindingTable(nil, bindings, nil, nil)
	if err == nil {
		t.Fatal("expected error for missing profile ref")
	}
}

func TestBuildBindingTable_MissingPromotionProfileRef(t *testing.T) {
	policies := map[string]*PromotionPolicyDef{
		"pol": {
			Name:         "pol",
			TargetLabels: []string{"X"},
			Enabled:      true,
			WhenClauses: []PromotionPolicyWhenClause{
				{ProfileRef: "nonexistent", Predicate: "n.accessCount > 10"},
			},
		},
	}

	_, err := BuildBindingTable(nil, nil, nil, policies)
	if err == nil {
		t.Fatal("expected error for missing promotion profile ref")
	}
}

func TestBuildBindingTable_PromotionPolicyAssociation(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}
	promoProfiles := map[string]*PromotionProfileDef{
		"boost": {Name: "boost", Multiplier: 2.0, ScoreFloor: 0.3, ScoreCap: 0.95, Enabled: true},
	}
	policies := map[string]*PromotionPolicyDef{
		"pol": {
			Name:         "pol",
			TargetLabels: []string{"User"},
			Enabled:      true,
			WhenClauses: []PromotionPolicyWhenClause{
				{ProfileRef: "boost", Predicate: "n.accessCount > 5"},
			},
		},
	}

	bt, err := BuildBindingTable(bundles, bindings, promoProfiles, policies)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("User")
	if cb.PromotionPolicy == nil {
		t.Fatal("expected promotion policy to be associated")
	}
	if cb.PromotionPolicy.Name != "pol" {
		t.Errorf("expected policy 'pol', got %q", cb.PromotionPolicy.Name)
	}
}

func TestBuildBindingTable_DisabledPolicyNotAssociated(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}
	policies := map[string]*PromotionPolicyDef{
		"pol": {Name: "pol", TargetLabels: []string{"User"}, Enabled: false},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, policies)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("User")
	if cb.PromotionPolicy != nil {
		t.Error("disabled policy should not be associated")
	}
}

func TestBuildBindingTable_DefaultFunctionIsExponential(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	if cb.Function != DecayFunctionExponential {
		t.Errorf("expected default function exponential, got %s", cb.Function)
	}
}

func TestBuildBindingTable_DecayFloorFromBundle(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10, ScoreFloor: 0.15},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cb := bt.LookupNode("X")
	if cb.DecayFloor != 0.15 {
		t.Errorf("expected decay floor 0.15, got %f", cb.DecayFloor)
	}
}
