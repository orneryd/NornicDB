package knowledgepolicy

import (
	"testing"
)

func buildTestResolver(t *testing.T, bundles map[string]*DecayProfileBundle, bindings map[string]*DecayProfileBinding, profiles map[string]*PromotionProfileDef, policies map[string]*PromotionPolicyDef) *Resolver {
	t.Helper()
	bt, err := BuildBindingTable(bundles, bindings, profiles, policies)
	if err != nil {
		t.Fatalf("BuildBindingTable: %v", err)
	}
	return NewResolver(bt, nil)
}

func TestResolveNode_SingleLabel(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)
	cb := r.ResolveNode([]string{"User"})
	if cb == nil {
		t.Fatal("expected binding for User")
	}
	if cb.HalfLifeNanos != 3600*1e9 {
		t.Errorf("unexpected halflife: %d", cb.HalfLifeNanos)
	}
}

func TestResolveNode_ExactMultiLabel(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 1800, Function: DecayFunctionLinear, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Recipe", "Tested"}, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"Tested", "Recipe"})
	if cb == nil {
		t.Fatal("expected multi-label binding")
	}
	if cb.HalfLifeNanos != 1800*1e9 {
		t.Errorf("expected halflife %d, got %d", int64(1800*1e9), cb.HalfLifeNanos)
	}
}

func TestResolveNode_SubsetFallback(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"User", "Admin"})
	if cb == nil {
		t.Fatal("expected subset fallback to User binding")
	}
	if cb.HalfLifeNanos != 3600*1e9 {
		t.Errorf("expected fallback binding halflife, got %d", cb.HalfLifeNanos)
	}
}

func TestResolveNode_MostSpecificWins(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"single": {Name: "single", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"multi":  {Name: "multi", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b1": {Name: "b1", TargetLabels: []string{"User"}, ProfileRef: "single"},
		"b2": {Name: "b2", TargetLabels: []string{"Admin", "User"}, ProfileRef: "multi"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"User", "Admin"})
	if cb == nil {
		t.Fatal("expected binding")
	}
	if cb.HalfLifeNanos != 7200*1e9 {
		t.Errorf("expected most-specific (multi, 7200s), got halflife %d", cb.HalfLifeNanos)
	}
}

func TestResolveNode_WildcardFallback(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 600, Function: DecayFunctionStep, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"w": {Name: "w", IsWildcard: true, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"AnyLabel"})
	if cb == nil {
		t.Fatal("expected wildcard fallback")
	}
	if cb.Function != DecayFunctionStep {
		t.Errorf("expected step, got %s", cb.Function)
	}
}

func TestResolveNode_NoMatchNoWildcard(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"Product"})
	if cb != nil {
		t.Error("expected nil for no match without wildcard")
	}
}

func TestResolveNode_EmptyLabels(t *testing.T) {
	r := buildTestResolver(t, nil, nil, nil, nil)
	cb := r.ResolveNode([]string{})
	if cb != nil {
		t.Error("expected nil for empty labels")
	}
}

func TestResolveEdge(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 900, Function: DecayFunctionLinear, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", IsEdge: true, TargetEdgeType: "KNOWS", ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveEdge("KNOWS")
	if cb == nil {
		t.Fatal("expected edge binding")
	}
	if cb.Function != DecayFunctionLinear {
		t.Errorf("expected linear, got %s", cb.Function)
	}

	cb2 := r.ResolveEdge("LIKES")
	if cb2 != nil {
		t.Error("expected nil for unmatched edge")
	}
}

func TestResolveProperty_InheritsParent(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10, ScoreFloor: 0.05},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveProperty([]string{"User"}, "unknownProp")
	if cb == nil {
		t.Fatal("expected parent binding as fallback")
	}
	if cb.HalfLifeNanos != 3600*1e9 {
		t.Errorf("expected parent halflife, got %d", cb.HalfLifeNanos)
	}
}

func TestResolveProperty_Override(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10, ScoreFloor: 0.05},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			TargetLabels: []string{"User"},
			ProfileRef:   "p",
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "bio", HalfLifeSeconds: 900},
			},
		},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveProperty([]string{"User"}, "bio")
	if cb == nil {
		t.Fatal("expected property-level binding")
	}
	if cb.HalfLifeNanos != 900*1e9 {
		t.Errorf("expected overridden halflife %d, got %d", int64(900*1e9), cb.HalfLifeNanos)
	}
	if cb.CompiledPropertyRules != nil {
		t.Error("property copy should not carry nested property rules")
	}
}

func TestResolveProperty_NoDecayOverride(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			TargetLabels: []string{"User"},
			ProfileRef:   "p",
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "name", NoDecay: true},
			},
		},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveProperty([]string{"User"}, "name")
	if cb == nil {
		t.Fatal("expected property binding")
	}
	if !cb.NoDecay {
		t.Error("expected NoDecay for name property")
	}
}

func TestResolveProperty_NilBindingReturnsNil(t *testing.T) {
	r := buildTestResolver(t, nil, nil, nil, nil)
	cb := r.ResolveProperty([]string{"Missing"}, "field")
	if cb != nil {
		t.Error("expected nil for unmatched node")
	}
}

func TestResolveNode_ThreeLabels_MostSpecificWins(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p1": {Name: "p1", HalfLifeSeconds: 1000, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"p2": {Name: "p2", HalfLifeSeconds: 2000, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"p3": {Name: "p3", HalfLifeSeconds: 3000, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"single": {Name: "single", TargetLabels: []string{"A"}, ProfileRef: "p1"},
		"double": {Name: "double", TargetLabels: []string{"A", "B"}, ProfileRef: "p2"},
		"triple": {Name: "triple", TargetLabels: []string{"A", "B", "C"}, ProfileRef: "p3"},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"C", "A", "B"})
	if cb == nil {
		t.Fatal("expected triple binding")
	}
	if cb.HalfLifeNanos != 3000*1e9 {
		t.Errorf("expected triple (3000s), got %d", cb.HalfLifeNanos)
	}

	cb2 := r.ResolveNode([]string{"A", "B"})
	if cb2 == nil {
		t.Fatal("expected double binding")
	}
	if cb2.HalfLifeNanos != 2000*1e9 {
		t.Errorf("expected double (2000s), got %d", cb2.HalfLifeNanos)
	}

	cb3 := r.ResolveNode([]string{"A", "B", "D"})
	if cb3 == nil {
		t.Fatal("expected fallback to double binding")
	}
	if cb3.HalfLifeNanos != 2000*1e9 {
		t.Errorf("expected double fallback (2000s), got %d", cb3.HalfLifeNanos)
	}
}

func TestResolveNode_EqualSpecificityDifferentOrder(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"fast": {Name: "fast", HalfLifeSeconds: 1800, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
		"slow": {Name: "slow", HalfLifeSeconds: 7200, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b_a": {Name: "b_a", TargetLabels: []string{"A"}, ProfileRef: "fast", Order: 1},
		"b_b": {Name: "b_b", TargetLabels: []string{"B"}, ProfileRef: "slow", Order: 5},
	}

	r := buildTestResolver(t, bundles, bindings, nil, nil)

	cb := r.ResolveNode([]string{"A", "B"})
	if cb == nil {
		t.Fatal("expected resolution via tie-break")
	}
	if cb.HalfLifeNanos != 1800*1e9 {
		t.Errorf("expected lower-Order (fast, 1800s), got %d", cb.HalfLifeNanos)
	}
}
