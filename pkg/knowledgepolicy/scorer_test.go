package knowledgepolicy

import (
	"math"
	"testing"
)

func buildTestScorer(t *testing.T, decayEnabled bool, bundles map[string]*DecayProfileBundle, bindings map[string]*DecayProfileBinding, profiles map[string]*PromotionProfileDef, policies map[string]*PromotionPolicyDef) *Scorer {
	t.Helper()
	for _, bundle := range bundles {
		if bundle != nil && bundle.Function != DecayFunctionNone && bundle.HalfLifeSeconds > 0 && !bundle.DecayEnabled {
			bundle.DecayEnabled = true
		}
	}
	bt, err := BuildBindingTable(bundles, bindings, profiles, policies)
	if err != nil {
		t.Fatalf("BuildBindingTable: %v", err)
	}
	r := NewResolver(bt, nil)
	return NewScorer(r, decayEnabled)
}

const (
	hour    int64 = 3600 * 1e9
	testNow int64 = 1000 * hour
)

func TestScorer_DecayDisabled(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, false, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"User"}, nil, testNow-hour, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 when disabled, got %f", res.FinalScore)
	}
	if !res.NoDecay {
		t.Error("expected NoDecay when disabled")
	}
}

func TestScorer_NilBinding(t *testing.T) {
	s := buildTestScorer(t, true, nil, nil, nil, nil)

	res := s.ScoreNode("n1", []string{"Unknown"}, nil, testNow-hour, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 for nil binding, got %f", res.FinalScore)
	}
	if !res.NoDecay {
		t.Error("expected NoDecay for nil binding")
	}
}

func TestScorer_NoDecayBinding(t *testing.T) {
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Permanent"}, NoDecay: true},
	}
	s := buildTestScorer(t, true, nil, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"Permanent"}, nil, testNow-10*hour, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 for NoDecay, got %f", res.FinalScore)
	}
	if !res.NoDecay {
		t.Error("expected NoDecay")
	}
}

func TestScorer_Exponential_AgeZero(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow, 0, testNow)
	if math.Abs(res.FinalScore-1.0) > 1e-9 {
		t.Errorf("expected ~1.0 at age=0, got %f", res.FinalScore)
	}
}

func TestScorer_Exponential_AgeHalfLife(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour, 0, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("expected ~0.5 at age=halfLife, got %f", res.FinalScore)
	}
}

func TestScorer_Exponential_AgeTwoHalfLives(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-2*hour, 0, testNow)
	if math.Abs(res.FinalScore-0.25) > 1e-9 {
		t.Errorf("expected ~0.25 at age=2×halfLife, got %f", res.FinalScore)
	}
}

func TestScorer_Linear_AgeZero(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionLinear, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow, 0, testNow)
	if math.Abs(res.FinalScore-1.0) > 1e-9 {
		t.Errorf("expected 1.0, got %f", res.FinalScore)
	}
}

func TestScorer_Linear_AgeHalfLife(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionLinear, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour, 0, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("expected 0.5, got %f", res.FinalScore)
	}
}

func TestScorer_Linear_AgeTwoHalfLives(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionLinear, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-2*hour, 0, testNow)
	if res.FinalScore != 0.0 {
		t.Errorf("expected 0.0 at 2×halfLife for linear, got %f", res.FinalScore)
	}
}

func TestScorer_Step_BeforeHalfLife(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionStep, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour/2, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 before halfLife for step, got %f", res.FinalScore)
	}
}

func TestScorer_Step_AtHalfLife(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionStep, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour, 0, testNow)
	if res.FinalScore != 0.0 {
		t.Errorf("expected 0.0 at halfLife for step, got %f", res.FinalScore)
	}
}

func TestScorer_Step_UnsetHalfLifeMeansNoDecay(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 0, Function: DecayFunctionStep, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-100*hour, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 for step decay with unset halfLife, got %f", res.FinalScore)
	}
}

func TestScorer_None(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionNone, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-100*hour, 0, testNow)
	if res.FinalScore != 1.0 {
		t.Errorf("expected 1.0 for none function, got %f", res.FinalScore)
	}
}

func TestScorer_ScoreFromVersion(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01, ScoreFrom: ScoreFromVersion},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	createdAt := testNow - 10*hour
	versionAt := testNow - hour

	res := s.ScoreNode("n1", []string{"X"}, nil, createdAt, versionAt, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("VERSION: expected ~0.5 (1 hour age), got %f", res.FinalScore)
	}

	resCreated := s.ScoreNode("n1", []string{"X"}, nil, createdAt, 0, testNow)
	if math.Abs(resCreated.BaseScore-exponentialDecay(10*hour, hour)) > 1e-9 {
		t.Errorf("VERSION with 0 versionAt should fall back to createdAt")
	}
}

func TestScorer_ScoreFromCustom(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01, ScoreFrom: ScoreFromCustom, ScoreFromProperty: "publishedAt"},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	customTime := testNow - hour
	meta := &AccessMetaEntry{
		Overflow: map[string]interface{}{"publishedAt": customTime},
	}

	res := s.ScoreNode("n1", []string{"X"}, meta, testNow-10*hour, 0, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("CUSTOM: expected ~0.5 from custom timestamp, got %f", res.FinalScore)
	}

	resNoMeta := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour, 0, testNow)
	if math.Abs(resNoMeta.FinalScore-0.5) > 1e-9 {
		t.Errorf("CUSTOM with nil meta should fall back to createdAt, got %f", resNoMeta.FinalScore)
	}
}

func TestScorer_DecayFloor(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01, ScoreFloor: 0.15},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow-100*hour, 0, testNow)
	if math.Abs(res.FinalScore-0.15) > 1e-9 {
		t.Errorf("expected decay floor 0.15, got %f", res.FinalScore)
	}
	if res.BaseScore >= 0.15 {
		t.Errorf("base score should be below floor, got %f", res.BaseScore)
	}
}

func TestScorer_SuppressionEligible(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.50},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	resAbove := s.ScoreNode("n1", []string{"X"}, nil, testNow-hour/2, 0, testNow)
	if resAbove.SuppressionEligible {
		t.Error("should not be suppression-eligible above threshold")
	}

	resBelow := s.ScoreNode("n1", []string{"X"}, nil, testNow-2*hour, 0, testNow)
	if !resBelow.SuppressionEligible {
		t.Errorf("should be suppression-eligible below threshold, score=%f threshold=%f",
			resBelow.FinalScore, resBelow.EffectiveThreshold)
	}
}

func TestScorer_ThresholdAgeNanos_FastPath(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cb := bt.LookupNode("X")

	scoreAtThreshold := exponentialDecay(cb.ThresholdAgeNanos, cb.HalfLifeNanos)
	if math.Abs(scoreAtThreshold-0.10) > 1e-6 {
		t.Errorf("score at ThresholdAgeNanos should match visibility threshold: got %f, want ~0.10",
			scoreAtThreshold)
	}
}

func TestScorer_NegativeAge_Clamped(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"X"}, ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n1", []string{"X"}, nil, testNow+hour, 0, testNow)
	if math.Abs(res.FinalScore-1.0) > 1e-9 {
		t.Errorf("negative age (future creation) should clamp to 1.0, got %f", res.FinalScore)
	}
}

func TestScorer_Edge(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionLinear, VisibilityThreshold: 0.01},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", IsEdge: true, TargetEdgeType: "KNOWS", ProfileRef: "p"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreEdge("e1", "KNOWS", nil, testNow-hour, 0, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("expected 0.5 for linear at halflife, got %f", res.FinalScore)
	}
	if res.TargetScope != ScopeEdge {
		t.Errorf("expected EDGE scope, got %s", res.TargetScope)
	}
}

func TestScorer_Property(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.01, ScoreFloor: 0.05},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			TargetLabels: []string{"User"},
			ProfileRef:   "p",
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "bio", HalfLifeSeconds: 1800},
			},
		},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreProperty("n1", []string{"User"}, "bio", nil, testNow-1800*1e9, 0, testNow)
	if math.Abs(res.FinalScore-0.5) > 1e-9 {
		t.Errorf("expected ~0.5 for property at its halflife, got %f", res.FinalScore)
	}
	if res.TargetScope != ScopeProperty {
		t.Errorf("expected PROPERTY scope, got %s", res.TargetScope)
	}
}

func TestScorer_ResolutionFields(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"fast": {Name: "fast", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10, ScoreFrom: ScoreFromCreated},
	}
	bindings := map[string]*DecayProfileBinding{
		"userBinding": {Name: "userBinding", TargetLabels: []string{"User"}, ProfileRef: "fast"},
	}
	s := buildTestScorer(t, true, bundles, bindings, nil, nil)

	res := s.ScoreNode("n42", []string{"User"}, nil, testNow, 0, testNow)

	if res.TargetID != "n42" {
		t.Errorf("TargetID: expected n42, got %s", res.TargetID)
	}
	if res.ResolvedDecayProfileID != "fast" {
		t.Errorf("ProfileID: expected fast, got %s", res.ResolvedDecayProfileID)
	}
	if res.ResolvedScoreFrom != ScoreFromCreated {
		t.Errorf("ScoreFrom: expected CREATED, got %s", res.ResolvedScoreFrom)
	}
	if len(res.ResolutionSourceChain) != 1 || res.ResolutionSourceChain[0] != "userBinding" {
		t.Errorf("SourceChain: expected [userBinding], got %v", res.ResolutionSourceChain)
	}
	if res.EffectiveRate <= 0 {
		t.Error("EffectiveRate should be positive")
	}
}

func TestComputeFinalScore(t *testing.T) {
	tests := []struct {
		name       string
		base       float64
		mult       float64
		promoFloor float64
		promoCap   float64
		decayFloor float64
		want       float64
	}{
		{"no promotion", 0.5, 1.0, 0.0, 1.0, 0.0, 0.5},
		{"multiplier", 0.3, 2.0, 0.0, 1.0, 0.0, 0.6},
		{"promotion floor", 0.1, 1.0, 0.3, 1.0, 0.0, 0.3},
		{"promotion cap", 0.8, 2.0, 0.0, 0.9, 0.0, 0.9},
		{"decay floor", 0.01, 1.0, 0.0, 1.0, 0.15, 0.15},
		{"decay floor beats promo cap", 0.01, 1.0, 0.0, 0.05, 0.15, 0.15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeFinalScore(tt.base, tt.mult, tt.promoFloor, tt.promoCap, tt.decayFloor)
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}
