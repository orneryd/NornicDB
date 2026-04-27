package knowledgepolicy

import (
	"testing"
	"time"
)

func BenchmarkScoreNode_Exponential(b *testing.B) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}
	bt, _ := BuildBindingTable(bundles, bindings, nil, nil)
	r := NewResolver(bt, nil)
	s := NewScorer(r, true)

	labels := []string{"User"}
	createdAt := testNow - hour

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ScoreNode("n1", labels, nil, createdAt, 0, testNow)
	}
}

func BenchmarkScoreNode_MultiLabel(b *testing.B) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Admin", "Manager", "User"}, ProfileRef: "p"},
	}
	bt, _ := BuildBindingTable(bundles, bindings, nil, nil)
	r := NewResolver(bt, nil)
	s := NewScorer(r, true)

	labels := []string{"User", "Admin", "Manager"}
	createdAt := testNow - hour

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ScoreNode("n1", labels, nil, createdAt, 0, testNow)
	}
}

func BenchmarkScoreNode_NoDecay(b *testing.B) {
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"Permanent"}, NoDecay: true},
	}
	bt, _ := BuildBindingTable(nil, bindings, nil, nil)
	r := NewResolver(bt, nil)
	s := NewScorer(r, true)

	labels := []string{"Permanent"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ScoreNode("n1", labels, nil, testNow-hour, 0, testNow)
	}
}

func BenchmarkScoreNode_DecayDisabled(b *testing.B) {
	bundles := map[string]*DecayProfileBundle{
		"p": {Name: "p", HalfLifeSeconds: 3600, Function: DecayFunctionExponential, VisibilityThreshold: 0.10},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"User"}, ProfileRef: "p"},
	}
	bt, _ := BuildBindingTable(bundles, bindings, nil, nil)
	r := NewResolver(bt, nil)
	s := NewScorer(r, false)

	labels := []string{"User"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ScoreNode("n1", labels, nil, testNow-hour, 0, testNow)
	}
}

func BenchmarkShouldSuppressNode_NoisyCorroborationAnchor(b *testing.B) {
	bundles := map[string]*DecayProfileBundle{
		"evidence_decay": {
			Name:                "evidence_decay",
			HalfLifeSeconds:     3600,
			Function:            DecayFunctionExponential,
			VisibilityThreshold: 0.20,
			ScoreFrom:           ScoreFromCustom,
			ScoreFromProperty:   "lastCorroboratedAt",
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {Name: "b", TargetLabels: []string{"KnowledgeFact"}, ProfileRef: "evidence_decay"},
	}
	profiles := map[string]*PromotionProfileDef{
		"reinforced_evidence": {Name: "reinforced_evidence", Scope: ScopeNode, Multiplier: 1.25, ScoreFloor: 0.25, ScoreCap: 1.0, Enabled: true},
	}
	policies := map[string]*PromotionPolicyDef{
		"corroboration_escalation": {
			Name:         "corroboration_escalation",
			TargetLabels: []string{"KnowledgeFact"},
			Enabled:      true,
			WhenClauses: []PromotionPolicyWhenClause{
				{Predicate: "n.evidenceCount >= 3 AND n.sourceAgreement >= 0.75", ProfileRef: "reinforced_evidence", Order: 0},
			},
		},
	}
	bt, _ := BuildBindingTable(bundles, bindings, profiles, policies)
	r := NewResolver(bt, nil)
	s := NewScorer(r, true)

	meta := &AccessMetaEntry{TargetID: "n1", Overflow: map[string]interface{}{"lastCorroboratedAt": testNow - int64((10 * time.Minute).Nanoseconds())}}
	confCfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}
	rateCfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}
	for idx, sessionID := range []string{"session-A", "session-B", "session-C", "session-D"} {
		measurement := []float64{0.91, 0.93, 0.90, 0.94}[idx]
		ProcessKalmanMutation("sourceAgreement", measurement, confCfg, meta)
		simulateSessionGatedAccess(meta, sessionID, rateCfg)
	}

	input := NodeScoringInput{EntityID: "n1", Labels: []string{"KnowledgeFact"}, CreatedAtNanos: testNow - 72*hour}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		suppress, res := ShouldSuppressNode(s, input, meta, testNow)
		if suppress || res.FinalScore <= 0 {
			b.Fatalf("unexpected suppressed benchmark node: suppress=%v score=%f", suppress, res.FinalScore)
		}
	}
}
