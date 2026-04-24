package knowledgepolicy

import "testing"

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
