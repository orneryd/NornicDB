package knowledgepolicy

import (
	"testing"
)

func buildScorerForTest(decayEnabled bool, bindings map[string]*DecayProfileBinding, bundles map[string]*DecayProfileBundle) *Scorer {
	if bundles == nil {
		bundles = make(map[string]*DecayProfileBundle)
	}
	if bindings == nil {
		bindings = make(map[string]*DecayProfileBinding)
	}
	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		panic(err)
	}
	return NewScorer(NewResolver(bt, nil), decayEnabled)
}

func TestShouldSuppressNode_DecayDisabled(t *testing.T) {
	scorer := buildScorerForTest(false, nil, nil)
	input := NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"MemoryEpisode"},
		CreatedAtNanos: 0,
	}
	suppress, res := ShouldSuppressNode(scorer, input, nil, 1e18)
	if suppress {
		t.Error("should not suppress when decay is disabled")
	}
	if res.FinalScore != 1.0 {
		t.Errorf("expected score 1.0, got %f", res.FinalScore)
	}
}

func TestShouldSuppressNode_NilScorer(t *testing.T) {
	suppress, res := ShouldSuppressNode(nil, NodeScoringInput{}, nil, 1e18)
	if suppress {
		t.Error("should not suppress with nil scorer")
	}
	if res.FinalScore != 1.0 {
		t.Errorf("expected neutral score, got %f", res.FinalScore)
	}
}

func TestShouldSuppressNode_BelowThreshold(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"episode_decay": {
			Name:                "episode_decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_episode": {
			Name:         "bind_episode",
			ProfileRef:   "episode_decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}
	scorer := buildScorerForTest(true, bindings, bundles)

	createdAt := int64(0)
	now := int64(3600 * 1e9 * 20) // 20 half-lives — score ~ 0

	input := NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"MemoryEpisode"},
		CreatedAtNanos: createdAt,
	}
	suppress, res := ShouldSuppressNode(scorer, input, nil, now)
	if !suppress {
		t.Errorf("should suppress node 20 half-lives old, score=%f", res.FinalScore)
	}
	if res.FinalScore > 0.01 {
		t.Errorf("score should be near zero, got %f", res.FinalScore)
	}
}

func TestShouldSuppressNode_AboveThreshold(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"episode_decay": {
			Name:                "episode_decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_episode": {
			Name:         "bind_episode",
			ProfileRef:   "episode_decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}
	scorer := buildScorerForTest(true, bindings, bundles)

	createdAt := int64(0)
	now := int64(100 * 1e9) // 100 seconds — well within 1 half-life

	input := NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"MemoryEpisode"},
		CreatedAtNanos: createdAt,
	}
	suppress, res := ShouldSuppressNode(scorer, input, nil, now)
	if suppress {
		t.Errorf("should not suppress recent node, score=%f", res.FinalScore)
	}
	if res.FinalScore < 0.5 {
		t.Errorf("score should be well above 0.1 for recent node, got %f", res.FinalScore)
	}
}

func TestShouldSuppressNode_NoDecayBinding(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"fact_nodecay": {
			Name:         "fact_nodecay",
			Scope:        ScopeNode,
			Function:     DecayFunctionNone,
			DecayEnabled: false,
			ScoreFrom:    ScoreFromCreated,
			Enabled:      true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_fact": {
			Name:         "bind_fact",
			ProfileRef:   "fact_nodecay",
			TargetLabels: []string{"KnowledgeFact"},
			NoDecay:      true,
		},
	}
	scorer := buildScorerForTest(true, bindings, bundles)

	input := NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"KnowledgeFact"},
		CreatedAtNanos: 0,
	}
	suppress, res := ShouldSuppressNode(scorer, input, nil, 1e18)
	if suppress {
		t.Error("should not suppress no-decay node")
	}
	if res.FinalScore != 1.0 {
		t.Errorf("no-decay score should be 1.0, got %f", res.FinalScore)
	}
}

func TestShouldSuppressNode_NoMatchingBinding(t *testing.T) {
	scorer := buildScorerForTest(true, nil, nil)
	input := NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"UnknownLabel"},
		CreatedAtNanos: 0,
	}
	suppress, res := ShouldSuppressNode(scorer, input, nil, 1e18)
	if suppress {
		t.Error("should not suppress when no binding matches")
	}
	if res.FinalScore != 1.0 {
		t.Errorf("unmatched label should get neutral score, got %f", res.FinalScore)
	}
}

func TestShouldSuppressEdge_DecayDisabled(t *testing.T) {
	scorer := buildScorerForTest(false, nil, nil)
	suppress, _ := ShouldSuppressEdge(scorer, "KNOWS", "e1", nil, 0, 1e18)
	if suppress {
		t.Error("should not suppress edge when decay is disabled")
	}
}

func TestShouldSuppressEdge_BelowThreshold(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"edge_decay": {
			Name:                "edge_decay",
			Scope:               ScopeEdge,
			Function:            DecayFunctionStep,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.50,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_edge": {
			Name:           "bind_edge",
			ProfileRef:     "edge_decay",
			IsEdge:         true,
			TargetEdgeType: "KNOWS",
		},
	}
	scorer := buildScorerForTest(true, bindings, bundles)

	createdAt := int64(0)
	now := int64(3600 * 1e9 * 2) // past half-life

	suppress, res := ShouldSuppressEdge(scorer, "KNOWS", "e1", nil, createdAt, now)
	if !suppress {
		t.Errorf("should suppress edge past step half-life, score=%f", res.FinalScore)
	}
}
