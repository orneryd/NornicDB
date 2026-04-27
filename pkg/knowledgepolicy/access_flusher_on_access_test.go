package knowledgepolicy

import (
	"testing"
	"time"
)

func TestAccessFlusher_AppliesOnAccessMutationsToNodeMeta(t *testing.T) {
	acc := NewAccessAccumulator(true, 0)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "episode_decay",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		ScoreFrom:           ScoreFromCustom,
		ScoreFromProperty:   "degradeAt",
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:         "bind_episode",
		ProfileRef:   bundle.Name,
		TargetLabels: []string{"MemoryEpisode"},
	}
	policy := &PromotionPolicyDef{
		Name:         "decay_on_access",
		TargetLabels: []string{"MemoryEpisode"},
		Enabled:      true,
		OnAccess: &PromotionPolicyOnAccess{Mutations: []OnAccessMutation{
			{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
			{Expression: "n.lastAccessedAt = timestamp()"},
			{Expression: "n.degradeAt = timestamp() - (n.accessCount * 100)"},
		}},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil,
		map[string]*PromotionPolicyDef{policy.Name: policy},
	)
	if err != nil {
		t.Fatal(err)
	}
	scorer := NewScorer(NewResolver(bt, nil), true)
	createdAt := time.Now().Add(-time.Hour).UnixNano()
	meta := &mockEntityMeta{
		labels:    []string{"MemoryEpisode"},
		propKeys:  []string{"title"},
		createdAt: createdAt,
		versionAt: createdAt,
	}
	flusher.SetPropertySuppression(func(ns string) *Scorer { return scorer }, meta, nil)

	acc.IncrementAccess("testns:episode-1")
	flusher.Flush()

	entry := store.entries["testns:episode-1"]
	if entry == nil {
		t.Fatal("expected persisted access meta entry")
	}
	if entry.Fixed.AccessCount != 1 {
		t.Fatalf("expected accessCount=1, got %d", entry.Fixed.AccessCount)
	}
	if entry.Fixed.LastAccessedAt == 0 {
		t.Fatal("expected lastAccessedAt to be set by ON ACCESS mutation")
	}
	degradeAt, ok := entry.Overflow["degradeAt"].(int64)
	if !ok {
		t.Fatalf("expected degradeAt int64, got %T", entry.Overflow["degradeAt"])
	}
	if degradeAt >= entry.Fixed.LastAccessedAt {
		t.Fatalf("expected degradeAt earlier than lastAccessedAt, got degradeAt=%d lastAccessedAt=%d", degradeAt, entry.Fixed.LastAccessedAt)
	}

	acc.IncrementAccess("testns:episode-1")
	flusher.Flush()
	if store.entries["testns:episode-1"].Fixed.AccessCount != 2 {
		t.Fatalf("expected accessCount=2 after second access, got %d", store.entries["testns:episode-1"].Fixed.AccessCount)
	}
}

func TestAccessFlusher_AppliesOnAccessMutationsToEdgeMeta(t *testing.T) {
	acc := NewAccessAccumulator(true, 0)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "edge_decay",
		Scope:               ScopeEdge,
		Function:            DecayFunctionExponential,
		ScoreFrom:           ScoreFromCustom,
		ScoreFromProperty:   "edgePenaltyAt",
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:           "bind_edge",
		ProfileRef:     bundle.Name,
		TargetEdgeType: "REFERENCES",
		IsEdge:         true,
		PropertyRules: []DecayProfilePropertyRule{
			{PropertyPath: "weight", NoDecay: true},
		},
	}
	policy := &PromotionPolicyDef{
		Name:           "edge_decay_on_access",
		TargetEdgeType: "REFERENCES",
		IsEdge:         true,
		Enabled:        true,
		OnAccess: &PromotionPolicyOnAccess{Mutations: []OnAccessMutation{
			{Expression: "r.accessCount = coalesce(r.accessCount, 0) + 1"},
			{Expression: "r.edgePenaltyAt = timestamp() - 5000"},
		}},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil,
		map[string]*PromotionPolicyDef{policy.Name: policy},
	)
	if err != nil {
		t.Fatal(err)
	}
	scorer := NewScorer(NewResolver(bt, nil), true)
	createdAt := time.Now().Add(-time.Hour).UnixNano()
	meta := &mockEntityMeta{
		scope:     ScopeEdge,
		edgeType:  "REFERENCES",
		propKeys:  []string{"summary", "weight"},
		createdAt: createdAt,
		versionAt: createdAt,
	}
	flusher.SetPropertySuppression(func(ns string) *Scorer { return scorer }, meta, nil)

	acc.IncrementAccess("testns:edge-1")
	flusher.Flush()

	entry := store.entries["testns:edge-1"]
	if entry == nil {
		t.Fatal("expected persisted edge access meta entry")
	}
	if entry.TargetScope != ScopeEdge {
		t.Fatalf("expected edge scope, got %s", entry.TargetScope)
	}
	if entry.Fixed.AccessCount != 1 {
		t.Fatalf("expected edge accessCount=1, got %d", entry.Fixed.AccessCount)
	}
	if _, ok := entry.Overflow["edgePenaltyAt"]; !ok {
		t.Fatal("expected edgePenaltyAt written by ON ACCESS")
	}
}

func TestEvalOnAccessExpression_CaseWhen(t *testing.T) {
	entry := &AccessMetaEntry{
		Overflow: map[string]interface{}{
			"_lastSessionId":      "session-a",
			"crossSessionSupport": int64(2),
		},
	}
	ctx := onAccessEvalContext{entry: entry, nowNanos: 1234, params: map[string]interface{}{"_session": "session-b"}}
	value, err := evalOnAccessExpression("CASE WHEN n._lastSessionId <> $_session THEN coalesce(n.crossSessionSupport, 0) + 1 ELSE n.crossSessionSupport END", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := toInt64(value); !ok || got != 3 {
		t.Fatalf("expected CASE result 3, got %#v", value)
	}
}
