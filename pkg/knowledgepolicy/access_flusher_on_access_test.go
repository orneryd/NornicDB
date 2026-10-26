package knowledgepolicy

import (
	"testing"
	"time"
)

// TestAccessFlusher_AppliesOnAccessMutations covers ON ACCESS mutation
// application for both target scopes. Each case keeps its own profile, binding,
// policy, entity metadata and post-flush assertions; only the
// accumulator/store/flusher/binding-table construction is shared through
// accessFlusherFixture.
func TestAccessFlusher_AppliesOnAccessMutations(t *testing.T) {
	nodeCreatedAt := time.Now().Add(-time.Hour).UnixNano()
	edgeCreatedAt := time.Now().Add(-time.Hour).UnixNano()

	cases := []struct {
		name      string
		namespace string
		suffix    string
		bundle    *DecayProfileBundle
		binding   *DecayProfileBinding
		policy    *PromotionPolicyDef
		meta      *mockEntityMeta
		verify    func(t *testing.T, f *accessFlusherFixture, entry *AccessMetaEntry)
	}{
		{
			name:      "NodeMeta",
			namespace: "testns",
			suffix:    "episode-1",
			bundle: &DecayProfileBundle{
				Name:                "episode_decay",
				Scope:               ScopeNode,
				Function:            DecayFunctionExponential,
				ScoreFrom:           ScoreFromCustom,
				ScoreFromProperty:   "degradeAt",
				HalfLifeSeconds:     3600,
				VisibilityThreshold: 0.10,
				Enabled:             true,
			},
			binding: &DecayProfileBinding{
				Name:         "bind_episode",
				ProfileRef:   "episode_decay",
				TargetLabels: []string{"MemoryEpisode"},
			},
			policy: &PromotionPolicyDef{
				Name:         "decay_on_access",
				TargetLabels: []string{"MemoryEpisode"},
				Enabled:      true,
				OnAccess: &PromotionPolicyOnAccess{Mutations: []OnAccessMutation{
					{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
					{Expression: "n.lastAccessedAt = timestamp()"},
					{Expression: "n.degradeAt = timestamp() - (n.accessCount * 100)"},
				}},
			},
			meta: &mockEntityMeta{
				labels:    []string{"MemoryEpisode"},
				propKeys:  []string{"title"},
				createdAt: nodeCreatedAt,
				versionAt: nodeCreatedAt,
			},
			verify: func(t *testing.T, f *accessFlusherFixture, entry *AccessMetaEntry) {
				t.Helper()

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

				// Second access increments once more through the same ON ACCESS
				// expression; the node access count must not be double-merged
				// with the flushed delta.
				f.flushAccesses("episode-1")
				if got := f.entryFor(t, "episode-1").Fixed.AccessCount; got != 2 {
					t.Fatalf("expected accessCount=2 after second access, got %d", got)
				}
			},
		},
		{
			name:      "EdgeMeta",
			namespace: "testns",
			suffix:    "edge-1",
			bundle: &DecayProfileBundle{
				Name:                "edge_decay",
				Scope:               ScopeEdge,
				Function:            DecayFunctionExponential,
				ScoreFrom:           ScoreFromCustom,
				ScoreFromProperty:   "edgePenaltyAt",
				HalfLifeSeconds:     3600,
				VisibilityThreshold: 0.10,
				Enabled:             true,
			},
			binding: &DecayProfileBinding{
				Name:           "bind_edge",
				ProfileRef:     "edge_decay",
				TargetEdgeType: "REFERENCES",
				IsEdge:         true,
				PropertyRules: []DecayProfilePropertyRule{
					{PropertyPath: "weight", NoDecay: true},
				},
			},
			policy: &PromotionPolicyDef{
				Name:           "edge_decay_on_access",
				TargetEdgeType: "REFERENCES",
				IsEdge:         true,
				Enabled:        true,
				OnAccess: &PromotionPolicyOnAccess{Mutations: []OnAccessMutation{
					{Expression: "r.accessCount = coalesce(r.accessCount, 0) + 1"},
					{Expression: "r.edgePenaltyAt = timestamp() - 5000"},
				}},
			},
			meta: &mockEntityMeta{
				scope:     ScopeEdge,
				edgeType:  "REFERENCES",
				propKeys:  []string{"summary", "weight"},
				createdAt: edgeCreatedAt,
				versionAt: edgeCreatedAt,
			},
			verify: func(t *testing.T, f *accessFlusherFixture, entry *AccessMetaEntry) {
				t.Helper()

				if entry.TargetScope != ScopeEdge {
					t.Fatalf("expected edge scope, got %s", entry.TargetScope)
				}
				if entry.Fixed.AccessCount != 1 {
					t.Fatalf("expected edge accessCount=1, got %d", entry.Fixed.AccessCount)
				}
				if _, ok := entry.Overflow["edgePenaltyAt"]; !ok {
					t.Fatal("expected edgePenaltyAt written by ON ACCESS")
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFlusherFixture(t, tc.bundle, tc.binding, tc.policy, tc.meta)
			// Keep the original namespaced identifiers so the persisted
			// target id is asserted exactly as before.
			f.namespace = tc.namespace
			f.install()

			f.flushAccesses(tc.suffix)
			entry := f.entryFor(t, tc.suffix)
			if entry.TargetID != f.entityID(tc.suffix) {
				t.Fatalf("expected target id %s, got %s", f.entityID(tc.suffix), entry.TargetID)
			}
			tc.verify(t, f, entry)
		})
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

func TestEvalOnAccessExpression_MissingNumericComparisonIsFalse(t *testing.T) {
	ctx := onAccessEvalContext{
		entry:    &AccessMetaEntry{Overflow: map[string]interface{}{}},
		nowNanos: 1234,
		params:   map[string]interface{}{},
	}
	value, err := evalOnAccessExpression("n.crossSessionSupport >= 3 AND n.sourceAgreement >= 0.90", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if truthy(value) {
		t.Fatalf("expected missing numeric comparisons to be false, got %#v", value)
	}
}
