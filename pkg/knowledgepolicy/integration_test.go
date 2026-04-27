package knowledgepolicy

import (
	"testing"
	"time"
)

func TestIntegration_DDL_Score_Visibility(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"episode_decay": {
			Name:                "episode_decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
		"fact_nodecay": {
			Name:      "fact_nodecay",
			Scope:     ScopeNode,
			Function:  DecayFunctionNone,
			ScoreFrom: ScoreFromCreated,
			Enabled:   true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_episode": {
			Name:         "bind_episode",
			ProfileRef:   "episode_decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
		"bind_fact": {
			Name:         "bind_fact",
			ProfileRef:   "fact_nodecay",
			TargetLabels: []string{"KnowledgeFact"},
			NoDecay:      true,
		},
	}

	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	scorer := NewScorer(NewResolver(bt, nil), true)
	now := time.Now().UnixNano()

	t.Run("OldEpisodeSuppressed", func(t *testing.T) {
		input := NodeScoringInput{
			EntityID:       "ns:old_episode",
			Labels:         []string{"MemoryEpisode"},
			CreatedAtNanos: time.Now().Add(-720 * time.Hour).UnixNano(),
		}
		suppress, res := ShouldSuppressNode(scorer, input, nil, now)
		if !suppress {
			t.Errorf("expected suppression, score=%.6f, threshold=%.2f", res.FinalScore, res.EffectiveThreshold)
		}
	})

	t.Run("RecentEpisodeVisible", func(t *testing.T) {
		input := NodeScoringInput{
			EntityID:       "ns:recent_episode",
			Labels:         []string{"MemoryEpisode"},
			CreatedAtNanos: time.Now().Add(-5 * time.Minute).UnixNano(),
		}
		suppress, _ := ShouldSuppressNode(scorer, input, nil, now)
		if suppress {
			t.Error("recent episode should not be suppressed")
		}
	})

	t.Run("KnowledgeFactAlwaysVisible", func(t *testing.T) {
		input := NodeScoringInput{
			EntityID:       "ns:old_fact",
			Labels:         []string{"KnowledgeFact"},
			CreatedAtNanos: time.Now().Add(-8760 * time.Hour).UnixNano(),
		}
		suppress, res := ShouldSuppressNode(scorer, input, nil, now)
		if suppress {
			t.Errorf("fact should never be suppressed, score=%.6f", res.FinalScore)
		}
		if !res.NoDecay {
			t.Error("fact resolution should have NoDecay=true")
		}
	})

	t.Run("DecayDisabledNoSuppression", func(t *testing.T) {
		disabledScorer := NewScorer(NewResolver(bt, nil), false)
		input := NodeScoringInput{
			EntityID:       "ns:old_episode",
			Labels:         []string{"MemoryEpisode"},
			CreatedAtNanos: time.Now().Add(-720 * time.Hour).UnixNano(),
		}
		suppress, _ := ShouldSuppressNode(disabledScorer, input, nil, now)
		if suppress {
			t.Error("decay disabled scorer should not suppress")
		}
	})

	t.Run("UnmatchedLabelNotSuppressed", func(t *testing.T) {
		input := NodeScoringInput{
			EntityID:       "ns:custom",
			Labels:         []string{"CustomLabel"},
			CreatedAtNanos: time.Now().Add(-720 * time.Hour).UnixNano(),
		}
		suppress, _ := ShouldSuppressNode(scorer, input, nil, now)
		if suppress {
			t.Error("unmatched label should not be suppressed")
		}
	})

}

func TestIntegration_SuppressionLayer_NoisyCorroborationSignals(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"evidence_decay": {
			Name:                "evidence_decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.20,
			ScoreFrom:           ScoreFromCustom,
			ScoreFromProperty:   "lastCorroboratedAt",
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind_fact": {
			Name:         "bind_fact",
			ProfileRef:   "evidence_decay",
			TargetLabels: []string{"KnowledgeFact"},
		},
	}
	profiles := map[string]*PromotionProfileDef{
		"reinforced_evidence": {Name: "reinforced_evidence", Scope: ScopeNode, Multiplier: 1.25, ScoreFloor: 0.25, ScoreCap: 1.0, Enabled: true},
		"canonical_evidence":  {Name: "canonical_evidence", Scope: ScopeNode, Multiplier: 1.6, ScoreFloor: 0.45, ScoreCap: 1.0, Enabled: true},
	}
	policies := map[string]*PromotionPolicyDef{
		"corroboration_escalation": {
			Name:         "corroboration_escalation",
			TargetLabels: []string{"KnowledgeFact"},
			Enabled:      true,
			WhenClauses: []PromotionPolicyWhenClause{
				{Predicate: "n.evidenceCount >= 3 AND n.sourceAgreement >= 0.75", ProfileRef: "reinforced_evidence", Order: 0},
				{Predicate: "n.evidenceCount >= 8 AND n.sourceAgreement >= 0.90 AND n.crossSessionSupport >= 3", ProfileRef: "canonical_evidence", Order: 1},
			},
		},
	}

	bt, err := BuildBindingTable(bundles, bindings, profiles, policies)
	if err != nil {
		t.Fatal(err)
	}
	scorer := NewScorer(NewResolver(bt, nil), true)
	now := time.Now().UnixNano()
	createdAt := time.Now().Add(-72 * time.Hour).UnixNano()

	meta := &AccessMetaEntry{
		TargetID: "fact-1",
		Overflow: map[string]interface{}{
			"lastCorroboratedAt": time.Now().Add(-72 * time.Hour).UnixNano(),
		},
	}
	confCfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}
	rateCfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	for _, measurement := range []float64{0.83, 0.20, 0.78, 0.31, 0.76, 0.98} {
		ProcessKalmanMutation("sourceAgreement", measurement, confCfg, meta)
		simulateSessionGatedAccess(meta, "session-A", rateCfg)
	}

	input := NodeScoringInput{
		EntityID:       "fact-1",
		Labels:         []string{"KnowledgeFact"},
		CreatedAtNanos: createdAt,
	}
	suppressBefore, resBefore := ShouldSuppressNode(scorer, input, meta, now)
	if !suppressBefore {
		t.Fatalf("stale fact with noisy same-session corroboration should still be suppressed, score=%f threshold=%f", resBefore.FinalScore, resBefore.EffectiveThreshold)
	}
	if resBefore.AppliedPromotionPolicyName != "corroboration_escalation" {
		t.Fatalf("expected promotion policy diagnostics to resolve, got %q", resBefore.AppliedPromotionPolicyName)
	}
	if got := meta.KalmanFilters["crossSessionAccessRate"].FilteredValue; got > 1.3 {
		t.Fatalf("same-session noise should not inflate cross-session support, got %f", got)
	}

	for idx, sessionID := range []string{"session-B", "session-C", "session-D", "session-E"} {
		measurement := []float64{0.92, 0.94, 0.93, 0.95}[idx]
		ProcessKalmanMutation("sourceAgreement", measurement, confCfg, meta)
		simulateSessionGatedAccess(meta, sessionID, rateCfg)
	}
	meta.Overflow["lastCorroboratedAt"] = time.Now().Add(-10 * time.Minute).UnixNano()

	suppressAfter, resAfter := ShouldSuppressNode(scorer, input, meta, now)
	if suppressAfter {
		t.Fatalf("fresh corroboration anchor should keep the fact visible, score=%f threshold=%f", resAfter.FinalScore, resAfter.EffectiveThreshold)
	}
	if meta.KalmanFilters["sourceAgreement"].FilteredValue <= 0.8 {
		t.Fatalf("expected sustained corroboration to lift filtered agreement above 0.8, got %f", meta.KalmanFilters["sourceAgreement"].FilteredValue)
	}
	if meta.KalmanFilters["crossSessionAccessRate"].FilteredValue <= 1.4 {
		t.Fatalf("expected multiple corroborating sessions to lift cross-session support materially above same-session noise, got %f", meta.KalmanFilters["crossSessionAccessRate"].FilteredValue)
	}
}
