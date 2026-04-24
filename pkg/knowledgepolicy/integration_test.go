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
		},
		"fact_nodeacay": {
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

	t.Run("LegacyFallbackScoresIdentically", func(t *testing.T) {
		accessMeta := &AccessMetaEntry{
			TargetID:    "ns:legacy",
			TargetScope: ScopeNode,
			Fixed: AccessMetaFixedFields{
				AccessCount:    50,
				LastAccessedAt: time.Now().Add(-1 * time.Hour).UnixNano(),
			},
		}

		legacyMeta := SynthesizeLegacyAccessMeta(
			"ns:legacy", 50, time.Now().Add(-1*time.Hour).UnixNano(),
		)

		input := NodeScoringInput{
			EntityID:       "ns:legacy",
			Labels:         []string{"MemoryEpisode"},
			CreatedAtNanos: time.Now().Add(-48 * time.Hour).UnixNano(),
		}

		_, resExplicit := ShouldSuppressNode(scorer, input, accessMeta, now)
		_, resLegacy := ShouldSuppressNode(scorer, input, legacyMeta, now)

		if resExplicit.FinalScore != resLegacy.FinalScore {
			t.Errorf("legacy fallback score %.6f != explicit score %.6f",
				resLegacy.FinalScore, resExplicit.FinalScore)
		}
	})
}
