package knowledgepolicy

import (
	"math"
	"testing"
)

func TestSynthesizeLegacyAccessMeta_NilWhenBothZero(t *testing.T) {
	entry := SynthesizeLegacyAccessMeta("n1", 0, 0)
	if entry != nil {
		t.Error("expected nil when both accessCount and lastAccessed are zero")
	}
}

func TestSynthesizeLegacyAccessMeta_WithAccessCount(t *testing.T) {
	entry := SynthesizeLegacyAccessMeta("n1", 42, 0)
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if entry.TargetID != "n1" {
		t.Errorf("expected targetID n1, got %s", entry.TargetID)
	}
	if entry.TargetScope != ScopeNode {
		t.Errorf("expected scope NODE, got %s", entry.TargetScope)
	}
	if entry.Fixed.AccessCount != 42 {
		t.Errorf("expected accessCount 42, got %d", entry.Fixed.AccessCount)
	}
}

func TestSynthesizeLegacyAccessMeta_WithLastAccessed(t *testing.T) {
	ts := int64(1700000000000000000) // ~2023 in nanos
	entry := SynthesizeLegacyAccessMeta("n1", 0, ts)
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if entry.Fixed.LastAccessedAt != ts {
		t.Errorf("expected lastAccessedAt %d, got %d", ts, entry.Fixed.LastAccessedAt)
	}
}

func TestLegacyFallback_ProducesSameScore(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"decay": {
			Name:                "decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind": {
			Name:         "bind",
			ProfileRef:   "decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}
	bt, _ := BuildBindingTable(bundles, bindings, nil, nil)
	scorer := NewScorer(NewResolver(bt, nil), true)

	createdAt := int64(0)
	now := int64(1800 * 1e9) // half a half-life

	// Score with explicit AccessMetaEntry (simulating post-migration state)
	explicitMeta := &AccessMetaEntry{
		TargetID:    "n1",
		TargetScope: ScopeNode,
		Fixed: AccessMetaFixedFields{
			AccessCount:    10,
			LastAccessedAt: 1000000000000,
		},
	}
	_, resExplicit := ShouldSuppressNode(scorer, NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"MemoryEpisode"},
		CreatedAtNanos: createdAt,
	}, explicitMeta, now)

	// Score with legacy fields (simulating pre-migration state)
	_, resLegacy := ShouldSuppressNode(scorer, NodeScoringInput{
		EntityID:           "n1",
		Labels:             []string{"MemoryEpisode"},
		CreatedAtNanos:     createdAt,
		LegacyAccessCount:  10,
		LegacyLastAccessed: 1000000000000,
	}, nil, now)

	if math.Abs(resExplicit.FinalScore-resLegacy.FinalScore) > 0.001 {
		t.Errorf("legacy fallback should produce same score: explicit=%f, legacy=%f",
			resExplicit.FinalScore, resLegacy.FinalScore)
	}
}

func TestLegacyFallback_NoopWhenAccessMetaExists(t *testing.T) {
	bundles := map[string]*DecayProfileBundle{
		"decay": {
			Name:                "decay",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"bind": {
			Name:         "bind",
			ProfileRef:   "decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}
	bt, _ := BuildBindingTable(bundles, bindings, nil, nil)
	scorer := NewScorer(NewResolver(bt, nil), true)

	createdAt := int64(0)
	now := int64(1800 * 1e9)

	realMeta := &AccessMetaEntry{
		TargetID:    "n1",
		TargetScope: ScopeNode,
		Fixed: AccessMetaFixedFields{
			AccessCount:    99,
			LastAccessedAt: 1234567890,
		},
	}

	// When AccessMetaEntry exists, legacy fields should be ignored
	_, resWithMeta := ShouldSuppressNode(scorer, NodeScoringInput{
		EntityID:           "n1",
		Labels:             []string{"MemoryEpisode"},
		CreatedAtNanos:     createdAt,
		LegacyAccessCount:  999, // different from realMeta
		LegacyLastAccessed: 9999999999,
	}, realMeta, now)

	_, resMetaOnly := ShouldSuppressNode(scorer, NodeScoringInput{
		EntityID:       "n1",
		Labels:         []string{"MemoryEpisode"},
		CreatedAtNanos: createdAt,
	}, realMeta, now)

	if resWithMeta.FinalScore != resMetaOnly.FinalScore {
		t.Errorf("legacy fields should be ignored when accessMeta exists: withLegacy=%f, metaOnly=%f",
			resWithMeta.FinalScore, resMetaOnly.FinalScore)
	}
}
