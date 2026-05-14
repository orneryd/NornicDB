package knowledgepolicy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin down the concrete scenarios documented in
// docs/user-guides/decay-profiles.md (the "Inverted Decay" section) and
// docs/user-guides/ebbinghaus-roynard-bootstrap.md (the "Optional:
// Inverted Memory Layer" appendix). Documentation that isn't tested is
// liable to drift; if any of these break, both the implementation and
// the docs prose need to change together.

// scoreInverseFull is the same harness as scoreInverse but accepts
// promotion profiles + policies so we can exercise the
// inverted-decay × negative-multiplier scenarios from the docs.
func scoreInverseFull(
	t *testing.T,
	bundle DecayProfileBundle,
	binding DecayProfileBinding,
	promoProfile *PromotionProfileDef,
	promoPolicy *PromotionPolicyDef,
	labels []string,
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, now int64,
) ScoringResolution {
	t.Helper()
	bundles := map[string]*DecayProfileBundle{bundle.Name: &bundle}
	bindings := map[string]*DecayProfileBinding{binding.Name: &binding}

	var profiles map[string]*PromotionProfileDef
	var policies map[string]*PromotionPolicyDef
	if promoProfile != nil {
		profiles = map[string]*PromotionProfileDef{promoProfile.Name: promoProfile}
	}
	if promoPolicy != nil {
		policies = map[string]*PromotionPolicyDef{promoPolicy.Name: promoPolicy}
	}

	bt, err := BuildBindingTable(bundles, bindings, profiles, policies)
	require.NoError(t, err)
	resolver := NewResolver(bt, nil)
	scorer := NewScorer(resolver, true)
	return scorer.ScoreNodeWithProperties("test:n", labels, nil, accessMeta, createdAt, versionAt, now)
}

// TestDocs_InverseDecayPlusNegativePromotion_FourCellTable pins the
// 4-cell scenario table in docs/user-guides/decay-profiles.md under
// "Use Case: Negative Promotion Combined With Inverted Decay". Each
// row of the documented table maps to one cell here.
func TestDocs_InverseDecayPlusNegativePromotion_FourCellTable(t *testing.T) {
	const halfLifeSec = int64(86400)                        // one-day half life
	const oneDayNanos = halfLifeSec * int64(oneSecondNanos) // 24 hours

	bundle := DecayProfileBundle{
		Name:                "consolidation_curve",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     -halfLifeSec, // negative → inverted
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromLastAccessed,
		Enabled:             true,
		DecayEnabled:        true,
	}
	binding := DecayProfileBinding{
		Name:         "memory_binding",
		ProfileRef:   "consolidation_curve",
		TargetLabels: []string{"Memory"},
	}
	dampener := &PromotionProfileDef{
		Name:       "access_dampener",
		Scope:      ScopeNode,
		Multiplier: 0.5,
		ScoreFloor: 0.0,
		ScoreCap:   1.0,
		Enabled:    true,
	}
	policy := &PromotionPolicyDef{
		Name:         "hot_path_dampening",
		TargetLabels: []string{"Memory"},
		WhenClauses: []PromotionPolicyWhenClause{
			{
				Predicate:  "n.accessCount >= 5",
				ProfileRef: "access_dampener",
				Order:      0,
			},
		},
		Enabled: true,
	}

	created := int64(1_000_000_000_000)
	dayLater := created + oneDayNanos

	mkAccess := func(lastAccessedAt, accessCount int64) *AccessMetaEntry {
		return &AccessMetaEntry{
			TargetID:    "test:n",
			TargetScope: ScopeNode,
			Fixed:       AccessMetaFixedFields{LastAccessedAt: lastAccessedAt, AccessCount: accessCount},
		}
	}

	// Cell 1: just accessed, low access count.
	//   inverted-curve at age=0 → ~0.0
	//   accessCount=1 < 5 → no promotion (multiplier 1.0)
	//   final ≈ 0.0 → suppressed
	c1 := scoreInverseFull(t, bundle, binding, dampener, policy,
		[]string{"Memory"}, mkAccess(created, 1), created, 0, created)
	require.InDelta(t, 0.0, c1.FinalScore, 1e-9, "cell 1: just-accessed score must be 0")
	require.True(t, c1.SuppressionEligible, "cell 1: must be suppressed (below threshold 0.10)")

	// Cell 2: idle for one half-life, low access count.
	//   inverted-curve at age=H → 0.5
	//   accessCount=1 < 5 → no promotion
	//   final = 0.5 → visible (above threshold 0.10)
	c2 := scoreInverseFull(t, bundle, binding, dampener, policy,
		[]string{"Memory"}, mkAccess(created, 1), created, 0, dayLater)
	require.InDelta(t, 0.5, c2.FinalScore, 1e-6,
		"cell 2: 1H-idle score must be 0.5 (got %.6f)", c2.FinalScore)
	require.False(t, c2.SuppressionEligible, "cell 2: must be visible")

	// Cell 3: idle for one half-life, high access count.
	//   inverted-curve at age=H → 0.5
	//   accessCount=5 ≥ 5 → multiplier 0.5
	//   final = 0.5 * 0.5 = 0.25 → still visible (above threshold 0.10)
	c3 := scoreInverseFull(t, bundle, binding, dampener, policy,
		[]string{"Memory"}, mkAccess(created, 5), created, 0, dayLater)
	require.InDelta(t, 0.25, c3.FinalScore, 1e-6,
		"cell 3: dampened 1H-idle score must be 0.25 (got %.6f)", c3.FinalScore)
	require.False(t, c3.SuppressionEligible,
		"cell 3: 0.25 stays above threshold 0.10 (visible-but-dampened)")
	require.Equal(t, 0.5, c3.EffectiveMultiplier,
		"cell 3: dampener must be selected when accessCount ≥ 5")

	// Cell 4: idle for one week, high access count.
	//   inverted-curve at age=7H → 1 - 2^-7 ≈ 0.9921875
	//   accessCount=5 → multiplier 0.5
	//   final ≈ 0.496 → visible (well above threshold 0.10)
	weekLater := created + 7*oneDayNanos
	expectedRaw := 1.0 - math.Exp(-7.0*math.Log(2))
	expectedFinal := expectedRaw * 0.5
	c4 := scoreInverseFull(t, bundle, binding, dampener, policy,
		[]string{"Memory"}, mkAccess(created, 5), created, 0, weekLater)
	require.InDelta(t, expectedRaw, c4.BaseScore, 1e-6,
		"cell 4: pre-promotion raw score must be ~0.99 (got %.6f)", c4.BaseScore)
	require.InDelta(t, expectedFinal, c4.FinalScore, 1e-6,
		"cell 4: dampened week-idle score must be ~0.496 (got %.6f)", c4.FinalScore)
	require.False(t, c4.SuppressionEligible)
}

// TestDocs_ConsolidationCurve_LiteralValues pins the prose claims in
// docs/user-guides/decay-profiles.md "Use Case: Ebbinghaus-Roynard
// Consolidation":
//   - "scores 0.0 immediately after access — suppressed"
//   - "reaching 0.5 at one day idle"
//   - "approaching 1.0 over a week"
//   - "drops back to 0.0 the instant an access is recorded"
//
// The doc uses halfLifeSeconds=-86400 (1 day) explicitly.
func TestDocs_ConsolidationCurve_LiteralValues(t *testing.T) {
	bundle := DecayProfileBundle{
		Name:                "consolidation_curve",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     -86400,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromLastAccessed,
		Enabled:             true,
		DecayEnabled:        true,
	}
	binding := DecayProfileBinding{
		Name:         "memory_binding",
		ProfileRef:   "consolidation_curve",
		TargetLabels: []string{"Memory"},
	}
	created := int64(1_000_000_000_000)
	const day = int64(86400) * oneSecondNanos

	mkAccess := func(lastAccessedAt int64) *AccessMetaEntry {
		return &AccessMetaEntry{
			TargetID:    "test:n",
			TargetScope: ScopeNode,
			Fixed:       AccessMetaFixedFields{LastAccessedAt: lastAccessedAt, AccessCount: 1},
		}
	}

	// "scores 0.0 immediately after access"
	now := created
	res := scoreInverse(t, bundle, binding, []string{"Memory"}, mkAccess(now), created, 0, now)
	require.InDelta(t, 0.0, res.FinalScore, 1e-9)
	require.True(t, res.SuppressionEligible, "doc claims 'suppressed unless reveal()'")

	// "reaching 0.5 at one day idle"
	res = scoreInverse(t, bundle, binding, []string{"Memory"}, mkAccess(created), created, 0, created+day)
	require.InDelta(t, 0.5, res.FinalScore, 1e-6,
		"doc claim: ~0.5 at one-day idle (got %.6f)", res.FinalScore)

	// "approaching 1.0 over a week" — closed form: 1 - 2^-7 = 0.9921875 exactly.
	weekIdle := scoreInverse(t, bundle, binding, []string{"Memory"}, mkAccess(created), created, 0, created+7*day)
	require.InDelta(t, 0.9921875, weekIdle.FinalScore, 1e-9,
		"doc claim 'approaches 1.0 over a week' must equal 1 - 2^-7 = 0.9921875 (got %.9f)",
		weekIdle.FinalScore)

	// "drops back to 0.0 the instant an access is recorded" — same now as
	// last access regardless of how long the entity has been alive.
	farFuture := created + 100*day
	res = scoreInverse(t, bundle, binding, []string{"Memory"}, mkAccess(farFuture), created, 0, farFuture)
	require.InDelta(t, 0.0, res.FinalScore, 1e-9,
		"doc claim: fresh access drops score back to 0 (got %.6f)", res.FinalScore)
}

// TestDocs_InverseDecayOnEdges pins the doc claim that the inverted
// curve applies to "frequently-accessed nodes AND edges". Build an
// edge-scope binding, score it via ScoreEdgeWithProperties, and verify
// the same idle-strengthens / fresh-access-resets behavior.
func TestDocs_InverseDecayOnEdges(t *testing.T) {
	bundle := DecayProfileBundle{
		Name:                "edge_consolidation",
		Scope:               ScopeEdge,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     -86400,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromLastAccessed,
		Enabled:             true,
		DecayEnabled:        true,
	}
	binding := DecayProfileBinding{
		Name:           "edge_binding",
		ProfileRef:     "edge_consolidation",
		TargetEdgeType: "ASSOCIATES_WITH",
		IsEdge:         true,
	}
	bundles := map[string]*DecayProfileBundle{bundle.Name: &bundle}
	bindings := map[string]*DecayProfileBinding{binding.Name: &binding}
	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	require.NoError(t, err)
	resolver := NewResolver(bt, nil)
	scorer := NewScorer(resolver, true)

	const day = int64(86400) * oneSecondNanos
	created := int64(1_000_000_000_000)

	freshAccess := &AccessMetaEntry{
		TargetID:    "test:e1",
		TargetScope: ScopeEdge,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}

	// Just-accessed edge: score 0, suppressed.
	now := created
	res := scorer.ScoreEdgeWithProperties("test:e1", "ASSOCIATES_WITH", nil, freshAccess, created, 0, now)
	require.InDelta(t, 0.0, res.FinalScore, 1e-9, "edge: just-accessed score must be 0")
	require.True(t, res.SuppressionEligible, "edge: just-accessed must suppress")

	// Idle for one half-life: score 0.5.
	res = scorer.ScoreEdgeWithProperties("test:e1", "ASSOCIATES_WITH", nil, freshAccess, created, 0, created+day)
	require.InDelta(t, 0.5, res.FinalScore, 1e-6, "edge: 1H-idle score must be 0.5")
	require.False(t, res.SuppressionEligible, "edge: visible after 1H idle")
}

// TestDocs_InverseDecay_ScoreFloor_KeepsEntityVisible pins the documented
// behavior of `scoreFloor` on inverted curves. With the inverted
// exponential, the raw score at age=0 is exactly 0.0, which is strictly
// below any positive `visibilityThreshold` and therefore suppresses the
// entity the instant it's accessed. Operators who want the entity to
// stay visible — relevant but not boosted — set `scoreFloor` to a value
// at or above `visibilityThreshold`. The clamp is the LAST step in the
// final-score pipeline, so it overrides the curve's own zero.
//
// Three scenarios pin the contract:
//
//  1. floor = 0.0           → fresh access → score 0.0 → suppressed.
//  2. floor < threshold      → fresh access → score = floor → still suppressed.
//  3. floor ≥ threshold      → fresh access → score = floor → visible.
//
// The score still GROWS with idle time above the floor, exactly as it
// would without the floor — we test that too so the floor doesn't
// flatten the consolidation curve.
func TestDocs_InverseDecay_ScoreFloor_KeepsEntityVisible(t *testing.T) {
	const halfLifeSec = int64(86400)
	const day = halfLifeSec * oneSecondNanos
	const threshold = 0.10

	mkBundle := func(name string, floor float64) DecayProfileBundle {
		return DecayProfileBundle{
			Name:                name,
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     -halfLifeSec,
			VisibilityThreshold: threshold,
			ScoreFloor:          floor,
			ScoreFrom:           ScoreFromLastAccessed,
			Enabled:             true,
			DecayEnabled:        true,
		}
	}
	mkBinding := func(name, profile string) DecayProfileBinding {
		return DecayProfileBinding{
			Name: name, ProfileRef: profile,
			TargetLabels: []string{"Memory"},
		}
	}

	created := int64(1_000_000_000_000)
	freshAccess := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}

	t.Run("floor_zero_immediate_suppression", func(t *testing.T) {
		bundle := mkBundle("floor_zero", 0.0)
		binding := mkBinding("binding_floor_zero", "floor_zero")

		res := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created)
		require.InDelta(t, 0.0, res.FinalScore, 1e-12,
			"floor=0: fresh access → score exactly 0")
		require.True(t, res.SuppressionEligible,
			"floor=0: 0.0 < threshold 0.10 → suppressed")
	})

	t.Run("floor_below_threshold_still_suppressed", func(t *testing.T) {
		// floor=0.05, threshold=0.10 → clamp pins score at 0.05, but
		// 0.05 < 0.10 still trips suppression. Operators sometimes pick
		// a tiny floor for "audit visible" use cases; this test pins
		// that they still need floor >= threshold to see the entity.
		bundle := mkBundle("floor_low", 0.05)
		binding := mkBinding("binding_floor_low", "floor_low")

		res := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created)
		require.InDelta(t, 0.05, res.FinalScore, 1e-12,
			"floor=0.05: fresh access → score clamped to 0.05")
		require.True(t, res.SuppressionEligible,
			"0.05 < threshold 0.10 → still suppressed; floor must be ≥ threshold to keep entity visible")
	})

	t.Run("floor_equal_threshold_visible", func(t *testing.T) {
		// floor=0.10, threshold=0.10 → clamp pins score at 0.10. The
		// suppression check is strict (`finalScore < threshold`), so
		// equality means visible.
		bundle := mkBundle("floor_at_threshold", 0.10)
		binding := mkBinding("binding_floor_at", "floor_at_threshold")

		res := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created)
		require.InDelta(t, 0.10, res.FinalScore, 1e-12,
			"floor=0.10: fresh access → score clamped to 0.10")
		require.False(t, res.SuppressionEligible,
			"floor==threshold → strict-less suppression check fails → visible")
	})

	t.Run("floor_above_threshold_visible_with_headroom", func(t *testing.T) {
		// Operator wants the entry to stay clearly visible even
		// immediately after access. floor=0.15 → score never drops
		// below 0.15.
		bundle := mkBundle("floor_above", 0.15)
		binding := mkBinding("binding_floor_above", "floor_above")

		res := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created)
		require.InDelta(t, 0.15, res.FinalScore, 1e-12,
			"floor=0.15: fresh access → score clamped to 0.15")
		require.False(t, res.SuppressionEligible)
	})

	t.Run("floor_does_not_flatten_consolidation_curve", func(t *testing.T) {
		// At one half-life idle, the raw inverted score is 0.5 — far
		// above any reasonable floor. The clamp must be a no-op there
		// so the consolidation gradient still discriminates between
		// "just-accessed" and "long-idle" entries.
		bundle := mkBundle("floor_curve", 0.10)
		binding := mkBinding("binding_floor_curve", "floor_curve")

		// At-access (age 0): clamped to floor 0.10.
		atAccess := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created)
		require.InDelta(t, 0.10, atAccess.FinalScore, 1e-12)

		// 1 half-life idle: raw score 0.5, far above floor.
		oneHL := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created+day)
		require.InDelta(t, 0.5, oneHL.FinalScore, 1e-9,
			"floor must not stomp the curve once raw score > floor (got %.9f)",
			oneHL.FinalScore)

		// 7 half-lives idle: 1 - 2^-7 = 0.9921875, also unaffected by
		// the floor.
		weekIdle := scoreInverse(t, bundle, binding, []string{"Memory"},
			freshAccess, created, 0, created+7*day)
		require.InDelta(t, 0.9921875, weekIdle.FinalScore, 1e-9)
	})

	t.Run("floor_resets_after_each_access", func(t *testing.T) {
		// After a long idle the score is high; a fresh access drops it
		// back to the floor (not all the way to 0).
		bundle := mkBundle("floor_reset", 0.10)
		binding := mkBinding("binding_floor_reset", "floor_reset")

		oldAccess := &AccessMetaEntry{
			TargetID:    "test:n",
			TargetScope: ScopeNode,
			Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
		}
		now := created + day
		before := scoreInverse(t, bundle, binding, []string{"Memory"},
			oldAccess, created, 0, now)
		require.InDelta(t, 0.5, before.FinalScore, 1e-9,
			"pre-access (1H idle) → 0.5 (curve, above floor)")

		newAccess := &AccessMetaEntry{
			TargetID:    "test:n",
			TargetScope: ScopeNode,
			Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 2},
		}
		after := scoreInverse(t, bundle, binding, []string{"Memory"},
			newAccess, created, 0, now)
		require.InDelta(t, 0.10, after.FinalScore, 1e-12,
			"post-access → clamped at floor 0.10, NOT 0.0")
		require.False(t, after.SuppressionEligible,
			"floor protects entry from immediate visibility loss on access")
	})
}

// TestDocs_InverseLinearAndStep_DocumentedTransitions pins specific
// values from the inverse-linear docs (table in TestInverseDecay_LinearFamily
// is duplicated here because the docs assert these exact transitions
// in the "Inverted Decay" doc table).
func TestDocs_InverseLinearAndStep_TransitionPoints(t *testing.T) {
	cases := []struct {
		name     string
		fn       DecayFunction
		halfLife int64
		// (ageSeconds, expectedScore) pairs from the docs' implicit
		// shape claim ("composes with linear and step").
		points []struct {
			age      int64
			expected float64
		}
	}{
		{
			name:     "linear_inverse",
			fn:       DecayFunctionLinear,
			halfLife: 5,
			points: []struct {
				age      int64
				expected float64
			}{
				{0, 0.0},   // 1 - 1.0 = 0
				{5, 0.5},   // 1 - 0.5 = 0.5 at one half-life
				{10, 1.0},  // 1 - 0 = 1 at two half-lives
				{100, 1.0}, // saturates
			},
		},
		{
			name:     "step_inverse",
			fn:       DecayFunctionStep,
			halfLife: 5,
			points: []struct {
				age      int64
				expected float64
			}{
				{0, 0.0}, // 1 - 1 = 0
				{4, 0.0}, // pre-half-life: 1 - 1 = 0
				{5, 1.0}, // at half-life: 1 - 0 = 1
				{50, 1.0},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := DecayProfileBundle{
				Name:                tc.name,
				Scope:               ScopeNode,
				Function:            tc.fn,
				HalfLifeSeconds:     -tc.halfLife,
				VisibilityThreshold: 0.0,
				ScoreFrom:           ScoreFromLastAccessed,
				Enabled:             true,
				DecayEnabled:        true,
			}
			binding := DecayProfileBinding{
				Name: tc.name + "_binding", ProfileRef: tc.name,
				TargetLabels: []string{"Memory"},
			}

			created := int64(1_000_000_000_000)
			access := &AccessMetaEntry{
				TargetID: "test:n", TargetScope: ScopeNode,
				Fixed: AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
			}
			for _, p := range tc.points {
				now := created + p.age*oneSecondNanos
				res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)
				require.InDelta(t, p.expected, res.FinalScore, 1e-6,
					"%s at age=%ds (expected %.4f, got %.6f)",
					tc.name, p.age, p.expected, res.FinalScore)
			}
		})
	}
}
