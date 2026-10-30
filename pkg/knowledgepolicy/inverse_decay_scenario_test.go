package knowledgepolicy

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Inverted (consolidation) decay: time strengthens a memory while
// access — by reading or interfering with it — weakens it. The curve is
// the mirror of the classic forgetting curve. A consolidated memory
// has had time to settle (high score). A memory recently accessed has
// been disturbed and its score is reset toward zero.
//
// The schema-level signal is a negative HalfLifeSeconds. Whatever decay
// function family the bundle picks (exponential, linear, step) is
// evaluated with |halfLife| and the result is then inverted: the
// compiled score becomes 1 - f(age, |halfLife|). No new function constant
// or modifier flag is required — the sign of the half-life carries the
// intent. Paired with ScoreFromLastAccessed, this yields a memory that
// strengthens with idle time and resets on access.
//
// These tests pin down both ingredients. They fail on engines that lack
// the negative-half-life inversion or the LAST_ACCESSED anchor.

const oneSecondNanos = int64(time.Second)

func scoreInverse(t *testing.T, bundle DecayProfileBundle, binding DecayProfileBinding,
	labels []string, accessMeta *AccessMetaEntry, createdAt, versionAt, now int64) ScoringResolution {
	t.Helper()
	bundles := map[string]*DecayProfileBundle{bundle.Name: &bundle}
	bindings := map[string]*DecayProfileBinding{binding.Name: &binding}
	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	require.NoError(t, err)
	resolver := NewResolver(bt, nil)
	scorer := NewScorer(resolver, true)
	return scorer.ScoreNodeWithProperties("test:n", labels, nil, accessMeta, createdAt, versionAt, now)
}

func inverseExpBundle(name string, halfLifeSeconds int64) DecayProfileBundle {
	return DecayProfileBundle{
		Name:                name,
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     -halfLifeSeconds, // negative → inverted
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromLastAccessed,
		Enabled:             true,
		DecayEnabled:        true,
	}
}

func TestInverseDecay_TimeStrengthens_NoAccess(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	binding := DecayProfileBinding{
		Name:         "memory_binding",
		ProfileRef:   "consolidation",
		TargetLabels: []string{"Memory"},
	}

	now := int64(1_000_000_000_000)
	created := now - 30*oneSecondNanos
	// No access recorded → anchor falls back to createdAt; 3 half-lives
	// elapsed (age=30s, halfLife=10s).
	// Closed form: score = 1 - 2^-3 = 0.875.
	res := scoreInverse(t, bundle, binding, []string{"Memory"}, nil, created, 0, now)

	require.InDelta(t, 0.875, res.FinalScore, 1e-9,
		"3-half-life inverted score must equal 1 - 2^-3 = 0.875 (got %.9f)",
		res.FinalScore)
	require.False(t, res.SuppressionEligible,
		"0.875 is well above the 0.10 threshold")
}

func TestInverseDecay_FreshAccess_WeakensScore(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	binding := DecayProfileBinding{
		Name: "memory_binding", ProfileRef: "consolidation",
		TargetLabels: []string{"Memory"},
	}

	now := int64(1_000_000_000_000)
	created := now - 30*oneSecondNanos

	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 1},
	}
	res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)

	// Closed form at age=0: score = 1 - 2^-0 = 0 exactly.
	require.InDelta(t, 0.0, res.FinalScore, 1e-12,
		"a memory accessed at now must have score exactly 0 — got %.12f",
		res.FinalScore)
	require.True(t, res.SuppressionEligible,
		"score below visibility threshold must mark suppressionEligible")
}

func TestInverseDecay_FreshAccessStartsAtScoreFloor(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	bundle.ScoreFloor = 0.20
	bundle.VisibilityThreshold = 0.10
	binding := DecayProfileBinding{
		Name:         "memory_binding",
		ProfileRef:   "consolidation",
		TargetLabels: []string{"Memory"},
	}

	now := int64(1_000_000_000_000)
	created := now - 30*oneSecondNanos
	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 1},
	}

	res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)
	require.InDelta(t, bundle.ScoreFloor, res.FinalScore, 1e-12,
		"fresh reverse decay must start at scoreFloor, not raw inverse score 0")
	require.False(t, res.SuppressionEligible,
		"scoreFloor above threshold must pass the scoring gate")
}

func TestInverseDecay_FreshAccessBelowThresholdStillStartsAtScoreFloor(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	bundle.ScoreFloor = 0.05
	bundle.VisibilityThreshold = 0.10
	binding := DecayProfileBinding{
		Name:         "memory_binding",
		ProfileRef:   "consolidation",
		TargetLabels: []string{"Memory"},
	}

	now := int64(1_000_000_000_000)
	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 1},
	}

	res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, now, 0, now)
	require.InDelta(t, bundle.ScoreFloor, res.FinalScore, 1e-12)
	require.True(t, res.SuppressionEligible,
		"scoreFloor below threshold must still fail the scoring gate")
}

func TestInverseDecay_ScoreGrowsMonotonicallyAfterAccess(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	binding := DecayProfileBinding{
		Name: "memory_binding", ProfileRef: "consolidation",
		TargetLabels: []string{"Memory"},
	}

	created := int64(1_000_000_000_000)
	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}

	// Closed form per dt: score = 1 - 2^(-dt/halfLife) with halfLife=10s.
	cases := []struct {
		dt       int64
		expected float64
	}{
		{0, 0.0},              // 1 - 1 = 0
		{5, 1 - math.Sqrt2/2}, // 1 - 2^-0.5 ≈ 0.292893
		{10, 0.5},             // 1 - 2^-1 = 0.5
		{20, 0.75},            // 1 - 2^-2 = 0.75
		{50, 0.96875},         // 1 - 2^-5
		{100, 1 - 1.0/1024.0}, // 1 - 2^-10 = 0.9990234375
	}
	prev := -1.0
	for _, tc := range cases {
		now := created + tc.dt*oneSecondNanos
		res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)
		require.InDelta(t, tc.expected, res.FinalScore, 1e-9,
			"score at dt=%ds must equal %.9f (got %.9f)",
			tc.dt, tc.expected, res.FinalScore)
		require.Greater(t, res.FinalScore, prev,
			"strict monotone growth (dt=%ds, prev=%.6f, got=%.6f)",
			tc.dt, prev, res.FinalScore)
		prev = res.FinalScore
	}
	// Asymptote check: at 10 half-lives we're at ~0.999, never reaches 1.0.
	require.Less(t, prev, 1.0,
		"score must asymptote strictly below 1.0 (got %.9f)", prev)
}

// TestInverseDecay_AccessResetsScore mirrors the Ebbinghaus principle that
// a fresh access disturbs the consolidation: the score before the new
// access is strong, and after it (with LastAccessedAt = now) drops.
func TestInverseDecay_AccessResetsScore(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 10)
	binding := DecayProfileBinding{
		Name: "memory_binding", ProfileRef: "consolidation",
		TargetLabels: []string{"Memory"},
	}

	created := int64(1_000_000_000_000)
	now := created + 60*oneSecondNanos

	oldAccess := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}
	consolidated := scoreInverse(t, bundle, binding, []string{"Memory"}, oldAccess, created, 0, now)

	freshAccess := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 2},
	}
	disturbed := scoreInverse(t, bundle, binding, []string{"Memory"}, freshAccess, created, 0, now)

	// Consolidated: age = 60s, halfLife = 10s → 1 - 2^-6 = 0.984375.
	// Disturbed: age = 0 → 0.0 exactly.
	require.InDelta(t, 0.984375, consolidated.FinalScore, 1e-9,
		"consolidated (60s idle, 10s half-life) must equal 1 - 2^-6 (got %.9f)",
		consolidated.FinalScore)
	require.InDelta(t, 0.0, disturbed.FinalScore, 1e-12,
		"freshly-accessed must equal 0.0 exactly (got %.12f)", disturbed.FinalScore)
	require.True(t, disturbed.SuppressionEligible,
		"score 0.0 is below the 0.10 threshold")
}

// TestInverseDecay_MathematicalIdentity pins the exact math of the inverse
// exponential curve: 1 - exp(-age * ln2 / |halfLife|). A miscoded sign
// flip or off-by-one in the dispatch would silently change the curve
// shape — this test catches that.
func TestInverseDecay_MathematicalIdentity(t *testing.T) {
	bundle := inverseExpBundle("consolidation", 1)
	bundle.VisibilityThreshold = 0.0 // disable suppression so we read raw scores
	binding := DecayProfileBinding{
		Name: "memory_binding", ProfileRef: "consolidation",
		TargetLabels: []string{"Memory"},
	}

	created := int64(1_000_000_000_000)
	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}

	for _, ageSec := range []int64{0, 1, 2, 5, 10} {
		now := created + ageSec*oneSecondNanos
		res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)
		expected := 1.0 - math.Exp(-float64(ageSec)*math.Log(2))
		require.InDelta(t, expected, res.FinalScore, 1e-6,
			"inverse-exp score at age=%ds must equal 1 - 2^(-age/halfLife)", ageSec)
	}
}

// TestInverseDecay_LinearFamily verifies the inversion modifier composes
// with the linear function family too: score is `1 - max(0, 1 - age/2H)`,
// i.e. it grows linearly from 0 to 1 over 2 half-lives, then plateaus.
func TestInverseDecay_LinearFamily(t *testing.T) {
	bundle := DecayProfileBundle{
		Name:                "linear_consolidation",
		Scope:               ScopeNode,
		Function:            DecayFunctionLinear,
		HalfLifeSeconds:     -5, // negative → inverted linear
		VisibilityThreshold: 0.0,
		ScoreFrom:           ScoreFromLastAccessed,
		Enabled:             true,
		DecayEnabled:        true,
	}
	binding := DecayProfileBinding{
		Name: "memory_binding", ProfileRef: "linear_consolidation",
		TargetLabels: []string{"Memory"},
	}

	created := int64(1_000_000_000_000)
	access := &AccessMetaEntry{
		TargetID:    "test:n",
		TargetScope: ScopeNode,
		Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
	}

	cases := []struct {
		ageSec   int64
		expected float64
	}{
		{0, 0.0},   // 1 - 1 = 0 at age=0
		{5, 0.5},   // 1 - 0.5 = 0.5 at one half-life
		{10, 1.0},  // 1 - 0   = 1   at two half-lives (saturates)
		{100, 1.0}, // stays saturated
	}
	for _, tc := range cases {
		now := created + tc.ageSec*oneSecondNanos
		res := scoreInverse(t, bundle, binding, []string{"Memory"}, access, created, 0, now)
		require.InDelta(t, tc.expected, res.FinalScore, 1e-6,
			"inverse-linear score at age=%ds (expected %.2f)", tc.ageSec, tc.expected)
	}
}

// TestInverseDecay_AllFunctionFamilies asserts the full contract for every
// decay function: with a NEGATIVE half-life, score=0 at age=0 (right after
// access), strictly grows with age, and access resets it back to ~0. This
// is the test the user asked for — every existing function gets a negative-
// value counterpart with both invariants pinned down.
func TestInverseDecay_AllFunctionFamilies(t *testing.T) {
	// Per-family closed-form expected values at ages {1, 5, 10, 30}s
	// with halfLife = 10s.
	//
	//   exponential:  1 - 2^(-age/halfLife)
	//   linear:       1 - max(0, 1 - age/(2*halfLife))
	//   step:         0 if age<halfLife else 1
	//
	// The 30s value also doubles as the "long-idle" probe for the
	// before-after access-reset invariant below.
	cases := []struct {
		name      string
		fn        DecayFunction
		halfLife  int64
		ages      []int64
		expected  []float64
		monotonic string // "strict" or "nondecreasing"
	}{
		{
			name:     "exponential",
			fn:       DecayFunctionExponential,
			halfLife: 10,
			ages:     []int64{1, 5, 10, 30},
			expected: []float64{
				1 - math.Exp(-0.1*math.Log(2)),
				1 - math.Sqrt2/2,
				0.5,
				0.875,
			},
			monotonic: "strict",
		},
		{
			name:     "linear",
			fn:       DecayFunctionLinear,
			halfLife: 10,
			ages:     []int64{1, 5, 10, 30},
			// 1 - max(0, 1 - age/20).
			expected:  []float64{0.05, 0.25, 0.5, 1.0},
			monotonic: "strict",
		},
		{
			name:     "step",
			fn:       DecayFunctionStep,
			halfLife: 10,
			ages:     []int64{1, 5, 10, 30},
			// Flat at 0 below halfLife (age<10), jumps to 1 at and beyond.
			expected:  []float64{0.0, 0.0, 1.0, 1.0},
			monotonic: "nondecreasing",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := DecayProfileBundle{
				Name:                "inv_" + tc.name,
				Scope:               ScopeNode,
				Function:            tc.fn,
				HalfLifeSeconds:     -tc.halfLife,
				VisibilityThreshold: 0.0, // raw scores, no suppression gating
				ScoreFrom:           ScoreFromLastAccessed,
				Enabled:             true,
				DecayEnabled:        true,
			}
			binding := DecayProfileBinding{
				Name:         "binding_" + tc.name,
				ProfileRef:   "inv_" + tc.name,
				TargetLabels: []string{"Memory"},
			}

			created := int64(1_000_000_000_000)
			freshAccess := &AccessMetaEntry{
				TargetID:    "test:n",
				TargetScope: ScopeNode,
				Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
			}

			// age=0 → score must be exactly 0 across every family.
			res := scoreInverse(t, bundle, binding, []string{"Memory"},
				freshAccess, created, 0, created)
			require.InDelta(t, 0.0, res.FinalScore, 1e-12,
				"%s: a freshly-accessed memory must score exactly 0 (got %.12f)",
				tc.name, res.FinalScore)

			// Pin the closed-form score at each age, then assert the
			// monotonicity property the family guarantees.
			scores := make([]float64, len(tc.ages))
			for i, ageSec := range tc.ages {
				now := created + ageSec*oneSecondNanos
				r := scoreInverse(t, bundle, binding, []string{"Memory"},
					freshAccess, created, 0, now)
				scores[i] = r.FinalScore
				require.InDelta(t, tc.expected[i], r.FinalScore, 1e-9,
					"%s at age=%ds: expected %.9f, got %.9f",
					tc.name, ageSec, tc.expected[i], r.FinalScore)
			}
			for i := 1; i < len(scores); i++ {
				if tc.monotonic == "strict" {
					require.Greater(t, scores[i], scores[i-1],
						"%s: strict growth violated at idx=%d (scores=%v)",
						tc.name, i, scores)
				} else {
					require.GreaterOrEqual(t, scores[i], scores[i-1],
						"%s: non-decreasing violated at idx=%d (scores=%v)",
						tc.name, i, scores)
				}
			}

			// Access-reset invariant: at age=30 the score equals the
			// last entry of the per-age expected table (which we just
			// pinned). After a fresh access at the same wall-clock,
			// score collapses back to exactly 0.
			oldAccess := &AccessMetaEntry{
				TargetID:    "test:n",
				TargetScope: ScopeNode,
				Fixed:       AccessMetaFixedFields{LastAccessedAt: created, AccessCount: 1},
			}
			now := created + 30*oneSecondNanos
			before := scoreInverse(t, bundle, binding, []string{"Memory"},
				oldAccess, created, 0, now)
			require.InDelta(t, tc.expected[len(tc.expected)-1], before.FinalScore, 1e-9,
				"%s: pre-access score must equal the 30s entry (got %.9f)",
				tc.name, before.FinalScore)

			newAccess := &AccessMetaEntry{
				TargetID:    "test:n",
				TargetScope: ScopeNode,
				Fixed:       AccessMetaFixedFields{LastAccessedAt: now, AccessCount: 2},
			}
			after := scoreInverse(t, bundle, binding, []string{"Memory"},
				newAccess, created, 0, now)
			require.InDelta(t, 0.0, after.FinalScore, 1e-12,
				"%s: a fresh access must reset score to exactly 0 (got %.12f)",
				tc.name, after.FinalScore)
		})
	}
}

// TestInverseDecay_ComposesWithAllAnchors pins down that the negative-
// half-life inversion is independent of which ScoreFrom anchor is used.
// The inversion is a property of the curve; the anchor only chooses
// which timestamp the "age" is measured from. Every supported anchor
// must produce the same family of inverted curves.
func TestInverseDecay_ComposesWithAllAnchors(t *testing.T) {
	created := int64(1_000_000_000_000)
	versionAt := created + 5*oneSecondNanos
	customAt := created + 7*oneSecondNanos
	lastAccessedAt := created + 3*oneSecondNanos
	now := created + 30*oneSecondNanos

	cases := []struct {
		name      string
		from      ScoreFromMode
		fromProp  string // for ScoreFromCustom
		expectAge int64  // expected (now - anchor) in seconds
	}{
		{"created", ScoreFromCreated, "", 30},
		{"version", ScoreFromVersion, "", 25},
		{"custom", ScoreFromCustom, "anchor", 23},
		{"last_accessed", ScoreFromLastAccessed, "", 27},
	}

	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			bundle := DecayProfileBundle{
				Name:                "compose_" + tc.name,
				Scope:               ScopeNode,
				Function:            DecayFunctionExponential,
				HalfLifeSeconds:     -10, // negative → inverted
				VisibilityThreshold: 0.0,
				ScoreFrom:           tc.from,
				ScoreFromProperty:   tc.fromProp,
				Enabled:             true,
				DecayEnabled:        true,
			}
			binding := DecayProfileBinding{
				Name:         "compose_binding_" + tc.name,
				ProfileRef:   "compose_" + tc.name,
				TargetLabels: []string{"Memory"},
			}

			access := &AccessMetaEntry{
				TargetID:    "test:n",
				TargetScope: ScopeNode,
				Fixed:       AccessMetaFixedFields{LastAccessedAt: lastAccessedAt, AccessCount: 1},
				Overflow:    map[string]interface{}{"anchor": customAt},
			}

			res := scoreInverse(t, bundle, binding, []string{"Memory"},
				access, created, versionAt, now)

			expected := 1.0 - math.Exp(-float64(tc.expectAge)*math.Log(2)/10.0)
			require.InDelta(t, expected, res.FinalScore, 1e-6,
				"%s anchor: inverted exp at age=%ds must equal %.6f (got %.6f)",
				tc.name, tc.expectAge, expected, res.FinalScore)
		})
	}
}

// TestInverseDecay_ForwardCounterpartsStillCorrect makes sure adding the
// negative-half-life path didn't regress the standard forward curves. For
// each family, with a POSITIVE half-life and createdAt anchor, the score
// at age=0 is 1.0 and at age=2*halfLife is at or below 0.5 (well below
// for exponential, exactly 0 for linear, exactly 0 for step).
func TestInverseDecay_ForwardCounterpartsStillCorrect(t *testing.T) {
	cases := []struct {
		name        string
		fn          DecayFunction
		halfLife    int64
		atTwoHL     float64
		atTwoHLDelt float64
	}{
		{"exponential", DecayFunctionExponential, 10, 0.25, 1e-6},
		{"linear", DecayFunctionLinear, 10, 0.0, 1e-6},
		{"step", DecayFunctionStep, 10, 0.0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := DecayProfileBundle{
				Name:                "fwd_" + tc.name,
				Scope:               ScopeNode,
				Function:            tc.fn,
				HalfLifeSeconds:     tc.halfLife,
				VisibilityThreshold: 0.0,
				ScoreFrom:           ScoreFromCreated,
				Enabled:             true,
				DecayEnabled:        true,
			}
			binding := DecayProfileBinding{
				Name:         "fwd_binding_" + tc.name,
				ProfileRef:   "fwd_" + tc.name,
				TargetLabels: []string{"Memory"},
			}
			created := int64(1_000_000_000_000)

			zero := scoreInverse(t, bundle, binding, []string{"Memory"}, nil, created, 0, created)
			require.InDelta(t, 1.0, zero.FinalScore, 1e-6,
				"%s: forward curve at age=0 must be 1.0", tc.name)

			twoHL := created + 2*tc.halfLife*oneSecondNanos
			at := scoreInverse(t, bundle, binding, []string{"Memory"}, nil, created, 0, twoHL)
			require.InDelta(t, tc.atTwoHL, at.FinalScore, tc.atTwoHLDelt,
				"%s: forward curve at age=2H must equal %.4f (got %.6f)",
				tc.name, tc.atTwoHL, at.FinalScore)
		})
	}
}
