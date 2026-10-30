package knowledgepolicy

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// makeScorerWithBinding builds a Scorer + binding-table for a single
// node-label decay profile with the given half-life and threshold.
func makeScorerWithBinding(t *testing.T, halfLifeSec int64, threshold float64) *Scorer {
	t.Helper()
	bundles := map[string]*DecayProfileBundle{
		"p": {
			Name:                "p",
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     halfLifeSec,
			VisibilityThreshold: threshold,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*DecayProfileBinding{
		"b": {
			Name:         "b",
			ProfileRef:   "p",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}
	bt, err := BuildBindingTable(bundles, bindings, nil, nil)
	require.NoError(t, err)
	return NewScorer(NewResolver(bt, nil), true)
}

// attachMetrics constructs a non-tenant knowledge-policy metrics bag on a
// fresh registry and attaches it to the scorer. Returns the bag so tests
// can read counter deltas via testutil.
func attachMetrics(t *testing.T, scorer *Scorer) (*prometheus.Registry, *observability.KnowledgePolicyMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	bag := observability.NewKnowledgePolicyMetrics(reg, false, func() float64 { return 0 })
	scorer.SetMetrics(bag, "test")
	return reg, bag
}

// TestScorerMetrics_NodeVisibleIncrementsVisibleCounter pins that a fresh
// node hits result="visible" (below threshold would hit suppressed).
func TestScorerMetrics_NodeVisibleIncrementsVisibleCounter(t *testing.T) {
	scorer := makeScorerWithBinding(t, 3600, 0.1)
	_, bag := attachMetrics(t, scorer)

	now := time.Now().UnixNano()
	// 10-second-old node — fully visible under a 1h half-life.
	createdAt := now - 10*int64(time.Second)
	_ = scorer.ScoreNode("ns:fresh", []string{"MemoryEpisode"}, nil, createdAt, createdAt, now)

	got := testutil.ToFloat64(bag.Scored.WithLabelValues("node", "visible"))
	require.Equal(t, float64(1), got, "visible counter must increment for recent node")
	got = testutil.ToFloat64(bag.Scored.WithLabelValues("node", "suppressed"))
	require.Equal(t, float64(0), got, "suppressed counter must NOT increment for recent node")
	got = testutil.ToFloat64(bag.Scored.WithLabelValues("node", "no_decay"))
	require.Equal(t, float64(0), got, "no_decay counter must NOT increment for a bound profile")
}

// TestScorerMetrics_NodeSuppressedIncrementsSuppressedCounter pins that a
// very old node hits result="suppressed" AND suppressions_total{reason}.
func TestScorerMetrics_NodeSuppressedIncrementsSuppressedCounter(t *testing.T) {
	scorer := makeScorerWithBinding(t, 3600, 0.1)
	_, bag := attachMetrics(t, scorer)

	now := time.Now().UnixNano()
	// 30-day-old node under a 1-hour half-life — deeply decayed.
	createdAt := now - int64(30*24*time.Hour)
	res := scorer.ScoreNode("ns:ancient", []string{"MemoryEpisode"}, nil, createdAt, createdAt, now)
	require.True(t, res.SuppressionEligible, "30-day-old node with 1h half-life must be suppressed")

	got := testutil.ToFloat64(bag.Scored.WithLabelValues("node", "suppressed"))
	require.Equal(t, float64(1), got)
	got = testutil.ToFloat64(bag.Scored.WithLabelValues("node", "visible"))
	require.Equal(t, float64(0), got)

	// One suppression with reason — either below_threshold (common case) or
	// score_floor if configured. This profile has DecayFloor=0, so the reason
	// must be below_threshold.
	got = testutil.ToFloat64(bag.Suppressions.WithLabelValues("node", "below_threshold"))
	require.Equal(t, float64(1), got, "below_threshold reason must fire")
}

// TestScorerMetrics_NoDecayIncrementsNoDecayCounter pins that disabled decay
// (via Scorer(decayEnabled=false)) produces a no_decay outcome.
func TestScorerMetrics_NoDecayIncrementsNoDecayCounter(t *testing.T) {
	scorer := NewScorer(NewResolver(&BindingTable{}, nil), false)
	_, bag := attachMetrics(t, scorer)

	now := time.Now().UnixNano()
	_ = scorer.ScoreNode("ns:n", []string{"MemoryEpisode"}, nil, now, now, now)

	got := testutil.ToFloat64(bag.Scored.WithLabelValues("node", "no_decay"))
	require.Equal(t, float64(1), got, "no_decay counter must increment when decay is off")
}

// TestScorerMetrics_NilMetricsSafe asserts the hot path is nil-safe — a
// Scorer without metrics attached must not panic or error. Guards against
// regression if SetMetrics is ever made mandatory.
func TestScorerMetrics_NilMetricsSafe(t *testing.T) {
	scorer := makeScorerWithBinding(t, 3600, 0.1)
	// Explicitly clear global + local — scorer.metrics was never set.
	prev := observability.GetKnowledgePolicyMetrics()
	observability.SetKnowledgePolicyMetrics(nil)
	t.Cleanup(func() { observability.SetKnowledgePolicyMetrics(prev) })

	now := time.Now().UnixNano()
	require.NotPanics(t, func() {
		scorer.ScoreNode("ns:n", []string{"MemoryEpisode"}, nil, now, now, now)
	}, "nil metrics handle must not panic")
}

// TestScorerMetrics_FallsBackToGlobal pins that SetMetrics(nil, ns) causes
// recordScoringOutcome to read observability.GetKnowledgePolicyMetrics()
// at fire time. This is the production init-order pattern where main.go
// publishes the bag AFTER db.Open has constructed the Scorer.
func TestScorerMetrics_FallsBackToGlobal(t *testing.T) {
	scorer := makeScorerWithBinding(t, 3600, 0.1)
	scorer.SetMetrics(nil, "fallback-ns")

	reg := prometheus.NewRegistry()
	bag := observability.NewKnowledgePolicyMetrics(reg, false, nil)
	prev := observability.GetKnowledgePolicyMetrics()
	observability.SetKnowledgePolicyMetrics(bag)
	t.Cleanup(func() { observability.SetKnowledgePolicyMetrics(prev) })

	now := time.Now().UnixNano()
	_ = scorer.ScoreNode("ns:n", []string{"MemoryEpisode"}, nil, now, now, now)

	got := testutil.ToFloat64(bag.Scored.WithLabelValues("node", "visible"))
	require.Equal(t, float64(1), got,
		"Scorer with nil local metrics must pick up the global ref at fire time")
}
