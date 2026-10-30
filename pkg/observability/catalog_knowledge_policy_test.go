package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKnowledgePolicyMetrics_RegistersAllFamilies asserts every declared
// instrument is registered and visible through Gather.
func TestKnowledgePolicyMetrics_RegistersAllFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewKnowledgePolicyMetrics(te.Registry, false, func() float64 { return 0.42 })
	require.NotNil(t, bag)

	// Drive at least one observation on each family.
	bag.IncScored("node", "visible", "")
	// Decay score is sampled 1/32 — force a hit by driving enough samples.
	ctx := context.Background()
	for i := 0; i < DecayScoreSampleDenominator; i++ {
		bag.ObserveDecayScoreSampled(ctx, "node", "", 0.5)
	}
	bag.IncSuppression("node", "below_threshold", "")
	bag.ObserveAccessFlushBatchRows(ctx, 100)
	bag.ObserveAccessFlushDuration(ctx, 0.01)
	bag.IncOnAccess("applied", "")
	bag.IncDeindexEnqueued("node", "")
	bag.IncReadFilterDropped("node", "")
	bag.IncReconcile("startup", "")

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_knowledge_policy_scored_total",
		"nornicdb_knowledge_policy_decay_score",
		"nornicdb_knowledge_policy_suppressions_total",
		"nornicdb_knowledge_policy_access_flush_batch_rows",
		"nornicdb_knowledge_policy_access_flush_duration_seconds",
		"nornicdb_knowledge_policy_access_flush_buffer_fullness",
		"nornicdb_knowledge_policy_on_access_mutations_total",
		"nornicdb_knowledge_policy_deindex_enqueued_total",
		"nornicdb_knowledge_policy_read_filter_dropped_total",
		"nornicdb_knowledge_policy_reconcile_total",
	} {
		assert.Contains(t, names, want, "family must register")
	}
}

// TestKnowledgePolicyMetrics_CardinalityCeilings walks every closed-enum
// combination to verify each family's total series count stays at the
// advertised ceiling. Labels forbidden by ForbiddenLabels would have
// panicked at construction — see validateLabels.
func TestKnowledgePolicyMetrics_CardinalityCeilings(t *testing.T) {
	// Non-tenant: scored_total = 3 entity_kind × 3 result = 9.
	t.Run("scored_total 9", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
		te.AssertCardinalityCeiling(t, "nornicdb_knowledge_policy_scored_total", 9, func(_ string) {
			for _, k := range AllowedKnowledgePolicyEntityKinds {
				for _, r := range AllowedKnowledgePolicyScoreResults {
					bag.IncScored(k, r, "")
				}
			}
		})
	})

	t.Run("suppressions_total 15", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
		te.AssertCardinalityCeiling(t, "nornicdb_knowledge_policy_suppressions_total", 15, func(_ string) {
			for _, k := range AllowedKnowledgePolicyEntityKinds {
				for _, r := range AllowedKnowledgePolicySuppressReasons {
					bag.IncSuppression(k, r, "")
				}
			}
		})
	})

	t.Run("on_access_mutations_total 3", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
		te.AssertCardinalityCeiling(t, "nornicdb_knowledge_policy_on_access_mutations_total", 3, func(_ string) {
			for _, r := range AllowedKnowledgePolicyOnAccessResults {
				bag.IncOnAccess(r, "")
			}
		})
	})

	t.Run("reconcile_total 3", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
		te.AssertCardinalityCeiling(t, "nornicdb_knowledge_policy_reconcile_total", 3, func(_ string) {
			for _, tr := range AllowedKnowledgePolicyReconcileTriggers {
				bag.IncReconcile(tr, "")
			}
		})
	})
}

// TestKnowledgePolicyMetrics_TenantLabelAxis asserts that enabling the
// tenant-flag doubles the labelset dimensions (D-08) on the families that
// carry `database` — and does NOT change the flush-level histograms which
// are intentionally cross-namespace.
func TestKnowledgePolicyMetrics_TenantLabelAxis(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewKnowledgePolicyMetrics(te.Registry, true, nil)
	// Two distinct tenants, one scoring result each → 2 series.
	bag.IncScored("node", "visible", "db1")
	bag.IncScored("node", "visible", "db2")
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_knowledge_policy_scored_total" {
			continue
		}
		assert.Len(t, mf.GetMetric(), 2, "two tenants → two series")
		labelsPresent := map[string]bool{}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				labelsPresent[lp.GetName()] = true
			}
		}
		assert.True(t, labelsPresent["database"],
			"tenant-enabled bag must carry a `database` label on scored_total")
	}
}

// TestKnowledgePolicyMetrics_GlobalRef asserts SetKnowledgePolicyMetrics /
// GetKnowledgePolicyMetrics round-trip correctly and that nil Set is safe.
func TestKnowledgePolicyMetrics_GlobalRef(t *testing.T) {
	// Clear state at test exit so later tests / live callers don't see
	// our stub.
	prev := GetKnowledgePolicyMetrics()
	t.Cleanup(func() { SetKnowledgePolicyMetrics(prev) })

	SetKnowledgePolicyMetrics(nil)
	require.Nil(t, GetKnowledgePolicyMetrics(), "nil ref returns nil getter")

	te := NewTestEnv(t)
	bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
	SetKnowledgePolicyMetrics(bag)
	require.Same(t, bag, GetKnowledgePolicyMetrics(), "Set → Get round-trip")
}

// TestDecayScoreSampling asserts the 1/N sampler really drops N-1 of every
// N observations.
func TestDecayScoreSampling(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewKnowledgePolicyMetrics(te.Registry, false, nil)
	ctx := context.Background()
	const iterations = DecayScoreSampleDenominator * 4
	for i := 0; i < iterations; i++ {
		bag.ObserveDecayScoreSampled(ctx, "node", "", 0.5)
	}
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_knowledge_policy_decay_score" {
			continue
		}
		for _, m := range mf.GetMetric() {
			// Expect 4 samples (iterations / DecayScoreSampleDenominator).
			count := m.GetHistogram().GetSampleCount()
			assert.EqualValues(t, 4, count,
				"expected 1/N sampling to yield %d observations, got %d",
				iterations/DecayScoreSampleDenominator, count)
		}
	}
}
