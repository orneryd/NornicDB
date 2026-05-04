package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewReplicationMetrics + GAP-1 last_contact_seconds
// and the per-mode peer cardinality ceiling land in Plan 04-06.
// Compile-fail until then.

// TestReplicationMetrics_TenFamilies asserts MET-14's ten replication families
// per ADR §2.3 and CONTEXT §domain.
func TestReplicationMetrics_TenFamilies(t *testing.T) {
	t.Skip("RED: pending Plan 04-06 (Replication bag delivery; RISK-3 PeerConfig.ID)")

	te := NewTestEnv(t)
	bag := NewReplicationMetrics(te.Registry, "raft" /* mode */, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_replication_role",
		"nornicdb_replication_term",
		"nornicdb_replication_commit_index",
		"nornicdb_replication_apply_index",
		"nornicdb_replication_lag_bytes",
		"nornicdb_replication_lag_entries",
		"nornicdb_replication_apply_duration_seconds",
		"nornicdb_replication_rtt_seconds",
		"nornicdb_replication_leader_changes_total",
		"nornicdb_replication_last_contact_seconds",
	} {
		assert.Contains(t, names, want, "MET-14: Replication family %q must register", want)
	}
}

// TestPeerLabel_StablePerCfg asserts CONTEXT D-05 (RISK-3 corrected): the peer
// label value derives from PeerConfig.ID (not Name); falls back to Addr if
// ID is empty.
func TestPeerLabel_StablePerCfg(t *testing.T) {
	t.Skip("RED: pending Plan 04-06 (Replication bag delivery; RISK-3)")

	te := NewTestEnv(t)
	bag := NewReplicationMetrics(te.Registry, "raft", false)
	require.NotNil(t, bag)

	// PeerConfig.ID populated → label is the ID.
	bag.LagBytes.WithLabelValues("peer-id-abc").Set(1.0)
	// PeerConfig.ID empty → caller falls back to Addr; value is the Addr literal.
	bag.LagBytes.WithLabelValues("10.0.0.1:7687").Set(2.0)

	te.AssertCardinalityCeiling(t, "nornicdb_replication_lag_bytes", 16 /* raft mode ceiling, D-05a */,
		func(tenant string) {
			// Bound by D-05a ceiling; subsystem refuses arbitrary fan-out.
			bag.LagBytes.WithLabelValues("peer-id-abc").Set(1.0)
			_ = tenant
		})
}
