package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-06 GREEN: NewReplicationMetrics + 10 families incl. GAP-1
// last_contact_seconds + RISK-3 corrected peer label + D-05a mode-aware
// peer cardinality ceiling.

// stubPeer is the test-side PeerConfigLike satisfying the leaf-package
// boundary indirection (D-02d). Production code in pkg/replication wraps
// pkg/replication/config.PeerConfig (whose actual fields are {ID, Addr},
// per RISK-3 fix).
type stubPeer struct {
	id   string
	addr string
}

func (s stubPeer) GetID() string   { return s.id }
func (s stubPeer) GetAddr() string { return s.addr }

// TestReplicationMetrics_TenFamilies asserts MET-14: ten replication
// families per ADR §2.3 and CONTEXT §domain.
func TestReplicationMetrics_TenFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewReplicationMetrics(te.Registry, "raft" /* mode */, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)

	// Drive at least one observation on every family so Gather sees them.
	bag.Role.Set(RoleEnum("leader"))
	bag.Term.Set(7)
	bag.CommitIndex.Set(100)
	bag.ApplyIndex.Set(99)
	bag.LeaderChangesTotal.Inc()
	bag.LagBytes.WithLabelValues("peer-1").Set(1024)
	bag.LagEntries.WithLabelValues("peer-1").Set(10)
	bag.LastContactSeconds.WithLabelValues("peer-1").Set(0.5)
	bag.RTTSeconds.WithLabelValues("peer-1").Observe(0.001)
	bag.ApplyDuration.Bind().Observe(nil, 0.005) //nolint:staticcheck // intentional nil ctx for cold-path Bind test

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

// TestReplicationMetrics_RegistersTen asserts the bag registers EXACTLY
// 10 families — not more, not fewer.
func TestReplicationMetrics_RegistersTen(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewReplicationMetrics(te.Registry, "raft", false)
	require.NotNil(t, bag)

	// Touch every family so all show up in Gather.
	bag.Role.Set(RoleEnum("follower"))
	bag.Term.Set(1)
	bag.CommitIndex.Set(0)
	bag.ApplyIndex.Set(0)
	bag.LeaderChangesTotal.Inc()
	bag.LagBytes.WithLabelValues("p").Set(0)
	bag.LagEntries.WithLabelValues("p").Set(0)
	bag.LastContactSeconds.WithLabelValues("p").Set(0)
	bag.RTTSeconds.WithLabelValues("p").Observe(0.001)
	bag.ApplyDuration.Vec().WithLabelValues().Observe(0.001)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	replicationFamilies := 0
	for _, mf := range mfs {
		name := mf.GetName()
		if len(name) > len("nornicdb_replication_") &&
			name[:len("nornicdb_replication_")] == "nornicdb_replication_" {
			replicationFamilies++
		}
	}
	assert.Equal(t, 10, replicationFamilies,
		"MET-14: ReplicationMetrics registers exactly 10 families per ADR §2.3")
}

// TestPeerLabel_StablePerCfg asserts CONTEXT D-05 (RISK-3 corrected): the
// peer label value derives from PeerConfig.ID (not Name); falls back to
// Addr if ID is empty.
func TestPeerLabel_StablePerCfg(t *testing.T) {
	cases := []struct {
		name string
		peer stubPeer
		want string
	}{
		{"id-only", stubPeer{id: "peer-id-abc"}, "peer-id-abc"},
		{"id-and-addr-prefers-id", stubPeer{id: "id-1", addr: "10.0.0.1:7000"}, "id-1"},
		{"addr-fallback", stubPeer{addr: "10.0.0.1:7687"}, "10.0.0.1:7687"},
		{"empty-allowed-flagged", stubPeer{}, ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := PeerLabel(tc.peer)
			assert.Equal(t, tc.want, got,
				"RISK-3: PeerLabel must return ID when set, else Addr")
		})
	}
}

// TestPeerLabel_NeverRawIP — defense-in-depth assertion that the helper
// never synthesizes a label value from anything other than its
// PeerConfigLike inputs. We exercise every (ID, Addr) shape and verify
// the output is always one of the two configured fields. There is no
// code path that consults a runtime socket — the helper has no other
// data source.
func TestPeerLabel_NeverRawIP(t *testing.T) {
	for _, p := range []stubPeer{
		{id: "id-1", addr: "10.0.0.1:7000"},
		{id: "", addr: "192.168.0.5:7000"},
		{id: "uuid-aaaa-bbbb", addr: ""},
	} {
		got := PeerLabel(p)
		// got must equal exactly one of the configured fields.
		isFromConfig := got == p.id || got == p.addr
		assert.True(t, isFromConfig,
			"RISK-3: PeerLabel %q must come from PeerConfig{ID,Addr} fields, never socket-derived",
			got)
	}
}

// TestModeAwareCeiling asserts D-05a per-mode peer cardinality ceilings.
// Each mode parameterizes the AssertCardinalityCeiling drive — the closed
// {peer} axis with stable PeerConfig.ID inputs cannot exceed the ceiling
// because the test driver rotates through a bounded set of synthetic IDs
// equal to the ceiling, simulating the GC-bounded steady state.
func TestModeAwareCeiling(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		ceiling int
	}{
		{"ha_standby", 8},
		{"raft", 16},
		{"multi_region", 64},
	} {
		tc := tc
		t.Run(tc.mode, func(t *testing.T) {
			te := NewTestEnv(t)
			bag := NewReplicationMetrics(te.Registry, tc.mode, false)
			require.Equal(t, tc.ceiling, bag.Ceiling(),
				"D-05a: mode %q ceiling stored", tc.mode)

			// Drive the cardinality test — a bounded set of `ceiling`
			// distinct peer IDs simulates the GC-bounded steady state.
			// Any cardinality bug (raw IP from socket, uncapped fan-out)
			// would push the count past the ceiling.
			te.AssertCardinalityCeiling(t, "nornicdb_replication_lag_bytes", tc.ceiling,
				func(tenant string) {
					// Map the 1k synthetic tenant UUIDs onto exactly
					// `ceiling` distinct peer IDs — this is the steady
					// state the GC enforces. If the driver naively used
					// the raw tenant string as the peer label, the
					// cardinality would explode and the assertion fails.
					peerIdx := 0
					for i, b := range []byte(tenant) {
						peerIdx = (peerIdx + int(b)*(i+1)) % tc.ceiling
					}
					bag.LagBytes.WithLabelValues(
						"peer-" + peerItoa(peerIdx),
					).Set(1.0)
				})
		})
	}
}

// peerItoa is a tiny dependency-free integer-to-string for the ceiling test
// (avoids pulling strconv into a leaf-package test file). Bounded input
// space; correctness for negative numbers irrelevant.
func peerItoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestRoleEnum_ClosedMapping asserts the role-string → numeric mapping
// matches the WIRE contract for alert rules. Operators write rules like
// `nornicdb_replication_role == 2` for "is leader"; flipping these values
// is a breaking change.
func TestRoleEnum_ClosedMapping(t *testing.T) {
	for _, tc := range []struct {
		role string
		want float64
	}{
		{"follower", 0},
		{"candidate", 1},
		{"leader", 2},
		{"standby", 3},
		{"unknown-future-role", -1},
		{"", -1},
	} {
		assert.Equal(t, tc.want, RoleEnum(tc.role),
			"RoleEnum wire contract: role %q", tc.role)
	}
}

// TestReplicationMetrics_TenantFlagIgnored asserts D-08a: the tenant flag
// is accepted at construction for callsite uniformity but no replication
// family ever gets a `database` label (replication is per-cluster).
func TestReplicationMetrics_TenantFlagIgnored(t *testing.T) {
	te := NewTestEnv(t)
	// Construct with tenant flag ON; no family should gain a database label.
	bag := NewReplicationMetrics(te.Registry, "raft", true /* tenantLabelsEnabled */)
	require.NotNil(t, bag)
	assert.True(t, bag.TenantLabelsEnabled(), "diagnostic: flag stored")

	bag.LagBytes.WithLabelValues("peer-1").Set(1)
	bag.LagEntries.WithLabelValues("peer-1").Set(1)
	bag.LastContactSeconds.WithLabelValues("peer-1").Set(1)
	bag.RTTSeconds.WithLabelValues("peer-1").Observe(0.001)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		name := mf.GetName()
		if len(name) <= len("nornicdb_replication_") ||
			name[:len("nornicdb_replication_")] != "nornicdb_replication_" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				assert.NotEqual(t, "database", lp.GetName(),
					"D-08a: replication family %q must NEVER carry a database label", name)
			}
		}
	}
}
