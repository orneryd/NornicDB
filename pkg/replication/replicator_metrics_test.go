package replication

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricsFixture builds a fresh registry + bag + tracker + replicatorMetrics
// for direct chokepoint testing — no full Raft state machine needed.
func metricsFixture(t *testing.T, mode string) (*observability.ReplicationMetrics, *PeerTracker, *replicatorMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	bag := observability.NewReplicationMetrics(reg, mode, false)
	tracker := NewPeerTracker()
	m := newReplicatorMetrics(bag, tracker)
	require.NotNil(t, m)
	return bag, tracker, m, reg
}

// TestRoleTransition_GaugeUpdate asserts D-15a: the role gauge updates
// at the chokepoint, and LeaderChangesTotal increments only on leader-
// boundary crossings.
func TestRoleTransition_GaugeUpdate(t *testing.T) {
	bag, _, m, reg := metricsFixture(t, "raft")

	// follower → candidate → leader: leader_changes_total increments once
	// (entered leader at the third step).
	m.observeRoleTransition("follower", 1, 0, 0)
	m.observeRoleTransition("candidate", 2, 0, 0)
	m.observeRoleTransition("leader", 2, 0, 0)

	assert.Equal(t, observability.RoleEnum("leader"), testutil.ToFloat64(bag.Role))
	assert.Equal(t, float64(2), testutil.ToFloat64(bag.Term))
	assert.Equal(t, float64(1), testutil.ToFloat64(bag.LeaderChangesTotal),
		"leader_changes_total must increment exactly once on follower→leader transition")

	// leader → follower: another increment (left leader boundary).
	m.observeRoleTransition("follower", 3, 5, 5)
	assert.Equal(t, float64(2), testutil.ToFloat64(bag.LeaderChangesTotal),
		"leader_changes_total must increment on leader→follower transition")
	assert.Equal(t, observability.RoleEnum("follower"), testutil.ToFloat64(bag.Role))
	assert.Equal(t, float64(3), testutil.ToFloat64(bag.Term))
	assert.Equal(t, float64(5), testutil.ToFloat64(bag.CommitIndex))
	assert.Equal(t, float64(5), testutil.ToFloat64(bag.ApplyIndex))

	// follower → follower (same role): NO increment (no boundary crossed).
	m.observeRoleTransition("follower", 4, 6, 6)
	assert.Equal(t, float64(2), testutil.ToFloat64(bag.LeaderChangesTotal),
		"no boundary crossed: leader_changes_total must NOT increment")

	// follower → leader: another increment.
	m.observeRoleTransition("leader", 5, 6, 6)
	assert.Equal(t, float64(3), testutil.ToFloat64(bag.LeaderChangesTotal))

	// All four scalar gauges register under their canonical names.
	for _, name := range []string{
		"nornicdb_replication_role",
		"nornicdb_replication_term",
		"nornicdb_replication_commit_index",
		"nornicdb_replication_apply_index",
		"nornicdb_replication_leader_changes_total",
	} {
		got, err := testutil.GatherAndCount(reg, name)
		require.NoError(t, err)
		assert.Equal(t, 1, got, "%q must have exactly one series", name)
	}
}

// TestPerPeer_LagBytes_Observation asserts the per-peer chokepoint
// updates lag_bytes / lag_entries / last_contact_seconds and Marks the
// tracker — the replicator's contractual side-effect for GC.
func TestPerPeer_LagBytes_Observation(t *testing.T) {
	bag, tracker, m, _ := metricsFixture(t, "raft")

	now := time.Now()
	m.observePeerLag("peer-A", 1024, 5, now.Add(-100*time.Millisecond))
	m.observePeerLag("peer-B", 2048, 10, now.Add(-200*time.Millisecond))

	assert.InDelta(t, 1024, testutil.ToFloat64(bag.LagBytes.WithLabelValues("peer-A")), 1)
	assert.InDelta(t, 5, testutil.ToFloat64(bag.LagEntries.WithLabelValues("peer-A")), 0.001)
	assert.InDelta(t, 0.1, testutil.ToFloat64(bag.LastContactSeconds.WithLabelValues("peer-A")), 0.05)

	assert.InDelta(t, 2048, testutil.ToFloat64(bag.LagBytes.WithLabelValues("peer-B")), 1)

	// Tracker must have Marked both peers.
	assert.Equal(t, 2, tracker.Len())
}

// TestApplyDuration_Observed asserts the pre-bound apply observer
// captures samples at the chokepoint.
func TestApplyDuration_Observed(t *testing.T) {
	bag, _, m, reg := metricsFixture(t, "raft")

	// Drive 100 synthetic apply observations.
	for i := 0; i < 100; i++ {
		m.observeApplyDuration(context.Background(), 0.001)
	}

	// Histogram should have 100 samples.
	got, err := testutil.GatherAndCount(reg, "nornicdb_replication_apply_duration_seconds")
	require.NoError(t, err)
	assert.Equal(t, 1, got, "apply_duration_seconds must register exactly one series")

	// Sample count = 100.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_replication_apply_duration_seconds" {
			require.Len(t, mf.Metric, 1)
			assert.Equal(t, uint64(100), mf.Metric[0].GetHistogram().GetSampleCount())
		}
	}
	_ = bag
}

// TestPeerReconnect_NoStaleHandle exercises the Pitfall 3 mitigation
// contract: after the GC evicts a peer, a subsequent observation
// re-creates the series — proving the replicator does NOT cache Bound
// observers across reconnects.
func TestPeerReconnect_NoStaleHandle(t *testing.T) {
	bag, _, m, reg := metricsFixture(t, "raft")

	// Initial observation creates the series.
	m.observePeerLag("peer-X", 100, 1, time.Now())
	got, err := testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
	require.NoError(t, err)
	require.Equal(t, 1, got, "initial observation creates series")

	// Simulate GC eviction (DeleteLabelValues on every per-peer family).
	bag.LagBytes.DeleteLabelValues("peer-X")
	bag.LagEntries.DeleteLabelValues("peer-X")
	bag.RTTSeconds.DeleteLabelValues("peer-X")
	bag.LastContactSeconds.DeleteLabelValues("peer-X")
	got, err = testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
	require.NoError(t, err)
	require.Equal(t, 0, got, "after GC, series is gone")

	// Re-observe (peer reconnects). The contract is rebind-on-reconnect:
	// because observePeerLag calls WithLabelValues fresh every time, the
	// series is recreated cleanly.
	m.observePeerLag("peer-X", 200, 2, time.Now())
	got, err = testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
	require.NoError(t, err)
	assert.Equal(t, 1, got, "rebind-on-reconnect must recreate series cleanly")
	assert.InDelta(t, 200, testutil.ToFloat64(bag.LagBytes.WithLabelValues("peer-X")), 1)
}

// TestReplicatorMetrics_NilSafe asserts that observation calls on a nil
// receiver are no-ops — production tests that build a Replicator without
// SetReplicatorMetrics must not panic.
func TestReplicatorMetrics_NilSafe(t *testing.T) {
	var m *replicatorMetrics
	assert.NotPanics(t, func() {
		m.observeRoleTransition("leader", 1, 0, 0)
		m.observePeerLag("peer", 1, 1, time.Now())
		m.observePeerRTT("peer", time.Millisecond)
		m.observeApplyDuration(context.Background(), 0.001)
	})
}

// TestStandaloneReplicator_SetReplicatorMetrics asserts the production
// MetricsAware contract: the standalone replicator accepts the bag for
// callsite uniformity but emits nothing.
func TestStandaloneReplicator_SetReplicatorMetrics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "test-standalone"
	cfg.Mode = ModeStandalone
	st, err := NewReplicator(cfg, &nullStorage{})
	require.NoError(t, err)
	standalone, ok := st.(*StandaloneReplicator)
	require.True(t, ok)

	reg := prometheus.NewRegistry()
	bag := observability.NewReplicationMetrics(reg, "standalone", false)
	tracker := NewPeerTracker()
	require.NotPanics(t, func() {
		standalone.SetReplicatorMetrics(bag, tracker)
	})

	// nil-safe — re-injection with nil disables observation.
	require.NotPanics(t, func() {
		standalone.SetReplicatorMetrics(nil, nil)
	})
}

// TestRaftReplicator_BootstrapEmitsRole asserts D-15a: the bootstrap
// "became leader (term 1)" log site emits role=leader, term=1.
func TestRaftReplicator_BootstrapEmitsRole(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = ModeRaft
	cfg.NodeID = "test-raft-bootstrap"
	cfg.Raft.Bootstrap = true
	cfg.Raft.Peers = nil // bootstrap mode with no peers
	cfg.Raft.HeartbeatTimeout = 50 * time.Millisecond
	cfg.Raft.ElectionTimeout = 200 * time.Millisecond

	rr, err := NewReplicator(cfg, &nullStorage{})
	require.NoError(t, err)
	raft, ok := rr.(*RaftReplicator)
	require.True(t, ok)

	reg := prometheus.NewRegistry()
	bag := observability.NewReplicationMetrics(reg, "raft", false)
	tracker := NewPeerTracker()
	raft.SetReplicatorMetrics(bag, tracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, raft.Start(ctx))
	defer raft.Shutdown() //nolint:errcheck // test cleanup

	// Wait briefly for the bootstrap-leader transition to emit.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(bag.Role) == observability.RoleEnum("leader")
	}, 2*time.Second, 20*time.Millisecond, "bootstrap leader role must publish")

	assert.Equal(t, float64(1), testutil.ToFloat64(bag.Term),
		"bootstrap term=1")
	assert.GreaterOrEqual(t, testutil.ToFloat64(bag.LeaderChangesTotal), float64(1),
		"leader_changes_total must increment on bootstrap")
}

// nullStorage is a tiny no-op Storage for replicator construction in
// tests that do not exercise data-plane paths.
type nullStorage struct{}

func (nullStorage) ApplyCommand(*Command) error { return nil }
func (nullStorage) GetWALPosition() (uint64, error) {
	return 0, nil
}
func (nullStorage) GetWALEntries(uint64, int) ([]*WALEntry, error) {
	return nil, nil
}
func (nullStorage) WriteSnapshot(SnapshotWriter) error  { return nil }
func (nullStorage) RestoreSnapshot(SnapshotReader) error { return nil }

// TestPerPeerRTTObservation asserts the RTT chokepoint records samples
// per peer and Marks the tracker.
func TestPerPeerRTTObservation(t *testing.T) {
	_, tracker, m, reg := metricsFixture(t, "raft")

	for i := 0; i < 5; i++ {
		m.observePeerRTT("peer-rtt-1", time.Duration(i+1)*time.Millisecond)
		m.observePeerRTT("peer-rtt-2", time.Duration(i+10)*time.Millisecond)
	}
	got, err := testutil.GatherAndCount(reg, "nornicdb_replication_rtt_seconds")
	require.NoError(t, err)
	assert.Equal(t, 2, got, "rtt_seconds must have one series per peer")
	assert.Equal(t, 2, tracker.Len())

	// Sample counts.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_replication_rtt_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			assert.Equal(t, uint64(5), m.GetHistogram().GetSampleCount(),
				"each peer should have 5 samples")
		}
	}
}

// TestRaftMetricsAwareInterface asserts the four MetricsAware
// implementations satisfy the interface.
func TestRaftMetricsAwareInterface(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "test-iface"

	// Standalone
	st, err := NewReplicator(cfg, &nullStorage{})
	require.NoError(t, err)
	_, ok := st.(MetricsAware)
	assert.True(t, ok, "StandaloneReplicator implements MetricsAware")

	// Raft
	cfg2 := DefaultConfig()
	cfg2.Mode = ModeRaft
	cfg2.NodeID = "test-iface-raft"
	cfg2.Raft.Bootstrap = true
	rr, err := NewReplicator(cfg2, &nullStorage{})
	require.NoError(t, err)
	_, ok = rr.(MetricsAware)
	assert.True(t, ok, "RaftReplicator implements MetricsAware")

	// HAStandby
	cfg3 := DefaultConfig()
	cfg3.Mode = ModeHAStandby
	cfg3.NodeID = "test-iface-ha"
	cfg3.HAStandby.Role = "primary"
	cfg3.HAStandby.PeerAddr = "localhost:7000"
	hr, err := NewReplicator(cfg3, &nullStorage{})
	require.NoError(t, err)
	_, ok = hr.(MetricsAware)
	assert.True(t, ok, "HAStandbyReplicator implements MetricsAware")
}

// _ silences strconv if some assertion paths are removed.
var _ = strconv.Itoa
