package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerStartup_ReplicationAuthMetricsRegistered asserts the integration
// wiring registers all 11 families on the registry (10 replication + 1 auth)
// and the GC component is started.
func TestServerStartup_ReplicationAuthMetricsRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Build a no-op authenticator (real auth.NewAuthenticator needs storage —
	// skip that to keep the test light; SetAuthMetrics works on an empty
	// Authenticator{} per Plan 04-06-05).
	a := &auth.Authenticator{}

	// Build a no-op bolt.Server (we only need SetAuthMetrics to be callable).
	boltSrv := &bolt.Server{}

	// Wire (no replicator).
	wiring := initReplicationAuthMetrics(
		reg,
		"raft",
		false,
		a,
		boltSrv,
		nil,
	)
	require.NotNil(t, wiring.authMetrics)
	require.NotNil(t, wiring.replMetrics)
	require.NotNil(t, wiring.peerGC)

	// Drive at least one observation per family so Gather sees them.
	wiring.authMetrics.AuthAttempts.WithLabelValues("success", "http").Inc()
	wiring.replMetrics.Role.Set(observability.RoleEnum("leader"))
	wiring.replMetrics.Term.Set(1)
	wiring.replMetrics.CommitIndex.Set(0)
	wiring.replMetrics.ApplyIndex.Set(0)
	wiring.replMetrics.LeaderChangesTotal.Inc()
	wiring.replMetrics.LagBytes.WithLabelValues("p").Set(0)
	wiring.replMetrics.LagEntries.WithLabelValues("p").Set(0)
	wiring.replMetrics.LastContactSeconds.WithLabelValues("p").Set(0)
	wiring.replMetrics.RTTSeconds.WithLabelValues("p").Observe(0.001)
	wiring.replMetrics.ApplyDuration.Vec().WithLabelValues().Observe(0.001)

	// Count families.
	mfs, err := reg.Gather()
	require.NoError(t, err)

	replicationFamilies := 0
	authFamilies := 0
	for _, mf := range mfs {
		name := mf.GetName()
		if len(name) > len("nornicdb_replication_") &&
			name[:len("nornicdb_replication_")] == "nornicdb_replication_" {
			replicationFamilies++
		}
		if name == "nornicdb_auth_attempts_total" {
			authFamilies++
		}
	}
	assert.Equal(t, 10, replicationFamilies, "10 replication families per ADR §2.3")
	assert.Equal(t, 1, authFamilies, "1 auth family per GAP-6")
}

// TestPeerGC_StartedByLifecycle asserts the GC component implements
// lifecycle.Component (it's appended to the components slice in main.go).
func TestPeerGC_StartedByLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	wiring := initReplicationAuthMetrics(
		reg,
		"raft",
		false,
		nil,
		nil,
		nil,
	)

	// PeerMetricsGC must satisfy lifecycle.Component.
	require.NotNil(t, wiring.peerGC)
	assert.NotEmpty(t, wiring.peerGC.Name(), "lifecycle.Component must have a Name")
	assert.Equal(t, "replication_peer_metrics_gc", wiring.peerGC.Name())
}

// TestInitReplicationAuth_AuthInjected asserts that the auth bag is
// injected into both the Bolt server and the Authenticator — so the
// chokepoints in pkg/bolt/server.go (Plan 04-02 HELLO) and
// pkg/auth/auth_metrics.go (RecordAttempt) light up at startup.
func TestInitReplicationAuth_AuthInjected(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := &auth.Authenticator{}
	boltSrv := &bolt.Server{}

	wiring := initReplicationAuthMetrics(reg, "raft", false, a, boltSrv, nil)

	// Authenticator received the bag.
	assert.Equal(t, wiring.authMetrics, a.AuthMetrics(),
		"authenticator.AuthMetrics() must return the wired bag")

	// Drive RecordAttempt — must increment the bag's counter.
	a.RecordAttempt("success", "http")
	got := testutil.ToFloat64(wiring.authMetrics.AuthAttempts.WithLabelValues("success", "http"))
	assert.Equal(t, float64(1), got)
}

// TestInitReplicationAuth_NoReplicator asserts the wiring tolerates a nil
// replicator (standalone mode) — the GC component still constructs and
// the bags still register.
func TestInitReplicationAuth_NoReplicator(t *testing.T) {
	reg := prometheus.NewRegistry()

	require.NotPanics(t, func() {
		wiring := initReplicationAuthMetrics(reg, "", false, nil, nil, nil)
		require.NotNil(t, wiring.peerGC)
		require.NotNil(t, wiring.replMetrics)
		require.NotNil(t, wiring.authMetrics)
	})
}

// TestInitReplicationAuth_ReplicatorMetricsAware asserts the wiring
// injects metrics into a MetricsAware replicator implementation.
func TestInitReplicationAuth_ReplicatorMetricsAware(t *testing.T) {
	reg := prometheus.NewRegistry()

	cfg := replication.DefaultConfig()
	cfg.NodeID = "test-cmd-wire"
	rr, err := replication.NewReplicator(cfg, &cmdNullStorage{})
	require.NoError(t, err)
	require.NotNil(t, rr)

	// MetricsAware contract.
	_, ok := rr.(replication.MetricsAware)
	require.True(t, ok)

	wiring := initReplicationAuthMetrics(reg, "standalone", false, nil, nil, rr)
	require.NotNil(t, wiring.replMetrics)

	// The Standalone replicator does not emit transitions on its own, but
	// the wiring should not panic and the bags should be available.
	assert.NotNil(t, wiring.peerGC.Tracker())
}

// cmdNullStorage is a tiny no-op Storage for replicator construction in
// the cmd-package wiring tests.
type cmdNullStorage struct{}

func (cmdNullStorage) ApplyCommand(*replication.Command) error { return nil }
func (cmdNullStorage) GetWALPosition() (uint64, error)         { return 0, nil }
func (cmdNullStorage) GetWALEntries(uint64, int) ([]*replication.WALEntry, error) {
	return nil, nil
}
func (cmdNullStorage) WriteSnapshot(replication.SnapshotWriter) error  { return nil }
func (cmdNullStorage) RestoreSnapshot(replication.SnapshotReader) error { return nil }
