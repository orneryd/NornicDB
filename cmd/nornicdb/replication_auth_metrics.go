// Plan 04-06-06: cmd/nornicdb wiring for ReplicationMetrics + AuthMetrics.
//
// Construction order (CONTEXT D-02c init-order chokepoint, mirrors
// Plans 04-01..04-05):
//
//   1. After observability provider is built (obs.Registry available)
//      and the authenticator + boltServer are constructed.
//   2. Construct AuthMetrics bag — single family, no probes.
//   3. Construct ReplicationMetrics bag — 10 families, mode-aware (D-05a).
//   4. Construct PeerMetricsGC lifecycle.Component (D-05b) and inject the
//      bag + tracker into the Replicator (when replication is enabled).
//   5. Inject AuthMetrics into:
//        - bolt.Server via SetAuthMetrics (HELLO call site already wired
//          by Plan 04-02; this lights it up).
//        - auth.Authenticator via SetAuthMetrics (HTTP/gRPC adapter chokepoints
//          consume the bag via authenticator.RecordAttempt).
//
// Component registration order:
//   telemetry → pprof? → bytesSweeper? → peerMetricsGC? → workersC → boltC → httpC
//
// peerMetricsGC sits between bytesSweeper and workersC per RESEARCH §Q4 —
// it drains AFTER workers and BEFORE telemetry so the final scrape during
// drain reflects the last GC pass.
package main

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/prometheus/client_golang/prometheus"
)

// replicationAuthWiring holds the four constructed values returned by
// initReplicationAuthMetrics so main.go can append the lifecycle.Component
// without re-wiring the bag.
type replicationAuthWiring struct {
	authMetrics *observability.AuthMetrics
	replMetrics *observability.ReplicationMetrics
	peerGC      *replication.PeerMetricsGC
}

// initReplicationAuthMetrics constructs both bags + the GC component and
// wires them into the Authenticator + Bolt server + Replicator (when
// available).
//
// Parameters:
//   - reg                 : telemetry registry (obs.Registry()).
//   - replicationMode     : cfg.Replication.Mode equivalent — for D-05a
//                           mode-aware ceiling. Defaults to "standalone"
//                           when unset.
//   - tenantLabelsEnabled : forwarded to NewReplicationMetrics for the
//                           D-08a accepted-but-ignored flag.
//   - authenticator       : *auth.Authenticator to inject the AuthMetrics
//                           bag into (SetAuthMetrics). nil tolerated.
//   - boltServer          : bolt.Server to inject the AuthMetrics bag into
//                           (SetAuthMetrics — Plan 04-02 call site).
//   - replicator          : optional replication.Replicator (any concrete
//                           type implementing MetricsAware). nil tolerated.
//
// The returned wiring may be appended to lifecycle.Components — the GC
// component is non-nil even when replication is disabled (it just runs
// idle waiting for ctx cancel), so the caller can append unconditionally.
func initReplicationAuthMetrics(
	reg *prometheus.Registry,
	replicationMode string,
	tenantLabelsEnabled bool,
	authenticator *auth.Authenticator,
	boltServer *bolt.Server,
	replicator any,
) replicationAuthWiring {
	if replicationMode == "" {
		replicationMode = "standalone"
	}

	authMetrics := observability.NewAuthMetrics(reg)

	replMetrics := observability.NewReplicationMetrics(
		reg,
		replicationMode,
		tenantLabelsEnabled,
	)

	// Construct the GC component with production cadence + staleness.
	// Production code uses the defaults; tests override via the optional
	// constructor params on NewPeerMetricsGC.
	peerGC := replication.NewPeerMetricsGC(
		replMetrics,
		replication.DefaultPeerGCInterval,  // 5 min
		replication.DefaultPeerGCStaleness, // 24 h
	)

	// Inject AuthMetrics into Bolt server (Plan 04-02 HELLO call site).
	if boltServer != nil {
		boltServer.SetAuthMetrics(authMetrics)
	}

	// Inject AuthMetrics into Authenticator for HTTP/gRPC adapter wiring.
	// Adapters call authenticator.RecordAttempt(result, protocol) at the
	// chokepoint; the bag is now non-nil so observations land.
	if authenticator != nil {
		authenticator.SetAuthMetrics(authMetrics)
	}

	// Inject ReplicationMetrics into the Replicator (when present).
	// MetricsAware is the optional interface every concrete impl
	// (Standalone, HAStandby, Raft, MultiRegion) satisfies.
	if replicator != nil {
		if mr, ok := replicator.(replication.MetricsAware); ok {
			mr.SetReplicatorMetrics(replMetrics, peerGC.Tracker())
		}
	}

	return replicationAuthWiring{
		authMetrics: authMetrics,
		replMetrics: replMetrics,
		peerGC:      peerGC,
	}
}

// peerGCInterval lets tests override the production cadence. Production
// path uses the package defaults from pkg/replication.
var peerGCInterval = replication.DefaultPeerGCInterval

// peerGCStaleness lets tests override the production threshold.
var peerGCStaleness = replication.DefaultPeerGCStaleness

// _ silences time import when production code paths are pruned.
var _ = time.Second
