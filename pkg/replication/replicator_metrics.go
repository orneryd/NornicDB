// Plan 04-06-03: D-15a per-event observation chokepoint for the replicator.
//
// This file holds the small "observability seam" that each Replicator
// implementation calls at the SAME sites that already emit role-transition
// log lines. No background polling — single-line additions at existing log
// sites per CONTEXT D-15a.
//
// Why a thin observation helper instead of weaving metric calls through
// raft.go / ha_standby.go / multi_region.go directly:
//
//   - **DRY**: the Role/Term/Counter triplet at every leader-boundary
//     transition is identical across implementations. Centralizing in
//     `observeRoleTransition` keeps the pkg/observability dependency
//     surface narrow.
//   - **Test seam**: TestRoleTransition_GaugeUpdate drives the helper
//     directly without spinning up the full Raft state machine.
//   - **Nil-safe**: production code injects metrics via SetReplicatorMetrics;
//     test fixtures that build a Replicator without metrics get a clean
//     no-op (existing replication tests do not need to know about metrics).
//
// **Pitfall 3 mitigation (RESEARCH §Q7)**: this helper does NOT cache
// per-peer Bound observers. Per-peer observation goes through
// `observePeerLag` / `observePeerRTT` / `observeLastContact` which call
// WithLabelValues every time AND Mark the tracker. The peer_metrics_gc
// component evicts stale series via DeleteLabelValues; a held Bound
// observer would point to a detached series after eviction. Re-binding on
// every observation site is the simple, correct contract.
package replication

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// replicatorMetrics is the non-exported struct holding the bag + tracker.
// Stored as a *atomic.Pointer field on each Replicator implementation so
// SetReplicatorMetrics is race-clean against in-flight observation calls
// (an extremely rare event — production code injects once at startup).
type replicatorMetrics struct {
	bag     *observability.ReplicationMetrics
	tracker *PeerTracker

	// applyDur is the pre-bound apply-duration observer per MET-25.
	// Constructed once when metrics are injected.
	applyDur observability.BoundLatencyObserver

	// roleMu serializes role/term/index gauge writes. The underlying
	// prometheus.Gauge is itself race-safe; the mutex prevents inconsistent
	// observed-state readouts during a transition burst (e.g., a follower
	// briefly stepping down before another candidate election).
	roleMu sync.Mutex

	// lastRoleWasLeader tracks the previous role boundary so we increment
	// LeaderChangesTotal exactly once per leader-boundary transition.
	lastRoleWasLeader bool
}

// MetricsAware is the optional interface a Replicator implementation
// implements to accept the observability bag. Implementations may embed
// *atomic.Pointer[replicatorMetrics] or guard with their own mutex; the
// interface itself is intentionally minimal.
type MetricsAware interface {
	// SetReplicatorMetrics injects the observability bag + peer tracker.
	// Idempotent. Calling with nil disables observation (defensive).
	SetReplicatorMetrics(bag *observability.ReplicationMetrics, tracker *PeerTracker)
}

// newReplicatorMetrics builds the cached observer bundle. nil bag returns
// nil — callers nil-check before observing.
func newReplicatorMetrics(bag *observability.ReplicationMetrics, tracker *PeerTracker) *replicatorMetrics {
	if bag == nil {
		return nil
	}
	if tracker == nil {
		tracker = NewPeerTracker()
	}
	return &replicatorMetrics{
		bag:      bag,
		tracker:  tracker,
		applyDur: bag.ApplyDuration.Bind(),
	}
}

// observeRoleTransition is the D-15a chokepoint for role/term/index updates.
// Called at the SAME log site that emits "became leader", "stepping down",
// etc. — a single-line addition.
//
// LeaderChangesTotal increments exactly once per leader-boundary transition
// (non-leader→leader OR leader→non-leader). The first call after init seeds
// `lastRoleWasLeader` without incrementing (we have no prior state to
// transition FROM).
//
// Nil-safe at the receiver level: if metrics is nil, the call is a no-op.
func (m *replicatorMetrics) observeRoleTransition(role string, term, commitIndex, applyIndex uint64) {
	if m == nil {
		return
	}
	m.roleMu.Lock()
	defer m.roleMu.Unlock()

	isLeader := role == "leader"
	if isLeader != m.lastRoleWasLeader {
		// Boundary crossed — increment ONLY when we cross a boundary, not
		// on every role write. This matches the Phase 9 alert intent:
		// rate(leader_changes_total[5m]) > 0.5 ⇒ unstable cluster.
		m.bag.LeaderChangesTotal.Inc()
	}
	m.lastRoleWasLeader = isLeader

	m.bag.Role.Set(observability.RoleEnum(role))
	m.bag.Term.Set(float64(term))
	m.bag.CommitIndex.Set(float64(commitIndex))
	m.bag.ApplyIndex.Set(float64(applyIndex))
}

// observePeerLag is the per-peer chokepoint for lag_bytes / lag_entries /
// last_contact. Called at heartbeat / replicate observation sites with the
// stable peer label per RISK-3 (PeerConfig.ID with Addr fallback).
//
// Pitfall 3 mitigation: WithLabelValues called fresh every time — no
// Bound observer caching across reconnects.
func (m *replicatorMetrics) observePeerLag(peer string, lagBytes, lagEntries int64, lastContact time.Time) {
	if m == nil {
		return
	}
	m.tracker.Mark(peer)
	m.bag.LagBytes.WithLabelValues(peer).Set(float64(lagBytes))
	m.bag.LagEntries.WithLabelValues(peer).Set(float64(lagEntries))
	m.bag.LastContactSeconds.WithLabelValues(peer).Set(time.Since(lastContact).Seconds())
}

// observePeerRTT observes a single round-trip-time sample for `peer`.
// Mark side-effect ensures the GC keeps the series alive while we observe.
func (m *replicatorMetrics) observePeerRTT(peer string, rtt time.Duration) {
	if m == nil {
		return
	}
	m.tracker.Mark(peer)
	m.bag.RTTSeconds.WithLabelValues(peer).Observe(rtt.Seconds())
}

// observeApplyDuration observes a single apply-command latency sample
// through the pre-bound MET-25 chokepoint.
func (m *replicatorMetrics) observeApplyDuration(ctx context.Context, sec float64) {
	if m == nil {
		return
	}
	m.applyDur.Observe(ctx, sec)
}

// metricsHolder is a tiny embeddable helper that each Replicator impl
// composes for race-clean SetReplicatorMetrics. Internal pattern only.
type metricsHolder struct {
	ptr atomic.Pointer[replicatorMetrics]
}

// set replaces the metrics pointer atomically.
func (h *metricsHolder) set(m *replicatorMetrics) { h.ptr.Store(m) }

// get returns the current metrics pointer; nil if unset.
func (h *metricsHolder) get() *replicatorMetrics { return h.ptr.Load() }
