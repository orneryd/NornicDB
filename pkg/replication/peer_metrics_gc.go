// Plan 04-06-04: D-05b stale-peer GC lifecycle.Component populating
// no metric — instead, periodically calling DeleteLabelValues on the
// per-peer GaugeVecs in *observability.ReplicationMetrics to evict
// stale peers (peers we have not observed within the staleness threshold).
//
// Why a GC component (RESEARCH §Q7 / Pitfall 3):
//   - Per-peer label values accumulate on every reconnect — even when the
//     peer label is stable per RISK-3, a misconfiguration that rotates
//     PeerConfig.ID across restarts would explode cardinality without GC.
//   - Without DeleteLabelValues, GaugeVec series persist forever in the
//     registry — a multi-region churn scenario could push the cardinality
//     past D-05a ceilings.
//   - DeleteLabelValues races with concurrent observation per the
//     prometheus client_golang documentation (the Bound observer cached
//     by the replicator becomes stale after delete). The replicator's
//     contract is to REBIND on reconnect — see replicator.go documentation.
//
// Lifecycle semantics (mirrors pkg/storage.BytesMetricsSweeper —
// Plan 04-04 reference):
//   - Start spawns a goroutine that ticks every interval (default 5min).
//   - Each tick wraps `defer recover()` so any panic during DeleteLabelValues
//     does not crash the supervisor errgroup (RISK-8 / T-04-08).
//   - Shutdown closes the ticker and waits for the goroutine to exit
//     within the supervisor's drain budget (5s typical).
//   - Registered between pprof and workersC components per RESEARCH §Q4.
package replication

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// DefaultPeerGCInterval is the D-05b default sweep cadence.
const DefaultPeerGCInterval = 5 * time.Minute

// DefaultPeerGCStaleness is the D-05b default staleness threshold —
// peers not observed within this window are evicted from the registry.
const DefaultPeerGCStaleness = 24 * time.Hour

// PeerTracker maintains a thread-safe map of peer-label → last-seen time.
// The replicator code calls Mark on every observation site (heartbeat,
// replicate, RTT measurement); the GC walks the map to find stale entries.
//
// Rebind-on-reconnect contract (Pitfall 3 mitigation per RESEARCH §Q7):
// after PeerMetricsGC.sweep evicts a peer via DeleteLabelValues, any held
// Bound observer in the replicator goes stale — observations land on a
// detached series that will not be re-aggregated. The replicator MUST
// re-bind via WithLabelValues at the next observation site (or simply
// avoid caching Bound observers across reconnects).
type PeerTracker struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// NewPeerTracker constructs an empty tracker.
func NewPeerTracker() *PeerTracker {
	return &PeerTracker{seen: make(map[string]time.Time)}
}

// Mark records that we just observed `peer`. Callers (replicator
// observation sites) invoke this every time they touch a per-peer metric.
func (t *PeerTracker) Mark(peer string) {
	t.mu.Lock()
	t.seen[peer] = time.Now()
	t.mu.Unlock()
}

// StaleSince returns peers not observed since `cutoff`. The result is a
// fresh slice owned by the caller — modifying it is safe.
func (t *PeerTracker) StaleSince(cutoff time.Time) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var stale []string
	for peer, ts := range t.seen {
		if ts.Before(cutoff) {
			stale = append(stale, peer)
		}
	}
	return stale
}

// Forget removes a peer entry — called by the GC after eviction.
func (t *PeerTracker) Forget(peer string) {
	t.mu.Lock()
	delete(t.seen, peer)
	t.mu.Unlock()
}

// Len reports the current number of tracked peers — diagnostic for tests.
func (t *PeerTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}

// PeerMetricsGC is the lifecycle.Component that GCs stale peer label
// values from the per-peer GaugeVecs in *observability.ReplicationMetrics.
type PeerMetricsGC struct {
	metrics   *observability.ReplicationMetrics
	tracker   *PeerTracker
	interval  time.Duration
	staleness time.Duration

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewPeerMetricsGC constructs the GC. interval ≤ 0 falls back to
// DefaultPeerGCInterval; staleness ≤ 0 falls back to DefaultPeerGCStaleness.
// metrics nil disables the sweep (Start returns nil immediately) — defensive
// for partial-init.
func NewPeerMetricsGC(metrics *observability.ReplicationMetrics, interval, staleness time.Duration) *PeerMetricsGC {
	if interval <= 0 {
		interval = DefaultPeerGCInterval
	}
	if staleness <= 0 {
		staleness = DefaultPeerGCStaleness
	}
	return &PeerMetricsGC{
		metrics:   metrics,
		tracker:   NewPeerTracker(),
		interval:  interval,
		staleness: staleness,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Tracker exposes the PeerTracker so the replicator can call Mark at
// observation sites. The GC owns the tracker so its sweep + Mark calls
// share the same mutex (no cross-component locking surprises).
func (g *PeerMetricsGC) Tracker() *PeerTracker { return g.tracker }

// Name implements lifecycle.Component.
func (g *PeerMetricsGC) Name() string { return "replication_peer_metrics_gc" }

// Start implements lifecycle.Component. Blocks until ctx is cancelled or
// Shutdown is called. Does NOT perform an initial sweep — peer state at
// startup is empty; sweeping immediately would be a no-op anyway.
func (g *PeerMetricsGC) Start(ctx context.Context) error {
	defer close(g.done)
	if g.metrics == nil {
		// Wait for cancellation but emit nothing.
		select {
		case <-ctx.Done():
		case <-g.stop:
		}
		return nil
	}
	t := time.NewTicker(g.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-g.stop:
			return nil
		case <-t.C:
			g.sweep()
		}
	}
}

// Shutdown implements lifecycle.Component. Idempotent; subsequent calls
// are no-ops.
func (g *PeerMetricsGC) Shutdown(ctx context.Context) error {
	g.stopOnce.Do(func() {
		close(g.stop)
	})
	select {
	case <-g.done:
	case <-ctx.Done():
	}
	return nil
}

// sweep evicts stale peers from the per-peer GaugeVec families. Wrapped
// in defer-recover per RISK-8 / T-04-08 — a Prometheus DeleteLabelValues
// panic during concurrent observation cannot poison the supervisor.
//
// Race-aware (Pitfall 3): we capture the stale list under the tracker
// lock, then forget AND DeleteLabelValues outside the lock. The replicator
// is contractually required to re-bind on reconnect (see replicator.go
// documentation), so a race where Mark fires between StaleSince and
// Forget is benign — the next sweep simply re-evaluates.
func (g *PeerMetricsGC) sweep() {
	defer func() {
		_ = recover() //nolint:errcheck // T-04-08 mitigation: better stale gauge than crashed sweeper
	}()
	cutoff := time.Now().Add(-g.staleness)
	stale := g.tracker.StaleSince(cutoff)
	for _, peer := range stale {
		g.metrics.LagBytes.DeleteLabelValues(peer)
		g.metrics.LagEntries.DeleteLabelValues(peer)
		g.metrics.RTTSeconds.DeleteLabelValues(peer)
		g.metrics.LastContactSeconds.DeleteLabelValues(peer)
		g.tracker.Forget(peer)
	}
}
