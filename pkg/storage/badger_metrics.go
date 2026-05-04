// Plan 04-04-06: BadgerEngine metric attachment + per-op observation
// helpers.
//
// AttachMetrics binds the StorageMetrics + MVCCMetrics bags into the
// engine and pre-binds the four op-duration observers (MET-25). Subsequent
// hot-path Get/Put/Delete/Scan calls use observeStorageOp(start, observer)
// which is zero-overhead when metrics are not attached (the BoundLatency
// Observer's zero value short-circuits via the typed wrapper's Observe
// path).
//
// D-16 boundary: storage layer never imports observability for ErrConflict
// counters — that wiring lives in pkg/cypher (Plan 04-03). This file is
// the LIMITED storage→observability dependency: storage emits its OWN
// subsystem families only (op_duration_seconds, bytes, compactions,
// index_rebuild). Cross-layer counters (transaction_conflicts) stay at
// the Cypher transaction wrapper.
package storage

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// AttachMetrics injects the observability bags into the engine and
// pre-binds the per-op observers. Idempotent: subsequent calls overwrite
// the previously-bound observers, which is safe because BoundLatency
// Observer is value-typed and concurrent Observe calls on either the new
// or old observer are race-clean (client_golang HistogramVec promise).
//
// Plan 04-04-07 calls AttachMetrics from cmd/nornicdb startup AFTER
// constructing the bags but BEFORE starting the supervisor.
func (b *BadgerEngine) AttachMetrics(storage *observability.StorageMetrics, mvcc *observability.MVCCMetrics) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.storageMetrics = storage
	b.mvccMetrics = mvcc
	if storage != nil {
		b.opDurGet = storage.OpDuration.Bind("get")
		b.opDurPut = storage.OpDuration.Bind("put")
		b.opDurDelete = storage.OpDuration.Bind("delete")
		b.opDurScan = storage.OpDuration.Bind("scan")
		b.metricsAttached.Store(true)
	} else {
		b.metricsAttached.Store(false)
	}
}

// observeStorageOp records the elapsed wall-clock since start on the
// supplied bound observer. No-op when metrics have not been attached.
// Designed for the deferred-call site:
//
//	func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
//	    start := time.Now()
//	    defer b.observeStorageOp(start, b.opDurGet)
//	    // ... existing impl
//	}
//
// Hot-path discipline: the metricsAttached atomic Load avoids the
// observeWithExemplar path entirely when no bag is wired (test mode,
// embedded library use). When attached, the BoundLatencyObserver carries
// a cached prometheus.Observer; observation pays a single Observe
// call without WithLabelValues lookup (MET-25).
func (b *BadgerEngine) observeStorageOp(start time.Time, observer observability.BoundLatencyObserver) {
	if !b.metricsAttached.Load() {
		return
	}
	observer.Observe(context.Background(), time.Since(start).Seconds())
}

// updatePressureBand recomputes the MVCC pressure band ratio and routes
// it to the bag. Called at MVCC reader open/close sites.
//
// budgetBytes is taken from cfg.MVCC.BudgetBytes when wired; for now
// the engine doesn't carry a configured budget, so callers MUST pass a
// non-zero budget themselves. The helper is a no-op when budget ≤ 0
// (defensive: avoids divide-by-zero and a meaningless ratio).
func (b *BadgerEngine) updatePressureBand(database string, budgetBytes int64) {
	if !b.metricsAttached.Load() || b.mvccMetrics == nil || budgetBytes <= 0 {
		return
	}
	pinned := b.PinnedBytes()
	if pinned < 0 {
		pinned = 0
	}
	ratio := float64(pinned) / float64(budgetBytes)
	b.mvccMetrics.UpdateBand(database, ratio)
}
