// Plan 04-04-04: D-07 30s sweep lifecycle.Component populating
// nornicdb_storage_bytes{kind} gauges by calling badger.DB.EstimateSize(prefix)
// for each kind bucket.
//
// Lifecycle semantics (RESEARCH §Architecture Pattern 2):
//   - Start spawns a goroutine that ticks every interval (default 30s).
//   - Each tick wraps `defer recover()` so a transient Badger panic during
//     compaction does not crash the supervisor errgroup (RISK-8).
//   - Shutdown closes the ticker and waits for the goroutine to exit
//     within the supervisor's drain budget (5s typical).
//
// Five `kind` buckets (closed enum AllowedStorageBytesKinds):
//   - nodes  — prefixNode
//   - edges  — prefixEdge
//   - index  — prefixLabelIndex + prefixEdgeBetweenIndex + prefixTemporalIndex
//              (sum across the three index prefixes; user-created indexes
//              are not separately accounted by D-13c)
//   - wal    — heuristic (vlog size - lsm size from db.Size()); RISK-6
//   - search — sum of IndexSizeBytes() across all per-database search
//              services (Plan 04-05 owns the IndexSizeBytes accessor;
//              this sweeper consumes via the SearchSizeFn callback so the
//              storage layer never imports pkg/search)
//
// The sweeper is registered in cmd/nornicdb between pprof and workers
// components per RESEARCH §Q4 — i.e. it Drains AFTER the workers and
// BEFORE the telemetry listener, so the final scrape during drain still
// reflects the last known sizes.
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// DefaultBytesMetricsInterval is the D-07 default sweep cadence.
const DefaultBytesMetricsInterval = 30 * time.Second

// SearchSizeFn returns the cumulative search-index byte size across all
// per-database search services. Plan 04-05 wires this via search.SearchService
// IndexSizeBytes(); for Plan 04-04 it can be nil (returns 0).
type SearchSizeFn func() int64

// BytesMetricsSweeper is the lifecycle.Component that populates the
// nornicdb_storage_bytes{kind} gauges every interval via
// badger.DB.EstimateSize(prefix).
type BytesMetricsSweeper struct {
	metrics    *observability.StorageMetrics
	db         *badger.DB
	searchSize SearchSizeFn
	interval   time.Duration

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewBytesMetricsSweeper constructs the sweeper. interval ≤ 0 falls back to
// DefaultBytesMetricsInterval. metrics or db nil disables the sweep
// (Start returns nil immediately) — defensive for partial-init.
func NewBytesMetricsSweeper(metrics *observability.StorageMetrics, db *badger.DB, searchSize SearchSizeFn, interval time.Duration) *BytesMetricsSweeper {
	if interval <= 0 {
		interval = DefaultBytesMetricsInterval
	}
	return &BytesMetricsSweeper{
		metrics:    metrics,
		db:         db,
		searchSize: searchSize,
		interval:   interval,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Name implements lifecycle.Component.
func (s *BytesMetricsSweeper) Name() string { return "storage_bytes_metrics" }

// Start implements lifecycle.Component. Blocks until ctx is cancelled or
// Shutdown is called. Performs an initial sweep immediately so the gauges
// have non-zero values by the first scrape.
func (s *BytesMetricsSweeper) Start(ctx context.Context) error {
	defer close(s.done)
	if s.metrics == nil || s.db == nil {
		// Wait for cancellation but emit nothing.
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		return nil
	}
	// Initial sweep — don't wait for the first tick.
	s.sweep()
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.stop:
			return nil
		case <-t.C:
			s.sweep()
		}
	}
}

// Shutdown implements lifecycle.Component. Idempotent; subsequent calls
// are no-ops.
func (s *BytesMetricsSweeper) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stop)
	})
	select {
	case <-s.done:
	case <-ctx.Done():
	}
	return nil
}

// sweep updates the five kind-gauges. Wrapped in defer-recover per
// RESEARCH RISK-8 so a Badger panic during concurrent compaction cannot
// poison the supervisor errgroup. Each kind is computed independently;
// failure of one accessor does not block the others.
func (s *BytesMetricsSweeper) sweep() {
	defer func() {
		if r := recover(); r != nil {
			// Swallow panic — better to publish stale gauges than crash
			// the supervisor. The /metrics scrape will still surface the
			// last known values.
			_ = r
		}
	}()

	// nodes
	if k, v := s.db.EstimateSize([]byte{prefixNode}); true {
		s.metrics.Bytes.WithLabelValues("nodes").Set(float64(k + v))
	}
	// edges
	if k, v := s.db.EstimateSize([]byte{prefixEdge}); true {
		s.metrics.Bytes.WithLabelValues("edges").Set(float64(k + v))
	}
	// index = label + edge_between + temporal (D-13c — user_created index
	// names live under the same Badger prefixes, so byte accounting
	// captures them transparently without leaking the user names).
	var ik, iv uint64
	for _, p := range []byte{prefixLabelIndex, prefixEdgeBetweenIndex, prefixTemporalIndex} {
		k, v := s.db.EstimateSize([]byte{p})
		ik += k
		iv += v
	}
	s.metrics.Bytes.WithLabelValues("index").Set(float64(ik + iv))

	// wal: best-effort heuristic (vlog - lsm) — RISK-6.
	lsm, vlog := s.db.Size()
	wal := vlog - lsm
	if wal < 0 {
		wal = 0
	}
	s.metrics.Bytes.WithLabelValues("wal").Set(float64(wal))
	// wal_lag_bytes carries the same heuristic on its dedicated gauge so
	// alert rules don't have to filter the kind label.
	s.metrics.WALLagBytes.Set(float64(wal))

	// search: callback is owned by Plan 04-05; nil tolerated.
	if s.searchSize != nil {
		s.metrics.Bytes.WithLabelValues("search").Set(float64(s.searchSize()))
	} else {
		s.metrics.Bytes.WithLabelValues("search").Set(0)
	}
}
