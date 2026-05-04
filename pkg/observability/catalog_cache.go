// Package observability — Cache + Runtime metric bag (Plan 04-01 GREEN).
//
// Owns six families per MET-16 + ADR §2.3:
//
//	nornicdb_cache_hits_total{cache}
//	nornicdb_cache_misses_total{cache}
//	nornicdb_cache_size_bytes{cache}
//	nornicdb_cache_evictions_total{cache, reason}
//	nornicdb_process_uptime_seconds       (GaugeFunc; time.Since(start).Seconds())
//	nornicdb_build_info                   (GaugeFunc; constant 1 + const labels)
//
// Closed enums (CONTEXT D-12, D-12b):
//
//	cache  ∈ AllowedCacheNames        = {query_result, schema, label, node_lookup}
//	reason ∈ AllowedEvictionReasons   = {lru, ttl, capacity, manual}
//
// RISK-4 carry-forward: if a downstream plan determines a cache name has no
// actual increment site, prune the slice here AND amend ADR §2.3. Today we
// keep all four to match the planning contract; the catalog_cache_test.go
// AssertCardinalityCeiling(4) passes whether 3 or 4 emit (helper has no
// lower bound — RESEARCH RISK-7 / Plan 04-07 ships the lower-bound test).
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `cache` and `reason` are NOT in the forbidden list — closed-enum string
// literals at the call site enforce cardinality. Subsystem callers MUST use
// only AllowedCacheNames / AllowedEvictionReasons values.
//
// Pitfall 1 / RESEARCH RISK-8 mitigation: every GaugeFunc body wraps a
// defer-recover that returns 0 on panic, preventing a buggy callback from
// poisoning the entire /metrics scrape (Pitfall 1: shutdown ordering vs.
// live scrape window — listener drains LAST per OBS-08).
//
// Leaf-package boundary (Phase 1 D-01a / boundary_test.go) preserved:
// imports limited to stdlib + prometheus/client_golang + pkg/buildinfo.
// pkg/buildinfo is a leaf utility (it embeds VERSION + carries Commit ldflag
// vars), not a forbidden subsystem package.
package observability

import (
	"runtime"
	"time"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedCacheNames is the closed enum for the `cache` label per CONTEXT
// D-12. Subsystem callers (cypher cache.go, schema/label/node_lookup
// callers) MUST pass only one of these values. Adding a new cache name
// requires a constants update here AND an ADR §2.3 amendment.
//
// Mirrors Phase 3 D-01d allowedSubsystems cadence — closure-by-construction
// rather than runtime panic (the registration-time forbidden-label panic
// catches label NAMES; closed-value enforcement is a per-call discipline).
var AllowedCacheNames = []string{
	"query_result", // pkg/cypher/cache.go (Plan 04-03 wiring)
	"schema",       // schema cache (Plan 04-04 wiring; pruned per RISK-4 if no site)
	"label",        // label index cache
	"node_lookup",  // pkg/cypher/StorageExecutor.nodeLookupCache (Plan 04-03 wiring)
}

// AllowedEvictionReasons is the closed enum for the `reason` label on
// cache_evictions_total per CONTEXT D-12b.
var AllowedEvictionReasons = []string{
	"lru",
	"ttl",
	"capacity",
	"manual",
}

// CacheMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the Cache
// + Runtime subsystem. One bag per Provider; constructed once at
// cmd/nornicdb startup between Phase 1's "registries" and "listeners" init
// steps (D-02c). Subsystems receive the bag via DI and call
// `bag.Hits.WithLabelValues("query_result").Inc()` etc.
//
// Hot-path discipline (MET-25): subsystem callers SHOULD pre-bind via
// `bag.Hits.WithLabelValues("query_result")` once at construction and cache
// the resulting prometheus.Counter in a struct field — Phase 3's BenchmarkObserve_Hot
// template applies (see exemplar_bench_test.go).
type CacheMetrics struct {
	// Hits / Misses / SizeBytes use the {cache} label set; closed enum per
	// AllowedCacheNames.
	Hits      *prometheus.CounterVec
	Misses    *prometheus.CounterVec
	SizeBytes *prometheus.GaugeVec

	// Evictions uses the {cache, reason} label set; closed enums per
	// AllowedCacheNames × AllowedEvictionReasons (cardinality ≤ 16).
	Evictions *prometheus.CounterVec

	// processStart is the wall-clock instant the bag was constructed.
	// process_uptime_seconds GaugeFunc reads time.Since(processStart).Seconds()
	// on every scrape — monotonically increasing per MET-16.
	processStart time.Time
}

// NewCacheMetrics constructs the Cache + Runtime bag against reg.
//
// Validation + Pitfall-8 panic semantics inherited from Phase 3 typed
// constructors: invalid subsystem names, missing _total/_bytes suffixes,
// or forbidden labels panic at registration. process_uptime_seconds and
// build_info register directly via prometheus.NewGaugeFunc (Phase 3's
// MetricOpts intentionally omits ConstLabels — CONTEXT D-13a uses
// GaugeFunc to side-step the omission).
//
// Construction is idempotent against this bag's six families ONLY for a
// fresh registry — re-constructing on the same registry triggers
// AlreadyRegisteredError per Pitfall 8.
func NewCacheMetrics(reg *prometheus.Registry) *CacheMetrics {
	cm := &CacheMetrics{
		processStart: time.Now(),
	}

	cm.Hits = NewCounterVec(reg,
		MetricOpts{Subsystem: "cache", Name: "hits_total",
			Help: "Cache hits per cache. Cache enum is closed (CONTEXT D-12)."},
		[]string{"cache"})

	cm.Misses = NewCounterVec(reg,
		MetricOpts{Subsystem: "cache", Name: "misses_total",
			Help: "Cache misses per cache. Cache enum is closed (CONTEXT D-12)."},
		[]string{"cache"})

	cm.SizeBytes = NewGaugeVec(reg,
		MetricOpts{Subsystem: "cache", Name: "size_bytes",
			Help: "Approximate cache size in bytes per cache (gauge)."},
		[]string{"cache"})

	cm.Evictions = NewCounterVec(reg,
		MetricOpts{Subsystem: "cache", Name: "evictions_total",
			Help: "Cache evictions per cache and reason. " +
				"Reason enum closed (CONTEXT D-12b: lru, ttl, capacity, manual)."},
		[]string{"cache", "reason"})

	// process_uptime_seconds: GaugeFunc reads time.Since(start) on each
	// scrape so the value is always current and monotonically increasing
	// (MET-16). RISK-8 / Pitfall 1 mitigation: callback wrapped in
	// defer-recover that returns 0 on panic, preventing scrape poisoning
	// during graceful shutdown.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "process",
		Name:      "uptime_seconds",
		Help:      "Wall-clock seconds since the bag was constructed (≈ process start).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		return time.Since(cm.processStart).Seconds()
	}))

	// build_info: GaugeFunc returning constant 1 with all four const labels
	// (D-13a). Version comes from the embedded VERSION file via
	// pkg/buildinfo (existing precedent — pkg/buildinfo embeds VERSION and
	// carries the -ldflags-injected Commit). go_version comes from
	// runtime.Version(); backend comes from the build-tagged var (build_*.go
	// files; D-13b).
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Name:      "build_info",
		Help: "Build identification (constant 1; metadata in const labels). " +
			"Labels: version, commit, go_version, backend.",
		ConstLabels: prometheus.Labels{
			"version":    buildinfo.Version(),
			"commit":     buildinfo.ShortCommit(),
			"go_version": runtime.Version(),
			"backend":    backend, // build-tagged var from build_*.go
		},
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		return 1
	}))

	return cm
}
