// Package observability — Embeddings metric bag (Plan 04-05 GREEN).
//
// Owns six families per MET-12 + ADR §2.3, plus the D-09 FFI panic
// counter:
//
//	nornicdb_embed_queue_depth                                (GaugeFunc; D-15b)
//	nornicdb_embed_processed_total{provider, model, result, mode}
//	nornicdb_embed_duration_seconds{provider, model, mode}    (long-tail; MET-05)
//	nornicdb_embed_cache_hits_total
//	nornicdb_embed_cache_misses_total
//	nornicdb_embed_worker_running                             (Set(0)/Set(1) at lifecycle)
//	nornicdb_embed_ffi_panics_total{mode}                     (D-09 — pkg/embed FFI recovery)
//
// Closed enums (CONTEXT D-06a):
//
//	mode     ∈ AllowedEmbedBackends  = {gpu, cpu, cuda, metal, vulkan}
//	result   ∈ AllowedEmbedResults   = {success, failure, cached}
//	provider ∈ AllowedEmbedProviders = {ollama, openai, local, other}
//	model    is open-ish (bounded ceiling 250 per RESEARCH §Q11)
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `embedding_text` is the keystone — embedding *content* MUST NEVER be a
// label value. The forbidden-label panic at registration prevents anyone
// from labeling by raw input text. The other ForbiddenLabels (path, query,
// user, user_id, ip, uuid, trace_id, span_id, email) are not reachable
// from this subsystem.
//
// MET-25 hot path: per-call observation uses pre-bound observers cached at
// the call site (see Plan 04-05 wiring + BenchmarkObserve_HotEmbed). The
// bag's Duration.Bind(provider, model, mode) returns a
// BoundEmbeddingLatencyObserver that is value-typed and concurrent-safe;
// callers store it in struct fields per MET-25.
//
// D-15b GaugeFunc panic safety: the queue_depth callback wraps
// defer-recover that returns 0 on probe panic — concurrent shutdown of
// the embed queue cannot poison /metrics. Same pattern as Plan 04-04
// MVCC GaugeFuncs.
//
// D-15b distinction (worker_running): binary lifecycle state uses
// `Set(0)` / `Set(1)` at the lifecycle start/stop hook sites, NOT a
// GaugeFunc — the lifecycle event boundary is the right observation
// chokepoint.
//
// D-02d leaf-package boundary: pkg/observability never imports pkg/embed
// or pkg/nornicdb. EmbedProbe is declared HERE; the embedder (or the
// embed-queue accessor adapter) satisfies it via a thin shim at the
// cmd/nornicdb wiring site.
//
// D-08 forward-compat: Embed metrics are NOT tenant-tagged. provider,
// model, and mode are global (not per-database). Per CONTEXT MET-21
// omission of embed/runtime/cache from the tenant axis. NewEmbedMetrics
// therefore omits the tenantLabelsEnabled bool parameter.
//
// D-09 FFI panic counter: ffi_panics_total{mode} is the only counter
// where pkg/embed observably crashed during a CGo call — incremented from
// the deferred-recover wrapper in pkg/embed/ffi_recover.go (Plan 04-05-03).
// Closed mode enum prevents arbitrary panic-class labeling.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedEmbedBackends is the closed enum for the `mode` label per CONTEXT
// D-06a. Mirrors the Embedder.Backend() return values declared in
// pkg/embed (build-tag matrix). Adding a new backend = enum update HERE +
// pkg/embed Backend() implementer + ADR §2.3 amendment.
var AllowedEmbedBackends = []string{"gpu", "cpu", "cuda", "metal", "vulkan"}

// AllowedEmbedResults is the closed enum for the `result` label on
// processed_total. Closed at the call site in pkg/embed wrappers (no user
// input flows here).
var AllowedEmbedResults = []string{"success", "failure", "cached"}

// AllowedEmbedProviders is the closed enum for the `provider` label on
// processed_total + duration_seconds. Adding a provider = enum update +
// ADR amendment. The "other" bucket catches custom embedder factories.
var AllowedEmbedProviders = []string{"ollama", "openai", "local", "other"}

// EmbedProbe is the seam between pkg/nornicdb embed-queue accessors and
// the observability queue_depth GaugeFunc callback (D-02d leaf-package
// boundary — pkg/observability never imports pkg/nornicdb or pkg/embed).
// The embed worker (or a thin adapter at cmd/nornicdb) satisfies this
// interface.
//
// QueueLen returns the current number of nodes pending embedding. The
// pull-based EmbedWorker model means this is typically 0 in steady state;
// during back-pressure / cold start it surfaces the real depth. Defensive
// callers may return 0 if no metric source is available.
type EmbedProbe interface {
	QueueLen() int
}

// EmbedMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the
// embeddings subsystem. One bag per Provider; constructed at cmd/nornicdb
// startup. The mode label is bound at observation time using the
// embedder's Backend() value (D-06).
//
// Hot-path discipline (MET-25): subsystem callers cache
// BoundEmbeddingLatencyObserver in struct fields at constructor time so
// per-call observation pays zero WithLabelValues lookup overhead. The
// processed_total CounterVec is incremented via `WithLabelValues(...)`
// at the embed completion site.
//
// Dual-access pattern (D-02): the bag exposes raw *prometheus.CounterVec
// / Gauge / *EmbeddingLatencyHistogram so subsystem tests can drive
// AssertCardinalityCeiling and edge cases.
type EmbedMetrics struct {
	// Processed counts embedding outcomes per (provider, model, result, mode).
	// Cardinality ceiling 250 per RESEARCH §Q11 (4 providers × ~30 models ×
	// 3 results × ~5 modes; over-provisioned to absorb new providers).
	Processed *prometheus.CounterVec

	// Duration is the per-call latency histogram with long-tail buckets
	// (EmbeddingLatencyBucketsSeconds, tail to 600s for slow local llama).
	// Hot-path: pre-bound via Duration.Bind(provider, model, mode) cached
	// in caller struct fields per MET-25.
	Duration *EmbeddingLatencyHistogram

	// CacheHits / CacheMisses count embed-cache outcomes — provider-agnostic
	// (the cache is a generic LRU keyed by hash). Bridges into the
	// cross-cutting Cache bag (Plan 04-01) at the cached_embedder.go call
	// sites. No labels — single flat counter pair per CONTEXT MET-12.
	CacheHits   prometheus.Counter
	CacheMisses prometheus.Counter

	// WorkerRunning is the binary lifecycle gauge (Set(0) on stop, Set(1)
	// on start). Per D-15b distinction: NOT a GaugeFunc — the lifecycle
	// event boundary is the right observation chokepoint.
	WorkerRunning prometheus.Gauge

	// FFIPanicTotal counts CGo / purego panics recovered in the FFI call
	// site wrapper (D-09; pkg/embed/ffi_recover.go). Mode = the build-tag
	// backend at the time of panic; closed enum.
	FFIPanicTotal *prometheus.CounterVec
}

// NewEmbedMetrics constructs the embeddings bag against reg.
//
// probe is the EmbedProbe accessor surface — typically the EmbedWorker or
// a thin adapter wrapping it. nil is tolerated (queue_depth GaugeFunc
// returns 0). The probe is consulted on every /metrics scrape; the
// callback wraps defer-recover so a probe panic does not poison the
// scrape (RISK-8 / Pitfall 1).
//
// No tenantLabelsEnabled parameter: embed families are global per CONTEXT
// MET-21 omission (provider/model/mode are not per-DB attributes; per-DB
// embedding latency lives at the Cypher subsystem if needed).
func NewEmbedMetrics(reg *prometheus.Registry, probe EmbedProbe) *EmbedMetrics {
	bag := &EmbedMetrics{}

	// MET-12 / D-15b: queue_depth via prometheus.NewGaugeFunc reading
	// EmbedProbe.QueueLen() on every scrape. defer-recover returns 0 on
	// panic per RESEARCH RISK-8 / Pitfall 1.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "embed",
		Name:      "queue_depth",
		Help: "Embedding queue depth (nodes pending embedding) at scrape " +
			"time. GaugeFunc — reads EmbedProbe.QueueLen() on every scrape; " +
			"returns 0 on probe panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.QueueLen())
	}))

	bag.Processed = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "embed",
			Name:      "processed_total",
			Help: "Embedding processing outcomes by provider (closed enum: " +
				"ollama, openai, local, other), model (open-ish; ceiling 250), " +
				"result (closed enum: success, failure, cached), and mode " +
				"(closed enum: gpu, cpu, cuda, metal, vulkan). Cardinality " +
				"bounded by RESEARCH §Q11.",
		},
		[]string{"provider", "model", "result", "mode"})

	bag.Duration = NewEmbeddingLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "embed",
			Name:      "duration_seconds",
			Help: "Per-call embedding latency by provider/model/mode. Uses " +
				"EmbeddingLatencyBucketsSeconds (long-tail to 600s per " +
				"MET-05) — local llama can be slow on first warmup.",
		},
		[]string{"provider", "model", "mode"})

	// Cache hits/misses — provider-agnostic counters. Bridge into the
	// cross-cutting Cache bag (Plan 04-01) at the cached_embedder.go call
	// sites; emitted here for subsystem-local visibility per MET-12.
	bag.CacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb",
		Subsystem: "embed",
		Name:      "cache_hits_total",
		Help: "Embedding cache hits (in-process LRU keyed by FNV-1a hash " +
			"of input text). Bridges into the cross-cutting Cache bag at " +
			"the call site.",
	})
	reg.MustRegister(bag.CacheHits)

	bag.CacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb",
		Subsystem: "embed",
		Name:      "cache_misses_total",
		Help: "Embedding cache misses (compute-and-store path). " +
			"Bridges into the cross-cutting Cache bag.",
	})
	reg.MustRegister(bag.CacheMisses)

	// worker_running uses Set(0)/Set(1) at lifecycle hooks per D-15b
	// distinction — binary state at the lifecycle event boundary, NOT
	// GaugeFunc.
	bag.WorkerRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "embed",
		Name:      "worker_running",
		Help: "Embedding worker lifecycle indicator (1 = running, 0 = " +
			"stopped). Set at StartWorkers/Stop hook sites per D-15b " +
			"binary-state distinction; NOT a GaugeFunc.",
	})
	reg.MustRegister(bag.WorkerRunning)

	// D-09: FFI panic counter. Incremented from the deferred-recover
	// wrapper in pkg/embed/ffi_recover.go on every recovered CGo panic.
	// mode is the build-tag-derived backend at panic time (closed enum
	// AllowedEmbedBackends). Cardinality ceiling = 5.
	bag.FFIPanicTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "embed",
			Name:      "ffi_panics_total",
			Help: "Recovered CGo / purego panics from local llama.cpp FFI " +
				"call sites. mode = build-tag-derived backend at panic " +
				"time (closed enum: gpu, cpu, cuda, metal, vulkan). Per " +
				"D-09: server stays up — counter increments and panic " +
				"converts to error.",
		},
		[]string{"mode"})

	return bag
}
