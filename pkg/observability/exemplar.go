// Package observability — typed exemplar-wrapper layer for histogram
// observation (Phase 3, MET-24 + MET-25).
//
// Design (CONTEXT D-02):
//   - Wrappers (LatencyHistogram etc.) own the MET-24 exemplar-emission
//     chokepoint. Subsystems use the wrapper for normal Observe paths;
//     the raw *prometheus.HistogramVec from metrics.go remains accessible
//     via Vec() for cardinality-ceiling assertions and edge cases.
//   - Bind(lvs ...) returns a Bound*Observer that subsystems cache as a
//     struct field at construction time — eliminates per-Observe
//     WithLabelValues lookup (MET-25 hot-path discipline).
//   - The IsValid()→ExemplarObserver chokepoint is a SINGLE function
//     (observeWithExemplar) per AGENTS.md §7 DRY. Every wrapper Observe
//     and every Bound Observe funnel through it.
//
// Forward-compat (CONTEXT D-02a + Phase 2 D-05 precedent):
//
//	Phase 1 ships sdktrace.NeverSample() ⇒ every emitted SpanContext has
//	IsSampled()=false ⇒ chokepoint short-circuits ⇒ ZERO exemplar-path
//	allocations today (also true on the truly-empty noop SpanContext where
//	IsValid()=false). Phase 6 flips the sampler in ONE place (provider.go);
//	ALL Phase-3-wrapped histograms emit exemplars automatically — no
//	second-pass migration of the ~60 Phase 4 metric families. Same posture
//	as Phase 2's mandatory_fields trace-id resolution.
//
// Allocation budget (CONTEXT D-02b, falsified by BenchmarkObserve_Hot):
//
//	hot_no_span sub-bench:  ≤ 2 allocs/op (target: 0 — chokepoint short-circuits)
//	hot_with_span sub-bench: ≤ 4 allocs/op (1 for Labels map, 2 for *.String() calls)
//	If hot_with_span exceeds 4, D-02b1 escalation triggers:
//	  sync.Pool[*prometheus.Labels] — captured here so future plans don't re-derive.
//
// Counter/gauge exemplars deferred (CONTEXT D-02e). Phase 3 ships histograms only.
package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// observeWithExemplar is the SINGLE chokepoint for histogram observation
// across all four wrapper types and all four Bound observer types
// (CONTEXT D-02a). Centralizing the sampled-span guard here means:
//  1. Phase 6's sampler flip lights up exemplars at every call site at once.
//  2. The hot-path alloc budget (D-02b) is measured at one location.
//  3. AGENTS.md §7 DRY: 8 Observe methods share one body.
//
// Sampling-vs-validity discipline (deviation from CONTEXT D-02a literal
// snippet — TestExemplarEmission_OnlyWhenSpanValid/ctx-with-noop-span is
// the falsifiability gate that pins this):
//
//	A SpanContext from sdktrace.NeverSample() is still IsValid()=true (the
//	trace/span IDs are populated for propagation) but IsSampled()=false.
//	Attaching an exemplar with a trace_id that the backend never persisted
//	is a debuggability foot-gun: operators click the exemplar in Grafana
//	and Tempo returns "trace not found". We therefore guard on
//	IsValid() && IsSampled() so exemplars correlate ONLY with traces the
//	tracer actually exported. Phase 2 D-05's mandatory_fields uses only
//	IsValid() because trace_id/span_id in logs are still useful even when
//	unsampled (the operator can grep the application logs); exemplars do
//	not have that fallback.
//
// Type assertion safety (RESEARCH §1): *prometheus.HistogramVec.WithLabelValues(...)
// always returns a value implementing prometheus.ExemplarObserver — verified at
// client_golang@v1.23.2/prometheus/histogram.go:520. The `ok` branch protects
// against a future client_golang change in case histogram.go ever stops
// implementing ExemplarObserver.
//
// Nil-safety (D-02c): In rare race conditions during startup (metricsAttached
// atomic is set before all observer fields are bound), obs may be nil.
// This guard prevents panics on concurrent observation during AttachMetrics.
func observeWithExemplar(ctx context.Context, obs prometheus.Observer, sec float64) {
	if obs == nil {
		return
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.IsSampled() {
		if eo, ok := obs.(prometheus.ExemplarObserver); ok {
			eo.ObserveWithExemplar(sec, prometheus.Labels{
				"trace_id": sc.TraceID().String(),
				"span_id":  sc.SpanID().String(),
			})
			return
		}
	}
	obs.Observe(sec)
}

// ----- LatencyHistogram (request latency, ≤10s tail) ------------------------

// LatencyHistogram wraps *prometheus.HistogramVec to centralize the MET-24
// exemplar-emission chokepoint for latency histograms. Subsystems use this
// wrapper for normal Observe paths; the raw *HistogramVec remains accessible
// via Vec() for cardinality-ceiling assertions (D-02 dual-access pattern).
type LatencyHistogram struct{ vec *prometheus.HistogramVec }

// NewLatencyHistogram constructs the wrapper around a fresh
// NewLatencyHistogramVec. Validation + Pitfall-8 panic semantics inherited
// from metrics.go.
func NewLatencyHistogram(reg *prometheus.Registry, opts MetricOpts, labels []string) *LatencyHistogram {
	return &LatencyHistogram{vec: NewLatencyHistogramVec(reg, opts, labels)}
}

// Vec returns the underlying *prometheus.HistogramVec — used by Phase 4
// subsystem tests for AssertCardinalityCeiling and edge-case observation
// (D-02 dual-access pattern; <specifics> §6).
func (h *LatencyHistogram) Vec() *prometheus.HistogramVec { return h.vec }

// Observe is the cold-path entry: pays a WithLabelValues lookup per call.
// Funnels through observeWithExemplar (D-02a chokepoint).
func (h *LatencyHistogram) Observe(ctx context.Context, lvs []string, sec float64) {
	observeWithExemplar(ctx, h.vec.WithLabelValues(lvs...), sec)
}

// Bind returns a pre-bound observer cached as a struct field at construction
// time (MET-25). WithLabelValues lookup amortized away from request-loop hot
// path. The returned BoundLatencyObserver is value-typed (no pointer alloc).
func (h *LatencyHistogram) Bind(lvs ...string) BoundLatencyObserver {
	return BoundLatencyObserver{obs: h.vec.WithLabelValues(lvs...)}
}

// BoundLatencyObserver is the struct-field-cacheable hot-path observer.
// Stateless beyond the embedded prometheus.Observer — concurrent Observe
// calls are race-clean per client_golang HistogramVec promise (RESEARCH §1/§4).
type BoundLatencyObserver struct{ obs prometheus.Observer }

// Observe routes through the sampled-span ExemplarObserver chokepoint (D-02a).
// On Phase 1 NeverSample default ⇒ IsSampled()=false ⇒ zero exemplar overhead.
// On Phase 6 sampler flip ⇒ exemplars emit automatically.
func (b BoundLatencyObserver) Observe(ctx context.Context, sec float64) {
	observeWithExemplar(ctx, b.obs, sec)
}

// ----- SizeHistogram (payload bytes, ≤64MB tail) ---------------------------

// SizeHistogram wraps *prometheus.HistogramVec for payload-size families
// (MET-03 SizeBucketsBytes). See LatencyHistogram for design notes.
type SizeHistogram struct{ vec *prometheus.HistogramVec }

func NewSizeHistogram(reg *prometheus.Registry, opts MetricOpts, labels []string) *SizeHistogram {
	return &SizeHistogram{vec: NewSizeHistogramVec(reg, opts, labels)}
}
func (h *SizeHistogram) Vec() *prometheus.HistogramVec { return h.vec }
func (h *SizeHistogram) Observe(ctx context.Context, lvs []string, bytes float64) {
	observeWithExemplar(ctx, h.vec.WithLabelValues(lvs...), bytes)
}
func (h *SizeHistogram) Bind(lvs ...string) BoundSizeObserver {
	return BoundSizeObserver{obs: h.vec.WithLabelValues(lvs...)}
}

// BoundSizeObserver is the pre-bound hot-path observer for SizeHistogram.
type BoundSizeObserver struct{ obs prometheus.Observer }

func (b BoundSizeObserver) Observe(ctx context.Context, bytes float64) {
	observeWithExemplar(ctx, b.obs, bytes)
}

// ----- RowCountHistogram (result rows, ≤1M tail) ---------------------------

// RowCountHistogram wraps *prometheus.HistogramVec for result-set-size
// families (MET-03 RowCountBuckets). See LatencyHistogram for design notes.
type RowCountHistogram struct{ vec *prometheus.HistogramVec }

func NewRowCountHistogram(reg *prometheus.Registry, opts MetricOpts, labels []string) *RowCountHistogram {
	return &RowCountHistogram{vec: NewRowCountHistogramVec(reg, opts, labels)}
}
func (h *RowCountHistogram) Vec() *prometheus.HistogramVec { return h.vec }
func (h *RowCountHistogram) Observe(ctx context.Context, lvs []string, rows float64) {
	observeWithExemplar(ctx, h.vec.WithLabelValues(lvs...), rows)
}
func (h *RowCountHistogram) Bind(lvs ...string) BoundRowCountObserver {
	return BoundRowCountObserver{obs: h.vec.WithLabelValues(lvs...)}
}

// BoundRowCountObserver is the pre-bound hot-path observer for RowCountHistogram.
type BoundRowCountObserver struct{ obs prometheus.Observer }

func (b BoundRowCountObserver) Observe(ctx context.Context, rows float64) {
	observeWithExemplar(ctx, b.obs, rows)
}

// ----- EmbeddingLatencyHistogram (LLM/embedding latency, ≤600s tail) -------

// EmbeddingLatencyHistogram wraps *prometheus.HistogramVec for LLM/embedding
// latency families (MET-05 long-tail buckets). See LatencyHistogram for
// design notes.
type EmbeddingLatencyHistogram struct{ vec *prometheus.HistogramVec }

func NewEmbeddingLatencyHistogram(reg *prometheus.Registry, opts MetricOpts, labels []string) *EmbeddingLatencyHistogram {
	return &EmbeddingLatencyHistogram{vec: NewEmbeddingLatencyHistogramVec(reg, opts, labels)}
}
func (h *EmbeddingLatencyHistogram) Vec() *prometheus.HistogramVec { return h.vec }
func (h *EmbeddingLatencyHistogram) Observe(ctx context.Context, lvs []string, sec float64) {
	observeWithExemplar(ctx, h.vec.WithLabelValues(lvs...), sec)
}
func (h *EmbeddingLatencyHistogram) Bind(lvs ...string) BoundEmbeddingLatencyObserver {
	return BoundEmbeddingLatencyObserver{obs: h.vec.WithLabelValues(lvs...)}
}

// BoundEmbeddingLatencyObserver is the pre-bound hot-path observer for
// EmbeddingLatencyHistogram.
type BoundEmbeddingLatencyObserver struct{ obs prometheus.Observer }

func (b BoundEmbeddingLatencyObserver) Observe(ctx context.Context, sec float64) {
	observeWithExemplar(ctx, b.obs, sec)
}
