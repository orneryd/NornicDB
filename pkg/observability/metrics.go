// Package observability — typed metric constructors that enforce naming,
// bucket, and label discipline at registration time (Phase 3, MET-01..MET-05).
//
// Design (CONTEXT D-01):
//   - Bucket choice is encoded in the constructor function name; subsystem
//     authors cannot pass arbitrary Buckets. Single source of truth (D-01).
//   - Namespace="nornicdb" is injected by the helper; caller never sets it
//     (D-01b/D-01c — final name is nornicdb_<subsystem>_<name>).
//   - Subsystem is enforced against the closed allowedSubsystems list
//     (registration.go).
//   - Forbidden labels (cardinality bombs + PII) panic at registration
//     (registration.go::validateLabels — D-03a).
//
// Dual-access pattern (CONTEXT D-02 + <specifics>):
//   - These constructors return native *prometheus.HistogramVec /
//     *prometheus.CounterVec / *prometheus.GaugeVec so subsystems can
//     pre-bind via WithLabelValues for MET-25 hot-path discipline.
//   - The typed wrapper structs in exemplar.go (Plan 03-03) wrap the same
//     *HistogramVec to centralize the MET-24 exemplar-emission chokepoint.
//   - Wrappers do NOT hide the raw types — they're parallel returns. Phase 4
//     subsystems receive both: the raw *Vec for cardinality assertions and
//     edge cases, the wrapper for normal Observe() paths.
//
// Pitfall 8 / MustRegister precedent (analog: registry.go:28-29):
// validation failure at construction is a programming bug; panic IS the
// desired startup behavior — surfaces the bug before any traffic reaches
// the binary.
package observability

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricOpts carries the helper-injected Subsystem/Name/Help triple
// (CONTEXT D-01). Namespace is always "nornicdb" — caller cannot override
// (D-01b). ConstLabels intentionally omitted in M1; see CONTEXT.md
// "Deferred Ideas" for rationale.
type MetricOpts struct {
	Subsystem string // MUST be one of allowedSubsystems (D-01d — declared below in this file)
	Name      string // MUST end in _total | _seconds | _bytes | _rows per type (MET-02)
	Help      string // MUST be non-empty (client_golang convention)
}

// allowedSubsystems is the closed enumeration of metric-subsystem prefixes
// the helper layer accepts (CONTEXT D-01d location lock: "Subsystem allow-list
// lives in pkg/observability/metrics.go as `var allowedSubsystems = []string{...}`").
// Consumed by validateSubsystem() in registration.go via package-private
// reference (same package, no import needed). Adding a new subsystem requires
// a code change here AND an ADR amendment per CONTEXT.md "<specifics>" —
// drift in subsystem prefixes is exactly what MET-01 forbids.
//
// Reconciliation note: ADR §2.2 lists `apoc_, plugin_` and omits `auth_`;
// this list reflects ADR §2.3 (the actual catalog Phase 4 will register)
// per RESEARCH §3 / §Existing Call-Site Survey. APOC/plugin work flows
// through the standard subsystem prefixes (e.g., a plugin Cypher query
// goes through `cypher`); `auth` lands per GAP-6 / MET-15.
var allowedSubsystems = []string{
	"http", "bolt", "cypher", "storage", "mvcc",
	"embed", "search", "replication", "auth", "cache", "process",
}

// Bucket constants — single source of truth per MET-03 (CONTEXT D-01).
// Subsystem authors get the canonical bucket set via the constructor name;
// they cannot pass arbitrary buckets to override. Mutating these requires
// an ADR amendment (changes are observable in dashboards / alert rules).

// LatencyBucketsSeconds covers ~100us to 10s for request-latency histograms.
// Used by NewLatencyHistogramVec (MET-03).
var LatencyBucketsSeconds = []float64{
	.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5, 10,
}

// SizeBucketsBytes covers 64B to 64MB for payload-size histograms.
// Used by NewSizeHistogramVec (MET-03).
var SizeBucketsBytes = []float64{
	64, 256, 1024, 4096, 16384, 65536,
	262144, 1048576, 4194304, 16777216, 67108864,
}

// RowCountBuckets covers 1 to 1M for query result-set sizes.
// Used by NewRowCountHistogramVec (MET-03).
var RowCountBuckets = []float64{
	1, 10, 100, 1000, 10000, 100000, 1000000,
}

// EmbeddingLatencyBucketsSeconds covers ~1ms to 600s for LLM/local embedding
// calls. Long-tail (last bucket ≥ 60s) per MET-05 — verified by
// TestEmbeddingLatency_UsesLongTailBuckets.
// Used by NewEmbeddingLatencyHistogramVec (MET-05).
var EmbeddingLatencyBucketsSeconds = []float64{
	.001, .01, .1, .5, 1, 5, 10, 30, 60, 120, 300, 600,
}

// requireNonEmptyHelp panics if h is empty — client_golang convention.
// Centralized so all six constructors emit the same panic message.
func requireNonEmptyHelp(h, name string) {
	if h == "" {
		panic(fmt.Sprintf("observability: Help must be non-empty for %q", name))
	}
}

// NewLatencyHistogramVec constructs a request-latency histogram on reg.
// Validates Subsystem/Name/labels/Help per D-01a (panics on violation —
// programming bug per Pitfall 8). Returns the raw *prometheus.HistogramVec
// for hot-path WithLabelValues pre-binding (MET-25); see exemplar.go for
// the typed LatencyHistogram wrapper that centralizes exemplar emission.
//
// Final metric name: nornicdb_<opts.Subsystem>_<opts.Name>.
// Buckets locked to LatencyBucketsSeconds (MET-03 single source of truth).
// Suffix locked to _seconds (MET-02).
func NewLatencyHistogramVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.HistogramVec {
	validateSubsystem(opts.Subsystem)
	validateNameSuffix(opts.Name, "_seconds")
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
		Buckets:   LatencyBucketsSeconds,
	}, labels)
	reg.MustRegister(vec)
	return vec
}

// NewSizeHistogramVec constructs a payload-size histogram on reg.
// Same validation + Pitfall-8 panic semantics as NewLatencyHistogramVec.
// Buckets: SizeBucketsBytes. Suffix: _bytes (MET-02).
func NewSizeHistogramVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.HistogramVec {
	validateSubsystem(opts.Subsystem)
	validateNameSuffix(opts.Name, "_bytes")
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
		Buckets:   SizeBucketsBytes,
	}, labels)
	reg.MustRegister(vec)
	return vec
}

// NewRowCountHistogramVec constructs a result-set-size histogram on reg.
// Same validation + Pitfall-8 panic semantics. Buckets: RowCountBuckets.
// Suffix: _rows (MET-02).
func NewRowCountHistogramVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.HistogramVec {
	validateSubsystem(opts.Subsystem)
	validateNameSuffix(opts.Name, "_rows")
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
		Buckets:   RowCountBuckets,
	}, labels)
	reg.MustRegister(vec)
	return vec
}

// NewEmbeddingLatencyHistogramVec constructs a long-tail latency histogram
// for LLM / local embedding calls (MET-05). Same validation + Pitfall-8
// panic semantics. Buckets: EmbeddingLatencyBucketsSeconds (tail ≥ 60s).
// Suffix: _seconds.
func NewEmbeddingLatencyHistogramVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.HistogramVec {
	validateSubsystem(opts.Subsystem)
	validateNameSuffix(opts.Name, "_seconds")
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
		Buckets:   EmbeddingLatencyBucketsSeconds,
	}, labels)
	reg.MustRegister(vec)
	return vec
}

// NewCounterVec constructs a counter on reg.
// Same validation + Pitfall-8 panic semantics. Suffix: _total (MET-02).
func NewCounterVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.CounterVec {
	validateSubsystem(opts.Subsystem)
	validateNameSuffix(opts.Name, "_total")
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	}, labels)
	reg.MustRegister(vec)
	return vec
}

// NewGaugeVec constructs a gauge on reg.
// No suffix validation — ADR §2.2 does not lock a single gauge suffix
// (gauges may be unitless like _ratio, sized like _bytes, count like _count
// — context-specific). Subsystem and label discipline still enforced.
func NewGaugeVec(reg *prometheus.Registry, opts MetricOpts, labels []string) *prometheus.GaugeVec {
	validateSubsystem(opts.Subsystem)
	validateLabels(labels)
	requireNonEmptyHelp(opts.Help, opts.Name)

	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	}, labels)
	reg.MustRegister(vec)
	return vec
}
