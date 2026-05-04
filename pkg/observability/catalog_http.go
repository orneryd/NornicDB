// Package observability — HTTP metric bag (Plan 04-02 GREEN).
//
// Owns five families per MET-06 + ADR §2.3:
//
//	nornicdb_http_requests_total{method, path_template, status_class[, database]}
//	nornicdb_http_request_duration_seconds{method, path_template, status_class[, database]}
//	nornicdb_http_in_flight_requests
//	nornicdb_http_request_body_bytes{method, path_template}
//	nornicdb_http_response_body_bytes{method, path_template}
//
// Closed enums:
//
//	status_class ∈ AllowedStatusClasses = {1xx, 2xx, 3xx, 4xx, 5xx}
//	method       — bounded by HTTP method enum (GET, POST, PUT, DELETE,
//	                PATCH, HEAD, OPTIONS, CONNECT) ≈ 8 entries
//	path_template — open shape but bounded by route table (~15 templates;
//	                see RESEARCH §Q11 ceiling=1000 for headroom)
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `path` is in ForbiddenLabels; passing the raw URL would panic at
// registration. The label name `path_template` is NOT forbidden — but
// values MUST come from `r.Pattern` (Go 1.22+ stdlib ServeMux), NEVER from
// `r.URL.Path`. The instrumentedMux wrapper in pkg/server/server.go is the
// SOLE call site (D-03 single chokepoint).
//
// Tenant-flag forward-compat (CONTEXT D-08):
// `NewHTTPMetrics(reg, tenantLabelsEnabled bool)` decides label-set shape
// ONCE at construction. When false (Phase 4 default outside K8s), the
// `database` label is OMITTED from the labelnames slice — not set to empty
// string. When true (Phase 5 K8s autodetect default), `database` is included.
// Subsystems use the bool-agnostic `BindRequestDuration` / `BindRequests`
// helpers; they thread the database arg through but the helper drops it
// when the bag was constructed with tenantLabelsEnabled=false.
package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// AllowedStatusClasses is the closed enum for the `status_class` HTTP label.
// Mirrors the standard 1xx-5xx HTTP response class buckets per ADR §2.3.
var AllowedStatusClasses = []string{"1xx", "2xx", "3xx", "4xx", "5xx"}

// HTTPMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the HTTP
// subsystem. One bag per Provider; constructed at cmd/nornicdb startup
// between Phase 1's "registries" and "listeners" init steps.
//
// Hot-path discipline (MET-25): the instrumentedMux chokepoint
// (pkg/server/server.go) is the SOLE call site; it threads
// (method, template, status_class[, database]) through the
// `BindRequestDuration` / `BindRequests` helpers which return tenant-flag-
// agnostic Bound observers. Per-template Bind cache lives at the chokepoint
// (not in the bag) because path_template is not known until route resolution.
//
// Dual-access pattern (D-02): the bag exposes the raw *prometheus.CounterVec
// and Phase-3 typed *LatencyHistogram / *SizeHistogram so subsystem tests
// can drive AssertCardinalityCeiling and edge cases.
type HTTPMetrics struct {
	// RequestDuration is a latency histogram with Phase-3-locked buckets
	// (LatencyBucketsSeconds: ~100us to 10s).
	RequestDuration *LatencyHistogram

	// Requests is the request count counter; same label-set as RequestDuration
	// so the `_total` and `_seconds_count` series have identical cardinality.
	Requests *prometheus.CounterVec

	// InFlight is the active-request gauge; instrumentedMux Inc() on entry,
	// Dec() on exit (deferred). Single value, no labels (cardinality=1).
	InFlight prometheus.Gauge

	// RequestBodyBytes / ResponseBodyBytes are payload-size histograms with
	// Phase-3-locked buckets (SizeBucketsBytes: 64B to 64MB).
	RequestBodyBytes  *SizeHistogram
	ResponseBodyBytes *SizeHistogram

	// tenantLabelsEnabled is captured at construction so Bind helpers can
	// drop the database arg when the bag was registered without it (D-08).
	// Subsystems are bool-agnostic; only this struct knows the shape.
	tenantLabelsEnabled bool
}

// NewHTTPMetrics constructs the HTTP bag against reg.
//
// tenantLabelsEnabled is the D-08 forward-compat hook. When true, the
// `database` label is included in RequestDuration and Requests; when false,
// it is omitted. Phase 5's K8s autodetect (MET-22) decides the value.
//
// Validation + Pitfall-8 panic semantics inherited from Phase 3 typed
// constructors: missing _total/_seconds/_bytes suffixes or forbidden
// labels (e.g. accidentally passing "path" instead of "path_template")
// panic at registration.
func NewHTTPMetrics(reg *prometheus.Registry, tenantLabelsEnabled bool) *HTTPMetrics {
	durationLabels := []string{"method", "path_template", "status_class"}
	if tenantLabelsEnabled {
		durationLabels = append(durationLabels, "database")
	}

	hm := &HTTPMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	hm.RequestDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help: "HTTP request latency seconds. Labels: method, path_template (r.Pattern), " +
				"status_class (1xx-5xx), database (when tenant-labels enabled per D-08).",
		},
		durationLabels)

	hm.Requests = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "http",
			Name:      "requests_total",
			Help: "HTTP requests by method, path_template, status_class. " +
				"Same label-set as request_duration_seconds (database when D-08 flag enabled).",
		},
		durationLabels)

	hm.InFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "http",
		Name:      "in_flight_requests",
		Help:      "Number of HTTP requests currently being processed.",
	})
	reg.MustRegister(hm.InFlight)

	hm.RequestBodyBytes = NewSizeHistogram(reg,
		MetricOpts{
			Subsystem: "http",
			Name:      "request_body_bytes",
			Help:      "HTTP request body size in bytes (Content-Length).",
		},
		[]string{"method", "path_template"})

	hm.ResponseBodyBytes = NewSizeHistogram(reg,
		MetricOpts{
			Subsystem: "http",
			Name:      "response_body_bytes",
			Help:      "HTTP response body size in bytes (bytes written by handler).",
		},
		[]string{"method", "path_template"})

	return hm
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled. Read-only after construction.
func (h *HTTPMetrics) TenantLabelsEnabled() bool { return h.tenantLabelsEnabled }

// BindRequestDuration returns a pre-bound BoundLatencyObserver for the
// (method, template, statusClass, database) tuple. When the bag was
// constructed with tenantLabelsEnabled=false, the database arg is dropped.
// Subsystems are bool-agnostic and pass database unconditionally.
//
// Hot-path discipline (MET-25): the per-template Bind cache lives at the
// instrumentedMux chokepoint, NOT inside this helper — calling
// BindRequestDuration per request still pays a WithLabelValues lookup.
// The chokepoint caches the BoundLatencyObserver in a sync.Map keyed by
// the label tuple so per-request cost is amortized to near-zero.
func (h *HTTPMetrics) BindRequestDuration(method, template, statusClass, database string) BoundLatencyObserver {
	if h.tenantLabelsEnabled {
		return h.RequestDuration.Bind(method, template, statusClass, database)
	}
	return h.RequestDuration.Bind(method, template, statusClass)
}

// BindRequests returns a pre-bound prometheus.Counter for the
// (method, template, statusClass, database) tuple. Tenant-flag-aware
// per BindRequestDuration above.
func (h *HTTPMetrics) BindRequests(method, template, statusClass, database string) prometheus.Counter {
	if h.tenantLabelsEnabled {
		return h.Requests.WithLabelValues(method, template, statusClass, database)
	}
	return h.Requests.WithLabelValues(method, template, statusClass)
}

// ObserveRequestDuration is a thin convenience wrapper around
// BindRequestDuration().Observe — used by tests and cold paths. Hot-path
// callers should hoist Bind calls out of the request loop.
func (h *HTTPMetrics) ObserveRequestDuration(ctx context.Context, method, template, statusClass, database string, sec float64) {
	h.BindRequestDuration(method, template, statusClass, database).Observe(ctx, sec)
}
