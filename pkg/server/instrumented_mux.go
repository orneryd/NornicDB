// Package server — HTTP instrumentation chokepoint (Plan 04-02 D-03).
//
// instrumentedMux is the SOLE observation site for HTTP requests per
// AGENTS.md §7 DRY (single chokepoint per subsystem). It wraps the
// existing *http.ServeMux at the http.Server.Handler mount site
// (server.go Start()), reading `r.Pattern` post-mux.ServeHTTP — the Go
// 1.22+ stdlib field populated AFTER pattern matching — to extract a
// closed-shape `path_template` label value (D-03 / KD-04: stdlib mux
// chosen, no router migration).
//
// Design (CONTEXT D-03 + Plan 04-02 task 04-02-02):
//   - Observation runs in a `defer func()` so a panicking handler still
//     emits an observation (with status 500) and the panic re-propagates
//     to the outer http.Server panic handler. RESEARCH §Q1 risk
//     addressed (T-04-08 mitigation).
//   - Empty `r.Pattern` (unmatched URL) bucketed as "_NOT_FOUND_" to
//     bound 404-path cardinality.
//   - `r.PathValue("database")` extracted at the chokepoint per D-10;
//     forwarded to BindRequestDuration which drops the arg when the bag
//     was constructed with tenantLabelsEnabled=false (D-08 forward-compat).
//   - `statusRecorder` captures the status code written by the handler
//     so status_class can be classified post-handler.
//   - InFlight gauge Inc'd before mux.ServeHTTP and Dec'd in defer pair
//     (deferred path always fires, even on panic).
//   - Bound observer cache keyed by (method, template, status_class[,
//     database]) tuple is hosted on the wrapper and amortized across
//     requests (MET-25). Per-request lookup is a single sync.Map Load
//     (no WithLabelValues alloc on hit) — Plan 04-07 cumulates the
//     BenchmarkObserve_Hot evidence.
//
// Forbidden-label discipline (Phase 3 D-03a / registration.go
// ForbiddenLabels): `r.URL.Path` is NEVER passed as a label value — only
// `r.Pattern` (the closed route-table template) reaches the
// path_template axis. The Phase-3 panic-at-registration guard catches
// any future regression that tries to slot raw `path` into the label set.
package server

import (
	"net/http"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// statusRecorder wraps http.ResponseWriter to capture the status code
// written by the handler. Default = 200 (Go's implicit status when no
// WriteHeader is called).
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// WriteHeader records the first explicit status code call. Subsequent
// WriteHeader calls are forwarded but the captured status reflects only
// the first call (matching net/http semantics where multiple
// WriteHeader calls log a warning and only the first takes effect).
func (sr *statusRecorder) WriteHeader(code int) {
	if !sr.wroteHeader {
		sr.status = code
		sr.wroteHeader = true
	}
	sr.ResponseWriter.WriteHeader(code)
}

// Write triggers an implicit WriteHeader(200) per net/http semantics —
// we record the implicit 200 if WriteHeader was not called explicitly.
func (sr *statusRecorder) Write(b []byte) (int, error) {
	if !sr.wroteHeader {
		sr.status = http.StatusOK
		sr.wroteHeader = true
	}
	return sr.ResponseWriter.Write(b)
}

// statusClass maps an HTTP status code to the closed-enum status_class
// label value (1xx-5xx) per CONTEXT D-03 / ADR §2.3.
func statusClass(code int) string {
	switch {
	case code >= 100 && code < 200:
		return "1xx"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		// Out-of-range codes (rare; e.g. 999 from a buggy handler) fall
		// to 5xx so they surface in error-rate alerts.
		return "5xx"
	}
}

// boundObservers caches per-template Bind() handles so the dispatch hot
// path pays no WithLabelValues lookup per request after warmup. Keyed by
// the full label tuple stringified; sync.Map chosen over map+RWMutex
// because the access pattern is read-mostly after the route table warms
// up (Phase 3 BenchmarkObserve_Hot precedent).
type boundObservers struct {
	durationCache sync.Map // key: cacheKey, val: observability.BoundLatencyObserver
	requestsCache sync.Map // key: cacheKey, val: prometheus.Counter
}

// cacheKey is a small struct used as a map key. Avoids string concat
// allocations on every lookup (RESEARCH §3 hot-path discipline).
type cacheKey struct {
	method   string
	template string
	class    string
	database string
}

// instrumentedMux wraps mux.ServeHTTP with the Plan 04-02 HTTP
// observation chokepoint. Returns an http.Handler that callers mount on
// http.Server.Handler in place of the bare *http.ServeMux.
//
// If m is nil, the wrapper is a no-op pass-through — keeps existing test
// fixtures and pre-Phase-1 callers compiling unchanged. Production
// startup (cmd/nornicdb/main.go) always supplies a non-nil bag.
//
// Phase 6 TRC-12: the same chokepoint also creates an `nornicdb.http.<route>`
// span per request. The span is started BEFORE ServeHTTP with a placeholder
// name (so handler code inherits the span via r.Context()), then renamed
// once r.Pattern resolves. W3C traceparent extraction is performed via the
// package-level propagator (set in observability.buildTracerProvider for
// TRC-11).
func instrumentedMux(mux http.Handler, m *observability.HTTPMetrics) http.Handler {
	if m == nil {
		return mux
	}
	cache := &boundObservers{}
	tracer := otel.Tracer("nornicdb/http")
	propagator := otel.GetTextMapPropagator()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		m.InFlight.Inc()

		// TRC-11 propagation: extract upstream traceparent/baggage from
		// incoming headers into ctx. TRC-12: start an HTTP span whose
		// parent is the extracted context (or empty for a fresh root
		// trace). Span name is a placeholder until r.Pattern resolves.
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, "nornicdb.http.pending",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
			),
		)
		// Propagate the span-bearing ctx to downstream handlers.
		r = r.WithContext(ctx)

		defer func() {
			// Always dec InFlight + observe — even on handler panic.
			m.InFlight.Dec()

			rec := recover()
			if rec != nil {
				// Handler panicked before/after writing status; surface
				// as 5xx in metrics so error-rate alerts fire.
				sw.status = http.StatusInternalServerError
			}

			// r.Pattern is populated by stdlib http.ServeMux AFTER
			// ServeHTTP completes pattern matching. Empty when no route
			// matched (404) — bucket to "_NOT_FOUND_" to bound 404-path
			// cardinality (D-03 / RESEARCH §Q11).
			tmpl := r.Pattern
			if tmpl == "" {
				tmpl = "_NOT_FOUND_"
			}
			class := statusClass(sw.status)

			// D-10: r.PathValue("database") extracts the {database}
			// pattern variable when present; empty when the matched
			// template has no {database} segment (e.g. /livez).
			db := r.PathValue("database")

			// TRC-12: rename the span now that the template is known;
			// attach status + database attributes. Mark 5xx spans as
			// error status per OTel semantic conventions.
			span.SetName("nornicdb.http." + tmpl)
			span.SetAttributes(
				attribute.String("http.route", tmpl),
				attribute.Int("http.response.status_code", sw.status),
			)
			if db != "" {
				span.SetAttributes(attribute.String("nornicdb.database", db))
			}
			if sw.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(sw.status))
			}
			span.End()

			key := cacheKey{method: r.Method, template: tmpl, class: class, database: db}

			// Pre-bound observer cache: lookup-or-Bind once per tuple.
			var dur observability.BoundLatencyObserver
			if v, ok := cache.durationCache.Load(key); ok {
				dur = v.(observability.BoundLatencyObserver)
			} else {
				dur = m.BindRequestDuration(r.Method, tmpl, class, db)
				cache.durationCache.Store(key, dur)
			}
			dur.Observe(r.Context(), time.Since(start).Seconds())

			// Same shape for the requests counter — paired axis with
			// duration so /metrics _total and _seconds_count series
			// have identical cardinality (CONTEXT D-02 dual-access).
			if v, ok := cache.requestsCache.Load(key); ok {
				v.(interface{ Inc() }).Inc()
			} else {
				ctr := m.BindRequests(r.Method, tmpl, class, db)
				cache.requestsCache.Store(key, ctr)
				ctr.Inc()
			}

			if rec != nil {
				// Re-panic AFTER observation per Go convention so the
				// outer http.Server panic handler fires (T-04-08).
				panic(rec)
			}
		}()

		mux.ServeHTTP(sw, r)
	})
}
