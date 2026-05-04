// Package server: tests for the public (unauthenticated discovery / health
// / metrics) handlers in server_public.go.
//
// Plan 05-04 (legacy translation server adapter):
//   - Test_HandleMetrics_DeprecationHeaders verifies that the rewritten
//     handleMetrics calls observability.RenderLegacy and sets the three
//     locked headers (Content-Type, Deprecation, Sunset) per MET-19/MET-20.
//   - Test_HandleMetrics_NilRegistry_ReturnsEmptyBodyWithHeaders verifies
//     the nil-safety contract — when SetObsRegistry was never called, the
//     handler must not panic; it must return 200 + headers + (possibly
//     empty) body bytes.
//
// These tests bypass the auth wrapper (server_router.go:117 `s.withAuth(...)`)
// and call s.handleMetrics directly. Auth-gate coverage is already provided
// by the existing pkg/server middleware test suite (e.g. server_middleware_auth_test.go);
// the D-04 invariant ("auth gate verbatim") is enforced separately at the
// plan-verification layer via `git diff --quiet pkg/server/server_router.go`.
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// newMinimalServerForMetricsHandler returns a *Server with only the fields
// needed for handleMetrics + obsRegistryForHandler to run safely. The
// rewritten Plan-05-04 handler depends solely on s.mu (zero-value RWMutex
// is usable) and s.obsRegistry (nil-safe via observability.RenderLegacy).
//
// We deliberately do NOT use the heavy setupTestServer fixture from
// server_test.go: that helper opens a Badger database, an authenticator,
// etc. — none of which the rewritten handleMetrics touches. Keeping this
// fixture minimal ensures the test asserts only the wire contract
// (RenderLegacy + headers + body) and runs in microseconds.
func newMinimalServerForMetricsHandler(t *testing.T) *Server {
	t.Helper()
	return &Server{}
}

// seedLegacyTestRegistry registers a tiny set of source families on `reg`
// so that RenderLegacy emits a recognisable body. Full 12-metric coverage
// is already validated in pkg/observability/legacy_translation_test.go
// (TestRenderLegacy_Snapshot against the locked golden file). The
// integration test only needs to prove the SERVER-LAYER WIRING:
// handler -> RenderLegacy -> headers + bytes.
func seedLegacyTestRegistry(t *testing.T, reg *prometheus.Registry) {
	t.Helper()

	// nornicdb_process_uptime_seconds → maps to nornicdb_uptime_seconds (gauge, %.2f)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: "nornicdb_process_uptime_seconds", Help: "test uptime"},
		func() float64 { return 42 },
	))

	// nornicdb_storage_nodes_total → maps to nornicdb_nodes_total (gauge, %d)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: "nornicdb_storage_nodes_total", Help: "test nodes"},
		func() float64 { return 10 },
	))

	// nornicdb_storage_edges_total → maps to nornicdb_edges_total (gauge, %d)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: "nornicdb_storage_edges_total", Help: "test edges"},
		func() float64 { return 0 },
	))

	// nornicdb_http_in_flight_requests → maps to nornicdb_active_requests (gauge, %d)
	inflight := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nornicdb_http_in_flight_requests", Help: "test inflight",
	})
	inflight.Set(1)
	reg.MustRegister(inflight)
}

// Test_HandleMetrics_DeprecationHeaders is the SERVER-LAYER wiring contract
// for Plan 05-04: handleMetrics must call observability.RenderLegacy AND
// set all three locked headers (Content-Type, Deprecation, Sunset) before
// writing the body. ROADMAP SC #1 + SC #2 wire-level satisfied.
func Test_HandleMetrics_DeprecationHeaders(t *testing.T) {
	reg := prometheus.NewRegistry()
	seedLegacyTestRegistry(t, reg)

	s := newMinimalServerForMetricsHandler(t)
	s.SetObsRegistry(reg)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	s.handleMetrics(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "status must be 200")
	require.Equal(t, observability.LegacyContentType, rec.Header().Get("Content-Type"),
		"Content-Type must come from observability.LegacyContentType const")
	require.Equal(t, observability.LegacyDeprecation, rec.Header().Get("Deprecation"),
		"Deprecation header must come from observability.LegacyDeprecation const")
	require.Equal(t, observability.LegacySunset, rec.Header().Get("Sunset"),
		"Sunset header must come from observability.LegacySunset const")

	// Sanity-check the three locked values match the public-API contract.
	require.Equal(t, "text/plain; version=0.0.4; charset=utf-8", rec.Header().Get("Content-Type"))
	require.Equal(t, "true", rec.Header().Get("Deprecation"))
	require.Equal(t, "Fri, 31 Dec 2027 23:59:59 GMT", rec.Header().Get("Sunset"))

	body := rec.Body.String()
	// Spot-check: at least one mapped metric line appears with the expected
	// reduced value. The full 12-metric byte-stream contract is locked by
	// pkg/observability/legacy_snapshot.golden — this assertion only
	// proves the server handed bytes off to RenderLegacy.
	assert.Contains(t, body, "nornicdb_uptime_seconds 42.00",
		"body must contain the legacy uptime line — proves RenderLegacy was invoked")
	assert.Contains(t, body, "nornicdb_nodes_total 10",
		"body must contain the legacy nodes_total line")
	assert.Contains(t, body, "nornicdb_active_requests 1",
		"body must contain the legacy active_requests line")
	assert.Contains(t, body, "# HELP nornicdb_uptime_seconds",
		"body must include Prometheus exposition HELP comments")
	assert.Contains(t, body, "# TYPE nornicdb_uptime_seconds gauge",
		"body must include Prometheus exposition TYPE comments")
}

// Test_HandleMetrics_NilRegistry_ReturnsEmptyBodyWithHeaders pins the
// nil-safety contract for handleMetrics. Server fixtures, pre-Phase-5
// callers, or any path where SetObsRegistry was never called must NOT
// crash :7474/metrics — the handler must return 200 + the three headers,
// even if the body is empty.
//
// This is the production fail-safe behaviour for the case where the
// startup hook in cmd/nornicdb/main.go (Plan 05-04 Task 03) is not yet
// wired or fails before its setter call.
func Test_HandleMetrics_NilRegistry_ReturnsEmptyBodyWithHeaders(t *testing.T) {
	s := newMinimalServerForMetricsHandler(t)
	// Deliberately do NOT call s.SetObsRegistry — leave obsRegistry == nil.

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	require.NotPanics(t, func() {
		s.handleMetrics(rec, req)
	}, "handleMetrics must not panic when obsRegistry is nil")

	require.Equal(t, http.StatusOK, rec.Code, "nil registry must still return 200")
	require.Equal(t, observability.LegacyContentType, rec.Header().Get("Content-Type"))
	require.Equal(t, observability.LegacyDeprecation, rec.Header().Get("Deprecation"))
	require.Equal(t, observability.LegacySunset, rec.Header().Get("Sunset"))
	// Body is allowed to be empty when registry is nil — RenderLegacy
	// returns []byte{} for a nil registry per Plan 05-02 contract. The
	// important guarantee is no panic + correct headers.
}
