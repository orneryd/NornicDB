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
//
// Lint-cardinality note (MET-04 / Plan 03-02): pkg/server is in scope of
// the Makefile lint-cardinality scanner, which forbids direct
// prometheus.New(Counter|Gauge|Histogram|Summary)(Vec) calls in business
// packages. Source-family seeding is therefore intentionally NOT done in
// this file — the byte-stream contract for RenderLegacy is already locked
// by pkg/observability/legacy_snapshot.golden (Plan 05-02 / Plan 05-04
// integration uses an empty registry, which still emits all 12 # HELP /
// # TYPE header lines + zero-value samples — sufficient to prove the
// server-layer wiring without registering any business metric).
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

// Test_HandleMetrics_DeprecationHeaders is the SERVER-LAYER wiring contract
// for Plan 05-04: handleMetrics must call observability.RenderLegacy AND
// set all three locked headers (Content-Type, Deprecation, Sunset) before
// writing the body. ROADMAP SC #1 + SC #2 wire-level satisfied.
//
// Uses an empty *prometheus.Registry: RenderLegacy still emits the 12
// `# HELP` + `# TYPE` header pairs and zero-value samples for every
// mapping row when no source family is registered (Plan 05-02 nil-safe
// emit contract). This proves the handler invoked RenderLegacy without
// requiring this server-layer test to register source metrics directly
// (lint-cardinality MET-04 forbids prometheus.NewGauge/Counter in
// pkg/server — the byte-stream contract under populated sources is
// locked separately by pkg/observability/legacy_snapshot.golden).
func Test_HandleMetrics_DeprecationHeaders(t *testing.T) {
	reg := prometheus.NewRegistry()

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

	// Spot-check: RenderLegacy emits all 12 # HELP / # TYPE header pairs
	// even with an empty registry — proves the handler invoked it. We
	// assert on three representative mapping rows (uptime, nodes_total,
	// info) covering the three emit-format branches (%.2f, %d, labeled).
	assert.Contains(t, body, "# HELP nornicdb_uptime_seconds",
		"body must include the legacy uptime HELP comment — proves RenderLegacy was invoked")
	assert.Contains(t, body, "# TYPE nornicdb_uptime_seconds gauge",
		"body must include the legacy uptime TYPE comment")
	assert.Contains(t, body, "nornicdb_uptime_seconds 0.00",
		"body must contain the legacy uptime sample line at zero (no source registered)")
	assert.Contains(t, body, "# HELP nornicdb_nodes_total",
		"body must include the legacy nodes_total HELP comment")
	assert.Contains(t, body, "nornicdb_nodes_total 0",
		"body must contain the legacy nodes_total sample line at zero")
	assert.Contains(t, body, "# HELP nornicdb_info",
		"body must include the legacy info HELP comment")
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
	// Body MUST be empty when registry is nil — RenderLegacy returns []byte{}
	// for a nil registry per Plan 05-02 contract. The important guarantee
	// is no panic + correct headers + zero-byte body (vs the empty-registry
	// path above, which emits 12 # HELP/# TYPE/zero-value triples).
	require.Equal(t, 0, rec.Body.Len(),
		"nil registry must produce empty body — distinct from empty-registry path")
}
