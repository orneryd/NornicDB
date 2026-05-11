package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerStartup_HTTPBoltMetricsRegistered verifies Plan 04-02-06:
// the HTTP and Bolt bags coexist with the Phase-1 go/process collectors,
// the Plan-04-01 Cache+Runtime bag, and each other on a single
// *prometheus.Registry. Mirrors the cache-bag startup smoke test from
// Plan 04-01.
//
// We do NOT boot the full server (CGO localllm dep unavailable without
// -tags localllm); we exercise the bag constructors directly against
// the same TestEnv-shaped registry the production startup uses.
func TestServerStartup_HTTPBoltMetricsRegistered(t *testing.T) {
	te := observability.NewTestEnv(t)

	// Construct all three Phase-4-so-far bags on the same registry.
	cacheBag := observability.NewCacheMetrics(te.Registry)
	require.NotNil(t, cacheBag)
	httpBag := observability.NewHTTPMetrics(te.Registry, false /* tenantLabelsEnabled */)
	require.NotNil(t, httpBag)
	boltBag := observability.NewBoltMetrics(te.Registry)
	require.NotNil(t, boltBag)

	// Materialize one series per *Vec so Gather() surfaces the family.
	cacheBag.Hits.WithLabelValues("query_result").Inc()
	cacheBag.Misses.WithLabelValues("query_result").Inc()
	cacheBag.SizeBytes.WithLabelValues("query_result").Set(0)
	cacheBag.Evictions.WithLabelValues("query_result", "lru").Inc()

	httpBag.Requests.WithLabelValues("GET", "/livez", "2xx").Inc()
	httpBag.RequestDuration.Bind("GET", "/livez", "2xx").Observe(nil, 0.001)
	httpBag.RequestBodyBytes.Bind("GET", "/livez").Observe(nil, 0)
	httpBag.ResponseBodyBytes.Bind("GET", "/livez").Observe(nil, 0)
	httpBag.InFlight.Inc()
	httpBag.InFlight.Dec()

	boltBag.ConnectionsActive.Inc()
	boltBag.ConnectionsActive.Dec()
	boltBag.ConnectionsTotal.WithLabelValues("success").Inc()
	boltBag.SessionDuration.Bind().Observe(nil, 0.001)
	boltBag.MessagesTotal.WithLabelValues("run", "success").Inc()
	boltBag.MessageDuration.Bind("run").Observe(nil, 0.001)
	boltBag.PackstreamDecodeErrors.WithLabelValues("truncated").Inc()

	// Verify all 11 Phase-4-so-far families are present (5 http + 6 bolt
	// = 11 new this plan; cache+runtime adds 6 more from Plan 04-01).
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}

	httpFamilies := []string{
		"nornicdb_http_requests_total",
		"nornicdb_http_request_duration_seconds",
		"nornicdb_http_in_flight_requests",
		"nornicdb_http_request_body_bytes",
		"nornicdb_http_response_body_bytes",
	}
	boltFamilies := []string{
		"nornicdb_bolt_connections_active",
		"nornicdb_bolt_connections_total",
		"nornicdb_bolt_session_duration_seconds",
		"nornicdb_bolt_messages_total",
		"nornicdb_bolt_message_duration_seconds",
		"nornicdb_bolt_packstream_decode_errors_total",
	}
	for _, want := range httpFamilies {
		assert.True(t, got[want], "MET-06: HTTP family %q must register at startup", want)
	}
	for _, want := range boltFamilies {
		assert.True(t, got[want], "MET-07: Bolt family %q must register at startup", want)
	}

	// Re-registering the same bags on the same registry MUST panic
	// (Pitfall 8 invariant — catches buggy double-init).
	require.Panics(t, func() {
		_ = observability.NewHTTPMetrics(te.Registry, false)
	}, "Re-registering HTTP bag on same registry must panic")
	require.Panics(t, func() {
		_ = observability.NewBoltMetrics(te.Registry)
	}, "Re-registering Bolt bag on same registry must panic")
}
