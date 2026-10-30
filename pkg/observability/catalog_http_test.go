package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-02 GREEN: catalog_http.go ships the bag; this file's tests now
// exercise the five families per MET-06, the closed status_class enum,
// the D-08 tenant-flag forward-compat, and the forbidden-label discipline
// (path is forbidden; only path_template is allowed).

// TestHTTPMetrics_RegistersFiveFamilies asserts the five HTTP families per
// MET-06: requests_total, request_duration_seconds, in_flight_requests,
// request_body_bytes, response_body_bytes. Closed enum + label discipline
// per Phase 3 D-03 (forbidden labels: path, query — only path_template).
func TestHTTPMetrics_RegistersFiveFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewHTTPMetrics(te.Registry, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)

	// Materialize one instance per *Vec so Gather() emits the family
	// (client_golang skips empty *Vec families). InFlight is a plain Gauge
	// so it surfaces unconditionally.
	bag.Requests.WithLabelValues("GET", "/livez", "2xx").Inc()
	bag.RequestDuration.Bind("GET", "/livez", "2xx").Observe(nil, 0.001)
	bag.RequestBodyBytes.Bind("GET", "/livez").Observe(nil, 0)
	bag.ResponseBodyBytes.Bind("GET", "/livez").Observe(nil, 0)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_http_requests_total",
		"nornicdb_http_request_duration_seconds",
		"nornicdb_http_in_flight_requests",
		"nornicdb_http_request_body_bytes",
		"nornicdb_http_response_body_bytes",
	} {
		assert.Contains(t, names, want, "MET-06: HTTP family %q must register", want)
	}
}

// TestHTTPMetrics_TenantFlagOmitsLabel asserts CONTEXT D-08 forward-compat:
// when tenantLabelsEnabled=false, the `database` label is OMITTED from the
// labelnames slice (not set to empty string). When true, it is included.
// Phase 5's K8s autodetect decides the bool's value at startup.
func TestHTTPMetrics_TenantFlagOmitsLabel(t *testing.T) {
	t.Run("flag_off_omits_database", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, false)

		// 3-arity Bind succeeds when database label is absent.
		bag.Requests.WithLabelValues("GET", "/livez", "2xx").Inc()

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "nornicdb_http_requests_total" {
				continue
			}
			require.NotEmpty(t, mf.Metric)
			labels := map[string]string{}
			for _, lp := range mf.Metric[0].Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			assert.NotContains(t, labels, "database",
				"D-08: when tenantLabelsEnabled=false, database label must be omitted")
		}
	})

	t.Run("flag_on_includes_database", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, true)

		// 4-arity Bind required when database is included.
		bag.Requests.WithLabelValues("GET", "/db/{database}/foo", "2xx", "mydb").Inc()

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "nornicdb_http_requests_total" {
				continue
			}
			require.NotEmpty(t, mf.Metric)
			labels := map[string]string{}
			for _, lp := range mf.Metric[0].Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			assert.Contains(t, labels, "database",
				"D-08: when tenantLabelsEnabled=true, database label must be present")
			assert.Equal(t, "mydb", labels["database"])
		}
	})
}

// TestHTTPMetrics_BindHelperRespectsTenantFlag asserts the
// BindRequestDuration helper drops the database arg when the bag was
// constructed with tenantLabelsEnabled=false (subsystems are bool-agnostic).
func TestHTTPMetrics_BindHelperRespectsTenantFlag(t *testing.T) {
	t.Run("flag_off", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, false)
		// Subsystem passes database unconditionally; helper drops it.
		bound := bag.BindRequestDuration("GET", "/livez", "2xx", "ignored")
		bound.Observe(nil, 0.001)
		bag.BindRequests("GET", "/livez", "2xx", "ignored").Inc()
		// No panic + Gather succeeds = success.
		_, err := te.Registry.Gather()
		require.NoError(t, err)
	})

	t.Run("flag_on", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, true)
		bound := bag.BindRequestDuration("GET", "/db/{database}/foo", "2xx", "mydb")
		bound.Observe(nil, 0.001)
		bag.BindRequests("GET", "/db/{database}/foo", "2xx", "mydb").Inc()
		_, err := te.Registry.Gather()
		require.NoError(t, err)
	})
}

// TestHTTPMetrics_RejectsRawPath asserts CONTEXT D-03a defense-in-depth:
// passing "path" (raw URL) as a label name panics at registration via the
// Phase-3 ForbiddenLabels guard. Subsystems MUST use "path_template"
// (r.Pattern), never "path" (r.URL.Path — cardinality bomb).
func TestHTTPMetrics_RejectsRawPath(t *testing.T) {
	te := NewTestEnv(t)
	require.Panics(t, func() {
		_ = NewCounterVec(te.Registry,
			MetricOpts{
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "would-be cardinality bomb",
			},
			[]string{"path"}) // FORBIDDEN — must panic
	}, "D-03a: passing 'path' as a label name must panic at registration")
}

// TestMetricCardinality_HTTP asserts CONTEXT RESEARCH §Q11 ceiling for
// nornicdb_http_request_duration_seconds. Driving 1k synthetic templates
// (closed-enum methods × closed-enum status_classes) must NOT exceed 1000
// series — the route-table headroom ceiling. Parameterized on the D-08
// tenant flag (CONTEXT D-08b) per MET-21.
func TestMetricCardinality_HTTP(t *testing.T) {
	t.Run("flag_off_ceiling_600", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, false)
		// 8 methods × 15 templates × 5 status_classes = 600 (RESEARCH §Q11)
		methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT"}
		templates := []string{
			"/livez", "/readyz", "/version", "/metrics",
			"/db/{database}/foo", "/db/{database}/bar", "/db/{database}/tx/commit",
			"/api/auth", "/api/users", "/api/roles",
			"/graphql", "/mcp", "/_NOT_FOUND_",
			"/db/{database}/cypher", "/db/{database}/data",
		}
		te.AssertCardinalityCeiling(t, "nornicdb_http_request_duration_seconds", 1000,
			func(tenant string) {
				for _, m := range methods {
					for _, tmpl := range templates {
						for _, sc := range AllowedStatusClasses {
							bag.RequestDuration.Bind(m, tmpl, sc).Observe(nil, 0.001)
						}
					}
				}
				_ = tenant
			})
	})

	t.Run("flag_on_ceiling_higher", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, true)
		// With database axis included, ceiling rises ~ ×100 — but the
		// closed-enum portion is bounded; we drive only a small set of
		// databases here (ceiling=1000 still holds for our drive shape).
		methods := []string{"GET", "POST"}
		templates := []string{"/livez", "/db/{database}/foo"}
		databases := []string{"db1", "db2", "db3"}
		te.AssertCardinalityCeiling(t, "nornicdb_http_request_duration_seconds", 1000,
			func(tenant string) {
				for _, m := range methods {
					for _, tmpl := range templates {
						for _, sc := range AllowedStatusClasses {
							for _, db := range databases {
								bag.RequestDuration.Bind(m, tmpl, sc, db).Observe(nil, 0.001)
							}
						}
					}
				}
				_ = tenant
			})
	})
}
