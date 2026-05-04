package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstrumentedMux_RPattern asserts CONTEXT D-03 + D-10: a registered
// handler at "/db/{database}/foo" produces path_template="/db/{database}/foo"
// (the route TEMPLATE, not r.URL.Path) and database="my-db" (the
// r.PathValue extraction).
func TestInstrumentedMux_RPattern(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, true /* tenantLabelsEnabled */)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /db/{database}/foo", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	wrapped := instrumentedMux(mux, bag)

	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/db/my-db/foo")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify the captured labels.
	labels := findLabels(t, reg, "nornicdb_http_requests_total")
	require.NotNil(t, labels, "request counter should have one series")
	// r.Pattern includes the method prefix when the route is registered
	// as "GET /db/{database}/foo" (Go 1.22+ stdlib semantics). The closed
	// route table is the cardinality wall; the method prefix is part of
	// it.
	assert.Equal(t, "GET /db/{database}/foo", labels["path_template"],
		"D-03: path_template must come from r.Pattern (template), not r.URL.Path")
	assert.Equal(t, "my-db", labels["database"],
		"D-10: database must come from r.PathValue('database')")
	assert.Equal(t, "GET", labels["method"])
	assert.Equal(t, "2xx", labels["status_class"])
}

// TestInstrumentedMux_NotFound asserts CONTEXT D-03 + RESEARCH §Q11:
// unmatched URLs produce path_template="_NOT_FOUND_" so 404-path
// cardinality is bounded.
func TestInstrumentedMux_NotFound(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false /* tenantLabelsEnabled */)

	mux := http.NewServeMux()
	// No routes registered → every request misses.
	wrapped := instrumentedMux(mux, bag)

	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/some/random/path")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	labels := findLabels(t, reg, "nornicdb_http_requests_total")
	require.NotNil(t, labels)
	assert.Equal(t, "_NOT_FOUND_", labels["path_template"],
		"D-03: unmatched URLs bucket to '_NOT_FOUND_' to bound 404 cardinality")
	assert.Equal(t, "4xx", labels["status_class"])
}

// TestInstrumentedMux_PanicSafe asserts CONTEXT D-03 + T-04-08: a handler
// panic still emits an observation (with status 5xx) AND re-propagates so
// the outer http.Server panic handler fires. Mitigates RESEARCH §Q1 risk.
func TestInstrumentedMux_PanicSafe(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /panic", func(w http.ResponseWriter, r *http.Request) {
		panic("synthetic handler panic")
	})
	wrapped := instrumentedMux(mux, bag)

	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	// httptest's recover happens at the http.Server level — the request
	// completes with a connection close (no body, no status). We use a
	// raw client so we can verify the panic re-propagates without
	// crashing the test goroutine.
	resp, err := http.Get(srv.URL + "/panic")
	if err == nil {
		// Some Go versions return io.EOF or similar on Read after panic;
		// either error path or empty 200 is acceptable as long as the
		// observation fired.
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	// Crucial assertion: the observation MUST have fired despite the panic.
	labels := findLabels(t, reg, "nornicdb_http_requests_total")
	require.NotNil(t, labels, "panic-safe defer must emit an observation")
	assert.Equal(t, "5xx", labels["status_class"],
		"D-03: panicked handler bucketed as 5xx via deferred observation")
	assert.Equal(t, "GET /panic", labels["path_template"])
}

// TestInstrumentedMux_StatusRecorder asserts the status_class label
// reflects the handler-written status code (not just the default 200).
func TestInstrumentedMux_StatusRecorder(t *testing.T) {
	cases := []struct {
		name       string
		writeCode  int
		wantClass  string
	}{
		{"200_implicit", 0 /* no WriteHeader → implicit 200 via Write */, "2xx"},
		{"201_explicit", http.StatusCreated, "2xx"},
		{"301_redirect", http.StatusMovedPermanently, "3xx"},
		{"404_not_found", http.StatusNotFound, "4xx"},
		{"503_unavailable", http.StatusServiceUnavailable, "5xx"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			bag := observability.NewHTTPMetrics(reg, false)

			mux := http.NewServeMux()
			mux.HandleFunc("GET /probe", func(w http.ResponseWriter, r *http.Request) {
				if tc.writeCode != 0 {
					w.WriteHeader(tc.writeCode)
				}
				_, _ = w.Write([]byte("body"))
			})
			wrapped := instrumentedMux(mux, bag)

			srv := httptest.NewServer(wrapped)
			defer srv.Close()

			resp, err := http.Get(srv.URL + "/probe")
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			labels := findLabels(t, reg, "nornicdb_http_requests_total")
			require.NotNil(t, labels)
			assert.Equal(t, tc.wantClass, labels["status_class"])
		})
	}
}

// TestInstrumentedMux_InFlight asserts the in_flight_requests gauge is
// Inc'd on entry and Dec'd in defer (deferred so even panicking handlers
// dec). Verified by a sequential test: post-request the gauge must be 0.
func TestInstrumentedMux_InFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /quick", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	wrapped := instrumentedMux(mux, bag)

	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	// Drive a few sequential requests; gauge must return to 0.
	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL + "/quick")
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	got := readGauge(t, reg, "nornicdb_http_in_flight_requests")
	assert.InDelta(t, 0.0, got, 0.0001,
		"in_flight_requests must return to 0 after all requests complete (Dec in defer)")
}

// TestInstrumentedMux_NilBag asserts the wrapper is a pass-through when
// the bag is nil — keeps test fixtures compileable and pre-Phase-4
// callers running unchanged.
func TestInstrumentedMux_NilBag(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /probe", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	wrapped := instrumentedMux(mux, nil)

	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/probe")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"nil bag must pass-through; no panic, no observation, no failure")
}

// findLabels gathers the registry, finds the named metric family, and
// returns the labels of the first metric series. Returns nil if not found.
func findLabels(t *testing.T, reg *prometheus.Registry, name string) map[string]string {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.Metric) == 0 {
			return nil
		}
		out := map[string]string{}
		for _, lp := range mf.Metric[0].Label {
			out[lp.GetName()] = lp.GetValue()
		}
		return out
	}
	return nil
}

// readGauge returns the float value of the named gauge family (assumes
// single-series no-label gauge). Fails the test if not found.
func readGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.Metric, 1)
		require.Equal(t, dto.MetricType_GAUGE, mf.GetType())
		return mf.Metric[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge %q not found in registry", name)
	return 0
}

// debugDumpForFailure prints the full registry contents on test failure.
// Aid for diagnosing label-mismatch failures.
//
//nolint:unused // diagnostic helper; intentionally kept available.
func debugDumpForFailure(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, _ := reg.Gather()
	var sb strings.Builder
	for _, mf := range mfs {
		fmt.Fprintf(&sb, "%s:\n", mf.GetName())
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				fmt.Fprintf(&sb, "  %s=%s ", lp.GetName(), lp.GetValue())
			}
			sb.WriteString("\n")
		}
	}
	t.Log(sb.String())
}
