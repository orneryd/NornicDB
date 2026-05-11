package main

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerStartup_NoMetricRegistrationPanic verifies Plan 04-01-04: the
// CacheMetrics bag must construct cleanly against a registry that already
// has the Phase 1 Go + Process collectors registered (registry.go:34-35).
// Pitfall 8 says re-registering identical collectors panics — but because
// the cache bag adds disjoint families (nornicdb_cache_*, nornicdb_process_uptime_seconds,
// nornicdb_build_info) it MUST construct without AlreadyRegisteredError.
//
// We do NOT boot the full server (CGO link to llama unavailable in CI tests
// without -tags localllm + libs); we exercise the failure-mode of
// observability.NewCacheMetrics directly against the same TestEnv-shaped
// registry the production startup uses.
func TestServerStartup_NoMetricRegistrationPanic(t *testing.T) {
	te := observability.NewTestEnv(t)

	// First construction — exercises the same call site cmd/nornicdb/main.go
	// uses (`observability.NewCacheMetrics(obs.Registry())`). The TestEnv
	// constructor pre-registers the Go + Process collectors against the same
	// registry; this proves the cache bag and runtime collectors coexist.
	var bag *observability.CacheMetrics
	require.NotPanics(t, func() {
		bag = observability.NewCacheMetrics(te.Registry)
	}, "Pitfall 8: cache bag must not collide with Phase-1 runtime collectors")
	require.NotNil(t, bag)

	// Second construction on the SAME registry MUST panic — proves the
	// registry sees the cache families and would catch a buggy double-init.
	require.Panics(t, func() {
		_ = observability.NewCacheMetrics(te.Registry)
	}, "Re-registering the same families on the same registry must panic (Pitfall 8 invariant)")

	// Materialize one instance per *Vec so Gather() surfaces the family.
	// Prometheus client_golang only emits metric families that have at
	// least one observed series for *Vec types; GaugeFunc surfaces
	// unconditionally (single series, value via callback).
	bag.Hits.WithLabelValues("query_result").Inc()
	bag.Misses.WithLabelValues("query_result").Inc()
	bag.SizeBytes.WithLabelValues("query_result").Set(0)
	bag.Evictions.WithLabelValues("query_result", "lru").Inc()

	// Verify all expected families are present.
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	wantFamilies := []string{
		"nornicdb_cache_hits_total",
		"nornicdb_cache_misses_total",
		"nornicdb_cache_size_bytes",
		"nornicdb_cache_evictions_total",
		"nornicdb_process_uptime_seconds",
		"nornicdb_build_info",
	}
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, want := range wantFamilies {
		assert.True(t, got[want], "expected family %q after NewCacheMetrics", want)
	}

	// Runtime collectors from Phase 1 (and TestEnv) coexist.
	var sawGo, sawProcess bool
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "go_") {
			sawGo = true
		}
		if strings.HasPrefix(mf.GetName(), "process_") {
			sawProcess = true
		}
	}
	assert.True(t, sawGo, "Phase 1 go_* collectors must coexist with cache bag")
	assert.True(t, sawProcess, "Phase 1 process_* collectors must coexist with cache bag")
}
