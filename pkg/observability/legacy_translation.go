// Package observability — Phase 5 legacy translation layer.
//
// RenderLegacy walks the unified pkg/observability registry and emits the
// 12 metric families that customer scrapers expect on :7474/metrics. Pure
// function: input *prometheus.Registry + time.Time, output []byte in
// Prometheus exposition format v0.0.4.
//
// Phase 5 / Plan 05-02 fills the legacyMappings table function fields and
// the RenderLegacy body; this file is the Wave-0 skeleton.
package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Public-API contract bytes — frozen in Wave-0. Any change requires ADR
// amendment per CLAUDE.md "Public API contract — Metric names and span
// names are versioned. Deprecations require Sunset header overlap of one
// minor release minimum."
const (
	LegacySunset      = "Fri, 31 Dec 2027 23:59:59 GMT"
	LegacyDeprecation = "true"
	LegacyContentType = "text/plain; version=0.0.4; charset=utf-8"
)

// reduceFn turns the gathered registry view into a single float64 for one
// legacy metric. byName is the gathered family index; mapping carries the
// Sources / ConstLabels the helper needs.
type reduceFn func(byName map[string]*dto.MetricFamily, mapping legacyMapping) float64

// unitFn applies an optional unit conversion (identity for most rows;
// secondsToMs for nornicdb_slow_query_threshold_ms).
type unitFn func(v float64) float64

// legacyMapping is the closed-set table entry. 12 entries total; adding
// a 13th requires explicit review + golden-file regeneration.
type legacyMapping struct {
	LegacyName  string
	LegacyHelp  string
	LegacyType  string
	Sources     []string
	Reduce      reduceFn
	UnitFn      unitFn
	ConstLabels prometheus.Labels
}

// legacyMappings is the static 12-entry table. Wave-0 populates Name +
// Help + Type + Sources only; Plan 05-02 fills Reduce + UnitFn so the
// mapping tests turn GREEN. ConstLabels is set only for nornicdb_info.
var legacyMappings = []legacyMapping{
	{LegacyName: "nornicdb_uptime_seconds", LegacyHelp: "Server uptime in seconds", LegacyType: "gauge", Sources: []string{"nornicdb_process_uptime_seconds"}},
	{LegacyName: "nornicdb_requests_total", LegacyHelp: "Total HTTP requests", LegacyType: "counter", Sources: []string{"nornicdb_http_requests_total"}},
	{LegacyName: "nornicdb_errors_total", LegacyHelp: "Total HTTP 5xx responses", LegacyType: "counter", Sources: []string{"nornicdb_http_requests_total"}},
	{LegacyName: "nornicdb_active_requests", LegacyHelp: "Currently active HTTP requests", LegacyType: "gauge", Sources: []string{"nornicdb_http_in_flight_requests"}},
	{LegacyName: "nornicdb_nodes_total", LegacyHelp: "Total nodes in database", LegacyType: "gauge", Sources: []string{"nornicdb_storage_nodes_total"}},
	{LegacyName: "nornicdb_edges_total", LegacyHelp: "Total edges in database", LegacyType: "gauge", Sources: []string{"nornicdb_storage_edges_total"}},
	{LegacyName: "nornicdb_embeddings_processed", LegacyHelp: "Total embeddings processed successfully", LegacyType: "counter", Sources: []string{"nornicdb_embed_processed_total"}},
	{LegacyName: "nornicdb_embeddings_failed", LegacyHelp: "Total embeddings that failed processing", LegacyType: "counter", Sources: []string{"nornicdb_embed_processed_total"}},
	{LegacyName: "nornicdb_embedding_worker_running", LegacyHelp: "Embedding worker running flag (1=running, 0=stopped)", LegacyType: "gauge", Sources: []string{"nornicdb_embed_worker_running"}},
	{LegacyName: "nornicdb_slow_queries_total", LegacyHelp: "Total slow Cypher queries observed", LegacyType: "counter", Sources: []string{"nornicdb_cypher_slow_queries_total"}},
	{LegacyName: "nornicdb_slow_query_threshold_ms", LegacyHelp: "Slow-query threshold in milliseconds (legacy unit; new metric is *_seconds)", LegacyType: "gauge", Sources: []string{"nornicdb_cypher_slow_query_threshold_seconds"}},
	{LegacyName: "nornicdb_info", LegacyHelp: "Build information (version, backend)", LegacyType: "gauge", Sources: []string{"nornicdb_build_info"}},
}

// identity is the no-op unitFn. Final implementation; not a stub.
func identity(v float64) float64 { return v }

// secondsToMs converts a value from seconds to milliseconds. Final.
func secondsToMs(v float64) float64 { return v * 1000.0 }

// RenderLegacy is the public entry point. Wave-0 stub returns nil; Plan
// 05-02 implements the gather-walk + per-mapping emit.
func RenderLegacy(reg *prometheus.Registry, now time.Time) []byte {
	_ = reg
	_ = now
	return nil
}

// Reduction helpers — Wave-0 stubs. Plan 05-02 implements.
func sumAcrossLabels(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	_ = byName
	_ = m
	return 0
}

func sumByMatchingLabel(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	_ = byName
	_ = m
	return 0
}

func takeLatest(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	_ = byName
	_ = m
	return 0
}

func dropExtraLabels(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	_ = byName
	_ = m
	return 0
}
