// Package observability — Phase 5 legacy translation layer.
//
// RenderLegacy walks the unified pkg/observability registry and emits the
// 12 metric families that customer scrapers expect on :7474/metrics. Pure
// function: input *prometheus.Registry + time.Time, output []byte in
// Prometheus exposition format v0.0.4.
//
// Phase 5 / Plan 05-02 fills the legacyMappings table function fields and
// the RenderLegacy body; Wave-0 (05-01) declared the public-API surface.
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
//
// MatchLabel + MatchValue carry sumByMatchingLabel filters (e.g. row 3
// errors_total filters status_class==5xx; rows 7/8 filter result==success
// or result==failure).
//
// KeepLabels is consulted by dropExtraLabels rows (only nornicdb_info in
// the Phase 5 catalog) to decide which ConstLabels propagate to the
// emitted sample line.
type legacyMapping struct {
	LegacyName  string
	LegacyHelp  string
	LegacyType  string
	Sources     []string
	Reduce      reduceFn
	UnitFn      unitFn
	ConstLabels prometheus.Labels
	MatchLabel  string
	MatchValue  string
	KeepLabels  []string
}

// legacyMappings is the static 12-entry table. Plan 05-02 wires Reduce +
// UnitFn function fields and the MatchLabel/MatchValue/KeepLabels rows
// that the corrected reduction policy (RESEARCH §Mapping Verification)
// requires. The order here is the source-of-truth; RenderLegacy emits
// in lexicographic LegacyName order per D-01d, decoupling source order
// from emission order.
var legacyMappings = []legacyMapping{
	{LegacyName: "nornicdb_uptime_seconds", LegacyHelp: "Server uptime in seconds", LegacyType: "gauge", Sources: []string{"nornicdb_process_uptime_seconds"}, Reduce: takeLatest, UnitFn: identity},
	{LegacyName: "nornicdb_requests_total", LegacyHelp: "Total HTTP requests", LegacyType: "counter", Sources: []string{"nornicdb_http_requests_total"}, Reduce: sumAcrossLabels, UnitFn: identity},
	{LegacyName: "nornicdb_errors_total", LegacyHelp: "Total HTTP 5xx responses", LegacyType: "counter", Sources: []string{"nornicdb_http_requests_total"}, Reduce: sumByMatchingLabel, UnitFn: identity, MatchLabel: "status_class", MatchValue: "5xx"},
	{LegacyName: "nornicdb_active_requests", LegacyHelp: "Currently active HTTP requests", LegacyType: "gauge", Sources: []string{"nornicdb_http_in_flight_requests"}, Reduce: takeLatest, UnitFn: identity},
	{LegacyName: "nornicdb_nodes_total", LegacyHelp: "Total nodes in database", LegacyType: "gauge", Sources: []string{"nornicdb_storage_nodes_total"}, Reduce: takeLatest, UnitFn: identity},
	{LegacyName: "nornicdb_edges_total", LegacyHelp: "Total edges in database", LegacyType: "gauge", Sources: []string{"nornicdb_storage_edges_total"}, Reduce: takeLatest, UnitFn: identity},
	{LegacyName: "nornicdb_embeddings_processed", LegacyHelp: "Total embeddings processed successfully", LegacyType: "counter", Sources: []string{"nornicdb_embed_processed_total"}, Reduce: sumByMatchingLabel, UnitFn: identity, MatchLabel: "result", MatchValue: "success"},
	{LegacyName: "nornicdb_embeddings_failed", LegacyHelp: "Total embeddings that failed processing", LegacyType: "counter", Sources: []string{"nornicdb_embed_processed_total"}, Reduce: sumByMatchingLabel, UnitFn: identity, MatchLabel: "result", MatchValue: "failure"},
	{LegacyName: "nornicdb_embedding_worker_running", LegacyHelp: "Embedding worker running flag (1=running, 0=stopped)", LegacyType: "gauge", Sources: []string{"nornicdb_embed_worker_running"}, Reduce: takeLatest, UnitFn: identity},
	{LegacyName: "nornicdb_slow_queries_total", LegacyHelp: "Total slow Cypher queries observed", LegacyType: "counter", Sources: []string{"nornicdb_cypher_slow_queries_total"}, Reduce: sumAcrossLabels, UnitFn: identity},
	{LegacyName: "nornicdb_slow_query_threshold_ms", LegacyHelp: "Slow-query threshold in milliseconds (legacy unit; new metric is *_seconds)", LegacyType: "gauge", Sources: []string{"nornicdb_cypher_slow_query_threshold_seconds"}, Reduce: takeLatest, UnitFn: secondsToMs},
	{LegacyName: "nornicdb_info", LegacyHelp: "Build information (version, backend)", LegacyType: "gauge", Sources: []string{"nornicdb_build_info"}, Reduce: dropExtraLabels, UnitFn: identity, KeepLabels: []string{"version", "backend"}},
}

// identity is the no-op unitFn. Final implementation; not a stub.
func identity(v float64) float64 { return v }

// secondsToMs converts a value from seconds to milliseconds. Final.
func secondsToMs(v float64) float64 { return v * 1000.0 }

// RenderLegacy is the public entry point. Plan 05-02 (Task 05-02-02)
// implements the gather-walk + per-mapping emit body.
func RenderLegacy(reg *prometheus.Registry, now time.Time) []byte {
	_ = reg
	_ = now
	return nil
}

// sumAcrossLabels returns the total numeric value of every Metric in every
// MetricFamily named in mapping.Sources. Tolerates nil families and empty
// metric slices (returns 0 contribution).
func sumAcrossLabels(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	var total float64
	for _, src := range m.Sources {
		mf := byName[src]
		if mf == nil {
			continue
		}
		mt := mf.GetType()
		for _, metric := range mf.GetMetric() {
			total += metricValue(mt, metric)
		}
	}
	return total
}

// sumByMatchingLabel sums only Metric samples whose LabelPair set contains
// (m.MatchLabel == m.MatchValue). Used by errors_total (status_class=5xx)
// and the embedding success/failure split.
func sumByMatchingLabel(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	var total float64
	for _, src := range m.Sources {
		mf := byName[src]
		if mf == nil {
			continue
		}
		mt := mf.GetType()
		for _, metric := range mf.GetMetric() {
			if !labelMatches(metric.GetLabel(), m.MatchLabel, m.MatchValue) {
				continue
			}
			total += metricValue(mt, metric)
		}
	}
	return total
}

// takeLatest returns the value of the first metric in the first non-nil
// source family. Phase 4's GaugeFuncs always emit a single sample per
// family, so "first sample" is "the" sample.
func takeLatest(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	for _, src := range m.Sources {
		mf := byName[src]
		if mf == nil {
			continue
		}
		metrics := mf.GetMetric()
		if len(metrics) == 0 {
			continue
		}
		return metricValue(mf.GetType(), metrics[0])
	}
	return 0
}

// dropExtraLabels returns the value of the first sample in the first
// non-nil source family. The "drop" semantics belong to the formatter
// (extractKeptConstLabels + emitSample): the helper just produces the
// gauge value, which is always 1.0 for nornicdb_build_info in Phase 4.
func dropExtraLabels(byName map[string]*dto.MetricFamily, m legacyMapping) float64 {
	return takeLatest(byName, m)
}

// metricValue returns the numeric value of a sample regardless of MF type.
// Returns 0 for unknown / unsupported MetricFamily types (histogram,
// summary) — those are not used by the legacy 12.
func metricValue(mt dto.MetricType, metric *dto.Metric) float64 {
	if metric == nil {
		return 0
	}
	switch mt {
	case dto.MetricType_COUNTER:
		if c := metric.GetCounter(); c != nil {
			return c.GetValue()
		}
	case dto.MetricType_GAUGE:
		if g := metric.GetGauge(); g != nil {
			return g.GetValue()
		}
	case dto.MetricType_UNTYPED:
		if u := metric.GetUntyped(); u != nil {
			return u.GetValue()
		}
	}
	return 0
}

// labelMatches reports whether labels contains a pair (name, value).
// Returns false on empty name (zero-value MatchLabel — defensive).
func labelMatches(labels []*dto.LabelPair, name, value string) bool {
	if name == "" {
		return false
	}
	for _, lp := range labels {
		if lp.GetName() == name && lp.GetValue() == value {
			return true
		}
	}
	return false
}
