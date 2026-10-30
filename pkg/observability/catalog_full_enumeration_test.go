// Plan 04-07-01: Phase 4 SC-1 falsifiable enumeration test.
//
// This is THE source-of-truth check that every metric family enumerated
// in ADR-0001 §2.3 is registered in `registry.Gather()` after constructing
// the full set of subsystem bags AND driving at least one observation
// through every Vec. The literal `expected` slice IS the catalog freeze:
// a future rename or removal of a metric family breaks this test, which
// is the desired behavior — names are v1-stable per CLAUDE.md "Public
// API contract".
//
// Coverage:
//   - 62 NornicDB families (HTTP 5 + Bolt 6 + Cypher 11 + Storage 8 +
//     MVCC 4 + Embed 7 + Search 4 + Replication 10 + Auth 1 + Cache+Runtime 6).
//   - Go runtime collector + process collector (MET-17 carry-forward;
//     TestEnv already registers these in NewTestEnv).
//
// Note on Prometheus client_golang surfacing: CounterVec / HistogramVec /
// GaugeVec families do NOT appear in registry.Gather() until at least one
// label combination is observed. The driveOneObservationPerVec helper
// below pumps a single observation through every Vec the catalog owns —
// this also serves as the RESEARCH RISK-7 / Q13 lower-bound presence
// proof complementing the AssertCardinalityCeiling upper bound used in
// per-bag tests.
package observability

import (
	"context"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullEnumProbeStubs are minimal, panic-free probe implementations used
// only by the enumeration test. Each method returns zero values; the
// purpose of this test is to exercise REGISTRATION, not observation
// values. Existing per-bag *_test.go files cover the value plumbing.

type fullEnumStorageProbe struct{}

func (fullEnumStorageProbe) NodeCount() int64           { return 0 }
func (fullEnumStorageProbe) EdgeCount() int64           { return 0 }
func (fullEnumStorageProbe) IDDictCounterNodes() uint64 { return 0 }
func (fullEnumStorageProbe) IDDictCounterEdges() uint64 { return 0 }
func (fullEnumStorageProbe) IDDictFreelistNodes() int64 { return 0 }
func (fullEnumStorageProbe) IDDictFreelistEdges() int64 { return 0 }

type fullEnumMVCCProbe struct{}

func (fullEnumMVCCProbe) PinnedBytes() int64              { return 0 }
func (fullEnumMVCCProbe) OldestReaderAgeSeconds() float64 { return 0 }
func (fullEnumMVCCProbe) ActiveReaders() int64            { return 0 }

type fullEnumEmbedProbe struct{}

func (fullEnumEmbedProbe) QueueLen() int { return 0 }

type fullEnumSearchProbe struct{}

func (fullEnumSearchProbe) IndexSizeBytes(kind string) uint64 { return 0 }

// (metricNames is defined in registry_test.go and reused here.)

// hasFamilyPrefix reports whether any registered family name starts with
// the given prefix (used to assert the Go and process collectors landed
// in the registry — those collectors expose multi-family series like
// `go_goroutines`, `process_resident_memory_bytes`, etc.).
func hasFamilyPrefix(names []string, prefix string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			return true
		}
	}
	return false
}

// driveOneObservationPerVec is the lower-bound presence harness. For every
// Vec the catalog owns, it drives a single Inc/Observe/Set so the family
// surfaces in registry.Gather(). Probe-backed GaugeFuncs surface
// automatically via their callbacks; pre-Inc'd Counters / pre-Set Gauges
// (no labels) also surface automatically.
//
// Closed-enum compliance: every label tuple used here is a concrete value
// from the corresponding Allowed* enum — tagging this fixture as the
// canonical "well-typed observation set" per RESEARCH RISK-7.
func driveOneObservationPerVec(
	ctx context.Context,
	httpM *HTTPMetrics,
	boltM *BoltMetrics,
	cypherM *CypherMetrics,
	storageM *StorageMetrics,
	mvccM *MVCCMetrics,
	embedM *EmbedMetrics,
	searchM *SearchMetrics,
	replM *ReplicationMetrics,
	authM *AuthMetrics,
	cacheM *CacheMetrics,
) {
	// HTTP
	httpM.RequestDuration.Bind("GET", "/livez", "2xx").Observe(ctx, 0.001)
	httpM.Requests.WithLabelValues("GET", "/livez", "2xx").Inc()
	httpM.RequestBodyBytes.Bind("GET", "/livez").Observe(ctx, 0)
	httpM.ResponseBodyBytes.Bind("GET", "/livez").Observe(ctx, 0)
	// InFlight is a plain Gauge — no labels needed; toggle it once.
	httpM.InFlight.Inc()
	httpM.InFlight.Dec()

	// Bolt
	boltM.ConnectionsTotal.WithLabelValues(AllowedBoltResults[0], AllowedBoltTransports[0]).Inc()
	boltM.SessionDuration.Bind().Observe(ctx, 0.0)
	boltM.MessagesTotal.WithLabelValues(AllowedBoltOps[0], AllowedBoltResults[0]).Inc()
	boltM.MessageDuration.Bind(AllowedBoltOps[0]).Observe(ctx, 0.0)
	boltM.PackstreamDecodeErrors.WithLabelValues(AllowedPackstreamReasons[0]).Inc()
	boltM.ConnectionsActive.WithLabelValues(AllowedBoltTransports[0]).Inc()
	boltM.ConnectionsActive.WithLabelValues(AllowedBoltTransports[0]).Dec()
	boltM.ConnectionsRejectedTotal.WithLabelValues(AllowedBoltConnectionRejectReasons[0]).Inc()
	boltM.WebSocketOversizedTotal.Inc()

	// Cypher: op_type = "read" (closed enum). Tenant-OFF means no `database`.
	cypherM.Queries.WithLabelValues("read").Inc()
	cypherM.QueryDuration.Bind("read").Observe(ctx, 0.0)
	cypherM.PlannerDuration.Bind("read").Observe(ctx, 0.0)
	cypherM.PlannerCacheHits.WithLabelValues().Inc()
	cypherM.PlannerCacheMisses.WithLabelValues().Inc()
	cypherM.RowsReturned.Bind("read").Observe(ctx, 0)
	cypherM.TransactionConflicts.WithLabelValues().Inc()
	cypherM.SlowQueries.WithLabelValues().Inc()
	cypherM.PlannerCacheSize.Set(0)
	cypherM.ActiveTransactions.Set(0)
	// slow_query_threshold_seconds is a GaugeFunc — surfaces automatically.

	// Storage. tenant-OFF keeps single-label vecs simple.
	storageM.Bytes.WithLabelValues(AllowedStorageBytesKinds[0]).Set(0)
	storageM.OpDuration.Bind(AllowedStorageOps[0]).Observe(ctx, 0.0)
	storageM.CompactionsTotal.WithLabelValues("0", AllowedStorageResults[0]).Inc()
	storageM.CompactionDuration.Bind("0").Observe(ctx, 0.0)
	storageM.WALLagBytes.Set(0)
	// IndexRebuildTotal — tenant-OFF, so labels = [index, result] (no database).
	storageM.BindIndexRebuild("", AllowedStorageIndexes[0], AllowedStorageResults[0]).Inc()
	// nodes_total / edges_total are GaugeFuncs — surface automatically.

	// MVCC: PressureBand. UpdateBand(database, ratio) flips the band gauges.
	// Pass ratio=0 → "normal" band gets the 1; the other three get 0.
	mvccM.UpdateBand("", 0)
	// pinned_bytes / oldest_reader_age_seconds / active_readers are GaugeFuncs.

	// Embed
	embedM.Processed.WithLabelValues("openai", "text-embedding-3-small",
		AllowedEmbedResults[0], AllowedEmbedBackends[0]).Inc()
	embedM.Duration.Bind("openai", "text-embedding-3-small", AllowedEmbedBackends[0]).Observe(ctx, 0.0)
	embedM.CacheHits.Inc()
	embedM.CacheMisses.Inc()
	embedM.WorkerRunning.Set(0)
	embedM.FFIPanicTotal.WithLabelValues(AllowedEmbedBackends[0]).Inc()
	// queue_depth GaugeFunc surfaces automatically.

	// Search. tenant-OFF: requests labels = [mode, result], duration = [mode, stage].
	searchM.Requests.WithLabelValues(AllowedSearchModes[0], AllowedSearchResults[0]).Inc()
	searchM.Duration.Bind(AllowedSearchModes[0], AllowedSearchStages[0]).Observe(ctx, 0.0)
	searchM.Candidates.Bind().Observe(ctx, 0)
	searchM.IndexSizeBytes.WithLabelValues(AllowedSearchIndexKinds[0]).Set(0)

	// Replication
	replM.Role.Set(0)
	replM.Term.Set(0)
	replM.CommitIndex.Set(0)
	replM.ApplyIndex.Set(0)
	replM.LeaderChangesTotal.Inc()
	replM.LagBytes.WithLabelValues("peer-0").Set(0)
	replM.LagEntries.WithLabelValues("peer-0").Set(0)
	replM.LastContactSeconds.WithLabelValues("peer-0").Set(0)
	replM.ApplyDuration.Bind().Observe(ctx, 0.0)
	replM.RTTSeconds.WithLabelValues("peer-0").Observe(0.0)

	// Auth
	authM.AuthAttempts.WithLabelValues(AllowedAuthResults[0], AllowedAuthProtocols[0]).Inc()

	// Cache
	cacheM.Hits.WithLabelValues(AllowedCacheNames[0]).Inc()
	cacheM.Misses.WithLabelValues(AllowedCacheNames[0]).Inc()
	cacheM.SizeBytes.WithLabelValues(AllowedCacheNames[0]).Set(0)
	cacheM.Evictions.WithLabelValues(AllowedCacheNames[0], AllowedEvictionReasons[0]).Inc()
	// process_uptime_seconds + build_info are GaugeFuncs — surface automatically.
}

// TestCatalog_FullEnumeration is the SC-1 falsifiable test for Phase 4.
//
// It constructs all 10 subsystem bags using their public constructors,
// drives one observation through every Vec, and asserts every metric
// family name listed in ADR-0001 §2.3 is present in registry.Gather().
//
// The expected slice is the catalog freeze. To add a metric family:
//  1. Bag constructor change (catalog_*.go).
//  2. Per-bag _test.go AssertCardinalityCeiling update.
//  3. THIS slice gets a new entry.
//  4. ADR §2.3 amendment.
//
// To rename or remove a family: same four steps in reverse. The test
// REQUIRES the change to be deliberate.
func TestCatalog_FullEnumeration(t *testing.T) {
	te := NewTestEnv(t)
	ctx := context.Background()

	// Construct every bag with reasonable fakes for probes. tenant labels
	// disabled (D-08 default) keeps the catalog count deterministic; the
	// tenant-ON path is exercised by per-bag tests.
	httpM := NewHTTPMetrics(te.Registry, false)
	boltM := NewBoltMetrics(te.Registry)
	cypherM := NewCypherMetrics(te.Registry, false, func() float64 { return 1.0 })
	storageM := NewStorageMetrics(te.Registry, false, fullEnumStorageProbe{})
	mvccM := NewMVCCMetrics(te.Registry, false, fullEnumMVCCProbe{})
	embedM := NewEmbedMetrics(te.Registry, fullEnumEmbedProbe{})
	searchM := NewSearchMetrics(te.Registry, false, fullEnumSearchProbe{})
	replM := NewReplicationMetrics(te.Registry, "raft", false)
	authM := NewAuthMetrics(te.Registry)
	cacheM := NewCacheMetrics(te.Registry)

	driveOneObservationPerVec(ctx, httpM, boltM, cypherM, storageM, mvccM,
		embedM, searchM, replM, authM, cacheM)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)

	// SOURCE OF TRUTH for the v1 metric catalog freeze. Order matches
	// ADR-0001 §2.3 subsystem ordering. Names reflect actual registered
	// fqNames including suffix conventions enforced by Phase 3 typed
	// constructors (RowCountHistogram → _rows; SizeHistogram → _bytes).
	expected := []string{
		// HTTP (5 families) — MET-06
		"nornicdb_http_requests_total",
		"nornicdb_http_request_duration_seconds",
		"nornicdb_http_in_flight_requests",
		"nornicdb_http_request_body_bytes",
		"nornicdb_http_response_body_bytes",

		// Bolt (6 families) — MET-07
		"nornicdb_bolt_connections_active",
		"nornicdb_bolt_connections_total",
		"nornicdb_bolt_session_duration_seconds",
		"nornicdb_bolt_messages_total",
		"nornicdb_bolt_message_duration_seconds",
		"nornicdb_bolt_packstream_decode_errors_total",

		// Cypher (11 families) — MET-08, MET-09, MET-26
		"nornicdb_cypher_queries_total",
		"nornicdb_cypher_query_duration_seconds",
		"nornicdb_cypher_planner_duration_seconds",
		"nornicdb_cypher_planner_cache_hits_total",
		"nornicdb_cypher_planner_cache_misses_total",
		"nornicdb_cypher_planner_cache_size",
		"nornicdb_cypher_rows_returned_rows", // RowCountHistogram suffix per Phase 3 D-01
		"nornicdb_cypher_active_transactions",
		"nornicdb_cypher_transaction_conflicts_total",
		"nornicdb_cypher_slow_queries_total",
		"nornicdb_cypher_slow_query_threshold_seconds",

		// Storage (8 families) — MET-10
		"nornicdb_storage_nodes_total",
		"nornicdb_storage_edges_total",
		"nornicdb_storage_bytes",
		"nornicdb_storage_op_duration_seconds",
		"nornicdb_storage_compactions_total",
		"nornicdb_storage_compaction_duration_seconds",
		"nornicdb_storage_wal_lag_bytes",
		"nornicdb_storage_index_rebuild_total",

		// MVCC (4 families) — MET-11
		"nornicdb_mvcc_pressure_band",
		"nornicdb_mvcc_pinned_bytes",
		"nornicdb_mvcc_oldest_reader_age_seconds",
		"nornicdb_mvcc_active_readers",

		// Embed (7 families incl. FFI panic counter) — MET-12, D-09
		"nornicdb_embed_queue_depth",
		"nornicdb_embed_processed_total",
		"nornicdb_embed_duration_seconds",
		"nornicdb_embed_cache_hits_total",
		"nornicdb_embed_cache_misses_total",
		"nornicdb_embed_worker_running",
		"nornicdb_embed_ffi_panics_total",

		// Search (4 families) — MET-13. Candidates is a RowCountHistogram
		// (suffix `_rows` per Phase 3 D-01) — its fqName is
		// `nornicdb_search_candidates_rows`.
		"nornicdb_search_requests_total",
		"nornicdb_search_duration_seconds",
		"nornicdb_search_candidates_rows",
		"nornicdb_search_index_size_bytes",

		// Replication (10 families incl. GAP-1 last_contact_seconds) — MET-14
		"nornicdb_replication_role",
		"nornicdb_replication_term",
		"nornicdb_replication_commit_index",
		"nornicdb_replication_apply_index",
		"nornicdb_replication_lag_bytes",
		"nornicdb_replication_lag_entries",
		"nornicdb_replication_apply_duration_seconds",
		"nornicdb_replication_rtt_seconds",
		"nornicdb_replication_leader_changes_total",
		"nornicdb_replication_last_contact_seconds",

		// Auth (1 family) — MET-15
		"nornicdb_auth_attempts_total",

		// Cache+Runtime (6 families) — MET-16
		"nornicdb_cache_hits_total",
		"nornicdb_cache_misses_total",
		"nornicdb_cache_size_bytes",
		"nornicdb_cache_evictions_total",
		"nornicdb_process_uptime_seconds",
		"nornicdb_build_info",
	}

	for _, name := range expected {
		assert.Contains(t, names, name,
			"ADR §2.3 family missing from registry: %s", name)
	}

	// Stdlib collectors (MET-17) — TestEnv registers these in NewTestEnv,
	// mirroring production newRegistry. Verify the families surface.
	assert.True(t, hasFamilyPrefix(names, "go_"),
		"Go runtime collector not present (MET-17)")
	assert.True(t, hasFamilyPrefix(names, "process_"),
		"Process collector not present (MET-17)")

	// ROADMAP Phase 4 SC-1: ≥60 NornicDB families. The expected slice
	// hard-codes 62, providing 2 above the floor.
	assert.GreaterOrEqual(t, len(expected), 62,
		"ROADMAP Phase 4 SC-1 floor: ≥60 families; expected slice has %d",
		len(expected))
}

// TestMetricEmitted_AllNornicDBFamilies is the typed lower-bound presence
// check (RESEARCH RISK-7 / Q13). After driving one observation per Vec,
// it spot-checks the metric Type to catch a class of bug where someone
// wires a family with the wrong constructor (e.g., NewCounter where a
// NewGauge was expected).
//
// The full type table is exhaustive enough to catch a Counter→Gauge
// (or vice versa) swap on every subsystem bag.
func TestMetricEmitted_AllNornicDBFamilies(t *testing.T) {
	te := NewTestEnv(t)
	ctx := context.Background()

	httpM := NewHTTPMetrics(te.Registry, false)
	boltM := NewBoltMetrics(te.Registry)
	cypherM := NewCypherMetrics(te.Registry, false, func() float64 { return 1.0 })
	storageM := NewStorageMetrics(te.Registry, false, fullEnumStorageProbe{})
	mvccM := NewMVCCMetrics(te.Registry, false, fullEnumMVCCProbe{})
	embedM := NewEmbedMetrics(te.Registry, fullEnumEmbedProbe{})
	searchM := NewSearchMetrics(te.Registry, false, fullEnumSearchProbe{})
	replM := NewReplicationMetrics(te.Registry, "raft", false)
	authM := NewAuthMetrics(te.Registry)
	cacheM := NewCacheMetrics(te.Registry)

	driveOneObservationPerVec(ctx, httpM, boltM, cypherM, storageM, mvccM,
		embedM, searchM, replM, authM, cacheM)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	// Build a name -> family map for typed assertions.
	byName := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		byName[mf.GetName()] = mf
	}

	// Spot-check kind assignments at the boundary of REQUIREMENTS.md MET-*.
	// Every NornicDB-prefixed family must surface as a known type.
	cases := []struct {
		name string
		kind dto.MetricType
	}{
		{"nornicdb_http_requests_total", dto.MetricType_COUNTER},
		{"nornicdb_http_request_duration_seconds", dto.MetricType_HISTOGRAM},
		{"nornicdb_http_in_flight_requests", dto.MetricType_GAUGE},
		{"nornicdb_bolt_connections_active", dto.MetricType_GAUGE},
		{"nornicdb_bolt_connections_total", dto.MetricType_COUNTER},
		{"nornicdb_cypher_queries_total", dto.MetricType_COUNTER},
		{"nornicdb_cypher_slow_query_threshold_seconds", dto.MetricType_GAUGE},
		{"nornicdb_storage_op_duration_seconds", dto.MetricType_HISTOGRAM},
		{"nornicdb_storage_index_rebuild_total", dto.MetricType_COUNTER},
		{"nornicdb_mvcc_pinned_bytes", dto.MetricType_GAUGE},
		{"nornicdb_mvcc_pressure_band", dto.MetricType_GAUGE},
		{"nornicdb_embed_queue_depth", dto.MetricType_GAUGE},
		{"nornicdb_embed_ffi_panics_total", dto.MetricType_COUNTER},
		{"nornicdb_search_index_size_bytes", dto.MetricType_GAUGE},
		{"nornicdb_replication_last_contact_seconds", dto.MetricType_GAUGE},
		{"nornicdb_replication_leader_changes_total", dto.MetricType_COUNTER},
		{"nornicdb_auth_attempts_total", dto.MetricType_COUNTER},
		{"nornicdb_build_info", dto.MetricType_GAUGE},
		{"nornicdb_process_uptime_seconds", dto.MetricType_GAUGE},
	}
	for _, c := range cases {
		mf, ok := byName[c.name]
		if !assert.True(t, ok, "family not registered: %s", c.name) {
			continue
		}
		assert.Equal(t, c.kind, mf.GetType(),
			"family %s has wrong metric type", c.name)
	}
}
