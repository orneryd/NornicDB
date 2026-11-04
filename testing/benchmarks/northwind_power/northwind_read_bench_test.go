package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// readBenchWarmups mirrors the benchmark script's WARMUP default (5): every
// statement is executed this many times, unrecorded, before it is timed.
const readBenchWarmups = 5

// BenchmarkReadQueriesInProcess times the Northwind runner's complete read
// query suite — the `queries` slice in main.go, used unchanged — against one
// seeded, disk-backed graph.
//
// Scope of the numbers:
//   - The graph is seeded by seedNorthwindInProcess with the script's default
//     payload (96 categories / 144 suppliers / 1200 customers / 48000 products
//     / 48000 orders, 1..6 order lines, batch 2000, seed 42), write-behind on,
//     flushed before any read is timed. The payload comes from the runner's own
//     seedConfig/buildSeedPlan/ensureIndexQueries; nothing is rewritten here.
//   - Only StorageExecutor.Execute is inside the timed loop. Bolt, HTTP,
//     server, auth, session and driver-dispatch cost are all excluded, and no
//     search index is built (equivalent to the script's GRAPH_ONLY=1 envelope,
//     minus its server/transport layer).
//   - One shared graph serves every child benchmark sequentially, so a query
//     starts with whatever cache state the previous one left behind — the same
//     warm, shared-process semantics as the script's in-server iteration loop.
//   - default_cache keeps the product-default result cache (1000 entries,
//     5m TTL), so its timed loop measures cache hits. execution disables only
//     result caching with the existing QueryCachePolicy. Its unchanged query
//     text executes every iteration with normal statement and storage caches.
//     Measured cache hit/miss deltas are logged for both modes.
//   - Results are NornicDB in-process latency/allocation numbers only. They
//     make no Neo4j-equivalence or cross-engine claim.
//
// Query normalization is disabled and the Nornic parser selected for the whole
// benchmark, matching the script's default NORNICDB_CYPHER_QUERY_NORMALIZATION=
// false and default parser mode; both process globals are restored afterwards.
//
// Correctness: each statement's result signature (row count + fingerprintRows
// over the same resultRow the Bolt runner uses) is captured after warmup and
// re-checked once the timed loop ends, outside the timer.
func BenchmarkReadQueriesInProcess(b *testing.B) {
	cfg := seedConfig{
		categories:    96,
		suppliers:     144,
		customers:     1200,
		products:      48000,
		orders:        48000,
		orderLinesMin: 1,
		orderLinesMax: 6,
		batchSize:     2000,
		parallel:      1, // in-process seeding is single-threaded
		seed:          42,
	}

	restoreNormalization := config.WithCypherQueryNormalizationDisabled()
	defer restoreNormalization()
	previousParser := config.GetParserType()
	config.SetParserType(config.ParserTypeNornic)
	defer config.SetParserType(previousParser)

	seeded := seedNorthwindInProcess(b, cfg)
	orderLineEdges := int64(seeded.rels - 2*cfg.products - cfg.orders)
	b.Logf("payload: categories=%d suppliers=%d customers=%d products=%d orders=%d order_lines=%d..%d batch=%d seed=%d parallel=%d write_behind=on",
		cfg.categories, cfg.suppliers, cfg.customers, cfg.products, cfg.orders,
		cfg.orderLinesMin, cfg.orderLinesMax, cfg.batchSize, cfg.seed, cfg.parallel)
	b.Logf("in-process seed of %d nodes / %d rels took %v (write-behind flushed before reads; one shared immutable graph)",
		seeded.nodes, seeded.rels, seeded.elapsed)

	verifySeededCounts(b, seeded.exec, cfg, orderLineEdges)

	b.Logf("read suite: %d statements from the runner's queries slice (unchanged)", len(queries))
	for _, q := range queries {
		b.Logf("  query %s cypher_sha256=%s", q.name, sha256Hex(q.cypher))
	}
	b.Logf("timing scope: StorageExecutor.Execute only; warmups=%d per statement, unrecorded; no Bolt/HTTP/server/search-path cost",
		readBenchWarmups)

	// Both modes use the same immutable graph and unchanged query text. The
	// execution mode disables only result caching through the existing Go
	// policy; compiled statements and storage caches keep their normal lifetime.
	for _, mode := range []struct {
		name   string
		policy []cypher.QueryCachePolicy
	}{
		{name: "default_cache"},
		{name: "execution", policy: []cypher.QueryCachePolicy{{MaxEntries: 0, TTL: 0}}},
	} {
		b.Run(mode.name, func(b *testing.B) {
			exec := cypher.NewStorageExecutor(seeded.store, mode.policy...)
			cacheMetrics := observability.NewCacheMetrics(prometheus.NewRegistry())
			exec.SetCacheMetrics(cacheMetrics)
			maxEntries, cacheTTL := exec.QueryCachePolicy()
			if mode.name == "execution" && maxEntries != 0 {
				b.Fatalf("execution benchmark requires result cache disabled, got %d", maxEntries)
			}
			b.Logf("query-result cache: max_entries=%d ttl=%v", maxEntries, cacheTTL)
			for _, q := range queries {
				b.Run(q.name, func(b *testing.B) {
					benchReadQueryInProcess(b, exec, cacheMetrics, q)
				})
			}
		})
	}
}

// benchReadQueryInProcess warms one statement, times `b.N` executions of it and
// verifies the final result signature against the warmed reference.
func benchReadQueryInProcess(b *testing.B, exec *cypher.StorageExecutor, cacheMetrics *observability.CacheMetrics, q benchQuery) {
	b.Helper()
	ctx := context.Background()

	hitsBefore, missesBefore := queryResultCacheCounts(cacheMetrics)

	var refHash string
	var refRows int
	for i := 0; i < readBenchWarmups; i++ {
		res, err := exec.Execute(ctx, q.cypher, nil)
		if err != nil {
			b.Fatalf("warmup %d: %v", i, err)
		}
		hash := inProcessResultHash(res)
		if i == 0 {
			refHash, refRows = hash, len(res.Rows)
			continue
		}
		if hash != refHash {
			b.Fatalf("warmup %d result signature %s != warmed reference %s", i, hash, refHash)
		}
	}
	warmHits, warmMisses := queryResultCacheCounts(cacheMetrics)

	var last *cypher.ExecuteResult
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, q.cypher, nil)
		if err != nil {
			b.Fatalf("iteration %d: %v", i, err)
		}
		last = res
	}
	b.StopTimer()

	timedHits, timedMisses := queryResultCacheCounts(cacheMetrics)
	if hash := inProcessResultHash(last); hash != refHash {
		b.Fatalf("timed result signature %s != warmed reference %s", hash, refHash)
	}
	b.Logf("rows=%d result_sha256=%s", refRows, refHash)
	b.Logf("query_result cache: warmup hits=%g misses=%g, timed hits=%g misses=%g",
		warmHits-hitsBefore, warmMisses-missesBefore, timedHits-warmHits, timedMisses-warmMisses)
}

// queryResultCacheCounts reads the executor's cache_hits_total and
// cache_misses_total counters for the query-result cache.
func queryResultCacheCounts(metrics *observability.CacheMetrics) (hits, misses float64) {
	hits = testutil.ToFloat64(metrics.Hits.WithLabelValues("query_result"))
	misses = testutil.ToFloat64(metrics.Misses.WithLabelValues("query_result"))
	return hits, misses
}

// inProcessResultRows adapts a cypher.ExecuteResult to the runner's
// backend-neutral resultRow (positional rows keyed by the returned columns) so
// the Bolt path's fingerprintRows applies unchanged.
func inProcessResultRows(res *cypher.ExecuteResult) ([]string, []resultRow) {
	rows := make([]resultRow, len(res.Rows))
	for i, values := range res.Rows {
		row := make(map[string]any, len(res.Columns))
		for j, column := range res.Columns {
			if j < len(values) {
				row[column] = values[j]
			}
		}
		rows[i] = resultRow{keys: res.Columns, values: row}
	}
	return res.Columns, rows
}

// inProcessResultHash fingerprints a result set exactly as the Bolt runner does.
func inProcessResultHash(res *cypher.ExecuteResult) string {
	keys, rows := inProcessResultRows(res)
	return fingerprintRows(keys, rows)
}

// verifySeededCounts confirms the shared graph holds the seeded payload before
// any read is timed, using the runner's own seedCountQueries.
func verifySeededCounts(b *testing.B, exec *cypher.StorageExecutor, cfg seedConfig, orderLineEdges int64) {
	b.Helper()
	ctx := context.Background()

	want := map[string]int64{
		"categories":      int64(cfg.categories),
		"suppliers":       int64(cfg.suppliers),
		"customers":       int64(cfg.customers),
		"products":        int64(cfg.products),
		"orders":          int64(cfg.orders),
		"part_of_edges":   int64(cfg.products),
		"supplies_edges":  int64(cfg.products),
		"purchased_edges": int64(cfg.orders),
		"orders_edges":    orderLineEdges,
	}
	for _, pair := range seedCountQueries {
		res, err := exec.Execute(ctx, pair.query, nil)
		if err != nil {
			b.Fatalf("seed count %s: %v", pair.field, err)
		}
		got := inProcessScalar(b, res, "n")
		if expected := want[pair.field]; got != expected {
			b.Fatalf("seed count %s = %d, want %d", pair.field, got, expected)
		}
		b.Logf("seed count %s = %d", pair.field, got)
	}
}

// inProcessScalar extracts a single integer-valued scalar column from a one-row
// result, the in-process counterpart of countSeedGraph's scalar helper.
func inProcessScalar(b *testing.B, res *cypher.ExecuteResult, key string) int64 {
	b.Helper()
	if res == nil || len(res.Rows) != 1 {
		b.Fatalf("expected exactly one result row for %q, got %d", key, len(res.Rows))
	}
	index := -1
	for j, column := range res.Columns {
		if column == key {
			index = j
			break
		}
	}
	if index < 0 || index >= len(res.Rows[0]) {
		b.Fatalf("column %q not present in result columns %v", key, res.Columns)
	}
	switch v := res.Rows[0][index].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		b.Fatalf("unexpected count type %T for column %q", v, key)
		return 0
	}
}

// sha256Hex returns the digest of the statement text, so the run records
// exactly which runner queries it measured.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
