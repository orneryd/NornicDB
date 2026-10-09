package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	falkordb "github.com/FalkorDB/falkordb-go/v2"
)

// runFalkorReport drives FalkorDB over its native RESP protocol using the
// official falkordb-go client. FalkorDB v6 (the current engine) no longer
// ships a Bolt listener — BOLT_PORT is accepted as a config value but read by
// nothing — so the sweep uses the production protocol instead of the
// experimental Bolt support of the legacy C engine. The same deterministic
// Northwind seed and query corpus runs here as on every other engine.
func runFalkorReport(ctx context.Context, uri, graphName, username, password string, noAuth bool, cfg seedConfig, label string, iterations, warmup int, skipSeed bool, out string) error {
	if strings.TrimSpace(graphName) == "" {
		graphName = "falkor"
	}
	addr := strings.TrimPrefix(strings.TrimPrefix(uri, "falkor://"), "falkors://")
	if addr == "" {
		addr = "localhost:6379"
	}

	options := &falkordb.ConnectionOption{Addr: addr}
	if !noAuth {
		options.Username = username
		options.Password = password
	}
	// The seed phases are large: each UNWIND batch MATCHes by index twice per
	// row, and at 48k-order scale a single phase runs far longer than the
	// go-redis default 3s read timeout. Set generous deadlines so the client
	// never aborts a legitimate long-running query.
	options.DialTimeout = 30 * time.Second
	options.ReadTimeout = 30 * time.Minute
	options.WriteTimeout = 30 * time.Minute
	db, err := falkordb.FalkorDBNew(options)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Conn.Close()
	if err := db.Conn.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	graph := db.SelectGraph(graphName)
	// Wipe any pre-existing graph so the run starts from a fresh store,
	// like every other engine in the sweep. The error is ignored: deleting
	// a graph that does not exist yet is also reported as an error by the
	// server.
	_ = graph.Delete()

	// Declare the same FK/name indexes the Bolt engines create, rewritten
	// to FalkorDB's syntax (no index name, no IF NOT EXISTS). FalkorDB uses
	// them automatically once a filtered query references the pair.
	for _, q := range ensureIndexQueries {
		rewritten, ok := falkorIndexQuery(q)
		if !ok {
			return fmt.Errorf("index statement does not match the expected Neo4j pattern: %q", q)
		}
		if _, err := graph.Query(rewritten, nil, nil); err != nil {
			return fmt.Errorf("create index %q: %w", q, err)
		}
	}

	report := &Report{
		Label:           label,
		URI:             uri,
		Database:        graphName,
		Iterations:      iterations,
		Warmup:          warmup,
		SeedBatchSize:   cfg.batchSize,
		SeedParallelism: cfg.parallel,
		Categories:      cfg.categories,
		Suppliers:       cfg.suppliers,
		Customers:       cfg.customers,
		Products:        cfg.products,
		Orders:          cfg.orders,
		OrderLinesMin:   cfg.orderLinesMin,
		OrderLinesMax:   cfg.orderLinesMax,
		RandomSeed:      cfg.seed,
		StartedAt:       time.Now(),
	}

	if !skipSeed {
		log("[%s] seeding Northwind (categories=%d suppliers=%d customers=%d products=%d orders=%d batch_size=%d parallel=%d seed=%d)",
			label, cfg.categories, cfg.suppliers, cfg.customers, cfg.products, cfg.orders, cfg.batchSize, cfg.parallel, cfg.seed)

		seedStart := time.Now()
		plan := buildSeedPlan(cfg)
		for _, phase := range plan.phases {
			if err := falkorSeedPhase(graph, phase.name, phase.cypher, phase.rows, cfg.batchSize); err != nil {
				return fmt.Errorf("seed %s: %w", phase.name, err)
			}
		}
		report.SeedDurationMs = float64(time.Since(seedStart).Microseconds()) / 1000.0
		report.SeedNodes, report.SeedRelationships = planSeedNodesAndRelationships(plan)
		report.ApproxSeedBytes = planApproxBytes(plan)
		log("[%s] seeded in %.1fms (%d nodes, %d rels, ~%.1f MiB payload)",
			label, report.SeedDurationMs, report.SeedNodes, report.SeedRelationships,
			float64(report.ApproxSeedBytes)/(1024*1024))

		sc, countErr := countSeedGraphFalkor(graph)
		if countErr != nil {
			return fmt.Errorf("seed verification: %w", countErr)
		}
		report.SeedCounts = sc
		if mismatches := verifySeedCounts(sc, cfg.categories, cfg.suppliers, cfg.customers, cfg.products, cfg.orders); len(mismatches) > 0 {
			for _, m := range mismatches {
				report.CorrectnessErrors = append(report.CorrectnessErrors, "seed: "+m)
				log("[%s] SEED MISMATCH: %s", label, m)
			}
			return fmt.Errorf("seed verification failed — see CorrectnessErrors in %s", out)
		}
		log("[%s] seed verified: categories=%d suppliers=%d customers=%d products=%d orders=%d part_of=%d supplies=%d purchased=%d orders_edges=%d",
			label, sc.Categories, sc.Suppliers, sc.Customers, sc.Products, sc.Orders,
			sc.PartOfEdges, sc.SuppliesEdges, sc.PurchasedEdges, sc.OrdersEdges)
	}

	benchStart := time.Now()
	totalOps := 0
	var allLatencies []float64
	for _, q := range queries {
		stat, err := runQueryFalkor(ctx, graph, q, iterations, warmup)
		if err != nil {
			return fmt.Errorf("query %s: %w", q.name, err)
		}
		report.Queries = append(report.Queries, stat)
		totalOps += stat.Iterations
		allLatencies = append(allLatencies, stat.LatenciesMs...)
		if !stat.CorrectnessOK {
			report.CorrectnessErrors = append(report.CorrectnessErrors,
				fmt.Sprintf("query %q: result set changed between iterations (row_count=%d hash=%s)",
					q.name, stat.RowCount, stat.ResultHash))
		}
		log("[%s] %-34s mean=%7.2fms p95=%7.2fms ops/s=%8.1f rows=%d",
			label, q.name, stat.MeanMs, stat.P95Ms, stat.OpsPerSecond, stat.RowCount)
	}
	if len(report.CorrectnessErrors) > 0 {
		log("[%s] CORRECTNESS: %d issue(s) recorded in report; see correctness_errors.", label, len(report.CorrectnessErrors))
	}
	report.TotalBenchMs = float64(time.Since(benchStart).Microseconds()) / 1000.0
	report.TotalBenchOps = totalOps
	report.FinishedAt = time.Now()
	if len(allLatencies) > 0 {
		report.OverallMeanMs = mean(allLatencies)
		report.QueryLatencyOpsPerSec = throughputOpsPerSecond(len(allLatencies), sumFloat64(allLatencies))
	}
	report.OverallOpsPerSec = throughputOpsPerSecond(totalOps, report.TotalBenchMs)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if out == "" {
		fmt.Println(string(data))
		return nil
	}
	if err := os.WriteFile(out, data, 0o644); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	log("[%s] wrote %s", label, out)
	return nil
}

// falkorSeedPhase writes one seed phase to the graph in UNWIND batches of at
// most batchSize rows, mirroring the Bolt seeder's chunking. A single giant
// GRAPH.QUERY (the order-lines phase is ~170k rows at the default scale) is
// both memory-heavy and slow enough to trip client read timeouts.
func falkorSeedPhase(graph *falkordb.Graph, phaseName, cypher string, rows []map[string]any, batchSize int) error {
	phaseCypher := foldChainedCreates(phaseName, cypher)
	if len(rows) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(rows)
	}
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := make([]any, end-start)
		for i, row := range rows[start:end] {
			batch[i] = row
		}
		if _, err := graph.Query(phaseCypher, map[string]any{"rows": batch}, nil); err != nil {
			return fmt.Errorf("%s batch %d..%d: %w", phaseName, start, end, err)
		}
	}
	return nil
}

// runQueryFalkor mirrors runQuery for the RESP backend: the reference
// fingerprint is captured on the first call, warmups re-verify it, and every
// timed iteration re-fingerprints the result set for intra-run stability.
func runQueryFalkor(ctx context.Context, graph *falkordb.Graph, q benchQuery, iterations, warmup int) (QueryStat, error) {
	execCollect := func() ([]resultRow, []string, error) {
		res, err := graph.Query(q.cypher, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		var rows []resultRow
		var keys []string
		for res.Next() {
			r := res.Record()
			if keys == nil {
				keys = r.Keys()
			}
			values := make(map[string]any, len(keys))
			for _, k := range keys {
				if v, ok := r.Get(k); ok {
					values[k] = v
				}
			}
			rows = append(rows, resultRow{keys: keys, values: values})
		}
		return rows, keys, nil
	}

	refRows, refKeys, err := execCollect()
	if err != nil {
		return QueryStat{}, fmt.Errorf("first call: %w", err)
	}
	refRowCount := len(refRows)
	refHash := fingerprintRows(refKeys, refRows)
	refFirstRows := snapshotRows(refKeys, refRows, MaxFingerprintRows)
	correctnessOK := true

	for i := 1; i < warmup; i++ {
		rows, keys, err := execCollect()
		if err != nil {
			return QueryStat{}, fmt.Errorf("warmup %d: %w", i, err)
		}
		if h := fingerprintRows(keys, rows); h != refHash {
			correctnessOK = false
		}
	}

	latencies := make([]float64, 0, iterations)
	runStart := time.Now()
	for i := 0; i < iterations; i++ {
		start := time.Now()
		rows, keys, err := execCollect()
		elapsed := time.Since(start)
		if err != nil {
			return QueryStat{}, fmt.Errorf("iter %d: %w", i, err)
		}
		latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
		if h := fingerprintRows(keys, rows); h != refHash {
			correctnessOK = false
		}
	}
	elapsed := time.Since(runStart).Seconds()

	stat := QueryStat{
		Name:          q.name,
		Description:   q.description,
		Iterations:    iterations,
		Cypher:        q.cypher,
		LatenciesMs:   latencies,
		MeanMs:        mean(latencies),
		MedianMs:      percentile(latencies, 50),
		P95Ms:         percentile(latencies, 95),
		P99Ms:         percentile(latencies, 99),
		MinMs:         minOf(latencies),
		MaxMs:         maxOf(latencies),
		StdDevMs:      stddev(latencies),
		RowCount:      refRowCount,
		ResultHash:    refHash,
		FirstRows:     refFirstRows,
		CorrectnessOK: correctnessOK,
	}
	if elapsed > 0 {
		stat.OpsPerSecond = float64(iterations) / elapsed
	}
	return stat, nil
}

// countSeedGraphFalkor runs the shared seed-count queries over RESP and
// reports what is actually in the graph.
func countSeedGraphFalkor(graph *falkordb.Graph) (SeedCounts, error) {
	var sc SeedCounts
	for _, pair := range seedCountQueries {
		res, err := graph.Query(pair.query, nil, nil)
		if err != nil {
			return sc, fmt.Errorf("count query %q: %w", pair.query, err)
		}
		var count int64
		if res.Next() {
			if r := res.Record(); r != nil {
				if v, err := r.GetByIndex(0); err == nil {
					count = falkorCountToInt64(v)
				}
			}
		}
		setSeedCount(&sc, pair.field, count)
	}
	return sc, nil
}

func falkorCountToInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}
