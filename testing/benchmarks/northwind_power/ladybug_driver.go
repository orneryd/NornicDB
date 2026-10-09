//go:build ladybug

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	lbug "github.com/LadybugDB/go-ladybug"
)

// runLadybugReport drives the embedded LadybugDB (Kuzu fork) backend with the
// same deterministic Northwind seed and query corpus the Bolt backends run.
// The data directory is wiped before opening so each run starts from a fresh
// store, exactly like the other engines in the sweep.
func runLadybugReport(ctx context.Context, dataDir string, cfg seedConfig, label string, iterations, warmup int, skipSeed bool, out string) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("-ladybug-dir is required with -driver ladybug")
	}
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("wipe data dir: %w", err)
	}

	db, err := lbug.OpenDatabase(dataDir, lbug.DefaultSystemConfig())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	conn, err := lbug.OpenConnection(db)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer conn.Close()

	report := &Report{
		Label:           label,
		URI:             "ladybug://embedded/" + dataDir,
		Database:        dataDir,
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
		log("[%s] note: LadybugDB (Kuzu) has no CREATE INDEX ... FOR syntax; index setup is skipped", label)

		seedStart := time.Now()
		plan := buildSeedPlan(cfg)
		for _, phase := range plan.phases {
			phaseCypher := ladybugCypherForPhase(phase.name, phase.cypher)
			stmt, err := conn.Prepare(phaseCypher)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", phase.name, err)
			}
			rowsAny := make([]any, len(phase.rows))
			for i := range phase.rows {
				rowsAny[i] = phase.rows[i]
			}
			result, execErr := conn.Execute(stmt, map[string]any{"rows": rowsAny})
			stmt.Close()
			if execErr != nil {
				return fmt.Errorf("seed %s: %w", phase.name, execErr)
			}
			result.Close()
		}
		report.SeedDurationMs = float64(time.Since(seedStart).Microseconds()) / 1000.0
		report.SeedNodes, report.SeedRelationships = planSeedNodesAndRelationships(plan)
		report.ApproxSeedBytes = planApproxBytes(plan)
		log("[%s] seeded in %.1fms (%d nodes, %d rels, ~%.1f MiB payload)",
			label, report.SeedDurationMs, report.SeedNodes, report.SeedRelationships,
			float64(report.ApproxSeedBytes)/(1024*1024))

		sc, countErr := countSeedGraphLadybug(ctx, conn)
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
		stat, err := runQueryLadybug(ctx, conn, q, iterations, warmup)
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

// runQueryLadybug mirrors runQuery for the embedded backend: the reference
// fingerprint is captured on the first call, warmups re-verify it, and every
// timed iteration re-fingerprints the result set for intra-run stability.
func runQueryLadybug(ctx context.Context, conn *lbug.Connection, q benchQuery, iterations, warmup int) (QueryStat, error) {
	execCollect := func() ([]resultRow, []string, error) {
		res, err := conn.Query(q.cypher)
		if err != nil {
			return nil, nil, err
		}
		defer res.Close()
		keys := res.GetColumnNames()
		var rows []resultRow
		for res.HasNext() {
			tup, err := res.Next()
			if err != nil {
				return nil, nil, err
			}
			values, err := tup.GetAsMap()
			tup.Close()
			if err != nil {
				return nil, nil, err
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

// countSeedGraphLadybug runs the shared seed-count queries against the
// embedded database and reports what is actually on disk.
func countSeedGraphLadybug(ctx context.Context, conn *lbug.Connection) (SeedCounts, error) {
	var sc SeedCounts
	for _, pair := range seedCountQueries {
		res, err := conn.Query(pair.query)
		if err != nil {
			return sc, fmt.Errorf("count query %q: %w", pair.query, err)
		}
		var count int64
		if res.HasNext() {
			tup, err := res.Next()
			if err != nil {
				res.Close()
				return sc, fmt.Errorf("count query %q: %w", pair.query, err)
			}
			values, err := tup.GetAsMap()
			tup.Close()
			if err != nil {
				res.Close()
				return sc, fmt.Errorf("count query %q: %w", pair.query, err)
			}
			// Every seedCountQueries entry aliases its count as `n`.
			if v, ok := values["n"]; ok {
				count = ladybugCountToInt64(v)
			}
		}
		res.Close()
		setSeedCount(&sc, pair.field, count)
	}
	return sc, nil
}

func ladybugCountToInt64(v any) int64 {
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
