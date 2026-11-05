package main

import (
	"context"
	"os"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// inProcessNorthwindSeed is the outcome of seeding the exact Northwind payload
// in-process: the executor positioned on the seeded graph, the seed's wall
// time, and the plan's node/relationship counts.
type inProcessNorthwindSeed struct {
	store   storage.Engine
	exec    *cypher.StorageExecutor
	elapsed time.Duration
	nodes   int
	rels    int
}

// seedNorthwindInProcess replays the exact Northwind seed plan (buildSeedPlan
// with the caller's seedConfig) through the same executor path the server uses
// — StorageExecutor -> NamespacedEngine -> BadgerEngine, on disk, write-behind
// on — and flushes write-behind so the graph is complete before it returns.
//
// The engine's lifetime belongs to tb (fixtureowner), so one call seeds one
// immutable graph that the caller may read from until its test or benchmark
// ends. The seed statements, row payloads, index DDL and batching are the
// runner's own; nothing here rewrites them.
func seedNorthwindInProcess(tb testing.TB, cfg seedConfig) inProcessNorthwindSeed {
	tb.Helper()

	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](tb, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:     tb.TempDir(),
		WriteBehind: true, // auto-sizing interval, like the benchmark harness
	}))
	if engine != nil {
		fixtureowner.Own(tb, engine)
	}
	if err != nil {
		tb.Fatalf("open engine: %v", err)
	}

	store := storage.NewNamespacedEngine(engine, "nornic")
	exec := cypher.NewStorageExecutor(store)
	ctx := context.Background()

	for _, q := range ensureIndexQueries {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			tb.Fatalf("create index %q: %v", q, err)
		}
	}

	plan := buildSeedPlan(cfg)

	start := time.Now()
	for _, phase := range plan.phases {
		for s := 0; s < len(phase.rows); s += cfg.batchSize {
			e := s + cfg.batchSize
			if e > len(phase.rows) {
				e = len(phase.rows)
			}
			if _, err := exec.Execute(ctx, phase.cypher, map[string]any{"rows": phase.rows[s:e]}); err != nil {
				tb.Fatalf("seed %s batch %d..%d: %v", phase.name, s, e, err)
			}
		}
	}
	if err := engine.FlushWriteBehind(context.Background()); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	elapsed := time.Since(start)

	nodes, rels := planSeedNodesAndRelationships(plan)
	return inProcessNorthwindSeed{store: store, exec: exec, elapsed: elapsed, nodes: nodes, rels: rels}
}

// TestSeedProfileInProcess replays the exact Northwind seed plan through the
// same executor path the server uses (StorageExecutor -> NamespacedEngine ->
// BadgerEngine, write-behind on), minus Bolt. It exists so the seed can be CPU
// profiled deterministically:
//
//	NORNICDB_SEED_PROFILE=1 go test -tags noui,nolocalllm \
//	  ./testing/benchmarks/northwind_power -run TestSeedProfileInProcess \
//	  -cpuprofile=/tmp/seed.prof -count=1
//	go tool pprof -top -nodecount=30 /tmp/seed.prof
func TestSeedProfileInProcess(t *testing.T) {
	if os.Getenv("NORNICDB_SEED_PROFILE") != "1" {
		t.Skip("set NORNICDB_SEED_PROFILE=1 to run the seed profile")
	}

	cfg := seedConfig{
		categories:    96,
		suppliers:     144,
		customers:     1200,
		products:      48000,
		orders:        48000,
		orderLinesMin: 1,
		orderLinesMax: 6,
		batchSize:     2000,
		parallel:      1, // in-process profile runs single-threaded
		seed:          42,
	}
	seeded := seedNorthwindInProcess(t, cfg)
	t.Logf("in-process seed of %d nodes / %d rels took %v (parallel=%d)",
		seeded.nodes, seeded.rels, seeded.elapsed, cfg.parallel)
}
