package main

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

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

	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:     t.TempDir(),
		WriteBehind: true, // auto-sizing interval, like the benchmark harness
	}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()

	for _, q := range ensureIndexQueries {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatalf("create index %q: %v", q, err)
		}
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
	plan := buildSeedPlan(cfg)

	start := time.Now()
	for _, phase := range plan.phases {
		for s := 0; s < len(phase.rows); s += cfg.batchSize {
			e := s + cfg.batchSize
			if e > len(phase.rows) {
				e = len(phase.rows)
			}
			if _, err := exec.Execute(ctx, phase.cypher, map[string]any{"rows": phase.rows[s:e]}); err != nil {
				t.Fatalf("seed %s batch %d..%d: %v", phase.name, s, e, err)
			}
		}
	}
	if err := engine.FlushWriteBehind(nil); err != nil {
		t.Fatalf("flush: %v", err)
	}
	nodes, rels := planSeedNodesAndRelationships(plan)
	t.Logf("in-process seed of %d nodes / %d rels took %v (parallel=%d)",
		nodes, rels, time.Since(start), cfg.parallel)
}
