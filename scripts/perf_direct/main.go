package main

import (
	"flag"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"log"
	"os"
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type stats struct {
	count      int
	min        time.Duration
	max        time.Duration
	avg        time.Duration
	median     time.Duration
	p95        time.Duration
	p99        time.Duration
	throughput float64
}

func main() {
	var (
		dataDir       = flag.String("data-dir", "./data/test-direct", "data directory for Badger")
		iterations    = flag.Int("iterations", 100, "number of operations per test")
		batchSize     = flag.Int("batch-size", 10, "batch size for BulkCreateNodes test")
		mode          = flag.String("mode", "badger", "engine mode: badger or async")
		namespace     = flag.String("namespace", "nornic", "database namespace for namespaced engine")
		clean         = flag.Bool("clean", false, "remove data directory before running")
		syncWrites    = flag.Bool("sync-writes", false, "enable badger sync writes (fsync on each write)")
		asyncInterval = flag.Duration("async-flush-interval", 50*time.Millisecond, "async flush interval")
		asyncTarget   = flag.Int("async-target-flush-size", 1000, "async target flush size")
		asyncMin      = flag.Duration("async-min-flush-interval", 10*time.Millisecond, "async min flush interval")
		asyncMax      = flag.Duration("async-max-flush-interval", 200*time.Millisecond, "async max flush interval")
		asyncAdaptive = flag.Bool("async-adaptive", true, "enable adaptive async flush timing")
	)
	flag.Parse()

	if *clean {
		if err := os.RemoveAll(*dataDir); err != nil {
			log.Fatalf("failed to clean data dir: %v", err)
		}
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:    *dataDir,
		SyncWrites: *syncWrites,
	})
	if err != nil {
		log.Fatalf("failed to create badger engine: %v", err)
	}

	var asyncEngine *storage.AsyncEngine
	var engine storage.Engine
	if *mode == "async" {
		asyncEngine = storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
			FlushInterval:    *asyncInterval,
			AdaptiveFlush:    *asyncAdaptive,
			MinFlushInterval: *asyncMin,
			MaxFlushInterval: *asyncMax,
			TargetFlushSize:  *asyncTarget,
		})
		engine = storage.NewNamespacedEngine(asyncEngine, *namespace)
	} else {
		engine = storage.NewNamespacedEngine(base, *namespace)
	}

	fmt.Println("NornicDB Direct Engine Write Performance")
	fmt.Printf("Engine: %s\n", *mode)
	fmt.Printf("Data dir: %s\n", *dataDir)
	fmt.Printf("Iterations: %d\n", *iterations)
	fmt.Printf("Batch size: %d\n", *batchSize)
	fmt.Println()

	runTest("Single node create", func() []time.Duration {
		return testSingleCreate(engine, *iterations)
	})

	runTest("Batch node create (BulkCreateNodes)", func() []time.Duration {
		return testBatchCreate(engine, *iterations, *batchSize)
	})

	runTest("Node update", func() []time.Duration {
		return testNodeUpdate(engine, *iterations)
	})

	runTest("Relationship create", func() []time.Duration {
		return testRelationshipCreate(engine, *iterations)
	})

	if asyncEngine != nil {
		start := time.Now()
		if err := asyncEngine.Flush(); err != nil {
			fmt.Printf("Async flush error: %v\n", err)
		}
		fmt.Printf("Async flush duration: %s\n", time.Since(start))
	}

	if asyncEngine != nil {
		if err := asyncEngine.Close(); err != nil {
			log.Printf("async close error: %v", err)
		}
	} else {
		if err := base.Close(); err != nil {
			log.Printf("badger close error: %v", err)
		}
	}
}

func runTest(label string, fn func() []time.Duration) {
	durations := fn()
	fmt.Printf("=== %s ===\n", label)
	if len(durations) == 0 {
		fmt.Println("No successful operations")
		fmt.Println()
		return
	}
	s := computeStats(durations)
	fmt.Printf("Count: %d\n", s.count)
	fmt.Printf("Min: %s\n", formatMs(s.min))
	fmt.Printf("Max: %s\n", formatMs(s.max))
	fmt.Printf("Avg: %s\n", formatMs(s.avg))
	fmt.Printf("Median: %s\n", formatMs(s.median))
	fmt.Printf("P95: %s\n", formatMs(s.p95))
	fmt.Printf("P99: %s\n", formatMs(s.p99))
	fmt.Printf("Throughput: %.2f ops/sec\n", s.throughput)
	fmt.Println()
}

func testSingleCreate(engine storage.Engine, iterations int) []time.Duration {
	var durations []time.Duration
	for i := 1; i <= iterations; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", i)),
			Labels:     []string{"TestNode"},
			Properties: map[string]any{"id": i, "ts": time.Now().UnixNano()},
		}
		start := time.Now()
		_, err := engine.CreateNode(node)
		if err != nil {
			continue
		}
		durations = append(durations, time.Since(start))
	}
	return durations
}

func testBatchCreate(engine storage.Engine, iterations, batchSize int) []time.Duration {
	if batchSize <= 0 {
		return nil
	}
	batches := int(math.Max(1, float64(iterations/batchSize)))
	var durations []time.Duration
	for b := 0; b < batches; b++ {
		nodes := make([]*storage.Node, 0, batchSize)
		startIndex := b*batchSize + 1
		endIndex := startIndex + batchSize
		for i := startIndex; i < endIndex; i++ {
			nodes = append(nodes, &storage.Node{
				ID:         storage.NodeID(fmt.Sprintf("batch-node-%d", i)),
				Labels:     []string{"TestNode"},
				Properties: map[string]any{"id": i, "batch": b},
			})
		}
		start := time.Now()
		if err := engine.BulkCreateNodes(nodes); err != nil {
			continue
		}
		durations = append(durations, time.Since(start))
	}
	return durations
}

func testNodeUpdate(engine storage.Engine, iterations int) []time.Duration {
	seedID := storage.NodeID("update-seed")
	actualID, _ := engine.CreateNode(&storage.Node{
		ID:         seedID,
		Labels:     []string{"TestNode"},
		Properties: map[string]any{"id": "seed"},
	})
	if actualID != "" {
		seedID = actualID
	}

	var durations []time.Duration
	for i := 1; i <= iterations; i++ {
		node, err := engine.GetNode(seedID)
		if err != nil {
			continue
		}
		node.Properties["value"] = i
		start := time.Now()
		if err := engine.UpdateNode(node); err != nil {
			continue
		}
		durations = append(durations, time.Since(start))
	}
	return durations
}

func testRelationshipCreate(engine storage.Engine, iterations int) []time.Duration {
	startNode := &storage.Node{
		ID:         storage.NodeID("rel-start"),
		Labels:     []string{"TestNode"},
		Properties: map[string]any{"id": "rel-start"},
	}
	endNode := &storage.Node{
		ID:         storage.NodeID("rel-end"),
		Labels:     []string{"TestNode"},
		Properties: map[string]any{"id": "rel-end"},
	}
	if actualID, _ := engine.CreateNode(startNode); actualID != "" {
		startNode.ID = actualID
	}
	if actualID, _ := engine.CreateNode(endNode); actualID != "" {
		endNode.ID = actualID
	}

	var durations []time.Duration
	for i := 1; i <= iterations; i++ {
		edge := &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("rel-%d", i)),
			Type:       "RELATES",
			StartNode:  startNode.ID,
			EndNode:    endNode.ID,
			Properties: map[string]any{"id": i},
		}
		start := time.Now()
		if err := engine.CreateEdge(edge); err != nil {
			continue
		}
		durations = append(durations, time.Since(start))
	}
	return durations
}

func computeStats(durations []time.Duration) stats {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	count := len(durations)
	min := durations[0]
	max := durations[count-1]

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	avg := total / time.Duration(count)

	median := percentile(durations, 0.5)
	p95 := percentile(durations, 0.95)
	p99 := percentile(durations, 0.99)

	throughput := float64(count) / total.Seconds()

	return stats{
		count:      count,
		min:        min,
		max:        max,
		avg:        avg,
		median:     median,
		p95:        p95,
		p99:        p99,
		throughput: throughput,
	}
}

func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	if p <= 0 {
		return durations[0]
	}
	if p >= 1 {
		return durations[len(durations)-1]
	}
	idx := int(math.Ceil(p*float64(len(durations)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	return durations[idx]
}

func formatMs(d time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
}
