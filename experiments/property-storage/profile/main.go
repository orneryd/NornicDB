// Command profile measures the public embedded storage API without test hooks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/experiments/property-storage/corpus"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type phase struct {
	Workers         int     `json:"workers"`
	ConflictRetries int64   `json:"conflict_retries"`
	Operations      int     `json:"operations"`
	WallMS          float64 `json:"wall_ms"`
	OpsSecond       float64 `json:"ops_second"`
	P50NS           int64   `json:"p50_ns"`
	P95NS           int64   `json:"p95_ns"`
	P99NS           int64   `json:"p99_ns"`
}

func measure(workers, count int, operation func(int) error) (phase, error) {
	var retries atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, workers)
	samples := make([][]int64, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			local := make([]int64, 0, (count+workers-1)/workers)
			<-start
			for i := worker; i < count; i += workers {
				begin := time.Now()
				var err error
				attempts := 0
				for attempt := 0; attempt < 32; attempt++ {
					attempts++
					err = operation(i)
					if err == nil {
						break
					}
					var accepted *storage.AcceptedCommitError
					if errors.As(err, &accepted) || (!errors.Is(err, badger.ErrConflict) && !errors.Is(err, storage.ErrConflict)) || attempt == 31 {
						break
					}
					retries.Add(1)
					// Yield increasingly on shared-endpoint conflicts. The fixed
					// retry cap and all backoff remain inside measured latency.
					delay := time.Duration(1<<min(attempt, 10)) * time.Microsecond
					jitter := time.Duration((worker*17+i*13+attempt*7)%31) * time.Microsecond
					time.Sleep(delay + jitter)
				}
				local = append(local, time.Since(begin).Nanoseconds())
				if err != nil {
					failures <- fmt.Errorf("operation %d after %d attempts (%T): %w", i, attempts, err, err)
					return
				}
			}
			samples[worker] = local
		}(worker)
	}
	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	close(failures)
	for err := range failures {
		return phase{}, err
	}
	all := make([]int64, 0, count)
	for _, sample := range samples {
		all = append(all, sample...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return phase{Workers: workers, ConflictRetries: retries.Load(), Operations: count, WallMS: float64(elapsed.Nanoseconds()) / 1e6, OpsSecond: float64(count) / elapsed.Seconds(), P50NS: all[len(all)/2], P95NS: all[len(all)*95/100], P99NS: all[len(all)*99/100]}, nil
}

func run() error {
	docs := flag.String("docs", "docs", "frozen documentation corpus")
	out := flag.String("out", "", "JSON receipt path")
	workers := flag.Int("workers", 1, "concurrent closed-loop clients")
	edgeWorkers := flag.Int("edge-workers", 1, "edge seed clients; use 4 to reproduce baseline shared-endpoint conflicts")
	syncWrites := flag.Bool("sync-writes", false, "fsync on each write; identical for compared processes")
	lowMemory := flag.Bool("low-memory", false, "use the engine's low-memory profile")
	readRounds := flag.Int("read-rounds", 3, "complete passes per timed warm-read phase")
	cpu := flag.String("cpu", "", "optional diagnostic CPU profile")
	heap := flag.String("heap", "", "optional diagnostic heap profile")
	flag.Parse()
	if *out == "" || *workers < 1 || *workers > 64 || *edgeWorkers < 1 || *edgeWorkers > 64 || *readRounds < 1 || *readRounds > 1024 {
		return fmt.Errorf("out is required; workers must be 1..64")
	}
	base, digest, err := corpus.Load(*docs)
	if err != nil {
		return err
	}
	records := corpus.Replicate(base, 16)
	nodes := make([]*storage.Node, len(records))
	for i, record := range records {
		nodes[i] = &storage.Node{ID: storage.NodeID(record.ID), Labels: []string{"Documentation"}, Properties: record.Properties}
	}
	dir, err := os.MkdirTemp("", "nornic-storage-profile-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	opts := storage.BadgerOptions{DataDir: dir, LowMemory: *lowMemory, SyncWrites: *syncWrites}
	engine, err := storage.NewBadgerEngineWithOptions(opts)
	if err != nil {
		return err
	}
	defer func() { _ = engine.Close() }()
	if *cpu != "" {
		file, e := os.Create(*cpu)
		if e != nil {
			return e
		}
		if e = pprof.StartCPUProfile(file); e != nil {
			file.Close()
			return e
		}
		defer func() { pprof.StopCPUProfile(); file.Close() }()
	}
	ctx := context.Background()
	phases := make(map[string]phase)
	collect := func(name string, count int, fn func(int) error) error {
		clients := *workers
		if name == "create_edge" {
			clients = *edgeWorkers
		}
		result, e := measure(clients, count, fn)
		if e != nil {
			return fmt.Errorf("%s: %w", name, e)
		}
		phases[name] = result
		return nil
	}
	var before, after, live runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if err = collect("create_node", len(nodes), func(i int) error { _, e := engine.CreateNode(ctx, nodes[i]); return e }); err != nil {
		return err
	}
	if err = collect("create_edge", 2*len(nodes), func(i int) error {
		from := i / 2
		to := (from + 1 + i%2) % len(nodes)
		return engine.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("docs:e-%06d", i)), StartNode: nodes[from].ID, EndNode: nodes[to].ID, Type: "RELATED", Properties: map[string]any{"weight": int64(i % 64)}})
	}); err != nil {
		return err
	}
	begin := time.Now()
	if err = engine.Close(); err != nil {
		return err
	}
	flushMS := float64(time.Since(begin).Nanoseconds()) / 1e6
	engine, err = storage.NewBadgerEngineWithOptions(opts)
	if err != nil {
		return err
	}
	// Correctness runs outside the timed phases. Every full value is compared;
	// adjacency endpoints/counts and every mixed write are also checked below.
	for _, want := range nodes {
		got, e := engine.GetNode(ctx, want.ID)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(want.Properties, got.Properties) {
			return fmt.Errorf("node mismatch %s", want.ID)
		}
	}
	for i := 0; i < 2*len(nodes); i++ {
		edge, e := engine.GetEdge(ctx, storage.EdgeID(fmt.Sprintf("docs:e-%06d", i)))
		if e != nil {
			return e
		}
		from := i / 2
		to := (from + 1 + i%2) % len(nodes)
		if edge.StartNode != nodes[from].ID || edge.EndNode != nodes[to].ID || edge.Type != "RELATED" || !reflect.DeepEqual(edge.Properties, map[string]any{"weight": int64(i % 64)}) {
			return fmt.Errorf("edge mismatch %d", i)
		}
	}
	for _, want := range nodes {
		edges, e := engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: want.ID, Direction: storage.AdjacentOutgoing, Headers: true})
		if e != nil {
			return e
		}
		if len(edges) != 2 {
			return fmt.Errorf("adjacency mismatch %s: %d", want.ID, len(edges))
		}
		for _, edge := range edges {
			if edge.StartNode != want.ID {
				return fmt.Errorf("adjacency endpoint mismatch")
			}
		}
	}
	readCount := *readRounds * len(nodes)
	if err = collect("projected_read", readCount, func(i int) error {
		_, e := engine.GetNodeProjected(ctx, nodes[i%len(nodes)].ID, []string{"path"})
		return e
	}); err != nil {
		return err
	}
	if err = collect("full_read", readCount, func(i int) error { _, e := engine.GetNode(ctx, nodes[i%len(nodes)].ID); return e }); err != nil {
		return err
	}
	if err = collect("adjacency_read", readCount, func(i int) error {
		_, e := engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: nodes[i%len(nodes)].ID, Direction: storage.AdjacentOutgoing, Headers: true})
		return e
	}); err != nil {
		return err
	}
	if err = collect("mixed_80_read_20_write", len(nodes), func(i int) error {
		if i%5 == 0 {
			_, e := engine.CreateNode(ctx, &storage.Node{ID: storage.NodeID(string(nodes[i].ID) + "-mixed"), Labels: nodes[i].Labels, Properties: nodes[i].Properties})
			return e
		}
		_, e := engine.GetNodeProjected(ctx, nodes[i].ID, []string{"path"})
		return e
	}); err != nil {
		return err
	}
	for i := 0; i < len(nodes); i += 5 {
		got, e := engine.GetNode(ctx, storage.NodeID(string(nodes[i].ID)+"-mixed"))
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(nodes[i].Properties, got.Properties) {
			return fmt.Errorf("mixed write mismatch")
		}
	}
	runtime.ReadMemStats(&after)
	runtime.GC()
	runtime.ReadMemStats(&live)
	if *heap != "" {
		file, e := os.Create(*heap)
		if e != nil {
			return e
		}
		e = pprof.WriteHeapProfile(file)
		file.Close()
		if e != nil {
			return e
		}
	}
	if err = engine.Close(); err != nil {
		return err
	}
	var size int64
	if err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		info, e := entry.Info()
		if e == nil {
			size += info.Size()
		}
		return e
	}); err != nil {
		return err
	}
	var rss uint64
	if status, e := os.ReadFile("/proc/self/status"); e == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss, e = strconv.ParseUint(strings.Fields(line)[1], 10, 64)
				if e != nil {
					return e
				}
			}
		}
	}
	result := map[string]any{"low_memory": *lowMemory, "enabled": config.IsExperimentalPropertyStorageEnabled(), "sync_writes": *syncWrites, "workers": *workers, "edge_workers": *edgeWorkers, "read_rounds": *readRounds, "gomaxprocs": runtime.GOMAXPROCS(0), "corpus_sha256": digest, "nodes": len(nodes), "edges": 2 * len(nodes), "mixed_writes": (len(nodes) + 4) / 5, "file_bytes": size, "seed_flush_ms": flushMS, "phases": phases, "peak_rss_kib": rss, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "live_heap_bytes": live.HeapAlloc, "gc_cycles": after.NumGC - before.NumGC, "correctness_ok": true}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*out, data, 0600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
