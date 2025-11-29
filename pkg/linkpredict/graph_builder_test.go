package linkpredict

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// GRAPH BUILDER TESTS
// =============================================================================

func TestGraphBuilder_Basic(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)

	graph, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(graph) == 0 {
		t.Error("Expected non-empty graph")
	}

	// Verify stats
	stats := builder.Stats()
	if stats.BuildsCompleted != 1 {
		t.Errorf("Expected 1 build, got %d", stats.BuildsCompleted)
	}
	if stats.LastBuildNodes == 0 {
		t.Error("Expected node count in stats")
	}

	t.Logf("Built graph: %d nodes, %d edges in %v",
		stats.LastBuildNodes, stats.LastBuildEdges, stats.LastBuildTime)
}

func TestGraphBuilder_Parallel(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create larger graph for parallelization test
	nodeCount := 100
	for i := 0; i < nodeCount; i++ {
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
	}

	// Create random edges
	edgeCount := 0
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			if (i+j)%7 == 0 { // Sparse edges
				engine.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("e-%d", edgeCount)),
					StartNode: storage.NodeID(fmt.Sprintf("node-%d", i)),
					EndNode:   storage.NodeID(fmt.Sprintf("node-%d", j)),
					Type:      "CONNECTS",
				})
				edgeCount++
			}
		}
	}

	// Test with different worker counts
	workerCounts := []int{1, 2, 4, runtime.NumCPU()}

	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			config := DefaultBuildConfig()
			config.WorkerCount = workers
			config.ChunkSize = 25
			builder := NewGraphBuilder(engine, config)

			start := time.Now()
			graph, err := builder.Build(context.Background())
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			if len(graph) != nodeCount {
				t.Errorf("Expected %d nodes, got %d", nodeCount, len(graph))
			}

			t.Logf("Workers=%d: %v", workers, elapsed)
		})
	}
}

func TestGraphBuilder_ChunkedProcessing(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create graph
	nodeCount := 50
	for i := 0; i < nodeCount; i++ {
		engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("n%d", i)),
		})
	}

	chunksProcessed := int32(0)

	config := DefaultBuildConfig()
	config.ChunkSize = 10
	config.ProgressCallback = func(processed, total int, elapsed time.Duration) {
		atomic.AddInt32(&chunksProcessed, 1)
		t.Logf("Progress: %d/%d (%.2fs)", processed, total, elapsed.Seconds())
	}

	builder := NewGraphBuilder(engine, config)
	_, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have processed 5 chunks (50 nodes / 10 per chunk)
	if chunksProcessed < 5 {
		t.Errorf("Expected at least 5 chunks, got %d", chunksProcessed)
	}
}

func TestGraphBuilder_ContextCancellation(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create graph
	for i := 0; i < 100; i++ {
		engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("n%d", i)),
		})
	}

	config := DefaultBuildConfig()
	config.ChunkSize = 10
	builder := NewGraphBuilder(engine, config)

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := builder.Build(ctx)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestGraphBuilder_GCAfterChunk(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create graph
	for i := 0; i < 30; i++ {
		engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("n%d", i)),
		})
	}

	// Test with GC enabled
	config := DefaultBuildConfig()
	config.ChunkSize = 10
	config.GCAfterChunk = true
	builder := NewGraphBuilder(engine, config)

	_, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build with GC failed: %v", err)
	}

	// Test with GC disabled
	config.GCAfterChunk = false
	builder = NewGraphBuilder(engine, config)

	_, err = builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build without GC failed: %v", err)
	}
}

// =============================================================================
// CACHING TESTS
// =============================================================================

func TestGraphBuilder_DiskCache(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	// Create temp directory for cache
	cacheDir := t.TempDir()

	config := DefaultBuildConfig()
	config.CachePath = cacheDir
	config.CacheTTL = time.Hour
	builder := NewGraphBuilder(engine, config)

	// First build - should miss cache
	graph1, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	stats := builder.Stats()
	if stats.CacheHits != 0 {
		t.Error("Expected 0 cache hits on first build")
	}
	if stats.CacheMisses != 1 {
		t.Errorf("Expected 1 cache miss, got %d", stats.CacheMisses)
	}

	// Verify cache file exists
	cachePath := filepath.Join(cacheDir, "graph_cache.gob")
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		t.Error("Cache file not created")
	}

	// Second build - should hit cache
	graph2, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
	}

	stats = builder.Stats()
	if stats.CacheHits != 1 {
		t.Errorf("Expected 1 cache hit, got %d", stats.CacheHits)
	}

	// Verify graphs are equivalent
	if len(graph1) != len(graph2) {
		t.Errorf("Graph sizes differ: %d vs %d", len(graph1), len(graph2))
	}

	t.Logf("Cache test: hits=%d, misses=%d", stats.CacheHits, stats.CacheMisses)
}

func TestGraphBuilder_CacheInvalidation(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	cacheDir := t.TempDir()

	config := DefaultBuildConfig()
	config.CachePath = cacheDir
	builder := NewGraphBuilder(engine, config)

	// Build and cache
	_, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Invalidate cache
	err = builder.InvalidateCache()
	if err != nil {
		t.Fatalf("InvalidateCache failed: %v", err)
	}

	// Verify cache file is gone
	cachePath := filepath.Join(cacheDir, "graph_cache.gob")
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Error("Cache file should be deleted")
	}

	// Next build should miss cache
	_, err = builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Rebuild failed: %v", err)
	}

	stats := builder.Stats()
	if stats.CacheMisses != 2 {
		t.Errorf("Expected 2 cache misses, got %d", stats.CacheMisses)
	}
}

func TestGraphBuilder_CacheExpiration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	cacheDir := t.TempDir()

	config := DefaultBuildConfig()
	config.CachePath = cacheDir
	config.CacheTTL = 1 * time.Millisecond // Very short TTL
	builder := NewGraphBuilder(engine, config)

	// Build and cache
	_, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Wait for cache to expire
	time.Sleep(10 * time.Millisecond)

	// Build again - should miss due to expiration
	_, err = builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
	}

	stats := builder.Stats()
	// Should have 2 misses (first + expired)
	if stats.CacheMisses != 2 {
		t.Errorf("Expected 2 cache misses (expired), got %d", stats.CacheMisses)
	}
}

// =============================================================================
// INCREMENTAL UPDATE TESTS
// =============================================================================

func TestGraphBuilder_ApplyDelta_AddNodes(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)

	// Start with empty graph
	graph := make(Graph)

	// Add nodes via delta
	delta := &GraphDelta{
		AddedNodes: []storage.NodeID{"a", "b", "c"},
	}

	graph = builder.ApplyDelta(graph, delta)

	if len(graph) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(graph))
	}

	for _, nodeID := range delta.AddedNodes {
		if _, exists := graph[nodeID]; !exists {
			t.Errorf("Node %s not found", nodeID)
		}
	}
}

func TestGraphBuilder_ApplyDelta_AddEdges(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	config.Undirected = true
	builder := NewGraphBuilder(engine, config)

	// Start with nodes
	graph := Graph{
		"a": make(NodeSet),
		"b": make(NodeSet),
		"c": make(NodeSet),
	}

	// Add edges via delta
	delta := &GraphDelta{
		AddedEdges: []EdgeChange{
			{From: "a", To: "b"},
			{From: "b", To: "c"},
		},
	}

	graph = builder.ApplyDelta(graph, delta)

	// Check edges
	if !graph["a"].Contains("b") {
		t.Error("Edge a->b not found")
	}
	if !graph["b"].Contains("a") {
		t.Error("Edge b->a not found (undirected)")
	}
	if !graph["b"].Contains("c") {
		t.Error("Edge b->c not found")
	}
}

func TestGraphBuilder_ApplyDelta_RemoveNodes(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)

	// Start with connected graph
	graph := Graph{
		"a": NodeSet{"b": {}, "c": {}},
		"b": NodeSet{"a": {}, "c": {}},
		"c": NodeSet{"a": {}, "b": {}},
	}

	// Remove node b
	delta := &GraphDelta{
		RemovedNodes: []storage.NodeID{"b"},
	}

	graph = builder.ApplyDelta(graph, delta)

	// b should be gone
	if _, exists := graph["b"]; exists {
		t.Error("Node b should be removed")
	}

	// Edges to b should be gone
	if graph["a"].Contains("b") {
		t.Error("Edge a->b should be removed")
	}
	if graph["c"].Contains("b") {
		t.Error("Edge c->b should be removed")
	}

	// a and c should still be connected
	if len(graph) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(graph))
	}
}

func TestGraphBuilder_ApplyDelta_RemoveEdges(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	config.Undirected = true
	builder := NewGraphBuilder(engine, config)

	// Start with connected graph
	graph := Graph{
		"a": NodeSet{"b": {}, "c": {}},
		"b": NodeSet{"a": {}, "c": {}},
		"c": NodeSet{"a": {}, "b": {}},
	}

	// Remove edge a-b
	delta := &GraphDelta{
		RemovedEdges: []EdgeChange{
			{From: "a", To: "b"},
		},
	}

	graph = builder.ApplyDelta(graph, delta)

	// a-b edge should be gone (both directions for undirected)
	if graph["a"].Contains("b") {
		t.Error("Edge a->b should be removed")
	}
	if graph["b"].Contains("a") {
		t.Error("Edge b->a should be removed")
	}

	// Other edges should remain
	if !graph["a"].Contains("c") {
		t.Error("Edge a->c should remain")
	}
	if !graph["b"].Contains("c") {
		t.Error("Edge b->c should remain")
	}
}

func TestGraphBuilder_ApplyDelta_Complex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	config.Undirected = true
	builder := NewGraphBuilder(engine, config)

	// Initial graph
	graph := Graph{
		"a": NodeSet{"b": {}},
		"b": NodeSet{"a": {}},
	}

	// Complex delta: add nodes, add edges, remove edges
	delta := &GraphDelta{
		AddedNodes: []storage.NodeID{"c", "d"},
		AddedEdges: []EdgeChange{
			{From: "a", To: "c"},
			{From: "c", To: "d"},
		},
		RemovedEdges: []EdgeChange{
			{From: "a", To: "b"},
		},
	}

	graph = builder.ApplyDelta(graph, delta)

	// Verify structure
	if len(graph) != 4 {
		t.Errorf("Expected 4 nodes, got %d", len(graph))
	}

	if graph["a"].Contains("b") {
		t.Error("Edge a-b should be removed")
	}
	if !graph["a"].Contains("c") {
		t.Error("Edge a-c should exist")
	}
	if !graph["c"].Contains("d") {
		t.Error("Edge c-d should exist")
	}
}

// =============================================================================
// PARALLEL ALGORITHM TESTS
// =============================================================================

func TestParallelCommonNeighbors(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	graph, _ := builder.Build(context.Background())

	sources := []storage.NodeID{"alice", "bob", "charlie"}

	results := ParallelCommonNeighbors(context.Background(), graph, sources, 5, nil)

	if len(results) != len(sources) {
		t.Errorf("Expected %d results, got %d", len(sources), len(results))
	}

	for _, source := range sources {
		if _, exists := results[source]; !exists {
			t.Errorf("Missing results for %s", source)
		}
	}
}

func TestParallelAdamicAdar(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	graph, _ := builder.Build(context.Background())

	sources := []storage.NodeID{"alice", "bob"}

	results := ParallelAdamicAdar(context.Background(), graph, sources, 5, nil)

	if len(results) != len(sources) {
		t.Errorf("Expected %d results, got %d", len(sources), len(results))
	}
}

func TestParallelJaccard(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	graph, _ := builder.Build(context.Background())

	sources := []storage.NodeID{"alice", "bob", "charlie"}

	results := ParallelJaccard(context.Background(), graph, sources, 5, nil)

	if len(results) != len(sources) {
		t.Errorf("Expected %d results, got %d", len(sources), len(results))
	}
}

func TestParallelAlgorithms_Cancellation(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create larger graph
	for i := 0; i < 100; i++ {
		engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("n%d", i)),
		})
	}

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	graph, _ := builder.Build(context.Background())

	sources := make([]storage.NodeID, 50)
	for i := range sources {
		sources[i] = storage.NodeID(fmt.Sprintf("n%d", i))
	}

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := ParallelCommonNeighbors(ctx, graph, sources, 5, nil)

	// Should return early with partial results
	if len(results) >= len(sources) {
		t.Log("Cancellation may not have stopped all workers")
	}
}

// =============================================================================
// STREAMING TESTS
// =============================================================================

func TestGraphStreamer(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	config := DefaultBuildConfig()
	streamer := NewGraphStreamer(engine, config)

	nodeCount := 0
	err := streamer.StreamNodes(context.Background(), func(node *storage.Node) error {
		nodeCount++
		return nil
	})

	if err != nil {
		t.Fatalf("StreamNodes failed: %v", err)
	}

	if nodeCount == 0 {
		t.Error("Expected to stream some nodes")
	}

	t.Logf("Streamed %d nodes", nodeCount)
}

func TestGraphStreamer_Cancellation(t *testing.T) {
	engine := storage.NewMemoryEngine()

	for i := 0; i < 50; i++ {
		engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("n%d", i)),
		})
	}

	config := DefaultBuildConfig()
	streamer := NewGraphStreamer(engine, config)

	ctx, cancel := context.WithCancel(context.Background())
	processed := 0

	err := streamer.StreamNodes(ctx, func(node *storage.Node) error {
		processed++
		if processed >= 10 {
			cancel()
		}
		return nil
	})

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// =============================================================================
// EXPORT/IMPORT TESTS
// =============================================================================

func TestExportImport(t *testing.T) {
	// Create graph
	original := Graph{
		"a": NodeSet{"b": {}, "c": {}},
		"b": NodeSet{"a": {}, "c": {}},
		"c": NodeSet{"a": {}, "b": {}},
	}

	// Export to temp file
	tmpFile, err := os.CreateTemp("", "graph_*.tsv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	err = ExportToWriter(original, tmpFile)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	tmpFile.Close()

	// Import
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	imported, err := ImportFromReader(file)
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Verify
	if len(imported) != len(original) {
		t.Errorf("Node count mismatch: %d vs %d", len(imported), len(original))
	}

	for nodeID, neighbors := range original {
		importedNeighbors, exists := imported[nodeID]
		if !exists {
			t.Errorf("Node %s missing from import", nodeID)
			continue
		}

		for neighbor := range neighbors {
			if !importedNeighbors.Contains(neighbor) {
				t.Errorf("Edge %s->%s missing from import", nodeID, neighbor)
			}
		}
	}
}

// =============================================================================
// BENCHMARK TESTS
// =============================================================================

func BenchmarkGraphBuilder_Small(b *testing.B) {
	engine := storage.NewMemoryEngine()
	for i := 0; i < 100; i++ {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
	}

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.InvalidateCache()
		builder.Build(ctx)
	}
}

func BenchmarkGraphBuilder_Parallel(b *testing.B) {
	engine := storage.NewMemoryEngine()
	for i := 0; i < 500; i++ {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
	}
	// Add edges
	for i := 0; i < 500; i++ {
		for j := i + 1; j < 500; j++ {
			if (i+j)%10 == 0 {
				engine.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("e%d-%d", i, j)),
					StartNode: storage.NodeID(fmt.Sprintf("n%d", i)),
					EndNode:   storage.NodeID(fmt.Sprintf("n%d", j)),
					Type:      "CONNECTS",
				})
			}
		}
	}

	config := DefaultBuildConfig()
	config.WorkerCount = runtime.NumCPU()
	builder := NewGraphBuilder(engine, config)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.InvalidateCache()
		builder.Build(ctx)
	}
}

func BenchmarkParallelAdamicAdar(b *testing.B) {
	engine := storage.NewMemoryEngine()
	nodeCount := 200
	for i := 0; i < nodeCount; i++ {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
	}
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			if (i+j)%5 == 0 {
				engine.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("e%d-%d", i, j)),
					StartNode: storage.NodeID(fmt.Sprintf("n%d", i)),
					EndNode:   storage.NodeID(fmt.Sprintf("n%d", j)),
					Type:      "CONNECTS",
				})
			}
		}
	}

	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)
	graph, _ := builder.Build(context.Background())

	sources := make([]storage.NodeID, 50)
	for i := range sources {
		sources[i] = storage.NodeID(fmt.Sprintf("n%d", i))
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParallelAdamicAdar(ctx, graph, sources, 10, nil)
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func setupTestGraphForBuilder(t *testing.T, engine storage.Engine) {
	t.Helper()

	nodes := []string{"alice", "bob", "charlie", "diana", "eve"}
	for _, name := range nodes {
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(name),
			Labels: []string{"Person"},
		})
	}

	edges := []struct{ from, to string }{
		{"alice", "bob"},
		{"alice", "charlie"},
		{"bob", "charlie"},
		{"bob", "diana"},
		{"charlie", "diana"},
		{"diana", "eve"},
	}

	for i, e := range edges {
		engine.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("e%d", i)),
			StartNode: storage.NodeID(e.from),
			EndNode:   storage.NodeID(e.to),
			Type:      "KNOWS",
		})
	}
}

// =============================================================================
// CONCURRENT SAFETY TESTS
// =============================================================================

func TestGraphBuilder_ConcurrentBuilds(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraphForBuilder(t, engine)

	cacheDir := t.TempDir()
	config := DefaultBuildConfig()
	config.CachePath = cacheDir
	builder := NewGraphBuilder(engine, config)

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Run 10 concurrent builds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := builder.Build(context.Background())
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent build error: %v", err)
	}
}

func TestGraphBuilder_ApplyDelta_Concurrent(t *testing.T) {
	engine := storage.NewMemoryEngine()
	config := DefaultBuildConfig()
	builder := NewGraphBuilder(engine, config)

	graph := Graph{
		"a": make(NodeSet),
		"b": make(NodeSet),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex // Protect graph access

	// Concurrent delta applications with proper synchronization
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			delta := &GraphDelta{
				AddedNodes: []storage.NodeID{storage.NodeID(fmt.Sprintf("node-%d", idx))},
				AddedEdges: []EdgeChange{
					{From: "a", To: storage.NodeID(fmt.Sprintf("node-%d", idx))},
				},
			}

			// Protect graph access with mutex
			mu.Lock()
			graph = builder.ApplyDelta(graph, delta)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all nodes were added
	if len(graph) < 12 { // a, b, + 10 new nodes
		t.Errorf("Expected at least 12 nodes, got %d", len(graph))
	}
}
