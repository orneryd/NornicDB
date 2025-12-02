# Performance Optimization Guide

**Purpose**: How to optimize NornicDB for maximum performance  
**Audience**: AI coding agents  
**Goal**: 3-52x faster than Neo4j (maintain or improve)

---

## Performance Philosophy

### Golden Rules

1. **Measure First** - Always benchmark before optimizing
2. **Profile** - Find the actual bottleneck, don't guess
3. **Prove Improvement** - Show before/after benchmarks
4. **No Regressions** - Don't sacrifice other metrics
5. **Document** - Explain why optimization works

### Performance Targets

**Current benchmarks (M3 Max, 64GB):**

| Operation                    | NornicDB      | Neo4j       | Target   |
| ---------------------------- | ------------- | ----------- | -------- |
| Message content lookup       | 6,389 ops/sec | 518 ops/sec | ≥6,000   |
| Recent messages (friends)    | 2,769 ops/sec | 108 ops/sec | ≥2,500   |
| Avg friends per city         | 4,713 ops/sec | 91 ops/sec  | ≥4,500   |
| Tag co-occurrence            | 2,076 ops/sec | 65 ops/sec  | ≥2,000   |
| Index lookup                 | 7,623 ops/sec | 2,143 ops/sec | ≥7,000 |
| Count nodes                  | 5,253 ops/sec | 798 ops/sec | ≥5,000   |
| Write: node                  | 5,578 ops/sec | 1,690 ops/sec | ≥5,000 |
| Write: edge                  | 6,626 ops/sec | 1,611 ops/sec | ≥6,000 |

**Memory targets:**
- Cold start: <1s (vs 10-30s for Neo4j)
- Memory footprint: 100-500 MB (vs 1-4 GB for Neo4j)
- No memory leaks
- Efficient garbage collection

---

## Optimization Process

### Step 1: Benchmark Baseline

**Create benchmark before optimizing:**

```go
func BenchmarkQueryExecution(b *testing.B) {
    // Setup
    store := setupBenchmarkData()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    query := "MATCH (n:Person) WHERE n.age > 25 RETURN n LIMIT 100"
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        _, err := exec.Execute(ctx, query, nil)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

**Run baseline:**
```bash
go test -bench=BenchmarkQueryExecution -benchmem -count=5 > before.txt
```

**Example output:**
```
BenchmarkQueryExecution-8    4252    282456 ns/op    2456 B/op    45 allocs/op
```

### Step 2: Profile

**CPU profiling:**
```bash
go test -bench=BenchmarkQueryExecution -cpuprofile=cpu.prof
go tool pprof cpu.prof

# In pprof:
(pprof) top10
(pprof) list FunctionName
(pprof) web  # Visual graph
```

**Memory profiling:**
```bash
go test -bench=BenchmarkQueryExecution -memprofile=mem.prof
go tool pprof mem.prof

# In pprof:
(pprof) top10
(pprof) list FunctionName
```

**Trace analysis:**
```bash
go test -bench=BenchmarkQueryExecution -trace=trace.out
go tool trace trace.out
```

### Step 3: Identify Bottleneck

**Common bottlenecks:**

1. **Allocations** - Too many heap allocations
2. **Locks** - Contention on mutexes
3. **I/O** - Disk or network operations
4. **Algorithm** - O(n²) instead of O(n log n)
5. **Serialization** - JSON encoding/decoding
6. **Reflection** - Runtime type inspection

**Example profile output:**
```
Total: 2.5s
1.2s (48%) - json.Marshal
0.5s (20%) - sync.(*Mutex).Lock
0.3s (12%) - runtime.mallocgc
0.2s (8%)  - findNodes
0.3s (12%) - other
```

**Analysis:** JSON marshaling is the bottleneck (48% of time)

### Step 4: Optimize

**Apply optimization technique:**

```go
// BEFORE: Slow JSON marshaling
func (e *Executor) formatResult(rows [][]interface{}) (*Result, error) {
    // Marshal each row individually - slow!
    for _, row := range rows {
        data, err := json.Marshal(row)
        if err != nil {
            return nil, err
        }
        result.Data = append(result.Data, data)
    }
    return result, nil
}

// AFTER: Pre-allocate and batch
func (e *Executor) formatResult(rows [][]interface{}) (*Result, error) {
    // Pre-allocate capacity
    result := &Result{
        Data: make([][]byte, 0, len(rows)),
    }
    
    // Use sync.Pool for buffers
    buf := bufferPool.Get().(*bytes.Buffer)
    defer bufferPool.Put(buf)
    
    encoder := json.NewEncoder(buf)
    
    for _, row := range rows {
        buf.Reset()
        if err := encoder.Encode(row); err != nil {
            return nil, err
        }
        result.Data = append(result.Data, buf.Bytes())
    }
    
    return result, nil
}
```

### Step 5: Benchmark After

**Run benchmark again:**
```bash
go test -bench=BenchmarkQueryExecution -benchmem -count=5 > after.txt
```

**Compare results:**
```bash
benchcmp before.txt after.txt
```

**Example output:**
```
benchmark                    old ns/op     new ns/op     delta
BenchmarkQueryExecution-8    282456        156234        -44.68%

benchmark                    old allocs    new allocs    delta
BenchmarkQueryExecution-8    45            23            -48.89%

benchmark                    old bytes     new bytes     delta
BenchmarkQueryExecution-8    2456          1234          -49.76%
```

**Result:** 44.68% faster, 48.89% fewer allocations! ✅

### Step 6: Document

**Add comment explaining optimization:**

```go
// formatResult formats query results for Neo4j compatibility.
//
// OPTIMIZATION: Uses sync.Pool for buffer reuse and pre-allocates capacity
// to reduce allocations. Benchmarks show 44% speedup over naive approach:
//   Before: 282,456 ns/op, 2,456 B/op, 45 allocs/op
//   After:  156,234 ns/op, 1,234 B/op, 23 allocs/op
//   Improvement: 44.68% faster, 48.89% fewer allocations
//
// See: BenchmarkQueryExecution in executor_test.go
func (e *Executor) formatResult(rows [][]interface{}) (*Result, error) {
    // Implementation with optimization
}
```

---

## Optimization Techniques

### 1. Reduce Allocations

**Problem:** Excessive heap allocations cause GC pressure

**Solution 1: Pre-allocate slices**

```go
// BEFORE: Append without capacity
func processNodes(nodes []*Node) []string {
    var names []string  // ← No capacity, will reallocate
    for _, node := range nodes {
        names = append(names, node.Name)
    }
    return names
}

// AFTER: Pre-allocate capacity
func processNodes(nodes []*Node) []string {
    names := make([]string, 0, len(nodes))  // ← Pre-allocate
    for _, node := range nodes {
        names = append(names, node.Name)
    }
    return names
}
```

**Solution 2: Use sync.Pool**

```go
// Pool of reusable buffers
var bufferPool = sync.Pool{
    New: func() interface{} {
        return new(bytes.Buffer)
    },
}

// BEFORE: Allocate new buffer each time
func formatData(data []byte) string {
    buf := new(bytes.Buffer)  // ← New allocation
    buf.Write(data)
    return buf.String()
}

// AFTER: Reuse buffers from pool
func formatData(data []byte) string {
    buf := bufferPool.Get().(*bytes.Buffer)
    defer bufferPool.Put(buf)
    
    buf.Reset()
    buf.Write(data)
    return buf.String()
}
```

**Solution 3: Reuse slices**

```go
// BEFORE: Allocate new slice each iteration
func processInBatches(items []Item) {
    for i := 0; i < len(items); i += 100 {
        batch := items[i:min(i+100, len(items))]
        processBatch(batch)
    }
}

// AFTER: Reuse slice (if safe)
func processInBatches(items []Item) {
    batch := make([]Item, 0, 100)  // ← Allocate once
    
    for _, item := range items {
        batch = append(batch, item)
        
        if len(batch) == 100 {
            processBatch(batch)
            batch = batch[:0]  // ← Reset, don't reallocate
        }
    }
    
    if len(batch) > 0 {
        processBatch(batch)
    }
}
```

### 2. Optimize Algorithms

**Problem:** O(n²) algorithm is slow for large datasets

**Solution: Use better algorithm**

```go
// BEFORE: O(n²) - nested loops
func findDuplicates(nodes []*Node) []*Node {
    var duplicates []*Node
    
    for i := 0; i < len(nodes); i++ {
        for j := i + 1; j < len(nodes); j++ {
            if nodes[i].ID == nodes[j].ID {
                duplicates = append(duplicates, nodes[j])
            }
        }
    }
    
    return duplicates
}

// AFTER: O(n) - use map
func findDuplicates(nodes []*Node) []*Node {
    seen := make(map[NodeID]bool, len(nodes))
    var duplicates []*Node
    
    for _, node := range nodes {
        if seen[node.ID] {
            duplicates = append(duplicates, node)
        } else {
            seen[node.ID] = true
        }
    }
    
    return duplicates
}
```

**Benchmark:**
```
Before (O(n²)): 1,000 nodes = 500ms
After (O(n)):   1,000 nodes = 5ms
Improvement: 100x faster!
```

### 3. Use Indexes

**Problem:** Full table scan is slow

**Solution: Create and use indexes**

```go
// BEFORE: Linear scan O(n)
func (e *MemoryEngine) FindNodesByLabel(label string) []*Node {
    var results []*Node
    
    for _, node := range e.nodes {
        for _, l := range node.Labels {
            if l == label {
                results = append(results, node)
                break
            }
        }
    }
    
    return results
}

// AFTER: Use index O(log n) or O(1)
type MemoryEngine struct {
    nodes       map[NodeID]*Node
    labelIndex  map[string][]NodeID  // ← Index
}

func (e *MemoryEngine) FindNodesByLabel(label string) []*Node {
    nodeIDs := e.labelIndex[label]  // ← O(1) lookup
    
    results := make([]*Node, 0, len(nodeIDs))
    for _, id := range nodeIDs {
        if node, ok := e.nodes[id]; ok {
            results = append(results, node)
        }
    }
    
    return results
}

func (e *MemoryEngine) CreateNode(node *Node) error {
    // Store node
    e.nodes[node.ID] = node
    
    // Update indexes
    for _, label := range node.Labels {
        e.labelIndex[label] = append(e.labelIndex[label], node.ID)
    }
    
    return nil
}
```

**Benchmark:**
```
Before (no index): 10,000 nodes = 50ms
After (with index): 10,000 nodes = 0.5ms
Improvement: 100x faster!
```

### 4. Batch Operations

**Problem:** Individual operations have overhead

**Solution: Batch multiple operations**

```go
// BEFORE: Individual creates
func importNodes(nodes []*Node) error {
    for _, node := range nodes {
        if err := engine.CreateNode(node); err != nil {
            return err
        }
    }
    return nil
}

// AFTER: Batch create
func importNodes(nodes []*Node) error {
    return engine.CreateNodesBatch(nodes)
}

func (e *BadgerEngine) CreateNodesBatch(nodes []*Node) error {
    return e.db.Update(func(txn *badger.Txn) error {
        for _, node := range nodes {
            data, err := json.Marshal(node)
            if err != nil {
                return err
            }
            
            key := []byte("node:" + string(node.ID))
            if err := txn.Set(key, data); err != nil {
                return err
            }
        }
        return nil
    })
}
```

**Benchmark:**
```
Before (individual): 1,000 nodes = 500ms
After (batch):       1,000 nodes = 50ms
Improvement: 10x faster!
```

### 5. Reduce Lock Contention

**Problem:** Multiple goroutines waiting on same lock

**Solution 1: Use RWMutex**

```go
// BEFORE: Mutex blocks all access
type Cache struct {
    mu   sync.Mutex
    data map[string]interface{}
}

func (c *Cache) Get(key string) (interface{}, bool) {
    c.mu.Lock()         // ← Blocks readers
    defer c.mu.Unlock()
    
    val, ok := c.data[key]
    return val, ok
}

// AFTER: RWMutex allows concurrent reads
type Cache struct {
    mu   sync.RWMutex  // ← Read/Write mutex
    data map[string]interface{}
}

func (c *Cache) Get(key string) (interface{}, bool) {
    c.mu.RLock()        // ← Multiple readers OK
    defer c.mu.RUnlock()
    
    val, ok := c.data[key]
    return val, ok
}

func (c *Cache) Set(key string, val interface{}) {
    c.mu.Lock()         // ← Exclusive write
    defer c.mu.Unlock()
    
    c.data[key] = val
}
```

**Solution 2: Shard locks**

```go
// BEFORE: Single lock for all data
type Cache struct {
    mu   sync.RWMutex
    data map[string]interface{}
}

// AFTER: Multiple locks (sharding)
type Cache struct {
    shards []*cacheShard
}

type cacheShard struct {
    mu   sync.RWMutex
    data map[string]interface{}
}

func (c *Cache) getShard(key string) *cacheShard {
    hash := fnv.New32a()
    hash.Write([]byte(key))
    return c.shards[hash.Sum32()%uint32(len(c.shards))]
}

func (c *Cache) Get(key string) (interface{}, bool) {
    shard := c.getShard(key)
    
    shard.mu.RLock()
    defer shard.mu.RUnlock()
    
    val, ok := shard.data[key]
    return val, ok
}
```

**Benchmark (100 concurrent goroutines):**
```
Before (single lock): 1,000 ops/sec
After (16 shards):    12,000 ops/sec
Improvement: 12x faster!
```

### 6. Optimize Serialization

**Problem:** JSON encoding/decoding is slow

**Solution 1: Use faster serialization**

```go
// BEFORE: Standard JSON
import "encoding/json"

func serialize(node *Node) ([]byte, error) {
    return json.Marshal(node)
}

// AFTER: Use msgpack (faster binary format)
import "github.com/vmihailenco/msgpack/v5"

func serialize(node *Node) ([]byte, error) {
    return msgpack.Marshal(node)
}
```

**Solution 2: Avoid serialization**

```go
// BEFORE: Serialize for cache
func (c *Cache) Set(key string, node *Node) {
    data, _ := json.Marshal(node)
    c.data[key] = data
}

func (c *Cache) Get(key string) (*Node, error) {
    data := c.data[key].([]byte)
    var node Node
    json.Unmarshal(data, &node)
    return &node, nil
}

// AFTER: Store object directly
func (c *Cache) Set(key string, node *Node) {
    c.data[key] = node  // ← No serialization!
}

func (c *Cache) Get(key string) (*Node, error) {
    return c.data[key].(*Node), nil
}
```

**Benchmark:**
```
Before (JSON):     10,000 ops = 500ms
After (msgpack):   10,000 ops = 200ms
After (no serial): 10,000 ops = 10ms
```

### 7. Parallelize

**Problem:** Sequential processing is slow

**Solution: Use goroutines**

```go
// BEFORE: Sequential processing
func processNodes(nodes []*Node) []Result {
    results := make([]Result, len(nodes))
    
    for i, node := range nodes {
        results[i] = expensiveOperation(node)
    }
    
    return results
}

// AFTER: Parallel processing
func processNodes(nodes []*Node) []Result {
    results := make([]Result, len(nodes))
    
    var wg sync.WaitGroup
    sem := make(chan struct{}, runtime.NumCPU())  // Limit concurrency
    
    for i, node := range nodes {
        wg.Add(1)
        
        go func(idx int, n *Node) {
            defer wg.Done()
            
            sem <- struct{}{}        // Acquire
            defer func() { <-sem }() // Release
            
            results[idx] = expensiveOperation(n)
        }(i, node)
    }
    
    wg.Wait()
    return results
}
```

**Benchmark (8 cores):**
```
Before (sequential): 1,000 nodes = 800ms
After (parallel):    1,000 nodes = 120ms
Improvement: 6.7x faster!
```

### 8. Cache Expensive Operations

**Problem:** Repeated expensive computations

**Solution: Cache results**

```go
// BEFORE: Compute every time
func (e *Executor) generateEmbedding(text string) ([]float32, error) {
    return expensiveMLModel.Generate(text)  // ← 100ms per call
}

// AFTER: Cache embeddings
type Executor struct {
    embeddingCache *sync.Map
}

func (e *Executor) generateEmbedding(text string) ([]float32, error) {
    // Check cache
    if cached, ok := e.embeddingCache.Load(text); ok {
        return cached.([]float32), nil
    }
    
    // Generate
    embedding, err := expensiveMLModel.Generate(text)
    if err != nil {
        return nil, err
    }
    
    // Cache
    e.embeddingCache.Store(text, embedding)
    
    return embedding, nil
}
```

**Benchmark:**
```
Before (no cache): 100 calls = 10,000ms (100ms each)
After (with cache): 100 calls = 100ms (1ms each, 99 cache hits)
Improvement: 100x faster!
```

---

## Memory Optimization

### 1. Reduce Memory Footprint

**Use smaller types:**

```go
// BEFORE: 64-bit for small values
type Node struct {
    AccessCount int64  // ← 8 bytes, but rarely > 1 million
}

// AFTER: 32-bit sufficient
type Node struct {
    AccessCount int32  // ← 4 bytes, max 2 billion
}
```

**Use pointers for large structs:**

```go
// BEFORE: Copy entire struct
type Cache struct {
    data map[string]Node  // ← Copies entire Node
}

// AFTER: Store pointer
type Cache struct {
    data map[string]*Node  // ← Only 8 bytes per entry
}
```

### 2. Avoid Memory Leaks

**Close resources:**

```go
// BEFORE: Goroutine leak
func processStream(ch <-chan Data) {
    for data := range ch {
        go process(data)  // ← Goroutines never stop!
    }
}

// AFTER: Use context for cancellation
func processStream(ctx context.Context, ch <-chan Data) {
    for {
        select {
        case data := <-ch:
            go func(d Data) {
                select {
                case <-ctx.Done():
                    return
                default:
                    process(d)
                }
            }(data)
        case <-ctx.Done():
            return
        }
    }
}
```

**Clear references:**

```go
// BEFORE: Cache grows forever
type Cache struct {
    data map[string]*Node
}

// AFTER: Implement eviction
type Cache struct {
    data     map[string]*cacheEntry
    maxSize  int
    eviction *list.List  // LRU list
}

func (c *Cache) Set(key string, node *Node) {
    if len(c.data) >= c.maxSize {
        c.evictOldest()  // ← Remove old entries
    }
    
    c.data[key] = &cacheEntry{
        node:      node,
        timestamp: time.Now(),
    }
}
```

---

## Benchmarking Best Practices

### 1. Realistic Data

```go
func BenchmarkQueryExecution(b *testing.B) {
    // Use realistic data size
    store := setupBenchmarkData(
        nodeCount: 10000,      // ← Realistic size
        edgeCount: 50000,
        avgDegree: 5,
    )
    
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Use realistic queries
    queries := []string{
        "MATCH (n:Person) WHERE n.age > 25 RETURN n LIMIT 100",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
        "MATCH (n:Person) RETURN count(n)",
    }
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        query := queries[i%len(queries)]
        _, err := exec.Execute(ctx, query, nil)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### 2. Multiple Runs

```bash
# Run benchmark 10 times for statistical significance
go test -bench=. -benchmem -count=10 > results.txt

# Analyze with benchstat
benchstat results.txt
```

### 3. Isolate Changes

```bash
# Benchmark one change at a time
git checkout main
go test -bench=. -benchmem -count=5 > before.txt

git checkout feature-branch
go test -bench=. -benchmem -count=5 > after.txt

benchcmp before.txt after.txt
```

---

## Performance Checklist

Before claiming optimization:

- [ ] Baseline benchmark exists
- [ ] Profiling identified bottleneck
- [ ] Optimization targets bottleneck
- [ ] After benchmark shows improvement
- [ ] No regression in other metrics
- [ ] Memory usage acceptable
- [ ] All tests still pass
- [ ] Documentation updated
- [ ] Commit includes benchmarks

---

## Quick Reference

### Benchmark Commands

```bash
# Run benchmarks
go test -bench=. -benchmem

# CPU profile
go test -bench=. -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Memory profile
go test -bench=. -memprofile=mem.prof
go tool pprof mem.prof

# Compare benchmarks
benchcmp before.txt after.txt

# Statistical analysis
benchstat results.txt
```

### Common Optimizations

1. **Reduce allocations** - Pre-allocate, use sync.Pool
2. **Better algorithms** - O(n) instead of O(n²)
3. **Use indexes** - O(1) or O(log n) lookups
4. **Batch operations** - Reduce overhead
5. **Reduce locks** - RWMutex, sharding
6. **Faster serialization** - msgpack, protobuf
7. **Parallelize** - Use goroutines
8. **Cache** - Expensive operations

---

**Remember**: Premature optimization is the root of all evil. Always measure first, optimize second, and prove improvement with benchmarks.
