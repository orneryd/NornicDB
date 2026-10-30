# HTTP Server Performance Optimization Options

**Last Updated:** 2025-01-27  
**Status:** Research & Recommendations

## Executive Summary

This document outlines performance optimization options for NornicDB's HTTP server, comparing Go-native optimizations against potential C implementations. Based on current research and benchmarks, **Go-native optimizations are recommended** as the primary path, with C reserved for specific hot paths if profiling reveals bottlenecks.

---

## Current Performance Profile

From recent profiling of NornicDB's HTTP write path:

- **Primary bottlenecks:** Network I/O and BadgerDB memtable initialization (startup cost)
- **Serialization overhead:** Minimal (Msgpack vs Gob difference is negligible at scale)
- **GC pressure:** Low (not a significant bottleneck)
- **Per-request overhead:** Acceptable for current throughput targets

**Conclusion:** The HTTP server itself is not the bottleneck. Optimization should focus on:
1. Reducing network I/O overhead
2. Optimizing BadgerDB write paths
3. Connection pooling and keep-alive optimization

---

## Option 1: Go-Native Optimizations (Recommended)

### 1.1 Profile-Guided Optimization (PGO)

**Status:** Available in Go 1.21+  
**Expected Improvement:** 2-14% performance gain  
**Effort:** Low (automatic after profile collection)

**Implementation:**

```bash
# 1. Collect CPU profile from production workload
go tool pprof http://127.0.0.1:9091/debug/pprof/profile?seconds=60

# 2. Save as default.pgo in main package
go tool pprof -proto profile.pb.gz > default.pgo

# 3. Rebuild - Go automatically detects and applies PGO
go build ./cmd/nornicdb
```

**Benefits:**
- Zero code changes required
- Automatic optimization based on real workload
- Compiler makes better inlining and branch prediction decisions

**References:**
- [Go PGO Documentation](https://go.dev/doc/pgo)
- [Cloudflare PGO Guide](https://cloud.google.com/blog/products/application-development/using-profile-guided-optimization-for-your-go-apps)

---

### 1.2 sync.Pool for Zero-Allocation Hot Paths

**Status:** Can be applied immediately  
**Expected Improvement:** 20-50% reduction in allocations, 5-15% throughput improvement  
**Effort:** Medium (requires profiling to identify hot paths)

**Current State:**
- Go's `net/http` already uses `sync.Pool` for `bufio.Reader` and `bufio.Writer`
- NornicDB can add pools for:
  - JSON encoding/decoding buffers
  - Cypher query parsing buffers
  - Response serialization buffers

**Implementation Example:**

```go
// pkg/server/pool.go
var (
    jsonEncoderPool = sync.Pool{
        New: func() interface{} {
            return json.NewEncoder(nil)
        },
    }
    
    responseBufferPool = sync.Pool{
        New: func() interface{} {
            return bytes.NewBuffer(make([]byte, 0, 4096))
        },
    }
)

// In handler:
buf := responseBufferPool.Get().(*bytes.Buffer)
defer func() {
    buf.Reset()
    responseBufferPool.Put(buf)
}()
// Use buf for response...
```

**Benefits:**
- Reduces GC pressure
- Improves cache locality
- Minimal code changes

**References:**
- [VictoriaMetrics sync.Pool Guide](https://victoriametrics.com/blog/go-sync-pool/)
- [Buffer Pooling Case Study](https://dev.to/uthman_dev/how-buffer-pooling-doubled-my-http-servers-throughput-4000-7721-rps-3i0g)

---

### 1.3 Connection Pooling Optimization

**Status:** Partially implemented  
**Expected Improvement:** 10-30% latency reduction for concurrent requests  
**Effort:** Low (tuning existing settings)

**Current Configuration:**

```go
Transport: &http.Transport{
    MaxIdleConns:        concurrency * 2,
    MaxIdleConnsPerHost: concurrency * 2,
    IdleConnTimeout:     60 * time.Second,
    DisableKeepAlives:   false,
}
```

**Optimizations:**
- Increase `MaxIdleConns` for high-concurrency workloads
- Tune `IdleConnTimeout` based on request patterns
- Consider HTTP/2 for multiplexing (if client support is available)

---

### 1.4 HTTP/2 Support (Implemented)

**Status:** ✅ Always enabled (backwards compatible)  
**Expected Improvement:** 10-20% latency reduction for concurrent requests  
**Implementation Date:** 2026-01-27

**Implementation:**

HTTP/2 is automatically enabled for all connections:
- **HTTPS mode:** HTTP/2 via ALPN (automatic protocol negotiation)
- **HTTP mode:** h2c (HTTP/2 cleartext) with automatic fallback to HTTP/1.1

```go
// HTTP/2 is automatically configured in server.Start()
// No additional configuration required
config := server.DefaultConfig()
config.HTTP2MaxConcurrentStreams = 500 // Optional: adjust concurrent streams (default: 250)

server, err := server.New(db, auth, config)
```

**Benefits:**
- ✅ Multiplexing multiple requests over single connection
- ✅ Header compression
- ✅ Backwards compatible with HTTP/1.1 clients
- ✅ Automatic protocol negotiation

**Configuration:**
- `HTTP2MaxConcurrentStreams`: Maximum concurrent streams per connection (default: 250, matches Go's internal default)

**See:** [HTTP/2 Implementation Guide](http2-implementation.md) for details.

---

### 1.5 Zero-Copy Response Writing

**Status:** Can be applied selectively  
**Expected Improvement:** 5-10% reduction in memory allocations  
**Effort:** Medium (requires careful buffer management)

**Implementation:**

```go
// Instead of:
json.NewEncoder(w).Encode(data)

// Use pre-allocated buffer:
buf := responseBufferPool.Get().(*bytes.Buffer)
json.NewEncoder(buf).Encode(data)
w.Write(buf.Bytes())
buf.Reset()
responseBufferPool.Put(buf)
```

**Benefits:**
- Reduces intermediate allocations
- Better cache locality

---

## Option 2: C Implementation (Not Recommended)

### 2.1 Performance Overhead Analysis

**CGO Call Overhead:**
- **Go → C:** ~40ns per call (Go 1.21)
- **C → Go:** 1-2ms per call
- **Pure Go:** ~1.83ns per call

**Conclusion:** CGO overhead (20-100x) eliminates performance benefits for HTTP request handling, which involves many small operations.

### 2.2 When C Makes Sense

C implementations are only beneficial for:

1. **CPU-intensive algorithms** (e.g., cryptographic operations, compression)
2. **Large batch operations** (amortize CGO overhead over many operations)
3. **Existing C libraries** (e.g., BadgerDB's C dependencies for LSM tree operations)

**HTTP request handling does NOT fit these criteria:**
- Many small operations (parsing, validation, routing)
- High call frequency (every request)
- CGO overhead dominates actual work

### 2.3 Alternative: Assembly Optimization

For specific hot paths, direct assembly insertion can bypass CGO:

- **fastcgo/rustgo:** Reduces overhead to ~30ns (still 15x slower than pure Go)
- **Complexity:** High (requires assembly expertise)
- **Maintenance:** Difficult (platform-specific code)

**Verdict:** Not worth the complexity for HTTP server optimization.

---

## Option 3: Hybrid Approach (Selective C for Hot Paths)

### 3.1 When to Consider

Only if profiling reveals:
1. Specific function consuming >20% of CPU time
2. Function performs CPU-intensive work (not I/O-bound)
3. Function can be batched or called infrequently

### 3.2 Example: JSON Serialization

If JSON encoding becomes a bottleneck:

```go
// C implementation for high-frequency JSON encoding
// #include "json_encode.h"
import "C"

func encodeJSONFast(data interface{}) []byte {
    // Batch encoding in C to amortize CGO overhead
    return C.encode_json_batch(data)
}
```

**Reality Check:**
- Go's `encoding/json` is already highly optimized
- `sync.Pool` reuse is more effective than C implementation
- CGO overhead likely exceeds any C performance gains

---

## Recommended Optimization Roadmap

### Phase 1: Immediate (Low Effort, High Impact)

1. **Enable PGO** (2-14% improvement)
   - Collect production CPU profile
   - Generate `default.pgo`
   - Rebuild with PGO

2. **Tune Connection Pooling** (10-30% latency reduction)
   - Increase `MaxIdleConns` for high concurrency
   - Profile connection reuse patterns

3. **Add sync.Pool for JSON Buffers** (5-15% throughput improvement)
   - Profile allocation hotspots
   - Add pools for high-frequency allocations

**Estimated Total Improvement:** 20-50% performance gain

### Phase 2: Medium-Term (Medium Effort, Medium Impact)

4. ✅ **HTTP/2 Support** - **COMPLETED** (10-20% latency reduction for concurrent requests)
   - Always enabled, backwards compatible
   - Supports both HTTPS (ALPN) and HTTP (h2c) modes

5. **Zero-Copy Response Writing** (5-10% allocation reduction)
   - Identify high-frequency response paths
   - Implement buffer pooling

**Estimated Additional Improvement:** 15-30% latency reduction (HTTP/2 already implemented)

### Phase 3: Advanced (High Effort, Variable Impact)

6. **Custom HTTP Parser** (if request parsing becomes bottleneck)
   - Only if profiling shows >10% CPU time in parsing
   - Consider [valyala/fastjson](https://github.com/valyala/fastjson) or similar

7. **C Implementation for Specific Hot Paths** (if Phase 1-2 insufficient)
   - Profile to identify candidates
   - Batch operations to amortize CGO overhead
   - Measure actual improvement vs. complexity cost

---

## Benchmarking Plan

### Test Harness

Use the HTTP write performance test harness:

```bash
# Start server with the opt-in observability pprof listener
NORNICDB_PPROF_ENABLED=true \
NORNICDB_PPROF_LISTEN=127.0.0.1:9091 \
./nornicdb --http-port 7474

# Run benchmark
go run testing/benchmarks/http_write_latency/main.go \
    -url http://localhost:7474 \
    -pprof-url http://127.0.0.1:9091 \
    -database neo4j \
    -requests 10000 \
    -concurrency 50 \
    -pprof-enabled \
    -pprof-duration 60s
```

### Metrics to Track

1. **Throughput:** Requests per second
2. **Latency:** P50, P95, P99, P99.9 percentiles
3. **Allocations:** Memory allocations per request (via pprof)
4. **CPU Usage:** CPU time per request (via pprof)

### Success Criteria

- **Phase 1:** 20-50% improvement in throughput or latency
- **Phase 2:** Additional 25-50% latency reduction for concurrent requests
- **Phase 3:** Further improvements based on profiling data

---

## Conclusion

**Recommendation: Focus on Go-native optimizations (Option 1)**

1. **PGO is free performance** - Enable immediately
2. **sync.Pool is proven** - Add for hot paths identified by profiling
3. **Connection pooling is low-hanging fruit** - Tune existing settings
4. **HTTP/2 provides real benefits** - Implement if client support exists

**C implementation is NOT recommended** for HTTP server optimization:
- CGO overhead (20-100x) eliminates performance benefits
- Complexity and maintenance burden is high
- Go-native optimizations provide better ROI

**Exception:** Consider C only for specific CPU-intensive algorithms (e.g., cryptographic operations, compression) that can be batched to amortize CGO overhead.

---

## References

- [Go Performance Patterns](https://goperf.dev/)
- [Profile-Guided Optimization](https://go.dev/doc/pgo)
- [sync.Pool Deep Dive](https://victoriametrics.com/blog/go-sync-pool/)
- [CGO Performance Analysis](https://shane.ai/posts/cgo-performance-in-go1.21/)
- [HTTP Server Benchmarks](https://sharkbench.dev/web/go)
