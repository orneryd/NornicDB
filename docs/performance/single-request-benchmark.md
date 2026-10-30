# Single Request Performance Benchmark

Last Updated: February 2026

HTTP write performance with a single sequential request (best case). Configuration: HTTP/2, no auth, Go 1.26.0.

## Test Configuration

- **Requests:** 50,000
- **Concurrency:** 1 (sequential, no concurrency)
- **Database:** nornic
- **Warmup:** 10 requests
- **HTTP/2:** Enabled (h2c cleartext mode)
- **Authentication:** Disabled (`-auth ""`)
- **Optimizations:** Executor caching + Search service reuse enabled
- **Purpose:** Measure best-case latency without concurrency overhead

## Results

### Performance Metrics

| Metric | Value |
|--------|-------|
| **Total Duration** | 5.61 seconds |
| **Throughput** | **8,917.61 req/s** |
| **Success Rate** | 100% (50,000/50,000) |
| **Min Latency** | **77.8µs (0.08 ms)** |
| **P50 (median)** | **109.0µs (0.11 ms)** |
| **P95** | **142.4µs (0.14 ms)** |
| **P99** | **165.5µs (0.17 ms)** |
| **P99.9** | **279.0µs (0.28 ms)** |
| **Max Latency** | **1.35 ms** |
| **Average Latency** | **112.0µs (0.11 ms)** |

## Analysis

### Best-Case Performance

With **1 concurrent request**, we achieve:
- **Sub-millisecond latency** for 99% of requests (P99: 0.17ms)
- **Ultra-low average latency**: 112.0µs (0.11ms)
- **Fastest possible request**: 77.8µs (0.08ms minimum)
- **Consistent performance**: Very tight latency distribution
- **High throughput**: 8,917.61 req/s even with sequential requests

### Latency Distribution

The latency distribution shows excellent consistency:
- **P50-P95 spread**: Only 33.4µs (109.0µs → 142.4µs)
- **P95-P99 spread**: Only 23.2µs (142.4µs → 165.5µs)
- **99.9th percentile**: Still under 0.3ms (279.0µs)
- **Maximum**: 1.35ms (likely due to occasional GC or system activity)

### Comparison: 1 vs 100 Concurrent Requests

| Metric | 1 Concurrent | 100 Concurrent | Difference | Speedup |
|--------|--------------|---------------------------|-----------|--------|
| **Throughput** | 8,917.61 req/s | **37,450.73 req/s** | **+320%** | **4.2x** |
| **Average Latency** | 0.11 ms | **2.67 ms** | +2,327% | **24.3x slower** |
| **P50 Latency** | 0.11 ms | **2.62 ms** | +2,282% | **23.8x slower** |
| **P99 Latency** | 0.17 ms | **4.12 ms** | +2,324% | **24.2x slower** |
| **P99.9 Latency** | 0.28 ms | **9.16 ms** | +3,171% | **32.7x slower** |

### Key Insights

1. **Sequential Processing is Fast**
   - Single requests achieve sub-millisecond latency (P99: 0.17ms)
   - **Fastest possible request: 77.8µs (0.08ms)**
   - No concurrency overhead or contention
   - Ideal for low-latency, single-user scenarios

2. **Concurrency Increases Throughput**
   - 100 concurrent requests: **4.2x higher throughput** (37,451 req/s)
   - Trade-off: Higher latency due to queuing and resource contention
   - Better for high-throughput, multi-user scenarios
   - **Memory optimizations improve both throughput and latency**

3. **Latency vs Throughput Trade-off**
   - **1 concurrent**: Best latency (0.11ms avg), lower throughput (8.9K req/s)
   - **100 concurrent**: Higher latency (2.67ms avg), best throughput (37.5K req/s)
   - Choose based on use case requirements

4. **Consistent Performance**
   - Even at P99.9, single requests stay under 0.3ms (279.0µs)
   - Very predictable latency for sequential workloads
   - Minimal variance in response times
   - **Memory optimizations maintain consistency**

## Use Cases

### Single Request (1 Concurrent) - Best For:
- ✅ **Low-latency APIs** - When response time is critical
- ✅ **Single-user applications** - Desktop apps, CLI tools
- ✅ **Real-time systems** - Where sub-millisecond latency matters
- ✅ **Interactive queries** - User-facing applications
- ✅ **Testing baseline** - Understanding best-case performance

### High Concurrency (100 Concurrent) - Best For:
- ✅ **High-throughput APIs** - When total requests/sec matters
- ✅ **Multi-user applications** - Web services, microservices
- ✅ **Batch processing** - Parallel data ingestion
- ✅ **Load testing** - Understanding system limits
- ✅ **Production workloads** - Real-world multi-user scenarios

## Conclusion

**Single request performance demonstrates:**
- ✅ **Sub-millisecond latency** for 99% of requests (P99: 0.17ms)
- ✅ **Fastest possible request: 77.8µs (0.08ms)**
- ✅ **Ultra-consistent** response times (low variance)
- ✅ **Excellent baseline** for understanding system overhead
- ✅ **8,917.61 req/s** throughput even with sequential processing
- ✅ **Memory optimizations** maintain performance (1.4 KB/request overhead)

**The system achieves excellent single-request latency (0.11ms average) while maintaining high throughput under concurrency (37.5K req/s with 100 concurrent requests).**

This shows NornicDB can handle both:
- **Low-latency use cases** (single requests: <0.3ms P99.9, 77.8µs minimum)
- **High-throughput use cases** (concurrent requests: 37.5K req/s with optimizations)
