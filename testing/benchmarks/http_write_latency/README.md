# HTTP Write Performance Benchmark

Benchmark harness for measuring HTTP write latency and throughput for NornicDB's HTTP API.

## Usage

### 1. Start NornicDB Server

```bash
# Start server with the opt-in observability pprof listener
NORNICDB_PPROF_ENABLED=true \
NORNICDB_PPROF_LISTEN=127.0.0.1:9091 \
./nornicdb --http-port 7474 --data-dir ./data/test
```

### 2. Run Benchmark

**Local (default database `testdb`):**
```bash
go run testing/benchmarks/http_write_latency/main.go \
    -url http://localhost:7474 \
    -pprof-url http://127.0.0.1:9091 \
    -database testdb \
    -requests 10000 \
    -concurrency 50 \
    -auth admin:admin
```

**Sit2 (Neo4j HTTP over base path; all writes to `testdb`):**
```bash
go run testing/benchmarks/http_write_latency/main.go \
    -url https://remote-url.com/remote-path \
    -database testdb \
    -requests 10000 \
    -concurrency 50 \
    -auth admin:password
```

**With pprof:**
```bash
go run testing/benchmarks/http_write_latency/main.go \
    -url http://localhost:7474 \
    -database testdb \
    -requests 10000 \
    -concurrency 50 \
    -auth admin:admin \
    -pprof-enabled \
    -pprof-duration 60s
```

### 3. View Results

The benchmark outputs:
- **Throughput:** Requests per second
- **Latency Statistics:** Min, P50, P95, P99, P99.9, Max, Average
- **Success Rate:** Percentage of successful requests
- **Detailed JSON:** Saved to `/tmp/http_write_bench_*.json`

### 4. Analyze Pprof Profile (if enabled)

```bash
# View CPU profile
go tool pprof http://127.0.0.1:9091/debug/pprof/profile?seconds=60

# View memory profile
go tool pprof http://127.0.0.1:9091/debug/pprof/heap

# Compare profiles
go tool pprof -base=before.pb.gz -http=:8080 after.pb.gz
```

## Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-url` | `http://localhost:7474` | NornicDB HTTP server URL |
| `-database` | `testdb` | Database name (URL path `/db/{database}/tx/commit`; use `testdb` to avoid default `nornic`) |
| `-requests` | `1000` | Total number of requests |
| `-concurrency` | `GOMAXPROCS` | Number of concurrent goroutines |
| `-auth` | `admin:admin` | Basic auth credentials (username:password) |
| `-pprof-enabled` | `false` | Enable pprof CPU profiling |
| `-pprof-url` | `http://127.0.0.1:9091` | pprof listener URL |
| `-pprof-duration` | `30s` | Duration for pprof CPU profile |
| `-warmup` | `10` | Number of warmup requests |
| `-verbose` | `false` | Print detailed per-request stats |

## Example Output

```
HTTP Write Performance Benchmark
================================
URL:           http://localhost:7474
Database:      neo4j
Requests:      10000
Concurrency:   50
Warmup:        10
Pprof enabled: true

Warming up...
Warmup complete.

Starting benchmark...
Starting pprof CPU profile (duration: 60s)...

Results
=======
Total duration:     15.234s
Successful:         10000
Errors:             0
Success rate:       100.00%
Throughput:         656.78 req/s
Total bytes:        2456789
Avg bytes/req:      245

Latency Statistics
------------------
Min:                2.345ms
P50 (median):       3.456ms
P95:                5.678ms
P99:                8.901ms
P99.9:              12.345ms
Max:                15.678ms
Average:            3.789ms

Detailed results saved to: /tmp/http_write_bench_1706342400.json
```

## Performance Optimization

See [HTTP Optimization Options](../../../../docs/performance/http-optimization-options.md) for:
- Profile-Guided Optimization (PGO)
- sync.Pool for zero-allocation hot paths
- Connection pooling tuning
- HTTP/2 support
- Comparison with C implementations

## Troubleshooting

### "Connection refused"
- Ensure NornicDB server is running
- Check the `-url` flag matches server address

### "401 Unauthorized"
- Verify `-auth` credentials match server configuration
- Check server authentication settings

### "Pprof endpoints not found"
- Set `NORNICDB_PPROF_ENABLED=true` on the server
- Use `-pprof-url` for the pprof listener, not the main HTTP API URL
- For Docker profiling, publish port `9091` and set `NORNICDB_PPROF_LISTEN=0.0.0.0:9091`

### Low throughput
- Increase `-concurrency` (but not beyond server capacity)
- Check server logs for errors
- Profile with `-pprof-enabled` to identify bottlenecks
