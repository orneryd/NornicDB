go test ./testing/e2e -tags=e2e -run TestEndpointParityAndBenchmark -count=1 -v


C:\Users\timot\Documents\GitHub\NornicDB>go test ./testing/e2e -tags=e2e -run TestEndpointParityAndBenchmark -count=1 -v
=== RUN   TestEndpointParityAndBenchmark
    endpoints_bench_test.go:92: default database: nornic
default database: nornic
    endpoints_bench_test.go:105: insert via bolt: n=1000 took=156.9711ms
insert via bolt: n=1000 took=156.9711ms
    endpoints_bench_test.go:111: verify via bolt: count=1000 took=1.2579ms
verify via bolt: count=1000 took=1.2579ms
    endpoints_bench_test.go:117: verify via neo4j http: count=1000 took=3.1912ms
verify via neo4j http: count=1000 took=3.1912ms
    endpoints_bench_test.go:123: verify via graphql: count=1000 took=2.7182ms
verify via graphql: count=1000 took=2.7182ms
    endpoints_bench_test.go:128: verify via /nornicdb/search: ok took=168.1639ms
verify via /nornicdb/search: ok took=168.1639ms
    endpoints_bench_test.go:140: qdrant grpc: detected addr=127.0.0.1:50619
qdrant grpc: detected addr=127.0.0.1:50619
    endpoints_bench_test.go:152: insert via qdrant grpc: col=bench_col_e2e dim=128 n=5000 took=899.0597ms
insert via qdrant grpc: col=bench_col_e2e dim=128 n=5000 took=899.0597ms
    endpoints_bench_test.go:160: verify qdrant points via bolt: label=QdrantPoint count=5000 took=10.3272ms
verify qdrant points via bolt: label=QdrantPoint count=5000 took=10.3272ms
    endpoints_bench_test.go:165: verify qdrant points via neo4j http: label=QdrantPoint count=5000 took=8.435ms
verify qdrant points via neo4j http: label=QdrantPoint count=5000 took=8.435ms
    endpoints_bench_test.go:181: benchmark config: concurrency=16 warmup=1s run=3s
benchmark config: concurrency=16 warmup=1s run=3s
=== RUN   TestEndpointParityAndBenchmark/bench:bolt
=== NAME  TestEndpointParityAndBenchmark
    endpoints_bench_test.go:202: TestEndpointParityAndBenchmark/bench:bolt: ops=82154 secs=33.000 ops/sec=2489.52 latency_ms: min=0.000 p50=0.532 p95=1.217 p99=2.651 max=16.360 mean=0.584
TestEndpointParityAndBenchmark/bench:bolt: ops=82154 secs=33.000 ops/sec=2489.52 latency_ms: min=0.000 p50=0.532 p95=1.217 p99=2.651 max=16.360 mean=0.584
=== RUN   TestEndpointParityAndBenchmark/bench:neo4j-http
=== NAME  TestEndpointParityAndBenchmark
    endpoints_bench_test.go:213: TestEndpointParityAndBenchmark/bench:neo4j-http: ops=12252 secs=3.001 ops/sec=4082.04 latency_ms: min=0.505 p50=3.489 p95=7.359 p99=10.048 max=47.738 mean=3.912
TestEndpointParityAndBenchmark/bench:neo4j-http: ops=12252 secs=3.001 ops/sec=4082.04 latency_ms: min=0.505 p50=3.489 p95=7.359 p99=10.048 max=47.738 mean=3.912
=== RUN   TestEndpointParityAndBenchmark/bench:graphql
=== NAME  TestEndpointParityAndBenchmark
    endpoints_bench_test.go:224: TestEndpointParityAndBenchmark/bench:graphql: ops=9605 secs=3.001 ops/sec=3200.12 latency_ms: min=0.593 p50=4.436 p95=9.394 p99=12.483 max=25.150 mean=4.988
TestEndpointParityAndBenchmark/bench:graphql: ops=9605 secs=3.001 ops/sec=3200.12 latency_ms: min=0.593 p50=4.436 p95=9.394 p99=12.483 max=25.150 mean=4.988
=== RUN   TestEndpointParityAndBenchmark/bench:nornic-search-rest
=== NAME  TestEndpointParityAndBenchmark
    endpoints_bench_test.go:234: TestEndpointParityAndBenchmark/bench:nornic-search-rest: ops=30898 secs=3.001 ops/sec=10295.85 latency_ms: min=0.000 p50=1.409 p95=2.894 p99=4.366 max=28.868 mean=1.553
TestEndpointParityAndBenchmark/bench:nornic-search-rest: ops=30898 secs=3.001 ops/sec=10295.85 latency_ms: min=0.000 p50=1.409 p95=2.894 p99=4.366 max=28.868 mean=1.553
=== RUN   TestEndpointParityAndBenchmark/bench:qdrant-grpc
=== NAME  TestEndpointParityAndBenchmark
    endpoints_bench_test.go:263: TestEndpointParityAndBenchmark/bench:qdrant-grpc: ops=88073 secs=3.003 ops/sec=29331.14 latency_ms: min=0.000 p50=0.526 p95=0.750 p99=1.162 max=464.280 mean=0.545
TestEndpointParityAndBenchmark/bench:qdrant-grpc: ops=88073 secs=3.003 ops/sec=29331.14 latency_ms: min=0.000 p50=0.526 p95=0.750 p99=1.162 max=464.280 mean=0.545
--- PASS: TestEndpointParityAndBenchmark (88.67s)
    --- PASS: TestEndpointParityAndBenchmark/bench:bolt (64.00s)
    --- PASS: TestEndpointParityAndBenchmark/bench:neo4j-http (4.00s)
    --- PASS: TestEndpointParityAndBenchmark/bench:graphql (4.00s)
    --- PASS: TestEndpointParityAndBenchmark/bench:nornic-search-rest (4.00s)
    --- PASS: TestEndpointParityAndBenchmark/bench:qdrant-grpc (4.01s)
PASS
ok      github.com/orneryd/nornicdb/testing/e2e 89.632s

C:\Users\timot\Documents\GitHub\NornicDB>