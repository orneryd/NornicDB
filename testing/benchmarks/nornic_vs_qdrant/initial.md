```bash
$ '/bin/zsh -lc '"'"'go run ./testing/benchmarks/nornic_vs_qdrant -qdrant-grpc-addr 127.0.0.1:6334 -points 20000 -dim 128 -k 10 -concurrency 32 -seconds 10 -warmup-seconds 2'"'"
Loading dataset into both targets: synthetic points=20000 dim=128 col=bench_col
Running benchmark: NornicDB (Qdrant gRPC compat)
NornicDB: ops=100059 secs=10.003 ops/sec=10003.21
NornicDB: latency ms: min=0.559 p50=2.442 p95=7.509 p99=16.086 max=72.994 mean=3.187
Running benchmark: Qdrant (local)
Qdrant: ops=64383 secs=10.002 ops/sec=6437.07
Qdrant: latency ms: min=0.543 p50=4.561 p95=8.221 p99=10.961 max=22.624 mean=4.957
CSV appended: /Users/c815719/src/NornicDB/testing/benchmarks/nornic_vs_qdrant/results.csv
```

```bash
$ '/bin/zsh -lc '"'"'go run ./testing/benchmarks/nornic_vs_qdrant -qdrant-grpc-addr 127.0.0.1:6334 -points 100000 -dim 128 -k 10 -concurrency 32 -seconds 10 -warmup-seconds 2 -load-timeout-seconds 900'"'"
Loading dataset into both targets: synthetic points=100000 dim=128 col=bench_col
Running benchmark: NornicDB (Qdrant gRPC compat)
NornicDB: ops=49996 secs=10.006 ops/sec=4996.76
NornicDB: latency ms: min=0.625 p50=4.466 p95=17.078 p99=34.184 max=174.294 mean=6.384
Running benchmark: Qdrant (local)
Qdrant: ops=22593 secs=10.002 ops/sec=2258.85
Qdrant: latency ms: min=1.515 p50=12.726 p95=22.315 p99=29.599 max=132.509 mean=14.107
CSV appended: /Users/c815719/src/NornicDB/testing/benchmarks/nornic_vs_qdrant/results.csv
```
