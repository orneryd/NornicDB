# Running Qdrant from Source

## Quick Command

```bash
cd ~/src/NornicDB
~/src/qdrant/target/release/qdrant \
  --config-path testing/benchmarks/nornic_vs_qdrant/qdrant-bench.yaml
```

## What This Does

- Starts Qdrant on `127.0.0.1:6333` (REST) and `127.0.0.1:6334` (gRPC)
- Uses isolated storage in `testing/benchmarks/nornic_vs_qdrant/qdrant_storage`
- Disables telemetry
- Ready for benchmarks

## Verify It's Running

```bash
# Check REST API
curl http://localhost:6333/health

# Should return: {"status":"ok"}
```

## Run Benchmark

Once Qdrant is running, in another terminal:

```bash
cd ~/src/NornicDB
go run ./testing/benchmarks/nornic_vs_qdrant \
  -qdrant-grpc-addr 127.0.0.1:6334 \
  -points 20000 -dim 128 -k 10 \
  -concurrency 32 -seconds 10 -warmup-seconds 2
```

## Alternative: Run Without Config

```bash
# Use default config (from ~/src/qdrant/config/config.yaml)
~/src/qdrant/target/release/qdrant

# Or with environment variables
QDRANT__SERVICE__HTTP_PORT=6333 \
QDRANT__SERVICE__GRPC_PORT=6334 \
~/src/qdrant/target/release/qdrant
```

## Stop Qdrant

Press `Ctrl+C` or:

```bash
pkill qdrant
```

