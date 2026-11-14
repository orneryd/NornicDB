# Experimental property storage

This branch integrates the Badger development fork into NornicDB's existing tokenized-property owner. It does not replace the graph engine, transaction model, property-name dictionaries, constraints or indexes.

## Enable

The feature is off by default. Set this before starting the server:

```sh
NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED=true
```

The runtime API is `config.SetExperimentalPropertyStorageEnabled(bool)`. `config.GetFeatureStatus()` reports its state. This single flag controls both property-value encoding and the cross-record SST layout, including adjacency fields. It defaults to false. Disabling it does not rewrite existing records or tables; newly built tables use ordinary compression. This branch can read its experimental records and tables with the flag off. No special build tag is required.

Use disposable test databases for this experiment. Stock release binaries cannot decode experimental physical property extensions. Turning the write flag off is not a downgrade conversion. Logical snapshots expand values to ordinary typed MessagePack; native Badger backups retain physical encoding.

## What is integrated

- Existing tokenized node and edge properties remain the outer representation. Property dictionary allocation still commits atomically with each body.
- Large property values can be independently Zstd-compressed through the fork. Small values remain unchanged. Compression is accepted only when the full extension saves at least 12.5%.
- Eligible `[]int64` values also try the fork's exact constant/FOR/raw column codec. The smaller accepted representation wins. Integer widths and API value semantics are preserved through the existing strict property decoder.
- Compressed values use reserved internal MessagePack extension 60. Individual values remain skippable in ordinary point projections, projected scans, hinted filters and WAL token rebinding. Compressed string equality uses the decoded string, preserving the existing scan contract.
- Float payloads retain their MessagePack representation and IEEE bits; no float-to-integer numeric casts or quantization are introduced.
- Physical property compression is bounded to 64 MiB per value; larger properties retain ordinary encoding.
- Newly built SST blocks try byte-column layouts across records, grouping property tokens and relationship/adjacency fields. The fork compares raw, ordinary Zstd and column-transposed Zstd payloads and stores the smallest complete payload. Record bytes, keys, timestamps and checksums reconstruct exactly before the existing reader sees them. Value-log pointers remain opaque.

The SST layout reconstructs a whole block; it does not yet provide independently addressable compressed property columns or physical partial reads. Badger still retrieves the enclosing Item value before Nornic selects properties. Decode currently uses a temporary Go buffer before copying into Badger's block allocation; memory and CPU costs need qualification. The directory prototype and test-only patched/delta candidates are not silently promoted into a new production format.

## Dependency

The branch pins `github.com/dgraph-io/badger/v4` to the public `orneryd/nornic-badger/v4` repository at commit `adf08d82495becf400f8d1afa1369cb3d993ab64`. The upstream module path is retained. The repository name is provisional pending the maintainer's naming choice.

The fork is public, so normal dependency downloads do not require GitHub credentials or GOPRIVATE. The local Linux laboratory primes Go's module cache with the checksum-verified archive of that exact fork revision and runs with GOWORK=off. This avoids copying host credentials into the container and lets source-analysis tests create independent temporary modules.

## Validation

The integration tests cover public node/edge writes reaching the fork, selected and omitted properties, compressed string filtering, mixed old/new records, byte-exact float special values, logical snapshots, native backup/restore, reopen and reads with the feature disabled. A deliberately damaged omitted compressed payload verifies that projection skips decompression while a selected corrupt payload remains an error. Configuration tests exercise default-off and explicit startup environment values in fresh processes.

Run focused checks:

```sh
go test -tags noui ./pkg/config ./pkg/storage -run '^TestExperimental' -count=1
go test -race -tags noui ./pkg/config ./pkg/storage -run '^TestExperimental' -count=1
```

The opt-in documentation fixture writes each Markdown document as a node with a stable path hash, original text, low-range integer metrics and independent 4 KiB byte data. It compares flag-off and flag-on writes, verifies projections/full values/reopen, and counts all closed-store files:

```sh
NORNIC_STORAGE_DOCS_ROOT=docs \
NORNIC_STORAGE_DOCS_RESULTS=/tmp/nornic-storage-docs.json \
go test -tags noui ./pkg/storage -run '^TestExperimentalDocsStorage$' -count=1
```

For Northwind, use `scripts/benchmark_northwind_vs_neo4j.sh --nornic-only` with identical parameters and fresh data directories. Use `NORNIC_ASYNC_WRITES=0` for durable commits in both runs. Disable power sampling on Linux with `SKIP_POWERMETRICS=1`. A server build without local inference libraries can use `GOFLAGS=-tags=noui,nolocalllm`. Run both `nornic` and `antlr` parser modes. Graph-only Northwind primarily exercises small scalar values; large codec-only savings are not a prediction of Northwind speedups.
