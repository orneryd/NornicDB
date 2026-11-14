# Cross-record layout checkpoint

Dependency: public Badger fork `adf08d82495becf400f8d1afa1369cb3d993ab64`.

`NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED` is the single default-off switch for property encodings and new SST column layouts. Existing table codecs remain readable after disabling it. Node property tokens, node metadata and compact relationship/adjacency fields supply pure byte-span annotations; the fork retains the authoritative keys, versions and values.

Focused checks on the combined source passed:

- Windows: `go test -tags noui,nolocalllm ./pkg/storage -run '^TestExperimental' -count=1 -timeout 10m` (the opt-in documentation fixture skips without its environment variables).
- Windows: `go test -race -tags noui,nolocalllm ./pkg/config ./pkg/storage -run '^TestExperimental' -count=1 -timeout 10m`.
- Linux laboratory: `GOWORK=off go test -tags noui,nolocalllm ./pkg/storage -run '^TestExperimental|^TestWAL.*(Binding|Token)' -count=1 -timeout 10m`. This includes pre-identity WAL upgrades whose read-only Badger fixture is unsupported on Windows.
- Explicit documentation fixture: 273 documents, full-value/projection/reopen checks passed. One run recorded 4,956,932 file bytes off and 3,649,146 on. Corpus SHA-256: `b9b0b4d332baaab9c68f209239fb1dfe288db444dd9e5a111842738c632366ba`. This changed corpus and single run do not establish improvement over the earlier three-run report or isolate transposition from Zstd.

The shared-flag regression inspects the persisted manifest after public Nornic writes: ordinary tables with the flag off, experimental tables with it on, followed by exact reads after reopening with it off. Field-span checks preserve property bytes, relationship endpoints and NaN bits. A malformed compact edge length initially panicked; the canonical header decoder now checks the unsigned length before converting it to an index, and the regression passes.

Full suites were stopped and remain deferred at the maintainer's request while implementation continues. Northwind measurements in REPORT.md predate this layout. New Northwind comparisons, ordinary-Zstd controls and memory/CPU qualification remain outstanding. Whole-block reconstruction is still required; independently addressable compressed columns and physical partial reads are not implemented.
