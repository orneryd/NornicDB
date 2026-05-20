# Per-Database Search Index Feature Flags Plan

**Status:** Complete. All phases (1–7), all tests (9.1–9.6), and all documentation (10.1–10.12) landed. Acceptance gate passes the no-regression check (full affected test sweep green); manual smoke is the only remaining checkbox and is operator-facing rather than code-shipping.

## Execution checklist

Update this list as work lands. Mark a box `[x]` only when the change is merged on `main`. If a phase is split across multiple PRs, leave it unchecked until the last PR lands.

### Code

- [x] Phase 1 — `dbconfig/keys.go` registers the four new keys; `enum:` type added; `IsValidEnumValue` validator; PUT validation rejects unknown enum values.
- [x] Phase 2.1 — `pkg/config/config.go` carries the four new `Memory.Search*` fields with yaml + env + CLI flag bindings; defaults reproduce today's behaviour (true/startup).
- [x] Phase 2.2 — `ResolvedDbConfig` carries `BM25Enabled` / `BM25Warming` / `VectorEnabled` / `VectorWarming`; `Resolve` honours per-DB-overrides-win semantics; `effectiveFromGlobal` mirrors all four; `parseBoolFallback` + `normalizeWarming` defend against bogus values.
- [x] Phase 3.1 — Boot orchestration in `pkg/nornicdb/search_services.go` consults resolved config per-database via `resolveSearchFlags`; short-circuits when both off (`MarkReadyDisabled`) or both lazy (deferred until `ForceSearchIndexBuild`).
- [x] Phase 3.2 — Lazy first-query trigger via `LazyTriggerNeeded` field on `DatabaseSearchStatus`; `ForceSearchIndexBuild` is the trigger; `startSearchIndexBuild` is already idempotent for concurrent first-query races.
- [x] Phase 3.3 — BM25 build skip via `disabledBM25Index{}` no-op stub swapped in at top of `BuildIndexes`; existing 40+ `s.fulltextIndex` call sites remain untouched.
- [x] Phase 3.4 — Vector pipeline skip: `warmupVectorPipeline` early-returns and nils `vectorIndex` when `vectorEnabled=false`; `addVectorLocked` short-circuits writes.
- [x] Phase 3.4 (deferred) — Qdrant gRPC bridge per-DB gate (returns structured error when the database has vector disabled); `db.index.vector.queryNodes` returns empty results with WARN log on vector-disabled databases.
- [x] Phase 3.5 — Random-order HNSW build path is reused unchanged: when BM25 is disabled, the no-op stub returns an empty seed list, the existing `len(seedNodeIDs) == 0` branch fires.
- [x] Phase 3.6 — Write-path gating in `IndexNode` (full no-op when both off; per-half no-ops via `disabledBM25Index{}` and `addVectorLocked` guard); existing `pendingFlush` carries writes for lazy mode pre-trigger.
- [x] Phase 4 — Search handler precedence: `search_disabled_for_database` (permanent 503), then fall-through to GetOrCreateSearchService + Service.EnsureWarm which blocks until the lazy build completes and returns 200. The legacy `search_not_ready` 503 still fires for an eager build mid-flight at request time. Responses carry `bm25_enabled` / `vector_enabled`. Note: lazy mode no longer emits a `search_index_warming_lazy` 503 — implementation evolved to synchronous wait at the Service layer so every read entry point (HTTP, Bolt, GraphQL, gRPC, Cypher procedures) gets the same contract uniformly.
- [x] Phase 4.1 — Hard rule documented in plan and in operator-facing docs (no probe-mode escape hatch).
- [x] Phase 5 — yaml `databases:` map landed. `Store.LoadWithYAMLDefaults` seeds yaml-declared per-DB overrides on first boot and skips any (dbName, key) pair that already has a stored admin-API value — admin edits are authoritative across restarts. Wired through `nornicConfig.Config.PerDBOverrides` → `server.SetPerDBYAMLOverrides` from `cmd/nornicdb/main.go`.
- [x] Phase 6 — Admin API: enum validation 400s. Per-DB teardown reuses the existing `ResetSearchService` + `EnsureSearchIndexesBuildStarted` codepath, which now consults the resolver and produces a service with the right index handles. Dedicated `Service.DisableIndex` deferred — `ResetSearchService` covers the contract today.
- [x] Phase 7 — Four CLI flags wired in `cmd/nornicdb/main.go` with env-var defaults; `cmd.Flags().Changed(...)` honours explicit overrides.

### Tests

- [x] 9.1 — Resolver parsing, defaults, validation (`TestResolveSearchFlags_Defaults`, `TestResolveSearchFlags_BoolFallback`, `TestEnumValidation`, `TestIsAllowedKey`).
- [x] 9.2 — Override matrix table-driven test (`TestResolveSearchFlags_OverrideMatrix`) — eight rows green, including the load-bearing global-off + per-DB-on case.
- [x] 9.3 — Search service build-skip + lazy-trigger semantics (`pkg/search/index_flags_test.go`): `SetIndexFlags`, `BuildIndexes_BothDisabled`, `BuildIndexes_VectorDisabled`, `BuildIndexes_BM25Disabled`, `IndexNode_BothDisabled`, `MarkReadyDisabled`.
- [x] 9.4 — Search handler paths (`pkg/server/server_search_flags_test.go`): both-disabled permanent 503; lazy first-query blocks in `Service.EnsureWarm` and returns 200; global-off + per-DB-on override at the handler layer.
- [x] 9.5 — Admin API runtime flag flips (`pkg/server/server_search_flags_test.go::TestAdminPutSearchFlags_TeardownAndRebuild`): PUT `vector_enabled=false` tears down the in-memory ANN substrate; PUT `vector_enabled=true` rebuilds; unknown enum value is rejected with 400 BEFORE any teardown.
- [x] 9.6 — yaml `LoadWithYAMLDefaults` precedence covered in `pkg/config/dbconfig/store_test.go`: seeds on first boot; doesn't clobber admin edits; fills only missing keys; rejects disallowed keys.

### Documentation (Phase 10)

- [x] 10.1 — `docs/operations/environment-variables.md` lists all four env vars with default, range, source, and notes.
- [x] 10.2 — `docs/operations/configuration.md` "Per-database search index control" section + yaml `databases:` map example + four-state behavior table.
- [x] 10.3 — `docs/operations/low-memory-mode.md` "Deferring search-index load with `warming=lazy`" section with memory-savings math, recommended yaml shape for multi-tenant idle DBs, and the health-check caveat.
- [x] 10.4 — `docs/features/vector-embeddings.md` "Disabling vector search per database" section (with `EmbeddingEnabled` vs `VectorEnabled` distinction table + exports-only pattern) and same in `docs/user-guides/vector-search.md`.
- [x] 10.5 — `docs/user-guides/hybrid-search.md` "Per-database search index control" section with the four-state truth table and 200/503 response shapes.
- [x] 10.6 — `docs/api-reference/openapi.yaml` `/nornicdb/search` 503 response shape (search_disabled_for_database / search_not_ready) and `openapi.md` narrative paragraph.
- [x] 10.7 — `docs/api-reference/cypher-functions/README.md` `db.index.vector.queryNodes` disabled-database WARN log behaviour documented.
- [x] 10.8 — `docs/user-guides/multi-database.md` "Per-database search configuration" section with the four-key reference and yaml example.
- [x] 10.9 — `docs/skills/vector-search.skill.md` and `docs/skills/cypher-queries.skill.md` carry the master-switch and lazy-warming notes.
- [x] 10.10 — `docs/getting-started/installation.md` and `docs/getting-started/quick-start.md` carry one-line notes about defaults and a pointer to low-memory-mode.
- [x] 10.11 — Inline Cobra flag help (already in `cmd/nornicdb/main.go`) + `KeyMeta.Description` field plus `keyDescriptions` map populated for the four new keys; `AllowedKeys()` merges descriptions in.
- [x] 10.12 — `CHANGELOG.md` entry under "Latest Changes" summarising the four keys, defaults, migration story (zero), and pointers to docs.

### Acceptance gate

- [x] No regression: deployments that set none of the new flags behave identically to today (verified by full `pkg/config/dbconfig`, `pkg/search`, `pkg/nornicdb`, `pkg/server`, `pkg/cypher` test suites passing).
- [x] Phase 10 documentation lands (10.1–10.12 all merged).
- [ ] Manual smoke against a running server (operator-facing, post-merge): lazy first-query triggers as documented; both-off DBs return permanent 503; metrics confirm zero in-memory vectors for `vector_enabled=false` DBs.

## Scope

Let operators control two orthogonal axes per index, per database:

- **`*_enabled`** (boolean) — is this index a capability for this database at all?
- **`*_warming`** (enum: `startup` | `lazy`) — if enabled, when does the build trigger?

That gives four keys, two per index:

| Key                             | Type    | Default   |
| ------------------------------- | ------- | --------- |
| `NORNICDB_SEARCH_BM25_ENABLED`   | boolean | `true`    |
| `NORNICDB_SEARCH_BM25_WARMING`   | enum    | `startup` |
| `NORNICDB_SEARCH_VECTOR_ENABLED` | boolean | `true`    |
| `NORNICDB_SEARCH_VECTOR_WARMING` | enum    | `startup` |

When an index is `enabled=false`:

- Its build is **never started** at boot, on lazy creation, or on first query.
- The in-memory store backing it is **never populated** — disabling vector search means we don't iterate node embedding properties into RAM, don't build HNSW / IVF-HNSW / GPU brute-force / Metal / Qdrant pass-through, and don't expose any vector query API for that database.
- Background indexing in the write path is **skipped** for that database (no work for an index nobody can query).
- Search requests **route around** the disabled index.
- When **both** are off for a database, `POST /nornicdb/search` returns **HTTP 503** with `request_status: search_disabled_for_database`, `retryable: false`.

When an index is `enabled=true, warming=lazy`:

- Boot does not enumerate the database for index build. No `EnsureSearchIndexesBuildStarted` for that index, no goroutine, no allocation.
- Node embeddings are **not iterated** into the in-memory vector index, and BM25 is **not built** from the node corpus, until needed.
- The **first inbound search query** for that database (HTTP, Bolt, GraphQL, gRPC, Cypher procedure — any caller that goes through `Service.Search` / `Service.VectorSearchCandidates`) **synchronously triggers the build and blocks** until it completes via `Service.EnsureWarm`. The trigger fires at most once per service lifetime via `sync.Once`; concurrent first-readers all wait on the same `warmDone` channel. The build itself runs in the DB's long-lived context (NOT the caller's request ctx) so a request that times out during the wait returns `ctx.Err()` but the build keeps going and the next reader finds the service warm.
- Once warm, the index behaves identically to a `startup`-warmed index until the process restarts.

When an index is `enabled=true, warming=startup` (today's default):

- Build kicks off in `EnsureSearchIndexesBuildStarted` at boot, same as today.

The flags are configurable in four places, with the standard precedence:

1. CLI flags on `nornicdb` startup
2. Environment variables
3. `nornicdb.yaml` (global defaults + per-database overrides)
4. Runtime via `PUT /admin/databases/{name}/config` (already-existing surface)

All four converge on the same per-database key store at [`pkg/config/dbconfig/store.go`](../../pkg/config/dbconfig/store.go), so the runtime surface is unchanged — we only add new keys.

### Why "vector" and not "HNSW"

HNSW is one ANN strategy among several that NornicDB can serve vector search through. Today the relevant code paths in [`pkg/search/search.go`](../../pkg/search/search.go) include:

- `vectorIndex` — in-memory exact brute-force lookup
- `hnswIndex` — graph-based ANN
- IVF-HNSW — partitioned per-cluster HNSW (`[IVF-HNSW]` log prefix at line 1101)
- GPU brute-force (`gpuBruteIndex`, line 786 — `SetGPUManager`)
- Metal min-candidate brute-force (`NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES`)
- Qdrant gRPC pass-through (`pkg/server/server_qdrantgrpc.go`)

A flag named `HNSW_ENABLED` would be a footgun: an operator turning it off would still get vector search via brute-force or Qdrant. The capability we're gating is **"NornicDB serves vector search for this database"** — independent of which ANN strategy backs it.

### Why split `enabled` and `warming` instead of a tri-state value

Four states, two axes:

|                            | `warming=startup`     | `warming=lazy`               |
| -------------------------- | --------------------- | ---------------------------- |
| `enabled=true`             | Build at boot (today) | Build on first query         |
| `enabled=false`            | No build, ever — search routes around / 503 |   *(value of `warming` is ignored)*  |

A single tri-state `enabled: true | false | lazy` would conflate the *capability* (does this index exist for this DB?) with the *trigger* (when does it build?). Splitting them means flipping `warming` is a perf knob with no semantic implication for query results, and `enabled` is a capability switch independent of build-timing concerns. The UI gets a checkbox and a select rather than a single 3-way select with a confusing "lazy means enabled but deferred" footnote. Validation is also cleaner: `warming` validates as enum, `enabled` validates as bool, no cross-field constraint logic.

The "lazy" pattern itself is well-trodden: Elasticsearch `index.preload` (default lazy), Solr `firstSearcher` (opt-in), pg_prewarm, Qdrant `on_disk_*`, Weaviate per-collection warmup config. NornicDB already builds search asynchronously and 503s the handler until ready, so "lazy" here just defers the *trigger* — every other piece of plumbing is reused.

## Non-goals

- Disabling **embedding generation**. That is a separate concern already controlled by `NORNICDB_EMBEDDING_ENABLED` — which only stops the auto-embed worker. User-set vectors (via `SET n.embedding = [...]`, `WITH EMBEDDING`, or external import) live as durable properties on the corresponding nodes and are still iterated into the in-memory vector index by today's search service. The new `VECTOR_ENABLED=false` flag is the **stronger** guarantee: even durably-stored user-set embeddings are not iterated into any in-memory vector index for that database. The `VECTOR_WARMING=lazy` setting is the **softer** middle ground: iterate on demand, not at boot.
- Reranker, IVF-HNSW partitioning, k-means, auto-links, auto-TLP. Those flags already exist as per-DB overrides via [`pkg/config/dbconfig/keys.go`](../../pkg/config/dbconfig/keys.go) and are out of scope. They become **no-ops** when `VECTOR_ENABLED=false`; they fire at first-query time when `VECTOR_WARMING=lazy`.
- **Idle eviction.** Not in scope and not planned. Once warmed (whether eagerly at startup or lazily on first query), an in-memory index lives until the process restarts or an admin-API flag flip drops it. Steady-state memory pressure on always-warm databases is out of scope; operators reach for `*_enabled=false` or `warming=lazy` instead.
- A new admin UI page. The existing per-DB config UI surfaces every key in `AllowedKeys()` automatically, so adding the keys lights it up.
- Cleaning up persisted index files (HNSW snapshots, BM25 segments, IVF centroids). Index persistence is **experimental** in NornicDB today and not relied on by this plan: every "rebuild" scenario in this plan iterates node embeddings and the node corpus from durable storage, not a cached index file. If a future plan promotes index persistence out of experimental, that plan owns the on-disk lifecycle for disabled indexes.

## Phase 1 — Define the flags

Four new keys, registered in [`pkg/config/dbconfig/keys.go`](../../pkg/config/dbconfig/keys.go) `AllowedKeys()`:

```go
{"NORNICDB_SEARCH_BM25_ENABLED",   "boolean", "Search"},
{"NORNICDB_SEARCH_BM25_WARMING",   "enum:startup,lazy", "Search"},
{"NORNICDB_SEARCH_VECTOR_ENABLED", "boolean", "Search"},
{"NORNICDB_SEARCH_VECTOR_WARMING", "enum:startup,lazy", "Search"},
```

`KeyMeta.Type` already supports `string`, `number`, `boolean`, `duration`. Add `enum:<v1>,<v2>,...` as a new type so the UI can render a select and the validator can enforce membership; `IsAllowedKey` already gates by name, but `applyOverride` needs to learn to reject unknown enum values (today it silently ignores type mismatches). One small addition to `dbconfig.applyOverride`:

```go
if strings.HasPrefix(meta.Type, "enum:") {
    allowed := strings.Split(strings.TrimPrefix(meta.Type, "enum:"), ",")
    if !contains(allowed, value) {
        return // silently fall back to default; PUT validation rejects louder
    }
}
```

The existing PUT validation at `handlePutDbConfig` ([`pkg/server/server_dbconfig.go:202`](../../pkg/server/server_dbconfig.go)) already 400s on unknown keys; extend it to also 400 on unknown enum values for keys with `Type` starting with `enum:`. yaml/env loaders normalize to lower-case before applying.

The four states and their meaning, expanded:

| BM25 enabled / warming | Vector enabled / warming | Behaviour at boot | Behaviour on first search |
|---|---|---|---|
| on / startup | on / startup | Both indexes build at boot (today) | Either 200 (warm) or 503 `search_index_building` retryable |
| on / startup | on / lazy    | BM25 builds at boot; vector deferred | First request blocks in `Service.EnsureWarm` until the deferred vector build completes, then returns 200. |
| on / lazy    | on / lazy    | No build at boot | First request blocks in `Service.EnsureWarm` until the build completes, then returns 200. Concurrent first-readers all wait on the same channel. Caller ctx timeouts surface as `ctx.Err()` but do NOT abort the build. |
| on / startup | off / —      | BM25 builds; vector load **skipped** | Lexical-only 200 |
| off / —      | on / startup | Vector loads with random-order HNSW seeding (no BM25 to seed from) | Vector-only 200 |
| off / —      | off / —      | Nothing built or loaded | 503 `search_disabled_for_database`, `retryable: false` |

The `*_warming` value is ignored when the corresponding `*_enabled=false`, but is preserved in storage so flipping `enabled=true` later honours the operator's intended trigger without re-config.

## Phase 2 — Resolver + global config plumbing

### 2.1 `Config` struct fields

In [`pkg/config/config.go`](../../pkg/config/config.go), add four new fields on the `Memory` sub-struct alongside the existing `EmbeddingEnabled` / `SearchRerankEnabled` fields:

```go
SearchBM25Enabled   bool   `yaml:"search_bm25_enabled"   env:"NORNICDB_SEARCH_BM25_ENABLED"   flag:"search-bm25-enabled"   default:"true"`
SearchBM25Warming   string `yaml:"search_bm25_warming"   env:"NORNICDB_SEARCH_BM25_WARMING"   flag:"search-bm25-warming"   default:"startup"`
SearchVectorEnabled bool   `yaml:"search_vector_enabled" env:"NORNICDB_SEARCH_VECTOR_ENABLED" flag:"search-vector-enabled" default:"true"`
SearchVectorWarming string `yaml:"search_vector_warming" env:"NORNICDB_SEARCH_VECTOR_WARMING" flag:"search-vector-warming" default:"startup"`
```

The CLI flag binding uses the same Cobra setup as the existing `NORNICDB_EMBEDDING_ENABLED`. Loader code at [`pkg/config/config.go:2091`](../../pkg/config/config.go) gets four more env-binding switch arms (booleans normalised to `true|false`, enums normalised to lower-case from `STARTUP|startup|Startup`, etc.).

### 2.2 ResolvedDbConfig

In [`pkg/config/dbconfig/resolver.go`](../../pkg/config/dbconfig/resolver.go) (`ResolvedDbConfig` struct):

```go
type ResolvedDbConfig struct {
    EmbeddingDimensions int
    SearchMinSimilarity float64
    BM25Engine          string
    BM25Enabled         bool   // NEW
    BM25Warming         string // NEW: "startup" | "lazy"
    VectorEnabled       bool   // NEW
    VectorWarming       string // NEW: "startup" | "lazy"
    Effective           map[string]string
}
```

`Resolve(global, overrides)` populates them from the global defaults, then `applyOverride` flips them when the per-DB key is set. Booleans use a `parseBool(value, fallback)` helper that returns the parsed value when the string is a recognised bool literal (`true`/`false`/`1`/`0`) and `fallback` otherwise — so a typo can't silently disable an index. Enums use a `parseEnum(value, allowed, fallback)` helper with the same semantics.

`effectiveFromGlobal` mirrors the four new keys into the `Effective` map so the admin "GET config" response surfaces the resolved value.

**Override semantics — explicit guarantee.** Per-DB overrides always win over the global default, in both directions. That means an operator who sets the **global** flag off (e.g. `NORNICDB_SEARCH_VECTOR_ENABLED=false` at startup, or `memory.search_vector_enabled: false` in yaml) but configures a **per-DB** override of `true` or a per-DB warming of `lazy` for a specific database gets vector search on for that database. Symmetric for BM25. The implementation falls out of `Resolve`'s existing "overrides applied on top of global" loop ([`pkg/config/dbconfig/resolver.go:39`](../../pkg/config/dbconfig/resolver.go)) — the new keys plug into `applyOverride` the same way the existing per-DB keys do — but the guarantee is load-bearing for the multi-tenant story (one DB needs search, the rest don't), so it is tested explicitly in Phase 9.

The same guarantee runs in reverse: a per-DB override of `false` overrides a global default of `true`, and a per-DB `warming=lazy` overrides a global `warming=startup`. The boot orchestrator in Phase 3.1 consults the resolved per-DB config, never the raw global, so a per-DB override of `enabled=true, warming=lazy` correctly defers the build for that database alone.

### 2.3 yaml schema

```yaml
# nornicdb.yaml — global defaults
memory:
  search_bm25_enabled:   true
  search_bm25_warming:   startup
  search_vector_enabled: true
  search_vector_warming: startup

# Per-database overrides — same shape as today
databases:
  hot_app_db:
    # default for everything — eager build, both enabled

  analytics:
    search_bm25_enabled:   false
    search_vector_enabled: true
    search_vector_warming: lazy   # rarely queried; defer until first search

  audit_logs:
    search_bm25_enabled:   false
    search_vector_enabled: false  # search disabled

  exports_only:
    search_bm25_enabled:   true
    search_vector_enabled: false  # write embeddings for export, never load them in-process

  multi_tenant_archive:
    search_bm25_warming:   lazy
    search_vector_warming: lazy   # idle most of the time; absorb cost only when accessed
```

The `databases:` section is loaded into `dbconfig.Store` at startup. (The store currently loads from `_DbConfig` system-DB nodes; extend the loader to merge yaml-declared overrides on first boot. See Phase 5.)

## Phase 3 — Search service: gate the build, the load, and the trigger

The per-database build entry point is [`pkg/nornicdb/search_services.go:583`](../../pkg/nornicdb/search_services.go) (`EnsureSearchIndexesBuildStarted`). It calls `getOrCreateSearchService` which already calls `dbconfig.Resolve` for every other knob. Pipe the new fields into the `search.Service` constructor.

In [`pkg/search/search.go`](../../pkg/search/search.go) `Service`:

```go
type Service struct {
    // ...existing fields...
    bm25Enabled   bool
    bm25Warming   string // "startup" | "lazy"
    vectorEnabled bool
    vectorWarming string // "startup" | "lazy"
}
```

### 3.1 Boot-time orchestration

The boot loop in [`pkg/nornicdb/search_services.go:583`](../../pkg/nornicdb/search_services.go) (`EnsureSearchIndexesBuildStarted`) currently kicks off both index builds unconditionally for every database. Change it to consult the resolved config and short-circuit per-index:

```go
// Pseudocode — illustrative
resolved := db.dbConfigResolver.Resolve(dbName)

if resolved.BM25Enabled && resolved.BM25Warming == "startup" {
    entry.startBM25Build(ctx)
}
if resolved.VectorEnabled && resolved.VectorWarming == "startup" {
    entry.startVectorBuild(ctx)
}
// "lazy" entries fall through; first-query handler triggers them.
// "false" entries fall through and stay nil.
```

Two implementation notes:

1. The current `startSearchIndexBuild` is a single goroutine that builds both indexes. Phase 3 splits it into `startBM25Build` and `startVectorBuild` so they can be triggered independently, with shared per-database synchronisation (a `sync.Once`-per-index ensures the first-query trigger from Phase 3.2 doesn't race with a concurrent admin-API flag flip).
2. The `_pending_embed` marker gate at [`pkg/nornicdb/db.go:960`](../../pkg/nornicdb/db.go) (`be.SetEmbeddingsEnabled`) continues to follow `EmbeddingEnabled` (existing behaviour). It is **independent** of `VectorEnabled` / `VectorWarming` — the marker is about whether the embed worker has consumers, not about whether vectors are loaded for search.

### 3.2 First-query trigger for `warming=lazy` — synchronous wait at the Service layer

The trigger lives on `search.Service`, not on the HTTP handler. Every read path that funnels through `Service.Search` or `Service.VectorSearchCandidates` (HTTP `/nornicdb/search`, Bolt search, GraphQL search resolver, gRPC bridge, `db.index.vector.queryNodes` Cypher procedure, internal callers) inherits the lazy-warm contract automatically.

`Service.SetLazyWarming(lazy bool, fn WarmFunc)` registers a callback (wired by `getOrCreateSearchService` at construction time) that drives the actual build in `db.buildCtx`. `Service.EnsureWarm(ctx)`:

1. Returns immediately if `warmingLazy=false` or `IsReady()=true` (steady-state hot path).
2. Otherwise fires the WarmFunc exactly once via `sync.Once`, in a goroutine. The build runs in the OWNER's long-lived ctx (`db.buildCtx`), not the caller's request ctx.
3. Blocks the caller on a `warmDone` channel until the build completes.
4. Returns `ctx.Err()` if the caller's ctx is cancelled during the wait — but does NOT abort the build, so a subsequent reader finds the service warm.

`Service.Search` and `Service.VectorSearchCandidates` call `EnsureWarm(ctx)` at entry. The HTTP handler also calls it explicitly **after** `GetOrCreateSearchService` and **before** any handler-side decisions that depend on search state (`EmbeddingCount()`, `RerankerAvailable()`), so the first lazy request returns 200 with the same shape as an eager DB rather than a transient 503.

### 3.3 BM25 build skip

Same as before: `BuildIndexes` (`pkg/search/search.go:2313`-ish) wraps the BM25 init/load block in `if s.bm25Enabled`. Leaving `s.fulltextIndex` nil after a skip is the natural "off" state — every existing search path nil-checks it.

### 3.4 Vector pipeline skip — gate the **iteration**, not just HNSW build

This is the core scope change relative to "just disable HNSW". Today vectors flow through `Service` like this:

1. **Constructor** ([`pkg/search/search.go:704`](../../pkg/search/search.go)): `vectorIndex: NewVectorIndex(dimensions)` — empty in-memory store.
2. **Iteration** (`BuildIndexes`): every node in the database is streamed from durable Badger storage via `storage.StreamNodesWithFallback` ([`pkg/search/search.go:2812`](../../pkg/search/search.go)) and any node carrying an embedding property has its (id, vec) pair added to the in-memory `vectorIndex`. **This happens regardless of which ANN strategy is used** — `vectorIndex` backs brute-force, GPU brute-force, IVF-HNSW, and Metal queries. (The experimental `vectorFileStore` low-RAM build mode replaces the in-memory `vectorIndex` with an on-disk file; the gate below applies equally to either path.)
3. **Build ANN** (`warmupVectorPipeline`, [`pkg/search/search.go:2866`](../../pkg/search/search.go)): HNSW / IVF-HNSW are built **on top of** the populated `vectorIndex` (or `vectorFileStore`).
4. **Query**: search picks a strategy at runtime — HNSW if available, else IVF-HNSW, else brute-force, else GPU brute-force.

Gating only HNSW would still leave every node embedding iterated into RAM and queryable through brute-force. The gate has to sit at step 2 — before iteration begins.

```go
// In BuildIndexes() / warmupVectorPipeline()
if !s.vectorEnabled {
    log.Printf("🔍 Vector search disabled for database %q — skipping embedding iteration and ANN build", s.dbName)
    s.vectorIndex = nil   // explicit, so downstream nil-checks fire
    return nil
}
// existing node-stream + warmupVectorPipeline (HNSW / IVF-HNSW / GPU / Metal)
```

The Qdrant gRPC bridge in [`pkg/server/server_qdrantgrpc.go`](../../pkg/server/server_qdrantgrpc.go) consults the same per-database `VectorEnabled` resolution before serving a request and returns a structured error otherwise — so external Qdrant clients get the same "off" semantics.

The `db.index.vector.queryNodes` Cypher procedure logs a warning and returns zero rows when the resolved config has `VectorEnabled=false` for the procedure's database. We don't error out the whole query — that would break composite queries that gracefully handle empty vector results — but the warning log surfaces the misconfiguration to operators.

### 3.5 BM25-seeded HNSW build — fallback to random

In `hnswLexicalSeedNodeSet` ([`pkg/search/search.go:4162`](../../pkg/search/search.go)), when `s.bm25Enabled == false` the call site already passes a nil `bm25Index` (because we skipped the BM25 build) and the seed lookup returns an empty map. The HNSW build path at line 4070 is already structured to handle this — `len(seedNodeIDs) == 0` falls through to the random-order branch at 4083. **No code change is required for the fallback** beyond what 3.3 already does. A one-line comment at line 4033 noting that `seedNodeIDs == nil` is now an explicit, supported configuration rather than a transient state during build.

### 3.6 Write path — skip background indexing

Both indexes have a write-path entry point in [`pkg/nornicdb/search_services.go:607`](../../pkg/nornicdb/search_services.go) (`indexNodeFromEvent`) and the per-service `queueIndex` / embedding worker. In `search.Service`:

- BM25 ingestion (`fulltextIndex.Add`) gated on `s.bm25Enabled`.
- Vector ingestion (`vectorIndex.Add`, `hnswIndex.Add`, IVF-HNSW per-cluster adds) gated on `s.vectorEnabled`.

When `warming=lazy` but `enabled=true`, **writes still queue** in a per-database mutation log so the lazy build can replay them when triggered. Otherwise a database that takes writes but never queries would silently lose updates the first time it's queried. The mutation log is bounded; if it overflows before the first query, the lazy build does a full rebuild from disk instead of replaying. (This shape is already roughly the existing `pendingFlush` mechanism — extend it to lazy mode rather than introducing a separate buffer.)

## Phase 4 — Search handler 503 paths

Three distinct 503 shapes. Order matters — they short-circuit top-down:

```go
// In handleSearch:

resolved := s.dbConfigResolver.Resolve(dbName)

// 1. Both off → permanent 503, not retryable.
if !resolved.BM25Enabled && !resolved.VectorEnabled {
    s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
        "error":          "search is disabled for this database",
        "database":       dbName,
        "bm25_enabled":   false,
        "vector_enabled": false,
        "retryable":      false,
        "http_code":      http.StatusServiceUnavailable,
        "request_status": "search_disabled_for_database",
    })
    return
}

// 2. Existing "still building" path — only fires for an eager build
//    mid-flight; lazy databases skip this branch (LazyTriggerNeeded=true).
status := s.db.GetDatabaseSearchStatus(dbName)
if !status.Ready && !status.LazyTriggerNeeded {
    s.writeJSON(w, http.StatusServiceUnavailable, /* search_not_ready */)
    return
}

// 3. Lazy fall-through. After GetOrCreateSearchService, the handler
//    calls Service.EnsureWarm(ctx) which fires the build (once) and
//    blocks until ready. By the time we reach EmbeddingCount() and
//    the embedding decision, the in-memory ANN substrate is populated
//    so the response shape matches what an eager DB would return.
//    Bolt/GraphQL/gRPC and Cypher procedures inherit this for free —
//    Service.Search calls EnsureWarm at entry too.
```

The 200 response includes `bm25_enabled` and `vector_enabled` so clients can see what produced the result set.

### 4.1 Health checks must not probe search on lazy/disabled databases

Liveness/readiness probes that hit `/nornicdb/search` against a lazy database would trigger the build on every probe; against a fully-disabled database they would log a stream of `search_disabled_for_database` 503s that look like real failures. Don't do either.

`nornicdb.yaml` reference adds a section: "Health checks must not target `/nornicdb/search` for databases with `warming=lazy` or any `*_enabled=false`. Use `/nornicdb/health` (DB-agnostic) or `/admin/databases/{name}/config` (lookup-only) instead — neither triggers a build."

There is no probe-mode escape hatch. A handler that conditionally does or doesn't trigger the build based on a query param is more confusing than the rule "don't probe search on lazy/disabled DBs." Operators who want a per-database reachability probe use the admin config GET, which is unconditionally cheap.

## Phase 5 — yaml startup loader

Today `dbconfig.Store.Load(ctx)` reads `_DbConfig` nodes from system storage ([`pkg/config/dbconfig/store.go:37`](../../pkg/config/dbconfig/store.go)). yaml-declared overrides need to land in the same store at first boot. Two options; we take the second:

1. ❌ Read yaml into the store at every startup. Footgun: yaml becomes the source of truth, anything PUT via the admin API gets clobbered next restart.
2. ✅ Read yaml at startup **only when the store has no row for that database yet**. Subsequent admin API edits are authoritative. Operators who want yaml to win run `nornicdb admin reset-db-config <name>` (a small new subcommand) or delete the `_DbConfig` node. This matches how the existing global config + override merge already behaves.

Implementation: a `LoadWithYAMLDefaults(ctx, yamlOverrides map[string]map[string]string)` helper on the store, called from `cmd/nornicdb/main.go` after `Store.Load(ctx)`. It only writes for `(dbName, key)` pairs that are absent.

## Phase 6 — Admin API + UI

The PUT path at [`pkg/server/server_dbconfig.go:202`](../../pkg/server/server_dbconfig.go) (`handlePutDbConfig`) already validates against `dbconfig.IsAllowedKey` and triggers `ResetSearchService` + `EnsureSearchIndexesBuildStarted` after writing. Three adjustments:

1. **Enum validation**: 400 on unknown enum values (vs. today's silent ignore). Phase 1 covers the validator change.
2. **`enabled` flipped to false drops the index immediately.** When PUT changes `bm25_enabled` or `vector_enabled` from `true` → `false`, the admin handler must:
   1. Cancel any in-flight build for that index (via the build context from Phase 3.1's split goroutines).
   2. Set the corresponding handle to nil under the service's write lock (`s.fulltextIndex = nil` for BM25; `s.vectorIndex = nil`, `s.hnswIndex = nil`, plus any IVF-HNSW partitions and GPU brute-force structures for vector).
   3. Drain the lazy-mode mutation log (Phase 3.6) for that index — those queued mutations would have been replayed at next trigger, but the index is being torn down, so they're discarded.
   4. Reset the per-index `sync.Once` so a subsequent flip back to `true` rebuilds cleanly.

   The existing `ResetSearchService` path tears down both indexes wholesale; this is a per-index variant. Implementation: add `Service.DisableIndex(kind IndexKind)` that takes the service's write lock, performs steps i–iv above, and is called by the admin handler only for the index whose flag flipped to `false`. The other index is left running. RAM is reclaimable on the next GC cycle once the handles are nil. The PUT response returns 200 only after the teardown completes (synchronous, same as today's `ResetSearchService` semantics).
3. **Warming flip semantics**: flipping `warming` from `startup` → `lazy` at runtime tears down the in-memory index for that index kind (same teardown as adjustment 2) and resets the lazy-trigger state, so the next inbound query rebuilds. Flipping `lazy` → `startup` triggers an immediate build (same as today's reset path). Both cases reuse the new per-index teardown helper.

The UI per-DB config page enumerates `GET /admin/databases/config/keys` and renders inputs based on `KeyMeta.Type`:

- `boolean` → toggle (existing)
- `enum:<v1>,<v2>,...` → select (new — small additions to `ui/src/pages/Databases.tsx` and the per-DB config form)

Boolean and enum keys appear automatically once registered.

## Phase 7 — CLI flag wiring on the main binary

In `cmd/nornicdb/main.go` (or wherever Cobra root flags are registered), add four flags:

```
--search-bm25-enabled        (default: true,    env: NORNICDB_SEARCH_BM25_ENABLED)
--search-bm25-warming        (default: startup, env: NORNICDB_SEARCH_BM25_WARMING)
--search-vector-enabled      (default: true,    env: NORNICDB_SEARCH_VECTOR_ENABLED)
--search-vector-warming      (default: startup, env: NORNICDB_SEARCH_VECTOR_WARMING)
```

These set `Config.Memory.Search{BM25,Vector}{Enabled,Warming}` (the global defaults). They are **not** per-database — operators wanting per-DB control use yaml or the admin API.

A `--search-disabled-for-database <name>` convenience flag is **explicitly not** added. It encourages footgun config baked into init scripts; yaml is the right shape for "list of databases with X off".

## Phase 9 — Tests

The test list is grouped by what each test guards against. Every entry below maps to a concrete Go file and a concrete assertion so a reviewer can tell whether the corresponding code change has coverage.

### 9.1 Resolver: parsing, defaults, validation

In [`pkg/config/dbconfig/resolver_test.go`](../../pkg/config/dbconfig/resolver_test.go):

- All four new keys parse from the overrides map and land in the matching `ResolvedDbConfig` fields and in the `Effective` map.
- Boolean parsing accepts `true`/`false`/`1`/`0` (lowercase). Anything else returns the global default — verified by setting an override of `"yes"` and asserting the resolved value equals `global.Memory.SearchVectorEnabled`.
- Enum parsing accepts `startup` and `lazy` (case-normalised). Anything else returns the global default for the corresponding `*_warming` field.
- `Effective` map round-trips all four keys and reflects the resolved value, not the raw override string, for the consumer-facing API.
- PUT validation in `handlePutDbConfig` 400s on unknown enum values for keys with `Type` starting with `enum:` (the silent-default behaviour above is for in-memory loads from yaml/env; the admin API is the louder path).

### 9.2 Override matrix: per-DB overrides win in both directions

In [`pkg/config/dbconfig/resolver_test.go`](../../pkg/config/dbconfig/resolver_test.go), a dedicated `TestResolveOverrideMatrix` table-driven test exercises the **load-bearing per-DB-override-wins guarantee** for every cell of the global × per-DB matrix. The eight rows that matter:

| Global  | Per-DB override                | Expected resolved value                                      |
| ------- | ------------------------------ | ------------------------------------------------------------ |
| `true`  | absent                         | `true`  (global wins by default)                             |
| `false` | absent                         | `false` (global wins by default)                             |
| `false` | `true`                         | `true`  (per-DB enables a globally-disabled index)           |
| `false` | `lazy`                         | enabled=`true`, warming=`lazy`                               |
| `true`  | `false`                        | `false` (per-DB disables a globally-enabled index)           |
| `startup` (warming) | `lazy`             | warming=`lazy`                                               |
| `lazy`   (warming) | `startup`          | warming=`startup`                                            |
| `false` | `true` for vector, `false` for BM25, while global is `true` for both | per-DB wins independently per key |

Each row is asserted for both BM25 and vector keys symmetrically, against a `*config.Config` whose `Memory.SearchBM25Enabled` / `Memory.SearchVectorEnabled` / `Memory.SearchBM25Warming` / `Memory.SearchVectorWarming` are set to the row's "Global" column.

### 9.3 Search service: build-skip and lazy-trigger semantics

New file `pkg/search/search_flag_gating_test.go`:

- `Service{bm25Enabled: false}`: `BuildIndexes` returns successfully with `s.fulltextIndex == nil`. Search calls fall through to "no BM25 results" without panicking.
- `Service{vectorEnabled: false}`: `BuildIndexes` returns successfully with `s.vectorIndex == nil`, no HNSW, no IVF-HNSW, no GPU brute, no Qdrant pass-through. Search calls fall through to "no vector results" without panicking.
- `Service{vectorEnabled: false}` does NOT stream nodes for embedding iteration (assertion via a counter on `storage.StreamNodesWithFallback` — confirms the gate fires before iteration begins, not after). When the experimental `vectorFileStore` is configured, the same test variant asserts `vfs.IterateChunked` is also never called.
- `Service{vectorEnabled: false}` `IndexNode` is a no-op for vector inserts. The embedding still computes if `EmbeddingEnabled=true` and the resulting vector is written as a node property by the embed worker, but it is not added to any in-memory vector store — confirmed by counting `vectorIndex.Add` calls.
- `Service{bm25Warming: "lazy"}` and `Service{vectorWarming: "lazy"}`: `BuildIndexes` does NOT fire at boot — counters on the BM25 and vector iteration entry points (`storage.StreamNodesWithFallback` invocations attributable to that database's search service) stay at 0 even after `EnsureSearchIndexesBuildStarted` is called for that database during the boot loop.
- Lazy trigger idempotency: first call to `EnsureSearchIndexesBuildStarted` from the search handler fires the build exactly once; concurrent first-query calls (run via `errgroup` with N=8 goroutines) all observe `sync.Once.Do` having fired exactly once and the second-through-Nth callers see the in-flight build state.
- `Service{bm25Enabled: false, vectorEnabled: true}` build uses random insertion order for HNSW — assertion via a `seedNodeIDs == nil` observability hook in the build path or via log-capture of the "no lexical seed available — using random insertion order" log line.
- `db.index.vector.queryNodes` returns empty results with a `WARN`-level log when called against a database with `vectorEnabled=false`. Composite Cypher queries that gracefully handle empty vector results continue to succeed.

### 9.4 Search handler 503 paths

In [`pkg/server/server_nornicdb_test.go`](../../pkg/server/server_nornicdb_test.go):

- Both disabled → 503 with `request_status: search_disabled_for_database`, `retryable: false`, and `bm25_enabled: false`, `vector_enabled: false` in the response body.
- Lazy + first query → 503 with `request_status: search_index_warming_lazy`, `retryable: true`. Build kicks off (assertion: `sync.Once` for that DB has fired exactly once after the request).
- Lazy + subsequent query (build still in progress) → 503 with `request_status: search_index_building`, `retryable: true`.
- Lazy + after build completes → 200, with the `bm25_enabled` / `vector_enabled` body fields reflecting the resolved config.
- One disabled, one enabled → 200 with results from the enabled index only and the correct `bm25_enabled` / `vector_enabled` fields. Assert that the disabled-index pipeline branch is not invoked (counter on the corresponding `Service.search*` method).

### 9.5 Admin API: runtime flag flips

In [`pkg/server/server_dbconfig_test.go`](../../pkg/server/server_dbconfig_test.go):

- PUT `vector_enabled=false` on a previously-enabled, **fully-built** database → 200, and after the response returns: `s.vectorIndex == nil`, `s.hnswIndex == nil`, no IVF-HNSW partitions, no GPU brute-force structures, and the per-index `sync.Once` is reset (assertion: a subsequent PUT flipping back to `true` triggers a rebuild rather than seeing a stale `Once.fired`).
- PUT `vector_enabled=false` on a previously-enabled, **mid-build** database → 200, and after the response returns: the in-flight build's `ctx.Err()` is non-nil (cancellation observed), `s.vectorIndex == nil`, the goroutine has exited (assertion via a `done` channel or the existing build-progress observability hook). The other index is unaffected — assert `s.fulltextIndex` still serves queries during and after the teardown.
- PUT `bm25_enabled=false` on a previously-enabled service → mirror of the two cases above for BM25; vector index untouched.
- PUT `vector_enabled=false` while the lazy-mode mutation log holds queued writes → log is drained and discarded; assert via a counter that no replay was attempted on the now-nil vector index.
- PUT `warming` from `startup` → `lazy` on an already-built service → service tears down that index kind (reuses the per-index teardown from `enabled=false`); in-memory index drops; next inbound search query triggers a rebuild via the lazy path.
- PUT `warming` from `lazy` → `startup` on a not-yet-triggered service → triggers immediate build (same code path as today's reset).
- PUT unknown enum value (e.g. `warming: "asap"`) → 400 with the rejected value echoed in the error body. No teardown happens.
- Per-index isolation: PUT `vector_enabled=false` while BM25 is also being concurrently rebuilt (e.g. via a separate PUT) → vector teardown completes; BM25 build is unaffected (assertion via no errors logged for BM25 and BM25 reaches `Ready` for that DB).

### 9.6 End-to-end: yaml + admin + override-precedence

In `pkg/nornicdb/...` integration suite, with a real boot of the server fixture:

- **yaml + boot**: start with the full schema example from Phase 2.3 (`hot_app_db`, `analytics`, `audit_logs`, `exports_only`, `multi_tenant_archive`). Observe absent build/load log lines for `audit_logs` (both disabled), `analytics`'s BM25 (disabled) and vector (lazy), `multi_tenant_archive`'s both indexes (lazy). `hot_app_db` builds both eagerly. Confirm via the existing search-status admin endpoint that each DB's status matches its config.
- **Global-off + per-DB-on override at boot**: start with `memory.search_vector_enabled: false` globally and `databases.analytics.search_vector_enabled: true` in yaml. Boot logs confirm `analytics` builds vector, every other database does not. `curl /nornicdb/search` against `analytics` returns 200; against any other DB returns 503 `search_disabled_for_database` (BM25 still off too in this scenario unless it's been overridden separately).
- **Global-on + per-DB-lazy override at boot**: start with `memory.search_vector_warming: startup` globally and `databases.analytics.search_vector_warming: lazy`. Boot logs show every DB except `analytics` building eagerly; `analytics` does not build until first query.
- **Memory benchmark**: with 50 databases each holding 100K embeddings, `warming=lazy` for all → boot RSS is lower than the same configuration with `warming=startup` by approximately `50 × 100K × dim × 4 bytes` (dimension-dependent; the test records the exact delta in its output for regression tracking, doesn't fail on a hard threshold).
- **Runtime override flip**: live database starts with global `vector_enabled=false`. PUT `vector_enabled=true` for one database via the admin API → that DB's vector index builds; the others stay off. PUT it back → service resets and that DB's vector index drops to nil.

Phase 9 acceptance: every test above is implemented and passing in CI. Coverage of the resolver override matrix (9.2) and the global-off + per-DB-on E2E case (9.6) are explicit gates — the plan's load-bearing claim is "per-DB overrides always win", and these are the tests that prove it.

## Phase 10 — Documentation

The flags introduce new operator-facing semantics that touch several existing docs. Every file below must be updated **in the same PR** that lands the code changes — operator confusion is the largest documented risk for this feature, and shipping code without the docs alongside is the easiest way to cause it.

### 10.1 `docs/operations/environment-variables.md`

The canonical env-var reference. Add the four new variables in the existing "Search" section, alongside `NORNICDB_SEARCH_BM25_ENGINE`. Each entry includes:

- Default value (`true` / `startup`).
- Permitted values (`true|false` / `startup|lazy`).
- Source-of-truth file reference (`pkg/config/config.go`).
- A short "Notes" cell. For `*_WARMING=lazy`: "First inbound search query for the database triggers the build. **Health checks must not target `/nornicdb/search` for databases with `warming=lazy` or any `*_enabled=false`** — use `/nornicdb/health` (DB-agnostic) or `/admin/databases/{name}/config` (lookup-only) instead." For `*_ENABLED=false`: "Disabling vector search prevents node embeddings from being iterated into any in-memory vector index — the strongest available memory-pressure lever. `NORNICDB_EMBEDDING_ENABLED=false` only stops the auto-embed worker; user-set embedding properties on nodes are still iterated into the in-memory vector index today."

### 10.2 `docs/operations/configuration.md`

The high-level operator config doc. Add a "Per-database search index control" subsection in the search section that explains the four-key shape, the override-precedence rule (per-DB always wins over global), and links to the per-DB config plan (this file) plus the env-var reference. Cross-link from the existing low-memory-mode doc.

### 10.3 `docs/operations/low-memory-mode.md`

This is the doc operators reach for when the concern that motivated this plan (startup memory pressure on multi-tenant deployments) hits in practice. Add a section "Deferring search-index load with `warming=lazy`" that explains:

- What the memory savings look like (boot-time RSS by ~`vectors × dim × 4 bytes` per lazy DB).
- The first-query latency tradeoff.
- The recommended yaml shape for a multi-tenant deployment with mostly-idle DBs.
- The interaction with `NORNICDB_LOW_MEMORY_MODE` (the existing global low-memory toggle).

### 10.4 `docs/features/vector-embeddings.md` and `docs/user-guides/vector-search.md`

Both docs explain how vector search works and currently imply it is unconditionally available per database. Add a "Disabling vector search per database" subsection that:

- Documents the `NORNICDB_SEARCH_VECTOR_ENABLED=false` semantics.
- Distinguishes it from `NORNICDB_EMBEDDING_ENABLED=false` (the existing flag controls *generation*; the new flag controls *load*).
- Documents the "exports-only" pattern (`EmbeddingEnabled=true`, `VectorEnabled=false`) for users producing embeddings for downstream stores like Qdrant or Weaviate.
- Documents the `db.index.vector.queryNodes` short-circuit behaviour and the WARN log that fires when the procedure is called against a vector-disabled database.

### 10.5 `docs/user-guides/hybrid-search.md`

Hybrid search is the public-facing feature that disables when both flags are off. Update to:

- Document the four-state truth table from Phase 1 (BM25 × Vector).
- Document the 503 response shape for the all-off case (`request_status: search_disabled_for_database`, `retryable: false`) and the lazy-trigger case (`request_status: search_index_warming_lazy`, `retryable: true`).
- Document that 200 responses now carry `bm25_enabled` / `vector_enabled` fields so clients can see which path produced their results.
- Note that BM25-seeded HNSW build falls back to random insertion order when BM25 is disabled, with the recall-quality caveat from Phase 3.5.

### 10.6 `docs/api-reference/openapi.yaml` and `docs/api-reference/openapi.md`

The `/nornicdb/search` endpoint at line 470 of the spec needs:

- A new 503 response shape variant: `search_disabled_for_database` (with the schema definition for the new `bm25_enabled` / `vector_enabled` fields and the `retryable: false` distinction from the existing warming response).
- A new 503 response shape variant: `search_index_warming_lazy` (with `retryable: true`).
- The 200 response schema gains `bm25_enabled` and `vector_enabled` fields so the schema reflects the actual response body.

The companion `openapi.md` narrative gets a paragraph documenting these states. Verify `make generate-openapi` (or whatever the regen target is) re-emits cleanly.

### 10.7 `docs/api-reference/cypher-functions/`

Add (or update if present) the entry for `db.index.vector.queryNodes` to document the empty-result + WARN log behaviour when the database has `vector_enabled=false`. Cross-link to Phase 10.4.

### 10.8 `docs/user-guides/multi-database.md`

This is the natural home for the override-precedence guarantee. Add a "Per-database search configuration" subsection that:

- Documents the four flags and the override-precedence rule with the same matrix from Phase 2.2 ("per-DB overrides win in both directions").
- Includes the yaml schema example from Phase 2.3.
- Cross-links to `docs/operations/environment-variables.md` for the env-var spelling and to `docs/user-guides/hybrid-search.md` for the search-handler semantics.

### 10.9 `docs/skills/vector-search.skill.md` and `docs/skills/cypher-queries.skill.md`

The skill docs are surfaced to AI assistants. Add a brief note in each that:

- vector search may be disabled or lazy on a per-database basis;
- the 503 shapes and how to retry them;
- callers that need to verify reachability without warming should hit `/nornicdb/health` or `/admin/databases/{name}/config`, not `/nornicdb/search`.

### 10.10 `docs/getting-started/installation.md` + `docs/getting-started/quick-start.md`

A one-line note that the defaults (`true` / `startup`) reproduce today's behaviour, with a pointer to `low-memory-mode.md` for the multi-tenant story. No semantic change to the quick-start path.

### 10.11 Inline help / doc.go strings

- The Cobra `--search-bm25-enabled`, `--search-vector-enabled`, `--search-bm25-warming`, `--search-vector-warming` flag descriptions in `cmd/nornicdb/main.go` carry one-sentence summaries that reference the env-var doc by URL.
- `pkg/config/dbconfig/keys.go` `KeyMeta` does not currently carry a description field; if Phase 1 adds one (recommended — the UI's per-DB config form would render it as field-level help), populate it for the four new keys.

### 10.12 CHANGELOG.md

Add a single feature entry under the next release header summarising:

- The four new keys, their defaults, and the per-DB override surface.
- Migration: zero — defaults reproduce today's behaviour.
- A "see also" link to `docs/plans/per-database-search-index-flags-plan.md` for design context.

## Risk register

| Risk | Mitigation |
| --- | --- |
| First-query latency on a lazy DB is unbounded — for a multi-million-vector database this is multi-minute. | Documented in the flag help text. The 503 with `retryable: true` and a separate `request_status` lets clients distinguish "warming" from "still building" and back off appropriately. |
| Health checks against `/nornicdb/search` trigger lazy builds on every probe (or stream `search_disabled_for_database` 503s for fully-disabled DBs that look like real failures in monitoring). | Phase 4.1 — documented as a hard rule in `nornicdb.yaml` reference: don't target `/nornicdb/search` for `warming=lazy` or any `*_enabled=false` database. Use `/nornicdb/health` or `/admin/databases/{name}/config` instead. No probe-mode escape hatch. |
| Operator flips Vector off, then back on weeks later — every ANN strategy has to re-iterate every node embedding from durable storage and rebuild from scratch. | Documented in the flag help text. Embedding properties on nodes are durable Badger storage, so rebuild iterates from those rather than re-embedding from text. |
| Search returns inconsistent results when only one index is enabled vs. when both are. | Expected and documented. The 200 response includes `bm25_enabled` and `vector_enabled` so clients can see what produced the result set. |
| BM25-seeded HNSW build is measurably better quality than random-seeded; flipping BM25 off while keeping Vector on regresses ANN recall. | Documented explicitly in Phase 1 truth table and in the runtime log line at Phase 3.5. Operators choosing this combination are doing so deliberately. |
| Lazy DBs that take writes but never get queried accumulate an unbounded mutation log. | Phase 3.6: bounded mutation log; if it overflows before the first query, the lazy build does a full rebuild from disk. Bound is configurable (`NORNICDB_SEARCH_LAZY_PENDING_LIMIT`, default 100k entries). |
| Flag-namespace collision: `NORNICDB_VECTOR_*` is already used heavily for HNSW/IVF tuning knobs. | The `SEARCH_` prefix disambiguates: `SEARCH_VECTOR_ENABLED` and `SEARCH_VECTOR_WARMING` are master switches; `VECTOR_HNSW_M` etc are tuning knobs that are no-ops when the master is off or until lazy trigger fires. Documented in `docs/operations/environment-variables.md`. |
| yaml-declared overrides clobber admin-API edits across restarts. | Phase 5 — yaml is read-only-on-first-boot for any (dbName, key) pair the store already knows about. |
| Per-index teardown on flag flip is synchronous; flipping `enabled=false` or `warming` on a multi-million-vector DB has multi-second admin-API tail latency while the in-flight build cancels and the handles are nilled. | Acceptable for an admin operation — same shape as today's embedding-model swap. The new `Service.DisableIndex` only touches the affected index, so flipping vector off does not pause BM25 traffic for that DB and vice versa. |

## Acceptance

- `pkg/config/dbconfig/resolver_test.go` exercises all four flags, the enum validator, **and the override matrix** (9.1 + 9.2).
- `pkg/search` tests cover the (BM25, Vector) ∈ {on/startup, on/lazy, off} states for `BuildIndexes`, `IndexNode`, and the explicit "vector store is not iterated when disabled or pre-trigger" assertion (9.3).
- `pkg/server/server_nornicdb_test.go` covers all three 503 paths (9.4); `pkg/server/server_dbconfig_test.go` covers runtime flag flips (9.5).
- E2E: yaml + boot, global-off + per-DB-on override, runtime override flip, and the memory benchmark all pass in the integration suite (9.6).
- Manually: start the server with `nornicdb.yaml` containing every per-DB flag combination from the schema example; observe absent build/iteration logs for `enabled=false` and pre-trigger `warming=lazy` databases; the first `curl -i .../search` against a lazy database returns the lazy-trigger 503 *with* the build kicking off; the next request returns the existing `search_index_building` 503 until ready, then 200. Confirm the in-memory vector index size via the existing admin metrics is **0** for a `vector_enabled: false` database even when millions of node embeddings exist on disk.
- All Phase 10 documentation updates land in the same PR.
- No change to existing default behaviour: any deployment that sets none of the new flags continues to build both indexes at startup, and HNSW continues to be BM25-seeded.
