# `nornicdb-admin database import` Plan

**Status:** Proposed (single-pass, storage-aligned review 2026-06-01)

## Scope

Ship a `nornicdb-admin` CLI binary whose `database import` subcommand mirrors `neo4j-admin database import` as closely as the license and trademark allow. Same file formats, same flag names where they don't collide with the Neo4j brand, same operational model: offline import into a stopped or empty target database, single-pass index population at the end.

The initial implementation is `database import full`. The `incremental` command name can be reserved in the CLI so the command tree matches Neo4j's shape, but it must return a clear "not implemented" error until a separate plan defines update/delete semantics, uniqueness-constraint requirements, and staged merge behavior. Neo4j incremental import is substantially more than "full import into a non-empty store", and NornicDB should not imply support until that model is designed.

This plan covers:

1. The CLI surface (`cmd/nornicdb-admin/`).
2. The Neo4j-compatible CSV header dialect for nodes and relationships.
3. The internal storage write path that bypasses incremental index maintenance.
4. The post-import index/constraint build phase.
5. Validation, recovery, and exit codes.

The use case driving this plan: shipping pre-built knowledge packs (4,000–50,000+ nodes with 384-dim embeddings) that today take ~13.5 hours per pack through the online Bolt path because the HNSW index is updated incrementally per batch. A bulk import that writes raw store entries and builds the index once at the end runs the same data in minutes.

For background and the alternatives we rejected, see the bottom of this plan.

## Non-goals

- Bulk _online_ import. The existing online path (Bolt + `apoc.periodic.iterate`-style batched MERGE) stays as-is. This plan is strictly the offline, fast path.
- A graphical or web import wizard.
- Streaming import from a running source database. The existing `scripts/migration/neo4j/migrate.go` Bolt-to-Bolt migrator continues to handle that case.
- Cypher-driven import. `LOAD CSV` is a separate online surface and not part of this plan.
- Parquet import. Neo4j supports `--input-type=csv|parquet`; this plan implements CSV only and rejects `--input-type=parquet` until a separate Parquet reader is designed.
- Neo4j incremental CSV update/delete actions (`:ACTION`, `:+LABEL`, `:-LABEL`, `:-PROPERTY`) in the first release. These belong to the reserved `database import incremental` surface, not `full`.

## Phase 1 — `nornicdb-admin` binary skeleton

1. Add `cmd/nornicdb-admin/` alongside the existing `cmd/nornicdb/`. Same Cobra style as the main binary so flag conventions match.

2. Subcommand layout (mirroring `neo4j-admin`'s shape):

   ```
   nornicdb-admin
   nornicdb-admin database
   nornicdb-admin database import full     <db-name> [flags]
   nornicdb-admin database import incremental <db-name> [flags]
   nornicdb-admin database info            <db-name>
   nornicdb-admin server status
   ```

   `database import full` is implemented in this plan. `database import incremental` is a reserved stub that exits with "not implemented". The other subcommands are stubs we add as needed; only `import full` is in scope for implementation.

3. Add a `make build-admin` target that produces `bin/nornicdb-admin{,.exe}` for every platform we already cross-compile for (`make cross-all` + admin). The binary embeds the same storage engine as `nornicdb` so it can open the data directory directly.

4. Acceptance: `nornicdb-admin --help` and `nornicdb-admin database import --help` print structured help that lists every flag.

## Phase 2 — File format compatibility

The import format is the documented Neo4j CSV format with one minor relabel: the trademark "Neo4j" is replaced with "NornicDB" or "nornic" wherever it appears in flag names or examples. The CSV header dialect itself is unchanged so files produced for `neo4j-admin import` work with `nornicdb-admin database import` byte-for-byte.

### Neo4j import format notes to preserve

These notes are from the current Neo4j Operations Manual import reference, reviewed on 2026-06-01:

- `neo4j-admin database import full` imports CSV into a non-existent or empty database; the database must be offline.
- Each `--nodes` or `--relationships` value describes one data source. A data source may contain multiple files, and the first file in that source must contain the header. Repeating `--nodes` / `--relationships` creates additional data sources with their own headers.
- The syntax is `--nodes=[<label>[:<label>]...=]<files>...` and `--relationships=[<type>=]<files>...`. Multiple labels in the option prefix are separated with `:`, not with the CSV array delimiter.
- The importer uses CSV by default, UTF-8 input, comma field delimiter, double quote character, semicolon array delimiter, semicolon vector delimiter, and RFC 4180 quote escaping by doubling quotes. Backslash escaping is not the default.
- The same field delimiter applies to every header and data file in the import.
- Header entries have the general form `<name>:<field_type>`. The `<name>` portion is used for property keys and for persisting ID values as properties; it is ignored for `:LABEL`, `:TYPE`, `:START_ID`, `:END_ID`, and `:IGNORE`.
- Fields without corresponding header entries are ignored only when `--ignore-extra-columns=true`; otherwise they are errors.
- Empty fields are treated as null/missing. Empty arrays are not representable in CSV because they cannot be distinguished from null arrays.
- Boolean values are true only when the text is exactly `true`; other values parse as false.
- `--id-type` accepts `string`, `integer`, and `actual` in Neo4j. NornicDB Phase 2 supports `string` and `integer`; `actual` is rejected because NornicDB storage IDs are namespaced strings, not externally supplied physical record IDs.
- `--normalize-types=true` is Neo4j's default and converts non-array numeric values to Cypher's normalized types, for example `int` to `long`. NornicDB should preserve this user-visible behavior where its property type system can represent it.
- Neo4j supports compressed `.gz` and `.zip` CSV files with one file inside each archive. NornicDB should support this in the reader layer if practical; otherwise the flag/help must state that compressed files are deferred.
- Neo4j's `--schema=<path>` creates and populates indexes/constraints during import. For NornicDB this should be spelled `--schema` for compatibility, with `--constraints-file` accepted as a deprecated alias only if we want the friendlier name.

Neo4j CSV header features that matter for compatibility:

- `:ID` and `<propertyName>:ID` identify nodes for relationship resolution. If a property name is provided, the import ID is also stored as that property. If no `:ID` is present, the node can be imported but cannot be referenced by relationships.
- ID spaces use `:ID(<space>)`, `:START_ID(<space>)`, and `:END_ID(<space>)`. IDs must be unique within an ID space, not necessarily globally.
- Full import supports multiple `:ID` columns as a composite ID. Relationship files must then use a matching number of `:START_ID` and `:END_ID` columns, either all single-column references or all composite references; do not mix both forms.
- A node `:LABEL` field contains one or more labels separated by the array delimiter, which defaults to `;`.
- Relationship files require `:START_ID`, `:END_ID`, and `:TYPE`. Names and type suffixes on `START_ID`/`END_ID` are ignored by Neo4j and should not change parsing.
- `:IGNORE` skips a column.
- Supported Neo4j property types are `byte`, `short`, `int`, `long`, `float`, `double`, `boolean`, `char`, `string`, `point`, `date`, `localtime`, `time`, `localdatetime`, `datetime`, `duration`, and `vector`, plus `[]` array suffixes.
- `point`, temporal, and `vector` headers may carry option maps, for example `location:point{crs:WGS-84}`, `created:datetime{timezone:Europe/Stockholm}`, or `"embedding:vector{coordinateType:float,dimensions:384}"`. Header values with commas inside option maps must be quoted like any other CSV field.
- Neo4j `vector` property values are coordinate lists separated by `--vector-delimiter` (default `;`) and must match the header dimensions. Coordinate type may be `byte`, `short`, `int`, `long`, `float`, or `double`.

### Node CSV header dialect

```
:ID(GroupName),name:string,age:int,:LABEL
n01,Alice,34,Person
n02,Acme,,Company
```

- `:ID(<group>)` or `id:ID(<group>)` — primary key column; the `(<group>)` is optional and lets you have multiple ID spaces in the same import. If a name is supplied before `:ID`, that value is stored as a node property as well as used for import-time lookup.
- Multiple `:ID` columns form a composite import ID for full import. This implies string ID composition and requires matching composite `:START_ID` / `:END_ID` columns in relationship files.
- `:LABEL` — array-delimiter-separated list of labels (`Person;User` by default). Labels from the `--nodes=LabelA:LabelB=...` option prefix are added to every node in that data source.
- `:IGNORE` — column to skip.
- Property columns: `name`, `name:string`, `age:int`, `score:float`, `created:datetime`, `tags:string[]`, `embedding:vector{coordinateType:float,dimensions:384}`. Same primitive types, list types, vector type, and delimiter behavior as Neo4j in the compatibility subset.
- Quote, escape, and array-delimiter rules: identical to Neo4j defaults.

Two NornicDB extensions for our content-pack use case:

- `:EMBEDDING(<index-name>)` — float-array column that is not a Neo4j header token. It writes the vector to `Node.NamedEmbeddings[<index-name>]` and the search service indexes it as a named embedding entry. Vector dimension is inferred from the first row and validated thereafter.
- `:NAMED_EMBEDDING(<key>)` — explicit alias for the same `Node.NamedEmbeddings[<key>]` path.

Both extensions are explicit and unambiguous, but they are NornicDB-only. Standard CSV files without them must work exactly as `neo4j-admin database import full` would interpret them. For portable Neo4j-compatible vectors, prefer the standard `property:vector{coordinateType:float,dimensions:N}` header; NornicDB stores that as a normal property value (`[]float32` where possible), and `search.Service.BuildIndexes` already indexes vector-shaped property values under property-vector IDs.

### Relationship CSV header dialect

```
:START_ID,:END_ID,:TYPE,since:date
n01,n02,WORKS_AT,2018-06-01
```

- `:START_ID(<group>)`, `:END_ID(<group>)`, `:TYPE` — required.
- If the node side used composite IDs, the relationship header must include the same number of `:START_ID` columns and the same number of `:END_ID` columns, with matching ID spaces.
- A `--relationships=TYPE=...` option prefix supplies a relationship type for rows whose `:TYPE` field is absent or empty; when `:TYPE` is present, it wins.
- Same property column rules as nodes.

### Multi-file imports

The same `--nodes` / `--relationships` flag form Neo4j uses:

```
nornicdb-admin database import full mydb \
  --nodes=Person=people.csv \
   --nodes=Person:Employee=employees_header.csv,employees_1.csv,employees_2.csv \
   --nodes=Company=companies.csv \
  --relationships=WORKS_AT=works_at.csv \
  --relationships=KNOWS=knows.csv
```

Multi-CSV-per-target is supported through comma-separated files inside one flag value, and multiple independent data sources are supported through repetition of the flag, identical to Neo4j. The first file in each comma-separated group is the header file.

### Flag set (Phase 2 scope)

```
nornicdb-admin database import full <db-name> [flags]

Input:
   --nodes=[<label>[:<label>]...=]<file.csv>[,<file.csv>...]  Node CSV files (repeatable)
   --relationships=[<type>=]<file.csv>[,<file.csv>...]         Relationship CSV files (repeatable)

Common:
  --data-dir=<path>                                Target data directory (default: ./data)
  --delimiter=<char>                               Field delimiter (default: ,)
  --array-delimiter=<char>                         Array element delimiter (default: ;)
   --vector-delimiter=<char>                        Vector coordinate delimiter (default: ;)
  --quote=<char>                                   Quote character (default: ")
   --id-type=string|integer                         ID column type (default: string; reject actual)
   --normalize-types=true|false                     Normalize scalar types to Cypher-compatible values (default: true)
   --ignore-extra-columns=true|false                Ignore trailing/unmapped columns (default: false)
   --ignore-empty-strings=true|false                Treat quoted empty strings as null (default: false)
   --bad-tolerance=<num>                            Number of bad entries tolerated before abort (default: 0)
  --skip-bad-relationships=true|false              Skip rels referencing missing nodes (default: false)
  --skip-duplicate-nodes=true|false                Skip duplicate :ID rows (default: false)
  --report-file=<path>                             Write a JSON report after import
   --schema=<path>                                  Cypher file with CREATE CONSTRAINT/INDEX statements run AFTER data load
  --verbose                                        Verbose logging

Index/constraint control:
  --build-indexes=true|false                       Build schema indexes after import (default: true)
   --constraints-file=<path>                        Alias for --schema (compatibility docs should prefer --schema)
```

At least one `--nodes` data source is required for `full` import. `--relationships` is optional.

This is the Phase 2 subset of Neo4j's shape. Unsupported Neo4j flags such as `--input-type=parquet`, `--format`, `--target-format`, `--max-off-heap-memory`, `--threads`, `--path-pattern-style`, `--multiline-fields`, and `--compress` should be accepted only if they are explicitly implemented; otherwise the CLI must fail fast with an unsupported-flag message rather than silently ignoring them.

## Phase 3 — Internal write path

This is where the speedup comes from. The reason `CREATE` over Bolt with a vector index is slow is that each commit replays through the executor and triggers an incremental HNSW insertion. The reason a one-shot `CREATE VECTOR INDEX` over the same data is ~100× faster is that it walks the storage engine in a tight loop and pushes the whole batch through the HNSW builder in one pass.

The import path in `nornicdb-admin database import full` writes through the current storage layer, with one small storage addition for import mode:

1. **Open the target data directory directly.** Use `storage.NewBadgerEngineWithOptions` plus `storage.NewNamespacedEngine` for the target database namespace, or a narrow `pkg/nornicdb` admin-open helper that creates the same storage/search wiring without starting Bolt, HTTP, inference, background embed queues, or periodic online services. The on-disk storage layer requires namespaced IDs such as `mydb:n01`; the importer must translate external Neo4j import IDs to internal namespaced `storage.NodeID` values.

2. **Refuse to start if the database is online.** Badger already prevents concurrent opens with its lock. The admin tool should surface this as a clear "database must be stopped" error and should not try to coordinate with a running server.

3. **Use the actual chunked storage APIs.** `BadgerEngine.BulkCreateNodes` and `BulkCreateEdges` already write primary records, label/type/adjacency indexes, MVCC heads, constraint checks, and cache invalidation in one Badger transaction per chunk. Chunk size should be bounded by Badger transaction size and memory pressure.

4. **Add or use an import-mode event suppression hook.** Today `BulkCreateNodes` calls `notifyNodeCreated` after a successful batch, and normal DB open wires that callback to `search.Service.IndexNode`. To avoid incremental HNSW/BM25 maintenance during offline import, the admin path must either avoid registering storage event callbacks or add an explicit `BulkImportOptions{SuppressEvents:true}` / `SetEventNotificationsEnabled(false)` surface around the load. This is a real implementation prerequisite.

5. **Stream nodes in chunks.** For each node:
   - Resolve labels and properties.
   - Create a `storage.Node` with internal ID `<db-name>:<generated-or-import-id>`, labels, typed properties, `CreatedAt`, `UpdatedAt`, and optional `NamedEmbeddings`.
   - For Neo4j `property:vector{...}` columns, store the property as a vector-shaped Go value (`[]float32` for float coordinate vectors when possible). `search.Service.BuildIndexes` indexes vector-shaped properties in one pass.
   - For `:EMBEDDING(<idx>)` and `:NAMED_EMBEDDING(<key>)`, write directly to `Node.NamedEmbeddings[<key>]`. Do not call `search.Service.IndexNode` during ingest.
   - Maintain an import ID map from `(id-space, composite-id)` to internal `storage.NodeID`. If the node has no `:ID`, generate an internal ID but do not add it to the relationship lookup map.

6. **Stream relationships after all nodes are loaded.** Resolve `:START_ID`/`:END_ID` against the import ID map, create namespaced `storage.Edge` IDs, and call chunked `BulkCreateEdges`. `BulkCreateEdges` already verifies endpoint existence and writes outgoing, incoming, type, edge-between, and MVCC indexes.

7. **Do not promise single-transaction atomicity for the whole import.** The existing primitives are atomic per chunk, not per database import. For `database import full`, write into an empty target database or a temporary import namespace/directory. On failure, remove the partial target or mark the report as failed; do not rely on WAL replay to roll back a partially imported database automatically.

8. **Build search artifacts at the end.** Instantiate `search.Service` after ingest or call it only after event suppression is lifted, then call `BuildIndexes(context.Background())`. The current `BuildIndexes` path iterates storage, batches BM25 indexing, indexes `NamedEmbeddings`, chunk embeddings, and vector-shaped property values, persists base vector/BM25 stores, and warms HNSW/k-means when needed.

9. **Apply schema from `--schema`.** Run supported `CREATE INDEX` / `CREATE CONSTRAINT` statements after data load. Because `BadgerEngine.BulkCreateNodes` validates constraints that already exist, the initial full-import path should either apply schema first when it contains constraints that must enforce during import, or apply schema after load and run validation before marking the import successful. The plan chooses post-load validation for Neo4j-like import behavior.

10. **Persist and close.** Let Badger/WAL use their configured sync semantics. If we wrap with `WALEngine`, each bulk chunk is one WAL operation (`OpBulkNodes` / `OpBulkEdges`); there is no current deferred-fsync API. Adding one would be a separate storage performance task.

The net write amplification goal is: one storage write per row plus built-in label/type/adjacency/MVCC indexes, and one search-index build after ingest. The key performance win is avoiding per-batch `search.Service.IndexNode` live HNSW updates.

### Library reuse

We don't write a new storage engine for this. Every primitive we need is already in `pkg/storage` and `pkg/search`:

- `storage.BadgerEngine.BulkCreateNodes` / `BulkCreateEdges` — chunked direct write path.
- `storage.NamespacedEngine` — maps database-local IDs to storage IDs with the `<db>:` prefix.
- `storage.StorageEventNotifier` — current callback surface that must be absent or suppressible during import.
- `pkg/search.Service.IndexNode` is what gets bypassed; `pkg/search.Service.BuildIndexes(ctx)` is the single-pass rebuild path already used after startup/recovery and by tests.
- `pkg/cypher` constraint parsing for the `--schema` / `--constraints-file` step.

What's new is the CSV reader, the column-spec parser, ID-space/composite-ID mapping, import-mode event suppression, and orchestration. The storage hot path is unchanged except for the small event-suppression hook if we choose to add one.

## Phase 4 — Index and constraint build phase

After all rows are written, the import runs:

1. For Neo4j `property:vector{coordinateType:...,dimensions:N}` columns, leave the vectors as node properties. `search.Service.BuildIndexes` indexes vector-shaped properties and tracks them as property vector entries.

2. For each NornicDB `:EMBEDDING(<name>)` or `:NAMED_EMBEDDING(<key>)` column, vectors are already stored in `Node.NamedEmbeddings`; `BuildIndexes` indexes them as named embedding entries.

3. Apply the schema file (`--schema=<path>`, or `--constraints-file=<path>` alias):
   - Parse and execute every `CREATE CONSTRAINT` / `CREATE INDEX` statement.
   - Constraints validate against the now-loaded data; violations fail the import.

4. Run `search.Service.BuildIndexes(ctx)` if `--build-indexes=true`:
   - BM25 fulltext entries are batched.
   - Vector-shaped properties, named embeddings, and chunk embeddings are indexed.
   - HNSW/k-means artifacts are warmed/persisted by the existing search build path when configured.

5. Write a JSON report to `--report-file` summarising rows imported, indexes built, durations.

The output of Phase 4 is a data directory that's logically equivalent to one produced by online ingestion of the same content: same graph records, built-in label/type/adjacency indexes, MVCC heads, schema metadata, and search artifacts. Do not require byte-equivalence because Badger key order, timestamps, MVCC versions, WAL segments, and search artifact serialization can legitimately differ.

## Phase 5 — Validation, recovery, exit codes

- **Pre-flight checks:**
  - Refuse to run if the target data dir is locked by `nornicdb serve`.
  - Refuse `database import full` if the target database has any user data (use `incremental` for that case, gated by an explicit flag).
  - Validate that `--nodes` / `--relationships` files exist and parse the headers before opening the engine.
  - Validate header/data delimiter consistency, required relationship columns, supported type suffixes, ID-space/composite-ID consistency, vector dimensions, and unsupported flags.

- **Failure modes:**
  - CSV parse error: report row, file, column index; exit code 2.
  - Bad relationship reference (no source/target node found): record in report; if `--skip-bad-relationships=false` (the default), exit code 3; if `true`, count and continue.
  - Duplicate `:ID`: same pattern, exit code 4.
  - Constraint violation during the post-load step: report row, constraint, conflicting node; exit code 5.
  - Unsupported Neo4j flag or header feature in the current implementation subset: report flag/header token; exit code 6.
  - Crash mid-import: already-committed chunks may remain in the target store. Because the current storage API is atomic per chunk, not per import, the full importer must either write to a temporary directory/namespace and promote it only after success, or delete/mark the partial target before retry. Do not document WAL replay as an automatic full-import rollback.

- **Exit code 0:** every row imported, every index built, every constraint satisfied. Report file written.

### Regression scenarios to keep covered

- Missing source folder or file path should fail fast with a CSV/open error before any writes begin.
- Importing into the same database namespace twice should fail the second time because the target is no longer empty.
- Importing into a different database namespace on the same underlying engine should succeed and keep the records isolated by namespace.
- Multi-file sources must treat the first file as the header-bearing file and read subsequent files as data without dropping their first row.
- After each scenario, verify the loaded database contents through the namespaced storage API, not just through the import report.

## Phase 6 — Cypher procedure helpers (post-import)

Two small additions to the running server, scoped to make the bulk-load UX cleaner for _online_ loads that still want the deferred-population behavior:

1. **`CALL db.awaitIndex(indexName, timeoutSeconds)`** — block until the named index reports `ONLINE`. Same shape as Neo4j's `db.awaitIndex`. Useful for the existing online drop-and-recreate workaround so loaders can wait for the rebuild without polling.

2. **`SHOW INDEXES` already exposes population state.** Confirm it reports `populationProgress` and lifecycle state (`POPULATING` / `ONLINE` / `FAILED`) consistent with Neo4j's surface so client-side fallback logic can detect a populating index.

These two helpers are independent of the offline `nornicdb-admin database import` work; they're useful for online loaders that prefer to keep using Bolt.

## Phase 7 — Documentation

1. **`docs/operations/admin-tool.md`** — operator-facing user guide for `nornicdb-admin`, with the import workflow, file formats, and recovery procedure.

2. **`docs/operations/cli-commands.md`** — add the `nornicdb-admin database import` table and a pointer to the user guide.

3. **`docs/user-guides/data-import-export.md`** — add an "Offline bulk import" section linking to the admin tool, alongside the existing online `LOAD CSV` and Bolt-driven import patterns.

4. **Format reference** in `docs/operations/admin-tool.md` covering:
   - `:ID`, `:START_ID`, `:END_ID`, `:LABEL`, `:TYPE`, `:IGNORE`
   - Property type suffixes (`:int`, `:string`, `:datetime`, `:string[]`, etc.)
   - The two NornicDB extensions: `:EMBEDDING(<idx>)`, `:NAMED_EMBEDDING(<key>)`

5. **Migration notes** for users coming from `neo4j-admin database import`: note the trademark-related rename of the binary (`nornicdb-admin`) and that CSV header syntax is unchanged.

## Phase 8 — Cross-cutting

- **Packaging:** the `nornicdb-admin` binary ships in the same release artifacts as `nornicdb`. Update the [native-package-distribution-plan](native-package-distribution-plan.md) Phase 1 to bundle both binaries.
- **Codesigning:** macOS Developer ID and Windows Authenticode cover both binaries.
- **Help links:** `nornicdb-admin database import --help` references `docs/operations/admin-tool.md`.

## Background: why this plan

### The benchmark that prompted the request

| Operation                                     | Time    | Notes                             |
| --------------------------------------------- | ------- | --------------------------------- |
| Insert 250 nodes (no vector index)            | ~1s     | Raw write speed is fine.          |
| Insert 250 nodes (with vector index)          | ~45 min | Per-batch HNSW rebuild dominates. |
| One-shot `CREATE VECTOR INDEX` over 32k nodes | ~20s    | Single-pass build is fast.        |

A 4,500-node content pack via 18 batches over Bolt: ~13.5 hours. The same 4,500 nodes loaded via a Neo4j-style admin import: minutes. The asymmetry exists because the HNSW builder already has a fast batch-build path; Bolt-driven ingestion just doesn't expose it.

### Why we picked the admin-tool path over the alternatives

- **`SET INDEX POPULATION OFF` session mode** would require designing visibility/correctness semantics from scratch (Neo4j has no analog) and would muddle the executor's invariants. Rejected.
- **`CREATE VECTOR INDEX OPTIONS {build: 'deferred'}`** is implicit in Neo4j (every `CREATE INDEX` is async-populated), and we get the same effect today by dropping and recreating. The piece worth adding is the `db.awaitIndex` helper (Phase 6 here).
- **`nornicdb-admin database import`** matches what Neo4j users already know. The format, the lifecycle, and the operational model carry over. It also gives us the "click install, wait a minute" UX without introducing a new session mode that has no precedent.

### What we explicitly are not changing

- Online ingest performance through Bolt is unchanged. Loaders that need online ingest continue to use the drop-and-recreate workaround documented in [Data Import/Export](../user-guides/data-import-export.md).
- The `MERGE`/`SET` UNIQUE-violation issue described in the user's report is filed as a separate bug; this plan does not address it.

## Acceptance Criteria

- [ ] `nornicdb-admin database import full <db>` accepts every flag and CSV header form documented in this plan.
- [ ] A 4,500-node, 384-dim-vector pack imports in <2 minutes on the same hardware where it currently takes 13.5 hours.
- [ ] A 50,000-node pack imports in <10 minutes.
- [ ] Output data directory is logically equivalent to one produced by the online path: same nodes, relationships, labels, properties, schema metadata, and query/index behavior, allowing timestamp/MVCC/WAL/search-artifact serialization differences.
- [ ] Pre-built Neo4j-format CSV files (no NornicDB extensions) import without modification.
- [ ] Standard Neo4j `property:vector{coordinateType:float,dimensions:N}` CSV files import without modification and produce searchable property-vector entries after `BuildIndexes`.
- [ ] CSV files using `:EMBEDDING(<idx>)` produce a populated vector index without a separate `CREATE VECTOR INDEX` step.
- [ ] All exit codes documented in Phase 5 are reachable in tests.
- [ ] The test suite covers ID spaces, multiple labels from both `:LABEL` and `--nodes=LabelA:LabelB=...`, composite IDs, bad relationships, duplicate IDs, `:IGNORE`, vector property dimensions, and unsupported-flag failures.
- [ ] `make build-admin` produces a working `bin/nornicdb-admin` for darwin/linux/windows.

## Related Documentation

- [Native Package Distribution Plan](native-package-distribution-plan.md) — admin binary ships alongside `nornicdb`.
- [Data Import/Export](../user-guides/data-import-export.md) — current online import patterns.
- [Vector Search](../user-guides/vector-search.md) — the index that benefits from one-shot construction.
- [Hot-Path Cypher Cookbook](../performance/hot-path-query-cookbook.md) — what the online ingest path looks like today.
- [Existing Neo4j → NornicDB migrator](https://github.com/orneryd/nornicdb/blob/main/scripts/migration/neo4j/migrate.go) — Bolt-to-Bolt complement to this CLI.
