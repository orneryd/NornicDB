# Projected Entity Reads and Per-Entity Byte Maps

**Status:** Proposed implementation plan; no storage changes implemented.
**Date:** 2026-10-04
**Scope:** Nodes and relationships, projected reads, a segmented-value Badger fork,
existing async overlays, and inverse-diff MVCC history.
**Related:** [#859](https://github.com/orneryd/NornicDB/issues/859),
[#857](https://github.com/orneryd/NornicDB/issues/857), and
[#858](https://github.com/orneryd/NornicDB/issues/858).

## 1. Objective and decision

Make projected entity access the mandatory internal execution contract. Store
each node/edge revision's body and property payload once. Its projection map is
an index of byte locations, not another body or another copy of the properties.
Keep this representation opaque to existing API callers.

The selected design supersedes the earlier self-contained sidecar proposal.
Resolve the entity once, look up the requested field in its directory, and begin
decoding at the authoritative bytes. The Badger fork co-locates that directory
with the entity manifest and reads only selected payload segments.

Implement this in four independently measurable tracks:

1. Enforce and consistently use the projected APIs already available.
2. Change their private backing representation to the byte map.
3. Prototype a Badger fork with versioned segmented values and projected physical
   reads behind the same internal reader.
4. Replace retained whole-entity historical copies with inverse diffs and
   projection-aware rewind, retaining checkpoints where needed.

The first track requires no persisted layout change. A stock-Badger transitional
directory can index the existing body without duplicating it, but still fetches
the body value as a unit. The fork's final directory is part of the authoritative
entity representation, not a separate projection-value key. Existing graph
indexes and external identities remain unchanged.
The fork and inverse-diff tracks also introduce versioned physical formats and
require explicit migration/capability gates. They preserve public APIs and
existing visibility semantics, not necessarily the old physical representation.

Async writes are the existing mechanism for amortizing physical write work.
This plan uses that mechanism and improves its batching; it does not promise
that queueing makes persistence free, that all transaction writes can immediately
become async, or that async acknowledgement has stronger durability than today.

### Non-goals

- Changing public Cypher, Bolt, HTTP, or native client result shapes.
- Replacing Badger or converting graph IDs to a new key layout.
- Changing relationship multiplicity, constraints, or isolation semantics.
- Putting managed embeddings into the property map.
- Adding one Badger key per property.
- Making a Badger value a publicly exposed pointer or mutable memory region.
- Discarding existing MVCC/history or weakening rollback to improve benchmarks.

## 2. Current implementation to build on

This plan was checked against a worktree whose HEAD was `39299545`. Earlier
analysis predates the recent #857 and #858 commits; do not reimplement them.
The overlay paths below were located with Graphify graph traversal and NornicDB
MCP vector search, then checked against the current source. Indexed symbol
locations are navigation evidence, not a substitute for the implementation.

| Surface | Existing implementation | Design implication |
|---|---|---|
| Node projection | [ProjectedNodeReader and projected stream interfaces](../../pkg/storage/types.go) | Preserve nil/full versus empty/no-properties compatibility |
| Stream options | [StreamNodesOptions and scan kernel](../../pkg/storage/badger_stats.go) | Extend the shared reader rather than adding a parallel scan stack |
| Scan decoder | [projectedNodeDecoder](../../pkg/storage/projected_node_decoder.go) | Already resolves tokens per namespace and reuses scratch state; use as baseline/fallback |
| Property values | [Property codec](../../pkg/storage/property_codec.go) | Preserve strict types and stored temporal-value handling |
| Node encoding | [encodeNodeInTxn](../../pkg/storage/badger_helpers.go) | Existing property list and metadata body remain readable |
| Edge projection | [EdgesBetweenMatcher](../../pkg/storage/edges_between_stream.go) | Existing projected candidate matching and whole-edge return contract must remain |
| Edge header | [Compact edge codec](../../pkg/storage/edge_compact.go) | Endpoints, type, and metadata have an existing compact representation |
| Async reads | [AsyncEngine](../../pkg/storage/async_engine.go): `GetNode`, `GetEdge`, `StreamNodesWithOptions`, and `StreamNodesByLabelProjected`; [light node reads](../../pkg/storage/async_engine_node_reads.go) | Existing node/edge caches and deletion sets already shadow persisted state |
| Transaction reads | [BadgerTransaction](../../pkg/storage/badger_transaction.go): `GetNode`, `GetEdge`, projected streams, and pending merge helpers | Existing own-write/deletion overlays already sit above pinned physical snapshots and logical version selection |
| Property-index candidates | [MergePendingPropertyMatches](../../pkg/storage/badger_transaction_pending_index.go) | Existing pending-property index replaces rewritten/deleted committed candidates and adds own matches |
| Projected relationship candidates | [MatchEdgesBetween wrappers](../../pkg/storage/edges_between_stream.go) | Async and transaction wrappers already merge edge replacements/deletes; preserve and validate committed snapshot routing |
| Snapshot admission | [beginTransactionSnapshot](../../pkg/cypher/transaction_admission.go), [Cypher transaction setup](../../pkg/cypher/transaction.go), and `FlushBeforeSnapshot` in [AsyncEngine](../../pkg/storage/async_engine.go) | Existing admission flushes acknowledged writes and opens the snapshot under a short-lived guard |
| Atomic publication | [commitWriter](../../pkg/storage/badger_commit_writer.go) | Directory/payload publication must participate in ordinary and large atomic commits |
| Cleanup | [DeleteByPrefix](../../pkg/storage/badger_backup.go) | New key families must be included in database deletion |

Projected streaming currently selects properties for decoding, but it is not
constant-time byte access to an arbitrary property: the existing tokenized
property list still walks/skips preceding values.

The three logical visibility layers are already implemented. The byte-map work
changes the persisted representation read beneath them, not their precedence or
ownership. It does not require a new overlay subsystem.

## 3. API contract: explicit inside, compatible outside

### 3.1 Do not break existing callers

Keep existing `Engine` operations and projected-reader signatures working.
Existing complete-node/complete-edge reads continue returning owned structs and
ordinary property maps. Existing projected operations return the same metadata
and requested property values as before.

Introduce one explicit internal read specification shared by node and edge
operators. Proposed concepts, not current APIs:

```text
EntityReadSpec:
  properties: All | None | Selected(property names/tokens)
  metadata: required identity, labels/type/endpoints, timestamps, visibility data
  embeddings: explicit existing policy

EntityReadContext:
  namespace
  committed read selector / pinned physical snapshot
  existing async visibility/admission boundary
  transaction-local overlay
```

The read specification determines materialization; the read context determines
which entity revision exists. Do not conflate property projection with MVCC
version selection.

Internal materialization entry points require a read specification with no
implicit "all" default. Legacy methods adapt to explicit `All`; legacy
`nil` properties map to `All`, and non-nil empty slices map to `None`.

Compilation must identify requirements from every downstream consumer:
`WHERE`, inline pattern predicates, `WITH`, grouping, ordering, later clauses,
relationship endpoints, path functions, returned entities, and mutations.
Dynamic property access or `properties(n)` may require `All`. Returning a whole
node/edge still requires complete user properties.

### 3.2 Enforce the contract mechanically

- Inventory `GetNode`, `GetEdge`, batch reads, scans, adjacency reads, and full
  node/edge copies throughout query execution and storage wrappers.
- Migrate executor reads to the required-spec internal entry points.
- Make direct full materialization an explicit escape hatch with a named reason.
- Add a focused architecture test/static check preventing new bypasses in
  migrated executor paths; use existing tooling rather than a new linter.
- Keep public adapters, exports, backups, and legitimate whole-entity reads.
- Make all-property reads observable so accidental hydration is diagnosable.

Implement node and edge behavior together. Do not use a fake partially populated
`Edge` where an existing caller expects a complete relationship.

### 3.3 Ownership and mutation safety

Internal borrowed byte views are callback-scoped and read-only. Badger `Item.Value`
bytes, scan scratch maps, and pooled buffers cannot escape their lifetimes.
Copy or pin owned immutable buffers before retaining them.

Materialized public maps/slices remain independently owned according to existing
contracts. A projected entity cannot be submitted as a full replacement update:
mutation operators must retain a revision-aware patch or load the required base
state. Otherwise `SET n.x = 1` could erase unprojected properties.

## 4. Representation: one shared codec, two entity kinds

### 4.1 Logical identity and storage ownership

The final representation uses the existing namespace-qualified node/edge
logical key. Its selected revision resolves to one authoritative manifest,
directory, and payload. Reuse existing numeric endpoint identities; do not
create another identity dictionary.

An optional stock-Badger transitional key contains only a source-revision-bound
directory of offsets into the existing encoded body. It contains no property
values or copied entity metadata. Reserve such a key family only after checking
the prefix registry, and include it in cleanup if implemented. It is a bridge,
not the final two-key read contract.

### 4.2 Locator-only projection map

The final versioned immutable structured entity is:

```text
header:
  magic, codec version, entity kind
  revision identity
  metadata/directory/payload lengths and entry count
  integrity information

metadata:
  fields needed to honor projected entity and visibility contracts

projection directory:
  sorted fixed-width entries:
    property token, segment reference, offset, length, encoding flags

authoritative payload:
  independently encoded typed property values
```

Metadata and property values each have one authoritative location for that
revision. The directory stores only locators/framing. Offsets are relative to
the referenced immutable payload segment, never raw process/file pointers.
In the transitional directory they are relative to the exact existing body
revision. A "dynamic byte map" means publishing a new immutable directory when
locations change, not updating Badger's bytes in place.

Use existing per-namespace property tokens and typed value encoding. Resolve
requested tokens once per read/scan scope. Binary search gives `O(log P)`
directory lookup per requested property; decode only its selected byte range.
For dense projections, merge the sorted requested-token list with the directory.

Define fixed byte order, integer widths, offset base, maximum sizes, and codec
version before implementation. Reject duplicate/unsorted tokens, integer
overflow, overlapping/out-of-bounds ranges, truncated headers, incompatible
versions, and invalid typed payloads. Run full validation during construction,
backfill, and integrity checks; hot reads must validate framing and each accessed
range without rewalking every unrequested value. Do not recompute a whole-value
checksum on every narrow read if that eliminates the benefit being measured.

An encoded null value and an absent property remain distinguishable internally.
Restore precisely the existing public behavior. Preserve strict scalar, array,
map, byte, numeric, and temporal types; typed Cypher comparisons still go through
the existing semantic helpers, not arbitrary byte equality.

### 4.3 Metadata and edge parity

Read required metadata from the entity's authoritative header/metadata region;
do not copy it into a projection sidecar:

- Node: labels, required timestamps, suppression flags, embedding metadata, and
  other fields required by the existing projected/visibility contract.
- Edge: type, start/end identity, timestamps, confidence, generation/suppression
  flags, and required visibility metadata.

External scoring/access state remains in its managed stores. Managed embedding
vectors remain separate and are read only when requested. Inline legacy vectors
must not be silently lost from complete reads.

An edge header-only read uses `None` properties and reads the authoritative
header. Unchanged adjacency keys still require an entity/header lookup to
discover type and neighbor.

## 5. Read routing and performance limits

### 5.1 Point/index/adjacency reads

```text
resolve visible entity revision
  -> existing async/transaction wrapper resolves deletes and replacements
  -> existing snapshot-scoped committed reader selects the persisted revision
  -> resolve the authoritative entity once and obtain its projection directory
  -> locate requested field bytes and decode only those ranges
     (legacy bodies use the existing decoder until converted)
materialize only the requested fields
```

Use one pinned read context across all required metadata, head, property, and
embedding reads. Accepted candidates that need complete entities are hydrated
from that same context, not from a fresh latest read.

Full and projected reads consume the same authoritative bytes. Do not perform
a projection-body lookup followed by another entity-body lookup. In the fork,
`GetStructured(key)` selects the entity once; directory lookup locates the
proper bytes without walking/skipping preceding property values.

"Single lookup" means one entity key/version resolution and one indexed
directory lookup per requested field. Segment fetching, disk pages, required
visibility/head reads, and historical inverse links can still involve multiple
physical reads. The contract does not promise one disk I/O for every query.

### 5.2 Sequential scans

Stream the existing entity family once. Each item selects its format: legacy
projected decoder or structured directory/selected segments. The structured
iterator must supply the same resolved manifest to projection, not issue a
second point lookup per row. If a transitional directory is tested, account
for its extra lookup explicitly and do not call it the final single-lookup path.

Coverage must be certified per namespace and entity kind under the publication
barrier. A live coverage marker does not certify an older snapshot. Readers
select the marker from their own pinned view. New writes and deletes must
maintain certified coverage atomically.

Do not infer coverage from approximate counts or from the presence of some
directories. Cancellation, early stop, label predicates, temporal/decay visibility,
and result cardinality must remain equivalent on both paths.

### 5.3 What stock Badger can and cannot do

Stock Badger retrieves a key's value as a unit. The directory provides direct
access *within the fetched value* and avoids decoding/skipping other values;
it does not guarantee that the disk reads only the selected property's bytes.

A large unrequested `body` property still shares the stock-Badger value with
requested fields. A locator directory saves property walking/decoding, not
necessarily disk I/O. The fork separates physical segments without making a
second copy of that property. A transitional directory adds index bytes/key
traffic only, never a duplicated property payload.

Keep caches byte-bounded and revision-aware. Never preload one directory or
decoded map for every node and relationship into permanent Go maps.

## 6. Layered visibility: storage, async, snapshot, own writes

These logical layers already exist:

```text
persisted state
  -> AsyncEngine cached creates/updates/deletes for ordinary async reads
  -> snapshot admission and snapshot-scoped committed reads for transactions
  -> BadgerTransaction pending creates/updates/deletes (read-your-own-writes)
  -> evaluate predicates and materialize requested properties
```

This describes logical precedence, not one literal stack that reads the latest
async cache on every transaction read. At transaction admission, existing
`FlushBeforeSnapshot` makes acknowledged async writes part of the persisted
snapshot. Subsequent transaction reads use that snapshot plus their own writes,
not later peer writes from the live async cache.

### 6.1 Verified implementation and existing regression evidence

| Layer | Verified behavior | Existing regression coverage to extend |
|---|---|---|
| Async node/edge contents | `GetNode`/`GetEdge` check deletion sets, then caches, then the inner engine | [Async overlay parity test](../../pkg/storage/async_engine_read_overlay_test.go) compares reads before and after flush |
| Async projected scans | `StreamNodesWithOptions` shadows persisted IDs; `StreamNodesByLabelProjected` projects pending label matches and suppresses overridden IDs | [Async overlay parity test](../../pkg/storage/async_engine_read_overlay_test.go) includes projected label reads, adjacency, edge-between reads, and type counts |
| Snapshot admission | `beginTransactionSnapshot` calls `FlushBeforeSnapshot`, which flushes and opens the snapshot while holding the flush guard | [Flush/snapshot guard tests](../../pkg/storage/async_engine_count_flush_race_test.go), including `TestAsyncEngineFlushBeforeSnapshotReleasesGuardAfterSnapshotOpens` |
| Transaction entity reads | `GetNode`/`GetEdge` return not-found for own deletes, copies of own pending replacements, otherwise snapshot-selected committed state | [Transaction read tests](../../pkg/storage/badger_transaction_reads_test.go) include pending/deleted edges and pending node merges |
| Snapshot selection | `getCommittedNodeLocked`/`getCommittedEdgeLocked` use snapshot views and reject `ErrNotVisibleAtSnapshot` instead of reading latest state | [Snapshot anomaly tests](../../pkg/storage/transaction_snapshot_anomalies_test.go) include traversal across concurrent deletion |
| Transaction projected scans | Projected label streams and `StreamNodesWithOptions` merge snapshot rows with projected pending nodes and suppress own deletes | [Projected transaction read tests](../../pkg/storage/badger_transaction_reads_test.go) and [transaction stream tests](../../pkg/storage/badger_transaction_stream_nodes_test.go) |
| Candidate overlays | `MergePendingPropertyMatches` replaces rewritten/deleted index entries; async/transaction `MatchEdgesBetween` overlays edge changes | [Pending-index tests](../../pkg/storage/badger_transaction_pending_index_test.go) and [projected edge matching](../../pkg/storage/edges_between_stream.go) |

These tests are existing source evidence, not a claim that they were rerun for
this documentation update. They demonstrate intended contracts; byte-map
integration still needs regression tests for each affected path.

### 6.2 Required work: thread byte views through the existing layers

1. Keep the existing caches, pending maps, deletion sets, snapshot selectors, and
   admission helper as the owners of visibility. Do not add parallel state stores
   merely because persisted property access changes.
2. Pending entities currently exist as decoded state. Project them with the
   shared read spec directly; they do not need a persisted sidecar or a flush to
   answer a property read. If immutable byte views are cached for performance,
   invalidate them whenever their owning pending revision changes.
3. Use byte maps only for the committed revision the existing snapshot reader
   selects. Node and edge map lookup must use that same pinned view.
4. Run residual predicates against the effective overlaid entity. A storage
   filter is only a hint: it must not eliminate a base candidate whose pending
   replacement now matches, and pending-created matches must still be emitted.
5. Preserve existing candidate overlays for labels, properties, adjacency, and
   counts. Review committed routing separately: existence of an own-write merge
   is not proof that every specialized helper uses the pinned snapshot.
6. Preserve same-context complete hydration, copy/borrow lifetimes, early-stop
   behavior, flush failures/rebase, write conflicts, and constraint validation.
7. Extend existing tests with all/none/subset byte-map reads for node and edge
   creates, replacements, property removals, and deletes before/after flush and
   across transaction begin/commit/rollback.

For example, acknowledged async `x=2` is admitted by the existing barrier before
begin. A later peer update to `x=3` must not change that transaction's result.
Its own `SET x=4` returns 4; its own property removal must not resurrect 2 from
the committed map.

Do not add forced flushes per property read, retain a flush guard for a whole
transaction, or bypass overlays to call the underlying Badger reader directly.

### 6.3 Admission redesign is not required

The prior draft made non-flushing async snapshot admission and retained async
revision history an implementation phase. Remove that requirement. Existing
overlays and the admission barrier are sufficient foundations for this plan.

If measurements later justify removing the admission flush, that is an optional
separate isolation design: it would need retained/versioned async state pinned
at begin, coordinated flush publication, and snapshot-safe index candidates.
The verified current path uses the barrier instead. Do not conflate those two
implementations or make an admission redesign a dependency of projected reads.

## 7. Writes, revision identity, and atomic publication

The entity's selected legacy or structured representation is authoritative.
Its projection directory references that representation; it is not a second
materialized entity.

- Encode property values once into the authoritative payload and derive
  directory locators from the bytes actually written.
- Coalesce async changes within the existing safe batching boundaries.
- Build a map from the final complete state, not an incomplete projection.
- Publish payload, directory, heads/history, indexes, and deletes through existing
  `commitWriter` ordinary/large-commit machinery.
- Rollback and failed large-commit recovery include directory/segment changes.
- Every node/edge mutation path participates: direct writes, transactions,
  bulk writes, async flush/rebase, imports, replay, suppression/metadata changes,
  and repair jobs.

The source revision identity must distinguish entity incarnations and every
body/metadata mutation. Use the actual selected MVCC version when available and
a physical/publication identity for head-only paths; `UpdatedAt` alone is not a
revision token. A backfill's write timestamp is not its source entity's revision.
Finalize this identity against both retained-history and head-only operation
before enabling indexed byte reads.

For pinned physical snapshots, read body/head/map through the same snapshot.
For logical historical reads, use a map only when its source identity matches
the selected entity revision. A latest map must never serve an older body.

Initial historical fallback decodes that selected canonical version. A later
inverse-diff history format (section 10) reconstructs requested fields from a
matching newer anchor instead of duplicating a complete map per revision.
Legacy whole-version records remain readable during migration. Retention,
pruning floors, and GC must protect every reconstruction dependency.

Partial writer participation is unsafe. Backfill alone cannot solve stale maps.
All writers must maintain directory/payload coherence before coverage certification; publication and
repair compare source identity under the normal conflict/locking rules.
Backfill of a changed/deleted entity retries or skips rather than publishing
stale state.

Async must report both acknowledgement latency and durable flush throughput.
Measure queue growth, flush lag, drain time, and failure recovery. A growing
queue is not evidence of sustainable faster writes.

## 8. Opaque rollout, fallback, and errors

Roll out with explicit internal feature/capability states:

```text
disabled -> validated indexed writes -> mixed-format projected reads -> certified structured scans
```

- Old stores remain readable using current decoders.
- Make writers format-aware first, then convert nodes and edges in bounded,
  resumable namespace-scoped batches without changing logical user state.
  Replace each representation atomically; do not retain two live property
  payloads for compatibility. Unconverted entities remain in the legacy format.
- Publish coverage only after validating an online consistent cut and fencing
  all mutation paths.
- Persist codec/readiness capability metadata. Readers must not mistake unknown
  future formats for valid current values.
- Keep a kill switch selecting full reads of the same authoritative representation.
  Reverting the storage format itself requires supported conversion tooling.
- Missing maps in uncertified data are an expected, instrumented fallback.
- Corrupt maps, unexpected missing maps in certified data, and revision mismatch
  are explicit integrity failures: return an error or use an explicitly enabled,
  logged repair mode that reads the exact canonical revision. Never silently
  return an empty map or call an unvalidated latest read "recovery."

Old binaries may ignore new keys but fail to maintain them on writes. Do not
permit mixed old/new writers. Before a writable downgrade, disable reads and
invalidate/delete readiness and derived maps with supported tooling, then
rebuild after upgrade. Merely preserving the old body format does not make
writable downgrade safe.

Extend physical backups/restores to carry capability metadata and map keys.
Logical exports keep their existing format; logical restore/replay regenerates
maps through the shared writer. Restores never trust stale coverage markers.
Include both families and any historical derivatives in namespace/prefix
deletion, cache invalidation, integrity checking, recovery, and garbage collection.

## 9. Badger fork: segmented values and projected physical reads

Build a bounded prototype against pinned Badger v4.9.6, then enable it only after
end-to-end evidence. A directory on stock Badger saves decoding but cannot
generally avoid fetching/decompressing the containing SST block or value-log
entry. A new projected API needs independently readable physical payload units
to raise the I/O ceiling.

### 9.1 Physical representation

```text
existing logical entity key, selected physical version
  -> immutable manifest
       entity metadata and source revision
       field-token directory: segment ID, offset, length, encoding
       references to immutable payload segments
  -> small packed-field segments
  -> separate segments for large fields
```

Keep graph identity, labels, adjacency, constraints, and Cypher outside Badger.
Badger understands generic field tokens, opaque typed bytes, physical versions,
and segment lifetimes. NornicDB supplies the encoding and logical MVCC selector.

Manifest, directory, and payload segments need independently verifiable and
decryptable framing. Do not place every segment back inside one compressed SST
block or one indivisible encrypted value-log record and call it projected I/O.
Evaluate manifest-in-LSM with segmented external payloads; benchmark the extra
indirection against ordinary inline values for small entities.

One logical write may create multiple physical segments. Reuse immutable
unchanged segments when profitable, with transactional reference publication
and reachability-based reclamation. Do not create one public key per property.
No physical absolute pointers survive compaction or GC.

Disk pages, readahead, and packing can still fetch neighboring bytes. The
guarantee is avoiding unrelated payload-segment reads/decompression, not that
the operating system never reads an unrequested byte.

### 9.2 Proposed fork API

The following is a proposed API sketch, not existing Go code or a finalized
signature. Finalize it against managed transactions and iterator lifetimes:

```text
FieldSelection = All | None | Tokens(sorted unique uint64 tokens)
EncodedField   = token + opaque independently encoded value bytes
StructuredEntry = logical key + metadata bytes + complete field set

txn.SetStructured(entry) -> error
txn.PatchStructured(key, expectedPhysicalVersion, fieldChanges, metadataChanges)
  -> error
txn.GetStructured(key) -> StructuredItem or not-found/error
item.Project(selection, callback(StructuredView)) -> error
txn.NewStructuredIterator(iteratorOptions, selection) -> StructuredIterator
iterator.Item().Project(selection, callback(StructuredView)) -> error

StructuredView:
  selected physical version
  immutable metadata
  Lookup(token) -> borrowed encoded bytes + present flag
```

- Reads use the transaction's existing read timestamp, including managed mode;
  no implicit fresh transaction inside `Project`.
- `None`, `All`, missing fields, and empty values have distinct semantics.
- `PatchStructured` has explicit set/remove operations and a version precondition.
  Full replacement remains `SetStructured`; an omitted field in a patch is
  unchanged, not deleted.
- Callback-scoped bytes do not escape. Owned results require an explicit copy.
- Ordinary `Get`/`Item.Value` must either reconstruct the precisely defined
  generic structured encoding or reject unsupported mixed access explicitly.
  NornicDB's legacy body readers are not silently fed a different encoding.
- Iterator selection prevents unneeded segment prefetch; selection after
  whole-value prefetch would be too late.
- Propagate corruption, I/O, encryption, cancellation, and unknown-format errors.
- `Delete` hides the manifest atomically; GC later reclaims unreachable segments.
- Entry/patch publication participates in Badger conflict tracking, rollback,
  managed timestamps, and NornicDB's large-commit publication gate.

An initial fork can support only complete structured writes plus projected
reads. Patch/reuse is a subsequent optimization, not a shortcut around atomicity.

### 9.3 NornicDB usage

```text
GetNodeProjected / projected node stream / internal projected edge reader
  -> existing wrapper chooses effective pending or committed state
  -> pending state: project existing in-memory entity
  -> committed state:
       select revision through existing snapshot/MVCC machinery
       translate property names to namespace tokens
       GetStructured or structured iterator in the same pinned read context
       Project(required properties + required metadata)
       decode only returned field slices
       evaluate residual predicates / return owned public entity
```

Node and edge adapters share the same codec and storage primitive. Node labels
and edge type/endpoints come from the selected manifest metadata; embeddings
keep their existing managed store and visibility contract. Whole-entity reads
request `All`, not a different latest revision.

On write/flush, encode the final effective node/edge state once and stage
`SetStructured` or a revision-checked patch through the shared commit writer.
Publish the one authoritative entity write and inverse-history writes atomically.
Existing async admission and own-write overlays remain unchanged.

Historical reconstruction uses the fork only to retrieve the selected anchor
and projected inverse-diff payloads. Badger does not interpret graph history,
entity incarnation, Cypher equality, or NornicDB retention policy.

### 9.4 Adoption and maintenance

- Pin the fork revision and maintain upstream merge, license, compatibility,
  recovery, and fuzz-test processes; do not modify the module cache.
- Cover memtables, SSTs, value logs, compression, encryption, checksums,
  segment relocation, caches, backup/restore, and concurrent compaction/GC.
- Keep legacy values and structured values readable with format discrimination.
- Validate shadow encodings in tests or bounded transient buffers, not persistent
  duplicate bodies. Convert individual entities atomically; no mixed old/new writers.
- Disabling projected reads falls back to full reads of the same representation.
  Downgrade to a binary that cannot read structured values needs an explicit
  export/rewrite, not just a feature toggle.
- Benchmark manifest overhead, read amplification, durable writes, and unchanged
  segment reuse. Select inline legacy storage where it is actually cheaper.

After proof, the gated conversion makes the structured value authoritative for
each converted entity immediately. There is no phase that stores a second
complete current body or a second live copy of its properties. Old physical
revisions retained for snapshots are normal MVCC dependencies, not sidecar copies.

## 10. Inverse-diff MVCC and a single rewind projection

### 10.1 Existing history and scope

[Current MVCC records](../../pkg/storage/badger_mvcc.go) encode complete node/edge
snapshots and tombstones, and readers select exact or at-or-before versions.
Node history currently strips managed embedding vectors through
`mvccSnapshotNode`; preserve that contract. [Default retention](../../pkg/storage/types.go)
is head-only (`MaxVersionsPerKey == 0`), so inverse diffs primarily save space
when historical retention is enabled. They do not eliminate Badger's own
physical versions needed by active snapshots.

Use one inverse-diff codec for nodes and edges, covering properties and all
historically observable metadata. Keep existing MVCC ordering, namespace
isolation, snapshot admission, own-write precedence, and history API results.
Historical adjacency/index membership remains independently versioned: rewinding
an entity body does not reconstruct the historical graph's candidate population.

### 10.2 Record direction and contents

For committed transition `Vprevious -> Vnew`, keep current state at `Vnew` and
write the inverse transformation `undo(Vnew -> Vprevious)` instead of another
complete historical entity:

```text
InverseRecord:
  codec version, entity kind, namespace/incarnation identity
  fromVersion = Vnew
  toVersion   = Vprevious
  before-state presence/tombstone information
  directory of changed field tokens and metadata fields
  independently encoded undo payloads
```

Undo operations:

- Previously present field changed/removed: `Restore(old typed value)`.
- Previously absent field added: `Remove`.
- Metadata changed: restore old labels/type/endpoints/timestamps/flags as needed.
- Create: rewind to absence.
- Delete: rewind to the prior complete live state or references to its retained
  immutable segments. A deletion cannot be undone from a payload-free tombstone.
- Delete/recreate: preserve distinct incarnations and existence intervals; never
  accidentally connect history through recycled numeric IDs.

Use field-level replacement initially, not a recursive diff of arbitrary arrays
or nested maps. Large changed fields may still require their complete old value.
Shared immutable segments can reduce that cost in the fork; they must stay
reachable through undo records.

Compute the inverse against the actual committed predecessor under conflict
validation/publication, not blindly against a stale transaction begin view.
Async rebase must finalize the new state and its inverse together. Coalesce only
updates whose intermediate versions are not part of the existing externally
observable history contract.

Current state, inverse record, version head, indexes/adjacency, and commit markers
publish atomically through the existing commit machinery. No additional durable
sync per inverse field. Abort/crash recovery must never leave a head without its
required inverse link.

### 10.3 Single historical read, composed rewind

Proposed internal API, remaining opaque to public callers:

```text
ReadEntityProjectedAt(readContext, kind, id, targetSelector, readSpec)
  -> entity at selected version or explicit absence/history-floor error

ComposeRewind(anchorVersion, selectedVersion, requestedFields)
  -> one revision-bound rewind projection
ApplyRewind(anchorView, rewindProjection)
  -> one owned projected entity
```

Implementation:

1. Resolve the exact eligible version at or before the target using existing
   MVCC order; reject targets below the retained floor.
2. Select a current anchor or nearest usable checkpoint at/after that version
   which is visible in the pinned storage context.
3. Read inverse headers in descending transition order, verifying contiguous
   `fromVersion/toVersion` links and matching incarnation.
4. Compose requested-property and required-metadata undo operations into one
   sparse projection. As traversal moves older, an older undo for a field
   overrides the newer undo for that same field.
5. Retrieve only the final required old-field payloads and unchanged anchor
   fields where the encoding/segment layout permits. Copy/pin references before
   iterator buffers expire. Do not decode every intermediate entity.
6. Apply the composed projection once, then run existing visibility checks and
   final materialization. Own transaction changes still overlay the committed
   result through existing paths.

Example:

```text
V1: {a: 1}
V2: {a: 2, b: 9}    undo V2->V1: restore a=1, remove b
V3: {a: 3, b: 9}    undo V3->V2: restore a=2

read V1 from V3:
  compose restore a=2, then restore a=1/remove b
  final rewind: restore a=1/remove b
  apply once -> {a: 1}
```

"Single operation" means one caller operation and one final materialization,
not one disk read or constant-time access to arbitrary old history. A basic
chain still examines intervening headers. Use per-field directories and
checkpoints/skip summaries to bound that cost; do not promise all the way back
to creation after retention has pruned the original version.

### 10.4 Checkpoints, compaction, and retention

- Add complete checkpoints when measured inverse-chain depth/bytes or large
  replacements justify them. Thresholds are explicit configuration selected
  from benchmarks; no silent arbitrary historical-read cutoff.
- Optional composed skip blocks summarize inverse ranges with indexed per-field
  undo payloads. A summary crossing the requested target cannot replace exact
  boundary transitions. Account for their storage/write overhead.
- Sparse rewind caches are byte-bounded and keyed by incarnation, anchor,
  target version, projection, and codec; never cache a latest-derived result
  under a snapshot-only key.
- Retention pruning becomes dependency-aware. Keep every link/segment/checkpoint
  required to reconstruct surviving versions, including active readers.
- Before removing an intermediate dependency, atomically replace it with an
  equivalent composed link or checkpoint for surviving targets. Preserve
  retained exact-version semantics; do not compose away an addressable version.
- Publish the pruning floor and replacement dependencies atomically. Old readers
  retain their physical snapshot; new readers see the new complete chain.
- Do not reclaim deleted-entity payloads or dictionary identities while retained
  history or active snapshots still reference them.
- Structural corruption, missing links, and unavailable anchors are errors, not
  permission to return latest state or an incomplete property map.

### 10.5 Migration and fallback

Discriminate legacy full records, inverse records, and checkpoints with explicit
format tags/capabilities. Keep legacy history reads throughout rollout. Initially
start new inverse chains from a verified full anchor; offline/online conversion
of existing history is a separately resumable and validated operation.

Dual-write history only for bounded shadow validation, then stop whole-version
duplication once correctness/storage gates pass. Otherwise inverse diffs would
increase storage instead of compacting it. Reverting after full versions are
removed requires materializing retained history for the old reader format.
Include undo/checkpoint/segment dependencies in physical backups and replication;
logical exports preserve current formats and reconstruct history if supported.

## 11. Implementation sequence and exit criteria

| Phase | Deliverable | Required exit |
|---|---|---|
| A | Read-call inventory, operation counters, disk-backed node/edge baseline | Exact API/protocol result comparisons; current #857/#858 behavior recorded |
| B | Required internal read spec and complete wrapper forwarding | Migrated hot paths cannot accidentally request all properties |
| C | Shared node/edge byte-map codec and private immutable views | Round-trip/type tests, fuzzing, corruption tests, bounded allocations |
| D | Atomic directory/payload writes, revision identity, rollback/recovery, cleanup | Every writer covered; no duplicated live bodies or split revisions |
| E | Point/label/adjacency reads and partial/direct scan routing | Matching-version maps used; correct fallback and snapshot-aware coverage |
| F | Resumable backfill, readiness, rollback controls | Concurrent mutations and reopen/restore do not create stale maps or coverage |
| G | Byte-map integration and regression coverage through existing async/snapshot/own-write overlays | Existing admission unchanged; node/edge flush/update/delete races, conflicts, own writes, and candidate completeness pass |
| H | Badger segmented-value fork prototype and node/edge adapters | Selected segments only; atomic managed/large writes, recovery, GC, and existing API equivalence |
| I | Inverse-diff codec, committed-predecessor capture, and composed projected rewind | Identical retained history with sparse changes, creates/deletes/recreates, and no intermediate entity hydration |
| J | Dependency-aware pruning, checkpoints, migration, and authoritative-format gates | Retained targets remain reconstructible; measured storage/I/O benefit and safe rollback/export path |

Phases A/B can ship without the new keys. C/D precede E/F. G is integration and
validation of existing visibility machinery, not a new overlay implementation;
carry its cases through B/E and require them before enabling byte-map reads.
No non-flushing snapshot redesign is a dependency. H/I can be prototyped
independently after B and their baseline tests; production adoption requires D/G.
Inverse diffs can use stock Badger before H, but cannot claim segment-level I/O
savings there. J depends on accepted H/I formats for whichever capabilities are
enabled. Entity conversion must pass its own recovery/format gates; do not remove
full legacy history before its inverse-history migration gates pass.

### Correctness matrix

Test nodes and edges across Memory, disk-backed Badger, Async, WAL, Namespaced,
Composite, transaction, and the actual multidb server wrapper stack:

- All/none/subset/dynamic property reads; missing properties; nested typed data.
- Whole-entity results, embeddings, label/type/endpoint metadata, and ownership.
- Async creates, updates, removals, deletes, flush-in-flight, retries, and rebase.
- Physical snapshots and logical retained-history selectors.
- Own creates/updates/deletes, commit/rollback, constraints, and peer commits.
- Delete/recreate under the same external identity and dictionary number reuse.
- Index and adjacency candidate completeness at the same snapshot.
- Self-loops, parallel relationships, type changes, and optional-match null rows.
- Mixed backfilled/unbackfilled data, corruption, crash/reopen, large commits.
- Early limit, cancellation, callback errors, prefix/database deletion.
- Backfill and coverage certification racing with every mutation type.
- Backups, logical restore, WAL replay, suppression/temporal visibility.
- Structured manifests/segments under encryption, compression, compaction/GC,
  managed timestamps, failed publication, and full-value compatibility reads.
- Inverse composition against full-version history for every retained target,
  randomized updates, null/absence, metadata, deletes/recreates, and large values.
- Async rebase and concurrent commits producing the correct predecessor inverse.
- Broken/missing inverse links, floor boundaries, checkpoints, skip summaries,
  pruning with active readers, and mixed legacy/diff history after reopen.

For bugs, first add and verify the failing reproduction before fixing them.
Require race tests and at least 90% new-code coverage; keep storage core coverage
aligned with the repository's higher core-logic target.

### Performance matrix and acceptance gates

Use identical hardware/configurations and query-result caches disabled. Test
warm/cold disk-backed data with varying property counts and payload sizes,
including small maps, large unrequested text, managed embeddings, and edges.
Compare full hydration, current projected decoder, offset-directory prototype,
locator-only directory, and forked segmented values. Compare full-version MVCC with
inverse diffs at equal retention for shallow/deep histories, sparse/dense changes,
and current/historical all/none/subset reads. Do not declare the design faster
from a codec-only test.

Measure:

- End-to-end p50/p95/p99, ops/sec, and correctness.
- Canonical body reads, map/head reads, directory entries examined, values
  decoded, map allocations, intermediate rows, and actual storage bytes read.
- Bytes/op, retained heap, cache occupancy, data size, restart/backfill time.
- Async acknowledgement and durable throughput, backlog/lag, drain time.
- Direct/transaction/bulk writes, search enabled and disabled separately.
- Manifest/segment I/O and decompression, inverse-header reads, payloads fetched,
  checkpoint frequency, chain length, and final versus intermediate hydration.
- Equal-retention history bytes, compaction/GC work, historical p95/p99, and
  checkpoint/summary amplification; include unavoidable full old values on delete.

Structural gates:

- A requested property uses directory lookup, not a walk of all payload values.
- A structured projected read resolves its entity key/version once, then decodes
  directly from indexed authoritative ranges; required head/visibility work is reported.
- Header-only edges do not decode user properties.
- Structured scans reuse the iterator's manifest without a second entity lookup.
- New caches are byte-bounded; retained state is not one permanent map per entity.
- All-property and legacy-format paths preserve existing behavior.
- No projection map contains copied body metadata or property payloads; live
  authoritative payload bytes are not duplicated for projected reads.
- Forked narrow reads avoid unrelated payload segments, rather than merely
  slicing an already fetched whole value; cold I/O evidence is required.
- Historical rewind is one API operation and one final materialization;
  structural tests prove no complete intermediate node/edge is decoded.
- Sparse changes store changed prior fields rather than every unchanged value;
  original/old reads remain correct wherever retention permits.

Before phase C implementation, record a baseline and agree workload-specific
latency/throughput targets. Before enabling phase E by default, require a
statistically repeatable improvement on the intended projected workloads and no
unexplained >5% existing-workload regression. Any >10% retained-memory increase
requires justification. Publish directory/manifest overhead, historical payload
retention, actual disk/write amplification, and sustainable flush capacity before
approval rather than hiding them behind async acknowledgement. A duplicate live
property payload is a design violation, not an accepted performance tradeoff.

Useful existing validation starting points, to run during implementation:

```bash
go test ./pkg/storage ./pkg/cypher -run 'TestProjected|TestBadgerEngine_GetNodeProjected|TestTxReads|TestBadgerTransactionStreamNodesWithOptions|TestIssue857'
go test ./pkg/storage -run '^$' -bench '^BenchmarkStreamNodes_' -benchmem
go test ./pkg/cypher -run '^$' -bench '^BenchmarkLabellessPropertyScan' -benchmem
```

These commands are not sufficient disk-backed acceptance coverage; add the new
node/edge codec, layered-visibility, and server-stack selectors as implemented,
then run the relevant race, compatibility, and build checks.

## 12. Definition of done

Existing callers observe the same entities, properties, visibility, errors, and
mutation guarantees. Internally, every migrated operator declares what it needs.
Nodes and relationships share one revision-aware projected access design; map
bytes, async state, and historical revisions do not leak through the public API.

The result is proven at the query and sustainable-write level, not merely a
faster decoder or an earlier acknowledgement. Full and projected reads share
one authoritative representation; the projection directory only locates bytes.
Choose dense/full versus sparse/range reads without maintaining another body.

Forked projected reads and inverse-diff history must additionally demonstrate
actual segment-I/O and equal-retention storage improvements, preserve every
retained historical result, and survive publication/pruning/GC failures. Their
physical formats and rewind machinery remain invisible to existing callers.
