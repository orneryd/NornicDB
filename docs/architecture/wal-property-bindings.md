# Property names across storage boundaries

Property tokens are private to one physical dictionary and namespace. A named
snapshot can allocate different tokens when it rebuilds a store. Every physical
buffered body therefore needs explicit authority for the dictionary that encoded
it. `importBufferedWALRecord` is the one authority and conversion owner; public
individual and batched replay both call it.

## Dictionary epochs

Steady-state buffered records use the base v3 payload, without per-record name
export or body walks. On attachment, a dictionary change, or a snapshot boundary,
the WAL owner writes one complete immutable dictionary epoch before its bodies.
Each sealed and active file starts with its own authority. Rotation, retention,
and compaction carry that authority with the records; they never infer it from
matching tokens or from an earlier writer's assignments.

The file reader attaches the epoch to each buffered record and hides dictionary
controls from logical consumers. Standalone JSON transport includes that attached
authority. A generic raw physical append clears the current epoch; it cannot
borrow the attached engine's dictionary. Historical self-contained per-record
tables remain readable and take precedence over an enclosing epoch.

The importer resolves an epoch once into destination names and IDs. Its immutable
plan remains valid across ordinary name additions and drain persistence. Replacing
an existing meaning invalidates the plan. Pending destination drains are shared
until persisted; only an actual remap requires a body walk. A matching epoch
leaves body bytes untouched. One active epoch plan is retained, not a WAL-wide
source token map. Older per-record tables retain a separate bounded compatibility
cache (4,096 names / 128 KiB); legacy omitted bindings never consult either cache.

Open epoch controls alone use framing version 4. Clean shutdown first drains and
syncs the native store, checks imported commits are applied, and then atomically
converts controls to v3 checkpoints. Snapshot compaction also seals retained
controls. Their byte and sequence spans are closed: later old-writer appends
cannot inherit them. Ordinary records are copied unchanged. Sealed files are
converted first and the active file last. The old reader refuses unclean v4
logs without truncating them; see the recovery instruction in
[WAL operations](../operations/wal-compaction.md#upgrading-and-downgrading-wal-stores).

The same close owner seals every attached WAL, including synchronous engines:
native restore invalidates dictionary authority regardless of write-behind mode.
No snapshot compaction is required before that clean shutdown. A durable
`property-epoch.unsealed` marker in the WAL directory is written before the
first control and removed only after every file is converted; clean shutdown
and compaction read the log only while it is present (or while a clean-stop
proof is still owed).

The public benchmarks retain one-key and 160-key workloads and report time per
record and per 32-record batch. Setup and validation are untimed; data is on tmpfs.

## Explicit authority and upgrades

The engine persists a dictionary UUID and stamps subsequent dictionary epochs.
The writer obtains the UUID from the physical dictionary that encoded the body.
Normal reopen and physical cache reload preserve it. A semantic snapshot or
native store replacement rotates it and persists a foreign-sourced marker.
Generic replay persists that marker before applying any imported body, even
when the first observed token happens to match. Once foreign, the marker stays
set; subsequent local records have their own UUID and complete bindings.

`RecoverBufferedCommitsAtOpen` is exclusively the constructor's recovery of a
store's own configured WAL directory, before traffic. This entry point is the
structural authority for genuinely historical records that have no UUID. It
preserves ordinary upgrades of an existing store and pending WAL. A later
stamped record must match the durable identity to use the original-store path.
A foreign-populated store without a clean ownership proof cannot borrow
constructor authority for unstamped records from the other writer. A successful
clean stop with an attached WAL proves that its commits reached the native store
and persists a proof bound to that dictionary UUID. This permits genuine
old-binary writes after a downgrade. Another foreign import or restore invalidates
the proof and re-arms the v4 refusal before importing bodies.
`RecoverBufferedCommits`, `ReplayWALEntry`, and
`RecoverIntoEngine` are foreign entry points; matching tokens prove nothing
about their origin.

On the proven original-store path, legacy bodies and their own staged drain are
used verbatim, without a body walk, read transaction, or binding allocation.
A complete new-format table that already matches both directions of the durable
destination dictionary also leaves body bytes untouched: only table entries are
checked. Pending destination names are persisted through the dictionary owner.
Only an actual numeric remap requires walking property values, retaining their
exact encoded byte spans while changing key varints.

For foreign historical records, a current-record drain must bind every token
that the body uses. A recognized lossless recovery envelope can instead supply
complete named property after-images through the same codec. Plain old JSON
does not preserve all property types and is not that authority. Otherwise
recovery fails explicitly. An earlier WAL record or a snapshot's name set is
never used to guess an omitted binding. Genuine base-written ZIP fixtures test
ordinary upgrade separately from synthetic typed-envelope compatibility cases.

Cross-store replay resolves or allocates names in the destination and passes
only destination drains to `markPersisted`. It never replaces the destination
dictionary or installs source IDs into its caches. Existing destination bodies
keep their interpretation; tests check forward/reverse cache agreement.

An old binary may already have corrupted a store during a cross-store restore.
This change prevents new corruption; it cannot detect or retroactively repair
that old corruption. To recover an ambiguous foreign legacy record, use its
original physical store and own WAL at open, then export a named snapshot or a
complete native backup. New writer records carry explicit epoch authority.

## Engine lifetime

Provenance and import use the engine's existing `beginHelperTxn` admission and
`txnWG` lifetime owner. The closed check and lease registration share `b.mu`;
Close rejects later admissions and waits for admitted work before clearing or
closing the database. Runtime metadata writes and importer reads use the retained
handle, including deferred read release. Public individual and batched replay
retain engine state through physical application and count repair as well.

The lifetime lease does not retain the write barrier across delegated CRUD.
Native restore keeps its existing exclusive barrier/read-drain order and passes
its protected handle into identity initialization; construction does the same
before publishing the engine. The source mutex still serializes identity changes,
and foreign provenance is durable before imported bodies can be applied.

## Boundary census

| Boundary | Representation and owner |
| --- | --- |
| Buffered WAL append | `WAL.AppendBufferedCommit` uses the dictionary epoch owner; steady records keep the base payload. |
| Recovery at open | The database constructor calls `RecoverBufferedCommitsAtOpen`; it passes explicit own-directory authority to the shared importer. |
| Generic buffered recovery and individual replay | `RecoverBufferedCommits` and `ReplayWALEntry` pass foreign authority to the same importer. |
| Snapshot and WAL-only rebuild | `RecoverIntoEngine` persists foreign provenance, restores semantic snapshot rows when supplied, and replays each remaining record independently. |
| Ordinary WAL and undo | Node, edge, bulk and before-image records carry named properties through `recovery_codec.go`. |
| Semantic snapshots | Snapshot visitors transfer named entities; destination writes use its local property encoder. |
| Replication and follower apply | `StorageAdapter.ApplyCommand`, batch/bulk and snapshot paths transfer semantic entities or named updates and use local encoders. |
| Native backup/restore | Backup transfers dictionaries and physical bodies together. Offline restore reloads that pair, rotates local identity and marks it foreign before reads resume. |
| Historical/MVCC records | Full images and undo deltas use property names. Buffered replay restores typed current bodies before generating history. |
| Transactional memory recovery | `RecoverWithTransactions` still rejects physical buffered commits at a `MemoryEngine`; support is not expanded. |
| Sidecars and derived indexes | Embeddings, adjacency and derived index values contain no property-name tokens. |

Numeric relationship endpoint IDs have a separate owner. The property payload
walker preserves their header and does not prove cross-store endpoint rebinding.
The managed catalog time-parse failures investigated separately are not claimed
as fixed by this property-name change.

The census uses the shared release/develop Graphify graphs read-only and current
source searches. `TestBufferedWALBindingOwner` uses go/types and SSA to inventory
physical producers and follow writes-map aliases, calls, closures, tuple results,
phi nodes and borrowed byte slices. Mutation cases are type-checked separately;
thin owner calls and unrelated fields remain positive controls. The permitted
functions are the transaction producer and the single codec/WAL-record owner,
including its common copy-on-write payload transform and export boundary.
