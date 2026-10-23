# Property names across storage boundaries

Property tokens are private to one store and namespace. Rebuilding a store from
a semantic snapshot can allocate different tokens for the same names. A physical
body must carry its source name bindings before it can enter a different store.

Buffered WAL records use `PropertyBindings` for every property token referenced by their
node and relationship bodies, including names persisted before the transaction.
`bufferedWALPropertyBindings` captures those dependencies at WAL append.
`importBufferedWALRecord` resolves the names through the destination dictionary
and remaps the bodies before either recovery reader applies the commit. The
property values and the rest of each body retain their original bytes. The
destination dictionary is never replaced during WAL replay.

The complete table uses versioned compact tuples of namespace, source ID and
name. Its size and capture work depend on the names referenced by this record,
not the size of the source dictionary. Repeated names are interned in the source
recovery context; unchanged token mappings reuse the original body bytes.

Historical writers emitted only newly staged `PropKeys`. Ordinary updates to
persisted names therefore legitimately omit them. Original-store startup through
`RecoverBufferedCommits` explicitly supplies the original store/WAL pair and may
use its loaded durable dictionary. Generic `ReplayWALEntry` never infers that an
arbitrary opened destination is the source. Both readers also accumulate source
bindings from preceding records, including records skipped by a durable flush
marker or a snapshot sequence. Mixed old/new logs use the same context and importer.

A semantic snapshot contains property names but no original numeric token table;
its name set cannot prove a missing source ID. Snapshot recovery marks the store
as a semantic destination before creating rows. A conflicting source binding or
token remap also persists this marker, so a later restart cannot mistake the
destination dictionary for the original one. Native backup/restore transfers the
marker with the complete matching dictionary and resets the in-memory context.

If neither explicit original-store context nor preceding source bindings resolve
a legacy token, recovery reports a missing property name binding. This affects
old property-bearing records whose required bindings predate the retained WAL
when replayed into a different store. Recover the original physical store with
its WAL first, then take a named snapshot or complete native backup. A retained
WAL prefix containing the bindings is also sufficient. Records without property
tokens need no bindings. No values are reconstructed from lossy operation JSON.

## Boundary census

| Boundary | Representation and owner |
| --- | --- |
| Buffered WAL append | `WAL.AppendBufferedCommit` captures the complete body dependencies through `bufferedWALPropertyBindings`. |
| Snapshot followed by WAL and individual replay | `RecoverIntoEngine` reaches `ReplayWALEntry`, which calls `importBufferedWALRecord`. |
| Transactional memory recovery | `RecoverWithTransactions` uses a `MemoryEngine`; buffered physical records are rejected before the importer. This inherited API does not support physical buffered commits. |
| Startup write-behind recovery | `RecoverBufferedCommits` calls the same importer before batching local commits. |
| Ordinary WAL | `WALNodeData`, `WALEdgeData`, bulk records and before images carry named properties through `recovery_codec.go`; they do not transport tokenized bodies. |
| Snapshot save and restore | `WriteSnapshot`/`SaveSnapshot` and their visitors transfer semantic node/relationship frames with names. Destination writes encode names through its local property codec. |
| Replication and follower apply | `StorageAdapter.WriteSnapshot`/`RestoreSnapshot`, `ApplyCommand`, and batch/bulk commands transfer semantic entities or named property updates; destination writes use the local property codec. |
| Native backup and restore | `BadgerEngine.Backup` transfers the complete store, including dictionaries and historical data. Offline `Restore` replaces the entire store and reloads its matching dictionaries before reads resume. It does not mix source bodies with destination bodies. |
| Historical/MVCC records | Full images and undo deltas contain named properties. Buffered replay restores typed current bodies through the importer before generating history. |
| Embedding sidecars, adjacency and derived indexes | These contain no property-key tokens. Their values are outside the property-binding representation. |

The tokenized property writers are `encodeNodeInTxn` and
`encodeEdgeCompactV2`, both using `encodeTokenizedProperties`. The physical
transfer walker recognizes their primary node and edge keys. The regression
`TestBufferedWALBindingOwner` inventories production commit construction and
type-resolves all production files and rejects a new producer outside the
transaction owner and checked WAL importer. Mutations cover empty literals,
zero declarations, aliases, pointer/field writes and allocation via `new`.
Behavior tests exercise both recovery readers, reordered snapshots, existing
destination bodies, historical values, namespaces, replication and native backup.

The census was checked using the shared read-only release/develop Graphify
graphs and source searches for tokenized encoders, physical commit producers,
WAL readers, snapshot visitors, follower command application and native backup
loading. Graph connections are candidates; the source establishes the boundary.
