# Index reads and cancellation in v1.4.4

Index candidate preparation belongs to the statement that requested it. The
following Go APIs now require a `context.Context` as their first argument and
return an error in addition to their existing result:

| Owner | Operations | Results |
| --- | --- | --- |
| `CompositeIndex` | `LookupFull`, `LookupPrefix`, `LookupWithFilter` | IDs, error |
| `CompositeIndex` | `IndexNode` | error |
| `SchemaManager` | `PropertyIndexLookup`, `PropertyIndexLookupAnyLabel`, `PropertyIndexTopK`, `PropertyIndexAllNonNil` | IDs, error |
| `SchemaManager` | `GetPropertyIndex` | index, found, error |
| `SchemaManager` | `HasPropertyIndex` | found, error |
| `SchemaManager` | `GetConstraintsForLabels` | constraints, error |
| `SchemaManager` | `SeekableCompositeIndexesForLabel` | filled indexes, error |
| `SchemaManager` | `PropertyIndexRange` | IDs, index availability, error |
| `SchemaManager` | `VisitPropertyIndexGroups`, `VisitPropertyIndexGroupsInRange` | index availability, error |
| `SchemaManager` | `LookupUniqueConstraintValue` | holder ID, found, constraint exists, cache complete, error |
| `BadgerTransaction` | `MergePendingPropertyMatches`, `MergePendingCompositeMatches` | IDs, error |
| `TransactionEqualityLookup` | `LookupNodesByEquality` | nodes, complete, error |
| `storage` | `NewCompositeKey` | composite key, error |
| `security` | `KeyedDigest`, `KeyedDigestHex` | digest, error |

Pass the actual statement context and handle its error before deciding whether
to use a scan fallback. Cancellation is an error, not an index miss. For example:

```go
ids, err := schema.PropertyIndexLookup(ctx, "Person", "email", email)
if err != nil {
    return err
}
// Evaluate the full query predicate against these candidates as usual.
```

The old signatures are retired; embedding applications and plugins must rebuild
and migrate their callers. HTTP, Bolt, Cypher syntax, Badger keys, stored payloads
and storage version do not change.

Cancellation applies while waiting for index admission, copying candidates,
merging pending writes, ordering keys and preparing groups. A canceled
materialization returns no partial result. A reusable key or pending-value cache
is published only after its construction succeeds. Group visitors run without
holding index locks; an intentional visitor stop remains successful unless the
request was also canceled.

Accepted writes retain their own integrity-check lifetime. Existing context-free
Go maintenance and integrity boundaries explicitly select `context.Background()`;
statement reads must not substitute that context for their caller's context.

`CompositeIndex.IndexNode` is the write-side counterpart of those reads and takes
the lifetime of the caller filing the node. Native maintenance and fills that
carry no request pass `context.Background()`; preparation made for a request, such
as a relationship constraint's candidate cache, passes that request's context.
The call acquires the index write lock under that lifetime, stages the scalar,
full and prefix keys before the first posting write, checks the lifetime once more
and only then publishes. A cancelled or failed preparation therefore leaves the
existing values, full and prefix populations, dirty flag and fill state exactly as
they were, and a healthy retry files the node normally. A complete composite key
reuses its full-key digest for the longest prefix, so the same values still
produce the same key hash; stored keys, payloads and storage version are
unchanged.

The regression budget is measured from actual cancellation or deadline expiry
until backend work stops, separately from total request time. The release target
for the reproduced indexed-read workload is 100 ms after cancellation. This is a
NornicDB regression target, not a claimed universal Neo4j timing guarantee.

## Discovery and key preparation

Carry the statement context through schema discovery and key construction too:

```go
index, found, err := schema.GetPropertyIndex(ctx, "Person", "email")
if err != nil {
    return err
}
if found {
    // Use index with the same statement context.
    _ = index
}
key, err := storage.NewCompositeKey(ctx, tenant, email)
if err != nil {
    return err
}
digest, err := security.KeyedDigest(ctx, "example.cache", key.Hash)
if err != nil {
    return err
}
_ = digest
```

Discovery checks cancellation during schema-lock admission and index enumeration.
Key preparation checks between formatted values; hashing checks before each
length prefix and each 32 KiB body chunk. Each digest reuses one bounded body buffer
and one eight-byte length buffer. Failed preparation returns no partial key;
constraint admission propagates the error and releases acquired authority.
Successful retries preserve the normal index/constraint semantics.

The context-first migration preserves the HMAC namespace, process-local random
key and big-endian length framing for identical parts. The separate composite
framing correction passes each typed value as its own framed part, preventing
embedded delimiters from aliasing distinct tuples. These index/cache/lock hashes
are ephemeral. Persisted temporal hashes, Badger keys/payloads and storage version
remain unchanged.

Go embedding callers supply a non-nil context. Existing explicitly nil-detached
transaction preparation/commit boundaries retain that contract and select their
engine owner's detached context for synchronous key preparation. Context-free
native imports, schema creation checks and auth-cache operations explicitly select
their documented native lifetime. Statement callers preserve their actual context.

The constructor retains Go's formatting behavior, including caller-supplied
`fmt.Stringer`/`fmt.Formatter` implementations. It can stop between formatting
calls; it cannot preempt arbitrary Go code executing inside one such method.
The 100 ms measured regression target applies to the reproduced indexed-read
workload, with input size and cancellation point recorded alongside the result.

`SchemaManager.BackfillCompositeIndex(ctx, name, entries)` retains the DDL
request through schema/pending admission and each node's prepared keys. A canceled
fill never marks an unfilled constraint index ready, and the enclosing schema
transaction publishes nothing;
a healthy retry fills the index. Context-free import/startup maintenance still
uses its documented native lifetime.

Relationship candidate-cache replacement prepares the complete posting projection
before removing an old entry, then removes and inserts under one index lock.
Cancellation leaves the old postings, key map and metadata unchanged. This is
a component atomicity guarantee: request-built relationship caches are local until
published, while maintenance of already-published caches uses the accepted write's
non-cancelable lifetime.
