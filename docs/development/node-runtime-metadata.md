# Node runtime metadata

The Go `storage.Node` API keeps graph data inline and stores runtime state in an
immutable record (`nodeRuntime`) shared by detached copies of a node image.
Changing runtime metadata on one image replaces its record; it never mutates
another copy's provenance or embedding identity. A node without a record
observes the zero metadata value, so ordinary graph-data construction and a
detached copy of a zero node allocate no record.

Use the typed metadata API instead of the former `Node.Database`,
`Node.EmbeddingsStoredSeparately` and `Node.VisibilitySuppressed` fields. All
three fields moved together onto `storage.NodeRuntimeMetadata`:

```go
metadata := node.RuntimeMetadata()
metadata.Database = "analytics"
node.SetRuntimeMetadata(metadata)
```

## API contract

- `RuntimeMetadata() NodeRuntimeMetadata` returns the metadata **by value**. An
  `nil` node, or a node without a runtime record, returns the zero value
  `NodeRuntimeMetadata{}`. The returned struct is a copy; editing it does not
  affect the node until `SetRuntimeMetadata` is called.
- `SetRuntimeMetadata(NodeRuntimeMetadata)` replaces the whole metadata value on
  this image only. Setting a value equal to the current one is a no-op and does
  not allocate. Setting the zero value drops the record when no embedding
  identity is attached. Like every other `Node` field write, it requires
  exclusive ownership of that image. `Node` values are not safe for concurrent
  mutation; the owning engine or caller synchronizes access.
- The former exported fields no longer exist on `Node`. Unqualified use of
  `node.Database`, `node.EmbeddingsStoredSeparately` or
  `node.VisibilitySuppressed` is a compile error, and the zero value of a struct
  literal no longer accepts them; migrate to the getter/setter above.
- `EmbeddingRevision()`, `EmbeddingAttempt()`, `SetEmbeddingAttempt` and
  `CopyEmbeddingIdentityFrom` retain their existing contracts. Owner, owner ID,
  revision, attempt identity and commit-publication proof live on the same
  record and are untouched by a metadata change. `SetEmbeddingAttempt` with an
  unchanged pointer does not allocate.

## What copies and caches keep

- `copyNode`, `CopyNode`, `copyNodeForCaller` and `copyNodeWithoutEmbeddings`
  share the immutable record with the source and therefore preserve metadata and
  embedding identity. The embedding-free shapes differ only in vectors.
- `cacheStoreNode` (a written image) keeps the written metadata, owner and any
  existing publication proof. Revision is zeroed and the worker attempt is
  cleared, as before. A cache hit is still the caller's own image.
- `cacheFillNode` keeps the read image's metadata and identity for both the full
  and the query cache form; a fill whose generation moved is dropped entirely.
- The versioned node-body cache stores a vector-free projection under the same
  metadata and refuses to cache inline vectors, whose payload cannot be
  reconstructed from the separate embedding keys.
- Namespace projection (`toUserNode`) rewrites only `Database` to the namespace
  and keeps the other metadata and the embedding identity.
- `Database` is never persisted. The stored MessagePack node map retains its
  existing ten keys and value types, including
  `EmbeddingsStoredSeparately` and `VisibilitySuppressed`; the replication
  transport keeps its eleven-key map with `Database`. Private embedding evidence
  is never serialized. Badger keys, WAL formats, storage versions and migration
  arms are unchanged, and the gob migration reader keeps its historical exported
  field schema explicitly.

## Limits

- This is a Go source API change only. Public JSON continues to expose graph node
  data alone; nothing in the change adds runtime metadata to it.
- Immutable cache entries and published images are never written in place.
  Changing metadata requires a copy, which is why the namespace and projection
  paths copy first.
- `nodeRuntime` is not a second persistence format. The two stored flags still
  round-trip under their existing keys; database provenance and private embedding
  identity remain excluded from native payloads. Historical gob payloads are
  decoded through the historical field schema before constructing the current node.
- Metadata-only records are reused through a bounded immutable-value table (at
  most 1,024 entries, database names at most 256 bytes). This avoids allocating
  another record for each repeated namespace projection. Embedding identity,
  attempts, revisions and publication evidence never enter that table. Eviction
  affects reuse only; attached records remain immutable and valid.
- No measured claim is made here. Allocation and size figures are asserted only
  by the focused tests (`node_runtime_test.go`, `node_runtime_payload_test.go`
  and `node_runtime_cache_contract_test.go`), which run at their own recorded
  scope; this document reports no benchmark result.
