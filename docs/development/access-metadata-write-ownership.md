# Access metadata writes

Go storage providers implement
`WriteAccessMeta(ctx context.Context, request AccessMetaWriteRequest) (written bool, err error)`.
Callers must choose an explicit, nonzero mode and pass the lifetime of the
operation that owns the write. The previous `PutAccessMeta` entry point is
retired; implementations and plugins must rebuild.

`ctx` owns admission, buffered-order drain and presence preparation. Once the
publishing update is admitted the write and its notification complete in the
store, so a canceled context returns its own error and leaves no published
entry. The access flusher passes its own flush-cycle lifetime: it has already
drained those deltas from the accumulator, and the run lifetime is canceled
before its final flush, so a request or run context would abandon accepted
counters. A store that is reached through a context-free queue entry
(`PutDeindexWorkItem`, `DeleteDeindexWorkItem`) passes that entry's own explicit
unbounded lifetime instead.

| Mode | Contract |
| --- | --- |
| `knowledgepolicy.RecordState` | Record or restore state without requiring a live entity. Preserve internal work keys and the existing index-catalog merge. |
| `knowledgepolicy.FlushExistingEntity` | Write only if the stored node or relationship exists at the atomic write boundary. Hidden entities remain eligible. Return `false, nil` for absence and preserve any retained state. |

```go
written, err := engine.WriteAccessMeta(ctx, knowledgepolicy.AccessMetaWriteRequest{
    Mode: knowledgepolicy.RecordState,
    EntityID: id,
    Entry: restoredEntry,
})
```

The native conditional writer uses the existing entity authority for both node and relationship IDs, orders after acknowledged buffered writes, and checks the stored-inclusive entity in the same managed transaction that writes the metadata. A lookup earlier in the flush is insufficient: deletion may commit after it. No new per-entity cache or lock registry is added.

The flusher skips genuine absence and retains real lookup/write errors for shutdown reporting. `GetEntityMeta` distinguishes absence with `knowledgepolicy.ErrEntityMetaNotFound`, also preserving `errors.Is(err, storage.ErrNotFound)` in the native provider. A node decoding or storage error is propagated rather than retried as a relationship lookup. Suppression and embedding callbacks run only after an accepted write.

Deletion preserves previously saved access state, and recreated IDs retain the existing ID-based state semantics. No entity-generation rule, Badger key, persisted payload or storage-version migration changes.
