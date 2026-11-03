# Storage preparation contexts

`SchemaManager.GetConstraints`, `HasAnyConstraintContract`, and
`GetConstraintContractsForTarget` take a context and return an error. They use
that context for metadata lock admission and enumeration; contract discovery also
uses it while cloning entries and sorting. Their only errors are cancellation or
deadline errors. A failed enumeration returns no partial result.

```go
constraints, err := schema.GetConstraints(ctx)
if err != nil {
    return err
}
for _, constraint := range constraints {
    // Inspect the detached unique-constraint metadata.
    _ = constraint
}

hasContracts, err := schema.HasAnyConstraintContract(ctx)
if err != nil {
    return err
}
if hasContracts {
    contracts, err := schema.GetConstraintContractsForTarget(ctx, storage.ConstraintEntityNode, "Doc")
    if err != nil {
        return err
    }
    _ = contracts
}
```

Pass the actual caller context for request-owned preparation. Native
`CompositeEngine.GetSchema()` remains a context-free `Engine` method: its
constituent metadata reads explicitly use `context.Background()`, for which these
cancellation-only errors cannot occur.

`PrepareEntityCommit` remains caller-owned until its immutable prepared handoff
and submission decision. `CommitContext` remains caller-owned through its final
publication decision. Conflict classification, temporal buffering, constraint
adjacency, change-image capture, observer preparation, publication dependency
keys, and change-event key discovery use the current phase's lifetime. Errors
before publication abort the transaction and do not invoke publication callbacks.
Native point reads and context-free staging APIs retain their existing ownership;
request operation lists poll between those staging operations.

After a prepared command is accepted, its existing nil `commitCtx` contract means
detached completion. The publication phase selects `context.Background()` for
private preparation helpers at that handoff, independently of both the original
caller cancellation and the engine stop signal. Receipt waiting does not cancel
that accepted continuation. No transaction context field or storage format was
added.


## UNIQUE cache preparation measurement

Cache staging reuses the selected `UniqueConstraint` claim owner directly,
avoiding a temporary `SchemaManager` for each staged claim. It retains the
same typed label/property identity and cancellation contract.

A paired component comparison on Apple M2 Max, Darwin arm64, 12 Go processors,
used 10,000 iterations per sample in AB/BA/AB order. Both sides included the
same preparation, creation-validation and typed-identity correctness fixes;
only the shared claim-owner implementation differed. Containers and local Qwen
were stopped during the accepted window; other Go activity was monitored.

| Workload | Reference median | Shared owner median | Memory and allocations |
| --- | --- | --- | --- |
| Rebuild four UNIQUE caches from 128 stored nodes | 161,337 ns/op | 139,874 ns/op | 185,610 → 185,193 B/op; 1,998 → 1,997 allocs/op |
| Empty target namespace with another populated database | 5,932 ns/op | 5,913 ns/op (within observed timing noise) | 4,802 → 4,401 B/op; 75 → 72 allocs/op |

Run the native populated component with:

```sh
go test ./pkg/storage -run '^$' -bench '^BenchmarkSchemaRefreshNativePopulated$' -benchmem -benchtime=10000x -count=3 -cpu=12
```

The committed benchmark also checks every resulting claim outside the timed
loop. These measurements qualify this cache-owner change, not total transaction
preparation or end-to-end ingestion. Buffered preparation has a separate workload.

### Buffered acknowledged-generation comparison

The matched public preparation fixture uses real acknowledged peer commits
after BEGIN, with identical typed-identity prerequisites on both sides. Five
alternating fixed30x pairs at12P compare the six-file overlay-admission and
poll-amortization correction. The1-node median is29471→27893ns/op with9299B
and166allocs unchanged; the64-node median is457968→447690ns/op,435886→435888B
and6971allocs unchanged. Timing ranges overlap, supporting no material regression
for these fixtures rather than a precise speedup or whole-ingest claim.

`BenchmarkPublicBufferedEntityCommitPreparation` retains the timed owner and
adds untimed physical/visible node counts, acknowledged operation counts and
natural pending-generation shape assertions. The private conflict-scan benchmark
remains separately available. Samples ran with no other Go/model/container
processes observed; OS background processes were sampled. Separate fixed300x
CPU profiles are coarse and include inherited preparation/abort labels; they
do not identify a universal bottleneck. The previously reported busy-host
comparison is unqualified.
