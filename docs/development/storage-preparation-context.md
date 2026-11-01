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
