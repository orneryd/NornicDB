# Query analysis and result-cache decisions

`pkg/cypher.QueryAnalyzer` combines cached text-shape hints with the canonical
statement execution contract. Effects come from grammar-owned updating clauses,
the live procedure registry, and the existing schema/administration owners.
Keyword occurrences in text do not independently establish an effect.

## Effect ownership

`Analyze(ctx, statement)` returns an analysis and an error. `Writes()` reports
whether the statement may change state; unresolved procedure effects and text
whose effect cannot be established are conservative writes for optimization.
`ReadOnly()` additionally requires the existing row-returning shape and excludes
schema commands. A registered WRITE or SCHEMA procedure and catalog mutation
cannot acquire a read-only optimization from a misleading text hint.

Cached effect decisions carry the procedure registry generation. A registration
change causes the next analysis to refresh its effect before returning it.
Cancellation is returned on cold analysis and cache hits; a canceled cold
analysis is not inserted into the cache. Syntax diagnostics remain the execution
admission owner's responsibility.

## Cache and routing consumers

Read-only streaming and vector-free reads require the conservative static read
effect. Result caching instead combines the row-returning shape with the bound
execution policy: a runtime statement such as `apoc.cypher.run` is eligible only
when every declared nested statement and parameter override resolves to a
deterministic read. Unknown or writing nested text remains ineligible. Existing
transaction, selection, revision, principal, continuation and decay-policy checks
still apply. The statement completion owner invalidates entries for fixed writes
and for runtime statements whose resolved execution recorded a write; a resolved
runtime read does not discard its newly cached result.

Fabric planners require an effect reader for each executable fragment and
propagate its errors. The Cypher executor supplies the same execution contract;
custom embedders must supply the canonical analyzer, not a separate keyword
classifier. Cached structural plans are re-resolved at admission so a procedure
registration change does not leave an obsolete write flag.

The distinct `pkg/storage.QueryAnalyzer` extracts placement hints and accepts the
caller's already-resolved write flag. Its label/property extraction is not an
authorization or statement-effect owner.

## Authorization and limits

Permissions are enforced by the authorization and procedure-admission owners.
Query-analysis hints do not grant access. An unknown effect disables read-only
optimizations; it does not admit or execute an unknown procedure.

Relevant regression controls include
`TestQueryAnalyzerRefreshesStatementEffectOnProcedureRegistryChange`,
`TestQueryAnalyzerWarmCacheHonorsCancellation`,
`TestCallSubqueryUncorrelatedWriteProcedureRunsPerInputRow`,
`TestSuppressionInvalidatesCachedMatch`, and
`TestRoutingConsumersFollowRegisteredWriteEffect`.
These establish their covered paths, not a blanket guarantee for every custom
procedure implementation or external effect reader. Embedders remain responsible
for accurate procedure declarations and callback behavior.
