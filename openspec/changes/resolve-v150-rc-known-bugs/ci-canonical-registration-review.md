# Final-head CI: canonical registration and refusal contracts

CI run 43211473937 at 8bae3f63effca7a09b0c994ae1c6b824e1f8c3db found two
Cypher package failures. This correction changes tests only; product runtime,
benchmarked source, stored formats and checker/census rules remain unchanged.

## Executable-input binding

The source inventory understood name-first/handler-last registrations, while
production's canonical registerProcedure takes ProcedureSpec, handler and policy.
Without the actual name-to-handler binding it loses the factory's immutable
export declaration. Both query and graph branches then remain conservatively
possible, so graph export incorrectly requires executable Cypher input metadata.

The operator explicitly approved this scoped test-analyzer correction after a
remote-readable explanation on 2026-11-12. Only the exact three-argument call with
a literal ProcedureSpec and string-literal Name binds argument 1 as its handler.
The existing factory/constant/write-provenance owner then decides reachability.
Unknown specs, names and declarations stay conservative; no export exemption or
invented CypherInputs is added, and the minimum discovery count remains 19.

Fail-first: adding the new provenance cases to the old analyzer fails exactly
canonical_graph_declaration (expected data-only, reported executable). After
correction all nine old/canonical graph/query/mutation/unknown cases pass. The
complete live registry still detects 19 executable-input procedures, including
apoc.export.csv.query and apoc.export.json.query, excluding graph-only exports.
Existing distinct-binding and mutable-binding rejection controls also pass.

## Composite refusal precedence

A data procedure now rejects schema operations before a second writer can be
admitted. The old composite test expected the later multi-database-write error.
The correction asserts the actual typed PermissionDeniedError with permission
schema; it preserves zero writes in both databases and now additionally asserts
unchanged schema in both. The direct fulltext schema case still asserts the
multi-writer refusal; periodic cases retain their independent-batch refusal.
No runtime gate is weakened to obtain the previous error text.

## Verification

The two failures are preserved in /tmp/v150-ci-watch-8bae-cypher-failure.txt.
The new fail-first control is /tmp/v150-canonical-registration-failfirst.log.
The integrated command passes in 1.404s:

go test -p 1 -ldflags=-w -tags=noui,nolocalllm ./pkg/cypher
  -run '^(TestExecutionInputRegistryInventory|TestExecutionInputInventory|TestCompositeRefusalsFailTheirStatement|TestStatementProcedure)' -count=1 -v

Raw output: /tmp/v150-canonical-registration-integrated-controls.log.
Final-head CI must repeat after this test-only commit; earlier-head passes are
not a full current-head result. The remaining old-head shards are allowed to
finish so additional failures are not discarded.
