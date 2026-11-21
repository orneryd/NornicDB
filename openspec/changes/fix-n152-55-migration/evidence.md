# Verification and ownership evidence

Base: `1a6eb031471688a71966a68147740ead6c43d6cb` (`origin/release/v1.5.2`).

## Failure before correction

`GOMAXPROCS=2 go test -p=1 -tags=noui,nolocalllm ./scripts/migration/neo4j -run TestMigrationCompletedGraph -count=1 -timeout=3m`

The initial regression executed the existing Go-generated node, edge-create and edge-property statements, followed by the actual README promotion for Person and Employee. Both parsers failed the completed-workflow read: expected Alice and Bob under Person, got zero rows. Runtime 0.640s, exit 1. Preserved local log: `/tmp/n152-55-baseline.log`. An earlier compile attempt had a test import-name collision with the script's config type; it was corrected before this behavioral reproduction.

## Focused corrected verification

`GOMAXPROCS=2 go test -p=1 -tags=noui,nolocalllm ./scripts/migration/neo4j -count=1 -timeout=3m`

Pass, 0.638s. TestMigrationCompletedGraph uses the actual embedded shared queries and both published promotion/cleanup snippets through Execute under both parsers. The Go client result consumer checks every batch. It verifies two nodes, a multi-label node, three relationships (parallel edges with distinct IDs and a self-loop), all endpoint/properties, both source labels, retained stable migration identity, removal of only label-list bookkeeping, and a second completed-workflow run updating properties with raw whole-graph counts unchanged. Missing endpoint returns zero applied and becomes a client error. Genuine internal nodes remain hidden.

TestMigrationBatchResultErrors verifies missing count, partial count, wrong count type and Run/Single/Consume failures; failures never return success counts.

`python3 -m unittest discover -s scripts/migration/neo4j -p test_migrate.py -v`

Pass, three tests: actual shared template loading, result consumption/count checking and errors, edge-loop progress versus dry-run/no writes. Sessions are mocks; driver names are import stubs.

`node --test scripts/migration/neo4j/migrate.test.mjs`

Pass, three tests: actual shared template loading, count and driver-error handling, edge-loop query dispatch and dry-run/no writes. Uses installed neo4j-driver 5.x with mock sessions; no network service. Temporary validation installation is outside the repository.

`node --check scripts/migration/neo4j/migrate.mjs` and `git diff --check`: pass.

## Consolidation and duplicate review

Compared all three existing generators and their callers at the base above. They emitted the same node MERGE and separate relationship CREATE/property SET semantics. `queries.json` now owns those destination operations once; the Go client embeds it and Python/Node load that same file. All former edge-property generators/calls and the unused Python addLabels placeholder are removed. The remaining small type-quoting adapters and result consumers bind distinct language SDK interfaces (Go ResultWithContext, Python Result, JavaScript Promise result); they do not contain alternative Cypher behavior.

The existing staged checker excludes `scripts/*` by its unmodified DEFAULT_IGNORE. Its result for this scope is a no-op, not a completed graph semantic search. Manual review establishes the migration query consolidation above; graph-wide duplicate analysis remains unexecuted for this excluded scope.

## Limits

No live Neo4j-source import, Bolt driver transport, source keyset pagination, schema DDL, full suite or performance benchmark is established by these tests. Applied counts mean rows matched/upserted, not newly created entities; multi-label source scans can revisit a node. The corrected counted MERGE statements make no claim to the prior CREATE-only hot-path timing.

`make verify-list BASE=origin/release/v1.5.2` selected localization, UI, llama-libs, build, tests, allocations, conformance and differential for Go changes; docs becomes applicable with the documentation correction. Parent owns the final canonical verifier run and records its explicit resource/dependency skips. An unrelated storage test was active on the Mac, so this subtask ran only resource-limited focused checks.

Historical hidden `_Migrated` records are not repaired or reused; the documented operation starts with a fresh target or a separately planned recovery. No fixed-population repair code or global visibility change was added.

## Independent parent verification

The parent reviewed all changed clients, shared statements, documentation and tests. Fresh parent Go tests passed in 0.613s; the three Python tests and three Node tests passed independently. The temporary root Node dependency symlink was removed afterwards.

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./scripts/migration/neo4j -count=1 -timeout=3m` passed in 2.146s. The documentation corpus manifest includes only the changed migration guide entry; incidental generated census hash churn was excluded.

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY='localization docs'` passed the selected localization and documentation build/contract checks (7s and 53s). UI, native libraries, build, full test groups, allocation benchmarks, conformance and Docker differential checks were explicitly unselected. These remain unverified, not passing: this is bounded source/clients/docs qualification while an unrelated storage test job is active, not full release qualification. Logs: `/tmp/n152-55-verify.log` and `.verify/runs/20261121-164103/`.
