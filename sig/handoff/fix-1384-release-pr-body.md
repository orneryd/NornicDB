Cherry-pick of #1384 (`159cfe273` → `f418585d4`), with the follow-up fixes from #1468: `928494086`, `7df810ec6`, `4d406da5f`, plus the supporting release API/EXISTS-expansion backport. Current head `c37a11cb4a013a8aea4ddb9b649bd4fb54057109`.

The compiled pattern-predicate owner now keeps the bound one-hop path inside the shared EXISTS matcher. The deleted node-only predicate matcher and executor-local boolean compiler stay deleted. Bound relationships, row-dependent property maps, rows without node bindings and nulls retain the complete-row semantics.

Neo4j 5.26.30 and TCK Pattern1 [10]: named unbound nodes/relationships in a WHERE pattern predicate raise `Neo.ClientError.Statement.SyntaxError`; explicit `EXISTS { ... }` can introduce locals. The formerly failing helper now records that error instead of binding local `c`.

Release compatibility: this branch predates current develop's `boundExpandCount`. The backport adds that helper once and uses it from both EXISTS and the compiled owner, through the release's existing traversal APIs. The release row plan returns bool values and stores row maps directly; those API differences are adapted explicitly.

TrueNAS validation (Go 1.26.9; `noui,nolocalllm`):
- Red on untouched develop, both parsers: `i20261018034726-96532-5686`.
- Focused pattern/EXISTS/WHERE/compiler/helper and row-oracle tests, both parsers: `i20261018035506-92340-20b0`, pass.
- Generated pattern-predicate/pattern/pattern-comprehension families: `i20261018035545-31396-5d8b`, 3,263 cases on each parser and each Bolt route (auto-commit and explicit transactions), zero differences.
- Full `pkg/cypher` on both parsers: running; exact final summary will be added.
- HTTP families and paired A/B: running/queued; exact final results will be added.

The focused scope covers the owner, its bindings and all #1384 regressions. Full package runs are the explicitly requested regression gate. No local workloads or extra CI runs; full make verify is background information under the current testing standard.

- [x] Base release/v1.4.4; bug label; milestone v1.4.4.
- [x] Named-variable semantics checked against the pinned oracle.
- [ ] Full package/differential runs and the within-5% A/B gate complete.

Remain draft until validation finishes. Do not merge: Codex review comes first.