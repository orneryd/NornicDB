Follow-up to #1384. Compile pattern predicates once in their shared owner and retain the bound-hop shortcut through the EXISTS owner's matcher. Static validation, row evaluation and compiled binding filters share the same named-variable rule. Bound relationships, row-dependent property maps, rows without node bindings and null bindings retain the complete-row behavior.

Neo4j 5.26.30 and openCypher TCK Pattern1 [10] reject a named unbound node or relationship with `Neo.ClientError.Statement.SyntaxError`; explicit `EXISTS { ... }` allows local variables and anonymous endpoints are valid. The internal helper now records the same error instead of introducing a local variable.

Validation on TrueNAS (Go 1.26.9, `noui,nolocalllm`):
- Red on untouched develop, both parsers: `i20261018034726-96532-5686`.
- Focused pattern/EXISTS/WHERE/compiler and row-oracle tests, both parsers: `i20261018035455-53504-03bb`, pass.
- Full `pkg/cypher`, both parsers, four shards: running; results will be added before ready.
- Generated pattern-predicate, pattern and pattern-comprehension families: running against pinned Neo4j.
- Paired 10-round A/B on dedicated bench CPUs against develop immediately before #1384 (`524b71ba3`): queued; results will be added before ready.

The focused filter covers the changed owner, all #1384 cases, binding adapters and expression oracle corpora. The full package runs are the explicitly requested regression gate. Formatting is clean. Full make verify is not run per the current testing standard; no local workloads or extra CI runs.

- [x] Base: develop; bug fix shipping in milestone v1.4.4.
- [x] Red/green regression tests and Neo4j semantics checked.
- [ ] Full package runs and paired A/B complete; draft until then.

Do not merge: Codex review comes first. Release copy: #1459.