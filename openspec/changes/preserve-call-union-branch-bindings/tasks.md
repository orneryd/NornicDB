# CALL UNION branch bindings (NOR-25)

- [x] Reproduce report29B on release f6ccb2539 under both parsers; preserve fail-first log.
- [x] Confirm rows and scalar/node shadowing against Neo4j Community5.26.30 on disposable data.
- [x] Use canonical compiled Incoming/Retained facts for branch execution and static types.
- [x] Preserve parameters, scoped imports, retained values and existing implicit-import behavior.
- [x] Share eight regression shapes with the public route corpus (451 cases).
- [x] Pass affected CALL/UNION scope race checks, public Execute/Bolt/HTTP auto/explicit matrix, and transaction-parameter controls.
- [x] Independent source review; retire the unused branch-local classifier and its two helper-only assertions.
- [x] Apply approved exact inventory retirement; missing-entry and false-owner controls reject; restored source passes.
- [x] Fresh focused compilation after dead-code retirement; affected-package vet.
- [ ] Paired affected-CALL allocation/timing sample with no other local tests running.
- [ ] Exact-head CI, duplicate dispositions, PR/Linear evidence and readiness cleanup.
