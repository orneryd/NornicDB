# NOR-120 / N152-47 tasks

Worktree base (unmodified start): `163cfa2e0e497b464eda5d9473625ad8c125bf54`.
Worker handoff left edits uncommitted/unstaged; parent publication is tracked below.

- [x] Read `AGENTS.md`, `CONTRIBUTING.md`, the standing design principles and the
      duplicate-review skill before changing anything.
- [x] Reproduce the unchanged-graph Adamic-Adar top-K instability through both
      parsers on unmodified production source, using the supplied original test
      files verbatim.
- [x] Re-establish the five passing public matrix owners on the same base
      (single-hub ties, identity permutations, distinct scores/YIELD, boundaries,
      identity/filters).
- [x] Record baseline benchmarks and CPU profiles before any production edit
      (small/medium/high common-neighbour sets, count-only controls, public
      topology owner and public `AdamicAdar`).
- [x] Add deterministic owner coverage that fails on the old accumulation
      (canonical degree order, equal multisets across different hub identities,
      exact top-1 tie selection) and a count-only allocation assertion.
- [x] Correct the accumulation at `PairScore`, the existing shared owner, with no
      parallel implementation and no comparator/formula/tie-policy change.
- [x] Re-run the unit coverage and the full public matrix (both parsers) on the
      fixed source.
- [x] Repeat the workload benchmarks after the edit (same settings, `-count=5`,
      separate CPU profiles) and a controlled before/after count-only A/B.
- [x] Run focused package and consumer verification plus `go vet` on the affected
      packages.
- [x] Source-level duplicate inventory: `PairScore` is the only accumulating
      owner; all consumers delegate to it. Configured code graph unavailable.
- [x] Record `proposal.md`, `evidence.md`, scoped `CHANGELOG.md` entry.
- [ ] Full suite, race, Linux and release verification (parent-owned).
- [ ] Staged duplicate checker (requires staged edits; this task must leave the
      diff unstaged, so it was not run) and indexed graph candidate analysis
      (server handshake timed out).

## Parent integration

- [x] Independently inspect shared owner, callbacks, public consumers and tests.
- [x] Remove avoidable weighted union work and use bounded stack storage with
      growth proportional to actual shared neighbors.
- [x] Exercise stack/growth boundary and large sets, original unchanged-graph
      score bits, public scalar/raw/predict/ensemble paths through both parsers.
- [x] Run final CPU-profiled repeated workloads and document high-degree costs.
- [x] Full linkpredict tests/coverage/race and focused public Cypher race matrix.
- [x] Run configured staged checker; record graph timeout as incomplete.
- [x] Final vet and selected verifier receipts; prepare draft release PR evidence.
- [ ] Full release and indexed duplicate qualification remain outstanding.
