# NOR-82 / N152-149 tasks

Checkout `fix/n152-149-addition-admission` at `18b36a85d10e775b9d5d3500ecf3482682754a8d`
(parent-recorded reproduction source: `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5`;
relevant static validation source unchanged from v1.5.1 `cb0a574f`). Production correction remains pending reference-boundary review. Tests added below
have not been executed; the original fixture remains unchanged.

## Extraction and investigation (this task)

- [x] Read `/tmp/n152-149-ticket.json`, `AGENTS.md`, `CONTRIBUTING.md`,
      `.agents/standing-design-principles.md` and
      `/Users/timothysweet/.codex/skills/nornicdb-duplicate-review/SKILL.md`.
- [x] Recover the ticket's inline `pkg/cypher/nor79_to_82_static_claims_test.go`
      verbatim into `fixtures/nor79_to_82_static_claims_test.go.txt`
      (391 lines, 17241 bytes, sha256
      `ced90f8c9729737ab2b8d81416426a418540bb990143fe852d176f8ee53918c5`).
      No placeholder, reformatting or expectation change: NOR-82 keeps the
      reference-corrected `[]` and `n.j` acceptance controls.
- [x] Confirm every symbol the fixture depends on exists at this checkout
      (`forEachParser`, `newTestMemoryEngine`, `NewStorageExecutor`,
      `storage.NewNamespacedEngine`, `ExecuteResult`, `nornicerrors.Neo4jStatus`,
      `staticTypeNeo4jVerdicts`, `staticConstructorTypesOracle`,
      `staticConstructorTypesPopulation`, `staticConstructorTypesParams`, and the
      oracle's `population`/`populationSha256`/`neo4jRejected`/`inherited` keys),
      so the recovered fixture is not a transcription against a different base.
- [x] Extract the recorded NOR-82 cell verdicts from the ticket's parent log:
      48 cells, 24 under-rejections (`date()`, `duration('P1D')`) and 24 matching
      acceptance controls (`[]`, `n.j`), identical in both parsers and both
      transaction modes for RUN/EXPLAIN/PROFILE.
- [x] Separate the sibling population: NOR-79/NOR-80/NOR-81 contribute 36
      failing and 60 matching cells in the same 144-cell matrix, all of them
      false rejections; they are not part of this card.
- [x] Source-level census of the existing canonical static operand/scope
      admission owners and of the `head`/`toUpper` consumer chain (see
      `proposal.md` and `evidence.md`); no parallel checker is implied.
- [ ] Configured code-graph candidate analysis: the `nornicdb`/`nornicdb_docs`
      MCP servers failed their handshake (30 s timeout), so no indexed
      duplicate/reuse result exists. Incomplete, not a clean pass.
- [ ] Staged Git duplicate checker: parent ran it on staged edits; analysis
      remained incomplete due to `/db/nornicdbcode/tx/commit` URL timeout.
      See `/tmp/n152-149-duplicate.log`; manual source review is not an indexed pass.
- [ ] Original 64 inherited H2 false-accept cases: not supplied inline and no
      canonical population of that size exists in the repository. Unrecovered;
      blocks whole-issue acceptance, not this card's probe family.

## Correction and tests (bounded implementation)

- [x] Accept the design: decide whether the reference verdict is reached in the
      `+` result type (`checkSingleOperator`/`additionResultTypes`) or in the
      `head` union read (`elementOperand`/`listPart`) plus its consumer, with no
      second type checker and no broad temporal/arithmetic rewrite.
- [x] Add the NOR-82 probes to an existing canonical verdict owner so the
      rejection is pinned in source rather than only in the recovered fixture
      (natural candidates: `staticTypeNeo4jVerdicts` in
      `static_types_neo4j_verdicts_test.go`; the `+` branch table in
      `TestCheckOperatorBranches`, `static_operator_types_branches_test.go`).
- [x] Keep the acceptance controls green in every cell: `n.k + []` and
      `n.k + n.j` through `toUpper(head(...))`, both parsers, auto/explicit,
      RUN/EXPLAIN/PROFILE, zero rows on RUN.
- [ ] Prove the public path: the rejected statement must reject before data is
      read in RUN, EXPLAIN and PROFILE, and the accepted controls must still
      execute with an empty row set from `UNWIND []`.
- [ ] Re-establish that no sibling NOR-79/NOR-80/NOR-81 cell changes verdict as
      a side effect.
- [ ] Do not generalize to other uncertain-string consumers until the original
      64-case family is recovered.
- [ ] Install the recovered fixture in `pkg/cypher` only with the parent's
      acceptance (this card deliberately leaves it as a `.go.txt` artifact so no
      package build is changed).

## Parent integration (unchecked)

- [ ] Fresh reference measurement or reuse of the recorded one, on the integrated
      head, including the exact message class per probe.
- [ ] Focused `go test -run '^TestNor79To82' ./pkg/cypher`, vet, and the
      applicable `make verify BASE=origin/<pr-base>` checks.
- [ ] Indexed duplicate review on the staged diff once the graph is reachable.

## Bounded implementation follow-up

- [x] Add `TestStaticUncertainAdditionAdmission`, reusing canonical verdicts and
      `requireStatus`, for both parsers, auto/explicit transaction and
      RUN/EXPLAIN/PROFILE. Tests are written, not executed.
- [x] Inspect the recorded reference log and constructor oracle population:
      neither establishes reverse Duration-plus-unknown or direct consumer types.
- [x] Obtain reference verdicts for those boundaries before modifying the shared
      unknown-addition table or its existing reverse-duration expectation.
- [x] Run fail-first focused test: 24 missing rejections, 24 accepted controls;
      `/tmp/n152-149-fail-first.log`, exit 1, 1.191s after compilation.
- [ ] Implement accepted correction in the existing producer owner and reverify.

- [x] Implement accepted producer correction (no consumer change); preserve known
      temporal arithmetic and add both-orientation branch controls.
- [x] Extend canonical reference verdict owner to 42 queries from the 90 fresh
      reference cells, excluding the unrelated labels diagnostic family.
- [x] Expand focused public test to 504 cells (42 queries × both parsers × both
      transaction modes × RUN/EXPLAIN/PROFILE). Not executed yet.
- [x] Add release changelog entry.
- [ ] Run green focused suite in parent-approved CPU window, including unchanged
      proof/constructor/property oracle populations and static function/operator
      families. No population/hash expectations changed.

- [x] Run first integrated static selection: failed only exact union ordering;
      preserve `/tmp/n152-149-focused-green.log` (34.280s).
- [x] Correct existing canonical ordering owner for `List<Any>`; add mixed union,
      immutable input, element composition and unknown-order controls.
- [x] Rerun integrated selection after ordering correction: PASS 37.238s,
      `/tmp/n152-149-focused-final.log`; 34 top-level tests and the 504-cell
      focused public matrix. The first integrated run remains a recorded failure.


## Final bounded verification

- [x] Normal static/operator/compile-time selection passed, 37.238s.
- [x] Focused race selection passed, 9.036s: uncertain-addition public matrix,
      canonical Any ordering, operator branches and canonical Neo4j verdicts.
- [x] `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher`
      passed with empty output.
- [ ] Parent-owned `make verify-list` / `make verify` and integrated release-head
      qualification; no full-suite or newer-release-head pass is claimed here.
- [ ] Original sibling fixture verdict-delta rerun remains unperformed; existing
      static oracle families passed, but that is not the same population.
