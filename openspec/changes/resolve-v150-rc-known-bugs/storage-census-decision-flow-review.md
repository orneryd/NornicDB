# Storage census decision-flow correction

The operator approved this scoped analyzer correction on 11 November 2026.
No runtime storage code, writer ownership exemption, taint source, duplicate
checker rule, or stored format changes as part of this correction.

## Established causes

The typed adjacency owner exposes two existing control-flow shapes in
`BadgerEngine.executeNodeDeletion` to the stored-reader decision analysis:

- The error selected from staged/native readers joins as an SSA Phi. The branch
  returns that exact error alongside the named aggregate result. Treating only
  calls/tuple extracts as propagated errors misclassifies the joined return.
- A label/type bookkeeping guard occurs after the entity-body deletion in a range
  iteration. One successor is already the next-iteration loop header. The old
  reachability helper excluded later back edges but included this initial one,
  falsely attributing the earlier deletion to the later guard.

`reachable(from, start)` now checks the initial edge as well as subsequent edges;
`gatedWrites` and type-context analysis share that owner. Exact error propagation
admits a joined error only under the existing nil/non-nil and exact-return proof.

## Compiler-generated aggregate return

The original proposed load-only proof failed its captured aggregate fixture.
Actual SSA lowers `return r, err` to load/store/load of the same local aggregate
cell. The integrated proof recognizes only that identity self-copy and reads of
the same returned allocation. An error cell, arbitrary pointer read, actual field
mutation, replacement aggregate, another cell's value, call, or defer rewrite
still fails the proof. The exact checked error must remain a returned SSA value.
There is no general permission for stores or effects on an error-return branch.

## Verification

The final new fixtures run against the pre-correction analyzer through a Go
overlay and fail on the joined error, aggregate return and backwards loop gate
(`/tmp/v150-census-final-controls-baseline-red.log`, 0.372 s). The integrated
isolated fixtures and existing propagation/type-context/selector controls pass
under race instrumentation (1.471 s):

```sh
go test -race -tags=noui,nolocalllm ./testing/storagecensus \
  -run '^Test(ErrorPropagation|DecisionFlow|HiddenDecision|BranchCondition|TypeGuard|SelectorImplies|ConditionKnown|MutexField)' \
  -count=1
```

Negative controls reject swallowed/substituted errors, effects on the return arm,
aggregate mutation/replacement/other-cell copies and defer rewrites. Same-iteration
hidden-state body writes and ordinary guarded loops remain unowned decisions.
Shape assertions pin the Phi, aggregate load and initial back edge so the tests
cannot pass by accidentally removing the causal SSA shape.

A wider local selection inadvertently included the full-repository kernel-context
control. It was canceled, is incomplete, and is not counted as a pass. Full census,
mutants, generated-artifact comparison and final integrated verification remain
CI gates. Generated reports must be reviewed from that run; these focused tests
do not prove that the whole census is clear.

## Reviewed generated report refresh

CI run 43086958507 at c78b08ada5fa8c57719855bb3980209869f62fc7 passed
the stored-write assertions and mutant controls; its census failure was solely
the generated-artifact comparison. The complete JSON/Markdown emitted by that
run is copied intact after review, not manually edited. Rules, key spaces,
acquisitions, exemptions, dispositions, pending cases, violations and unordered
sets are identical. Both reports have zero violations/unordered entries.

All 279 writer records and 395 field-write records have identical semantic
multisets after source-line locations are normalized; only those locations
move. Decision records move from eight to ten: namespace-schema reconciliation
is now traced through PublishCommit and validateCommitLocked, replacing the
former nodeConstraintValidation.validate entry. The same suppression owners
remain required. No writer or field-write fact is dropped, and no rule or
exemption is added. Final candidate regeneration and comparison remain a CI
gate; the prior run does not establish a full current-head census pass.
