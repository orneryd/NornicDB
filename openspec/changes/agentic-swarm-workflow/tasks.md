# Implementation milestones

All tasks below are unimplemented. Drafting or merging this specification does
not complete an implementation task. Keep checkboxes unchecked until its public
contract and acceptance evidence are verified.

## 1. Establish integration capabilities

- [ ] Pin upstream T3, Codex and Copilot CLI versions; inventory enabled tools,
  hooks, permissions, session identity and isolated-launch APIs (SW-19/20).
- [ ] Prove trusted T3-to-Nornic session attestation using supported interfaces;
  record any missing upstream capability explicitly before implementation (SW-02).
- [ ] Prove the managed executor can constrain/stop every enabled mutation path,
  including stdin and children; do not mark advisory integration strict (SW-11/19/20).
- [ ] Confirm plugin packaging and shared-service injection without import cycles
  or local inference/native model dependencies (SW-01/18).

## 2. Canonical service and API contracts

- [ ] Implement typed version-1 registry, common envelopes, schemas, error mapping,
  modes, pure helpers, capability inventory and contract generation (SW-01/17/18).
- [ ] Implement database-scoped service, storage transaction guard, durable
  receipts, role-scoped context and standalone-autocommit invocation guard (SW-18).
- [ ] Wire both plugin adapters to that same instance; register explicit external
  actions and preserve collision/unload semantics (SW-01).
- [ ] Apply and verify embedding exclusions to coordination/delta records before
  their first creation (SW-21).

## 3. Ownership and execution

- [ ] Implement sessions/epochs, batched renewals and close/drain behavior (SW-02).
- [ ] Implement complete resource overlap, atomic set claims, replacement,
  generations, expiry, release and accepted handoff (SW-09/10).
- [ ] Implement one-use action permits, execution fences, hash/scope checks,
  receipts and unresolved-action reconciliation (SW-11).
- [ ] Prove crash/restart and transport-loss behavior from fresh public sessions;
  ensure no partial grants or replayed side effects (SW-02/09/10/11/18).

## 4. One corpus and worktree resource management

- [ ] Extend the existing importer/ref-hook owner to honor the active release ref,
  serialize refresh, coalesce updates and report partial-import readiness (SW-07).
- [ ] Implement unembedded changed-file/symbol deltas and context masking with
  explicit freshness/completeness (SW-08/21).
- [ ] Implement isolated worktree lifecycle and compatible protected dependency
  reuse, including the enumerated shared-workspace exceptions (SW-06).
- [ ] Integrate bounded host-resource admission and shared embedding budgets;
  preserve existing model lifetime and memory-policy ownership (SW-22).
- [ ] Prove unchanged corpus/no-op refresh and multi-worktree embedding/model/index
  counts, plus preservation of unrelated graph/coordination records (SW-07/21).

## 5. Full autonomous development lifecycle

- [ ] Implement tasks, assignment, dependencies, versioned plans and phase-driving
  next/advance operations (SW-03/04).
- [ ] Implement evidence provenance, invalidation and policy-checked reuse for
  tests, documentation and performance (SW-12/22).
- [ ] Implement pause/resume/cancel, checkpoint preservation and drain/release (SW-05).
- [ ] Implement freeze/pending changes, independent review and reacquisition for
  rework (SW-13/14).
- [ ] Implement publication intents, GitHub receipt reconciliation and exact-head
  review readiness; preserve merge/release boundary (SW-15).

## 6. T3 communication and provider delivery

- [ ] Route claim conflicts and lifecycle notifications through T3 owner IDs and
  messages; implement bounded delivery receipts and event deduplication (SW-16).
- [ ] Wire automatic workflow progression, safe isolated coding launch and
  read-only reviewer delegation through upstream T3 (SW-04/06/14/23).
- [ ] Complete Codex adapter and public enforcement acceptance on supported OSes
  with hooks trusted and all enabled paths covered (SW-19).
- [ ] Complete Copilot CLI ACP adapter and the same acceptance matrix; distinguish
  it from VS Code and SDK-only hook claims (SW-20).

## 7. Verification, documentation and readiness

- [ ] Map every generated API entry to deterministic tests; execute the matrix
  over Bolt, HTTP, embedded and MCP paths with fresh-session state checks (SW-01–18).
- [ ] Run scoped race, memory/disk isolation and full-suite checks appropriate to
  changed code; reproduce any discovered defect before correcting it.
- [ ] Run the two-provider end-to-end scenario including conflict, message,
  expiry, review rework and final remote PR verification (SW-23).
- [ ] Record before/after workload measurements and regression analysis from
  `verification.md`; resolve measured regressions without weakening assertions.
- [ ] Publish installation/configuration/API examples, provider capability matrix,
  resource tuning and recovery docs; document all incomplete/unsupported paths.
- [ ] Run `make verify BASE=origin/release/v1.4.4` for the implementation PR and
  post checked head/base, command, results and skips. Do not equate a path-filtered
  no-op or documentation-only check with runtime feature verification.
