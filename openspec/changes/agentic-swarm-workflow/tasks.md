# Implementation milestones

The full lifecycle milestones below remain unqualified unless explicitly checked.
The pinned local experiment implements a subset; its setup and actual checks are
documented in `docs/development/swarm-local.md`. A partial implementation does
not complete the larger milestone's acceptance contract.

## Local experiment: model defaults

- [x] Default fresh UI chats to the Nornic local agent and configure this installation accordingly; preserve explicitly saved choices during later setup.

- [x] Name the managed agent Nornic across provider/model labels, settings,
  diagnostics, launchers and docs; retain valid upstream runtime identifiers.
- [x] Reproduce the Codex dropdown choosing Astra despite the harness's saved
  Sol selection; default the shared catalog to GPT-6.1 Sol with low reasoning.
- [x] Verify catalog selection, reasoning defaults and provider/model consumers
  with focused T3 tests; preserve manual Astra selection and its agent-call guard.
- [x] Document that automatic classification starts through the Nornic local agent;
  directly selecting another provider does not automatically classify its requests.

## Local experiment: actionable intent help

- [x] Add read-only authenticated-user Linear issue listing with pagination and
  explicit tool examples, rather than suggesting unavailable operations.
- [x] Clone `tommyvo/useful-prompts` under `~/src` without running its installers.
- [x] Build one cited help corpus from its skills and recursively linked local
  guidance starting at NornicDB's `AGENTS.md`, using the existing document importer.
- [x] Retrieve this help during intent routing and expose explicit help search.
- [ ] Verify mocked boundaries and live read-only Linear/retrieval paths; publish
  the actual ingestion and test evidence on the harness PR.

## 1. Establish integration capabilities

- [ ] Pin upstream T3, Codex and Copilot CLI versions; inventory enabled tools,
  hooks, permissions, session identity and isolated-launch APIs (SW-19/20).
- [ ] Prove trusted T3-to-Nornic session attestation using supported interfaces;
  record any missing upstream capability explicitly before implementation (SW-02).
- [ ] Prove the managed executor can constrain/stop every enabled mutation path,
  including stdin and children; do not mark advisory integration strict (SW-11/19/20).
- [ ] Qualify pinned T3 extension packaging, authenticated MCP service injection
  and existing NornicDB API persistence without custom DB plugins (SW-01/18).

## 2. Canonical service and API contracts

- [ ] Implement typed version-1 registry, common envelopes, schemas, error mapping,
  modes, pure helpers, capability inventory and contract generation (SW-01/17/18).
- [ ] Qualify T3-hosted service, durable compare-and-set receipts, scoped identity
  and atomic command persistence through existing database APIs (SW-18).
- [ ] Qualify the authenticated MCP toolkit against that one service instance;
  advertise only implemented operations and preserve unload semantics (SW-01).
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

## 7. Linear workflow integration

- [ ] Resolve the existing NOR project, states and label groups into private
  configuration; qualify branch automation and identity mapping (SW-24).
- [ ] Implement tracker APIs, unique outcome binding and current criteria/
  dependency snapshots; preserve human assignee vs T3 execution identity (SW-24).
- [ ] Gate source mutation on confirmed Dev In Progress and current scope/criteria;
  invalidate plans and readiness on relevant human changes (SW-24/25).
- [ ] Implement one-writer outbox, read-before/write/readback reconciliation,
  uncertain-response recovery, webhook deduplication and bounded polling (SW-25).
- [ ] Implement the exact status/Delivery mapping and paired non-closing PR links;
  preserve private discussions and final human shipment authority (SW-25).
- [ ] Verify outage/drain, cancellation, human-edit races and label preservation
  using synthetic issues before an explicitly configured live rollout (SW-24/25).

## 8. Verification, documentation and readiness

- [ ] Map every generated API entry to deterministic tests; execute the matrix
  over Bolt, HTTP, embedded and MCP paths with fresh-session state checks (SW-01–18).
- [ ] Run scoped race, memory/disk isolation and full-suite checks appropriate to
  changed code; reproduce any discovered defect before correcting it.
- [ ] Run the two-provider end-to-end scenario including conflict, message,
  expiry, review rework and final remote PR and Linear projection verification (SW-23/24/25).
- [ ] Record before/after workload measurements and regression analysis from
  `verification.md`; resolve measured regressions without weakening assertions.
- [ ] Publish installation/configuration/API examples, provider capability matrix,
  resource tuning and recovery docs; document all incomplete/unsupported paths.
- [ ] Run `make verify BASE=origin/release/v1.4.4` for the implementation PR and
  post checked head/base, command, results and skips. Do not equate a path-filtered
  no-op or documentation-only check with runtime feature verification.

## Local experiment: warm native-context model serving

- [x] Stop local Ollama and remove its requested local model cache; retain the remote router.
- [x] Pin and checksum the Ornith APEX MTP Compact artifact and install llama.cpp.
- [x] Verify the persistent local server, native 256K allocation, tool calls and warm reuse.
- [x] Keep the router at 32K and use serving-model context sizes for agent sessions; leave Codex native defaults untouched.

## Local experiment: delegated coding workers

- [x] Admit only a live T3-owned delegated child on a configured local/remote worker provider in its isolated worktree.
- [x] Use the parent's recorded task without reclassification or further delegation; preserve native permission modes.
- [x] Configure local Ornith and remote Qwen workers separately from default intake.
- [x] Prove rejection boundaries and actual controlled coding work on both servers; document Linear setup steps and limitations.

- [x] Configure LAN binding for both frontend and backend, retain pairing authentication, and verify the operator's advertised address.


## Local experiment: exact completed-intake delivery

- [x] Reproduce a completed Linear read being rewritten as literal tool markup; verify the missing direct-reply stream with a failing regression.
- [x] Deliver terminal workflow outcomes through the supported runtime assistant stream without another inference call; restore native SDK inference for ordinary turns.
- [x] Verify exact runtime output, zero terminal inference, and same-session admission reset; retain native coding-worker checks.
- [x] Publish focused regression and real-runtime fixture evidence with the pinned patch update.

## Local experiment: workflow transport failure recovery

- [x] Reproduce a lost workflow-read response with a controlled HTTP server and retain fail-first evidence.
- [x] Retry only known workflow reads once on transport failure, within the existing request deadline; never replay uncertain writes or database rejections.
- [x] Preserve the service diagnostic in the terminal routing-failure reply without another model call.
- [x] Reproduce Linear rejection of an empty model-supplied cursor and normalize it in the shared adapter; preserve nonempty continuations.
- [x] Preserve router timeout diagnostics through nested errors.
- [x] Verify live workflow storage, remote intake/Linear reads and runtime delivery; record failures and practical limits.
