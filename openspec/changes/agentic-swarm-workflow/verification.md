# Verification and acceptance plan

This is an implementation acceptance plan, not a claim that these tests exist
or pass. The planning PR's actual checks belong in its PR verification comment.

## 1. Deterministic test rules

- Use an injected clock and explicit synchronization barriers, not timing sleeps
  or scheduler luck. Assertions cover values, ownership, generation, persistent
  state, event counts, absence of partial writes and fresh-session observations.
- Use isolated temporary directories, databases, ports, Git configs and fake
  external transports. Restore environment/settings with cleanup. Never mutate
  user worktrees, shared test globals, model caches or real GitHub/T3/Linear state.
- Concurrency tests launch known competing requests behind a barrier and join all
  workers. No leaked fibers, subprocesses, locks, streams or temporary disks.
- Real-provider acceptance is opt-in with dedicated fixtures/accounts. Unit tests
  use deterministic adapters and never spend inference/API credits.
- Run memory, race, disk and full-suite compatibility checks appropriate to the
  changed paths. A narrow helper test is not proof of the public workflow.

## 2. Contract matrix

| Test ID | Operations / contract | Required proof |
| --- | --- | --- |
| SW-01 | capabilities, project.configure/get, all registrations | Exact inventory, signatures, role checks, database scope, duplicate names, atomic startup and unload |
| SW-02 | session.open/heartbeat/close | Attested T3 binding, new epoch on restart, batched renewal, no resurrection, secret-free results |
| SW-03 | task.create/get/list/assign/dependencies | Stable criteria, acyclic dependency graph, assignment without implicit claim transfer, isolation and pagination |
| SW-04 | plan.put, workflow.next/advance | Every legal/illegal phase edge, exact missing obligations, plan version races, no skipped lifecycle |
| SW-05 | task.pause/resume/cancel/observeClosure | Drain/release order, patch preservation, unresolved effects, verified external closure only |
| SW-06 | workspace.prepare/record/retire | Correct base and T3 binding, dependency fingerprints, no dirty cleanup, launch retry ambiguity |
| SW-07 | code.status/refresh/refreshBegin/refreshFinish | One writer, queued coalescing, no worker/main overwrite, mixed-import readiness and retry |
| SW-08 | code.context/overlay | Exact revision labels, changed/deleted masking, bounded extraction, no full-worktree corpus |
| SW-09 | claim.acquire/inspect/replace | Exactly one concurrent winner for file/symbol/directory overlap; zero partial grants; cross-target/repo scoping |
| SW-10 | claim.release/handoff/accept | Quiescent transfer, destination prerequisites, generation fencing, expiry and pending-change visibility |
| SW-11 | action.authorize/begin/finish/get | One-use permits, exact scope/hashes, duplicate replay, actual effects, lost response and draining actions |
| SW-12 | evidence.record/list | Trusted provenance, failed/skipped results, hash binding, source changes and justified reuse |
| SW-13 | change.freeze/rework | Atomic freeze/release, no active writes, supersession, required reacquisition |
| SW-14 | review.request/record | Separate reviewer identity, new T3 round, exact revision, unresolved findings block |
| SW-15 | pr.prepare/record | Remote head/base checks, draft vs ready, lost response recovery, no merge authority |
| SW-16 | signal.send/pending/recordDelivery, events.read/status | T3 route, permission ceilings, event deduplication, unknown delivery, bounded records |
| SW-17 | resourceKey/resourcesOverlap | Pure deterministic helpers, path segment/case/rename behavior, ambiguous symbols rejected |
| SW-18 | All W operations over Bolt/HTTP/embedded/MCP | Matching durable results, modes, auth, autocommit-only guard, rollback on failed commit and fresh-session reads |
| SW-19 | Codex execution profile | All enabled write paths blocked before effects without prerequisites, then successful permitted work |
| SW-20 | Copilot CLI ACP execution profile | Same assertions as Codex; test actual ACP hooks/tool filtering, not only SDK mocks |
| SW-21 | Single corpus / embedding policy | Zero embeddings for coordination/deltas, no duplicate model/index/DB, unchanged content remains unchanged |
| SW-22 | Resource scheduler / performance | Bounded bytes/jobs, pressure pause, live coordination, exclusive benchmark admission, correct measured workload |
| SW-23 | End-to-end lifecycle | Two providers, Linear intake/projection, conflict/message/handoff, validation/docs/tuning/review/rework, final PR receipt |
| SW-24 | tracker.configure/bind/refresh/get | Exact existing mappings, unique outcome binding, authenticated snapshots, criteria/dependency invalidation, distinct human and execution identities |
| SW-25 | tracker.planUpdate/pending/recordUpdate | Evidence-derived transitions, one writer, human-edit races, unknown-response reconciliation, preserved labels, outage gates and bounded retries |

Each API registry entry must map to at least one matrix row and each row to
implementation test names before the corresponding milestone is checked off.
Enumerate every operation from the actual registry; manual happy-path selection
does not establish complete API coverage.

## 3. Execution-boundary fixtures

For each supported provider and OS, enumerate the actual enabled tool inventory
and run missing-prerequisite and permitted variants of:

- Direct file patch/write, shell redirection and a nested interpreter write.
- An interactive shell accepting further stdin after its initial tool request.
- Formatter, generator and test command that attempts a source-file mutation.
- Git checkout/reset/rebase/commit, branch publication and PR creation.
- A dependency install through a link into shared artifacts.
- MCP filesystem mutation and provider-native/T3 child execution.
- Session replacement, hook disabled/untrusted, hook/gateway timeout and T3 outage.
- Claim expiry during a running process, including descendants; prove write
  revocation/quiescence before another owner can execute against that scope.

Use filesystem witnesses and process state, not only returned denial text.
No fixture may leave an unclaimed source edit behind and still count as a pass.
A runtime that fails coverage is unsupported until the exact gap is fixed via
supported configuration/integration or a separately approved upstream change.

## 4. End-to-end acceptance story

1. Start NornicDB with existing APIs and the T3 workflow extension; bind upstream
   T3 and authenticated Codex/Copilot CLI fixture sessions. Bind a synthetic Linear
   Todo issue with criteria and dependencies using the observed NOR mapping.
   Confirm Dev In Progress before the first source mutation.
2. Index one release commit once. Launch two separate worktrees sharing protected,
   compatible dependencies. Count databases/models/indexes/embedding jobs.
3. A claims a file. B requests a contained symbol and receives A's task/reason/T3
   identity before any source edit. B asks a question through T3 and works on
   disjoint code while waiting.
4. A completes implementation, tests, docs and applicable measured tuning. Freeze
   releases its claims atomically. B sees the pending change and reconciles its
   dependency on A's interface.
5. An independent reviewer finds a defect. A reacquires ownership under a new
   generation, fixes it, repeats affected checks and freezes a new revision.
6. Publish the branch and create a review-ready PR against the active release
   target. Verify the actual remote head and attached evidence. Reconcile the
   appropriate Test Complete/Delivery projection and paired PR links without
   claiming Merged or Released. No merge occurs.
7. Repeat a failure variant with A disconnected during execution and verify
   draining/epoch fencing; repeat release-ref advancement and partial ingestion
   failure without creating another corpus or admitting stale graph evidence.

Repeat tracker failure fixtures with human criteria edits, cancellation,
duplicate outcomes, dependency changes, stale webhooks, rate limits, lost write
responses and manual status/label changes. Verify that private discussion never
enters GitHub publication, unrelated labels survive, and no outage permits new
source writes or false readiness. Run fixtures against a fake tracker by default.

## 5. Workload-scoped performance evidence

Do not adopt latency/throughput promises without measurements. Establish the
same workload before and after implementation and report correctness alongside
speed. Use 1, 2, 4 and 8 workers (higher counts are opt-in within host limits),
1/16/128 resources per claim, and disjoint/hot-file/file-symbol overlap workloads.

Report:

- p50/p95/p99 acquisition, denial, renewal, release and execution-gate latency;
  successful operations/s and exact contention outcomes.
- CPU, RSS/peak memory, Go allocations, native/model memory where measurable,
  bytes written, durable commits/fsyncs and queue depth.
- One-session renewal overhead versus resource count; retained terminal records
  and event polling/output sizes; idle swarm I/O.
- Canonical no-change refresh, one-symbol edit, rename/delete and a larger release
  diff: extraction/import time, content updates, embedding jobs, peak memory and
  stale/readiness duration.
- Startup of additional worktrees: dependency storage reused, private disk delta,
  zero full-corpus embedding, and actual per-worker memory growth.
- Baseline/candidate code benchmarks on pinned SHAs, identical fixtures/toolchain,
  named hardware, iteration/warmup policy and background workload. Apply the
  repository's relevant regression thresholds; do not substitute unrelated tests.

Benchmarks run outside ordinary deterministic unit-test timing assertions.
Counts and memory/resource bounds that are deterministic can be unit-tested;
wall-clock performance is reported with variability, never a flaky sleep-based
unit-test threshold. No performance improvement is claimed by this docs-only PR.
