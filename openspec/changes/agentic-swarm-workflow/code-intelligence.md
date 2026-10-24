# One release corpus, isolated worktrees, bounded local resources

## 1. Canonical source of code knowledge

The project selects one immutable commit from its configured active release ref
(`refs/heads/release/v1.4.4` initially). That commit is the canonical indexed
corpus in the existing configured code database, such as `nornicdbcode`.
All agent worktrees share this database and its vector indexes. The choice is
project configuration, not an inference from whichever checkout runs a command.

Use the existing `scripts/code_intelligence.py` and `scripts/graphify_local.py`
incremental importer. Extend their canonical ref selection rather than installing
a competing pipeline. The existing `main` hooks must honor the project-selected
ref when swarm mode is configured, so a main update cannot overwrite release
knowledge. Unconfigured projects retain their current behavior. Never ingest a
worker's HEAD through a post-commit hook into the shared release corpus.

Resolve the ref once to a commit, then extract that immutable archive outside
agent worktrees. One per-project refresh owner serializes writes. A newer release
head observed during refresh becomes the next desired commit; coalesce duplicate
requests and superseded queued work. The running job's content cannot silently
change underneath it. Observe release changes through existing local ref hooks
and a low-frequency reconciliation of the configured fetched ref; do not create
a separate poller per agent or make every query fetch GitHub.

Ref observation and Git fetch are harness responsibilities. A remote change is
not magically known before fetch. Report `lastRefObservationAt`, observed commit,
indexed commit and lag. Default reconcile interval: 60 seconds while a project
is active, configurable; one scheduler owns it. Sleep or disconnect preserves
the last known state and explicitly stale status.

## 2. Incremental refresh without a duplicate graph

Reuse `(repo, importer id)` ownership and content hashing. Refresh changed code
properties/edges; keep unchanged vectors. Remove stale importer-owned edges and
nodes only within the selected repository. Coordination nodes, claims and task
deltas are outside importer deletion selectors. Reuse the existing extraction
deletion guard; an incomplete extraction cannot become an authoritative empty
repository. Large bodies stay in the existing code representation, not copied
onto each task or message.

The current importer can write partial batches before its successful commit
marker. The swarm contract therefore adds explicit `refreshing`/`failed` state
before the first batch and publishes `indexedCommit` only after complete
structural validation. During that interval, canonical graph reads used for
phase prerequisites return `INDEX_NOT_READY`; they must not serve a mixed graph
as the previous successful revision. Existing claims, local file reads, test
execution and messaging can continue where they do not require a fresh graph.

Do not stage a second full embedded graph for atomic swap. Retrying a failed
refresh converges through the existing hash/idempotency mechanism. Persist
the failed job ID, target commit and cause. The indexer resumes that owner's
job or explicitly supersedes it with a complete newer refresh; it never labels
partial data ready. Failed readiness is visible until successfully repaired.

Structural readiness and vector readiness are separate:

| State | Permitted use |
| --- | --- |
| Structural refresh active/failed | No claim that canonical impact/search is complete; reads of local Git source remain usable |
| Structure ready, embeddings queued/paused | Exact symbol/path lookup and graph traversal; hybrid search reports vector coverage/lag |
| Structure and required embeddings ready | Semantic retrieval with model/policy generation and bounded-search limitations |

Locks never depend on embedding availability. Exact file claims can proceed
without vectors; an explicit impact obligation requiring graph context waits
for structural readiness. Semantic duplicate checks follow their own configured
readiness rule; never call a partial search a passed exhaustive analysis.

## 3. Worktree deltas, not worktree corpora

Each worktree keeps ordinary source files locally. Store only a delta manifest:
base commit, current tree/head, changed/new/deleted paths, source hashes, changed
symbol IDs/spans, selected changed dependency edges and patch/Git references.
The default performs **zero persistent worktree embeddings**. Task reasons,
plans, logs and review chatter are also excluded from embedding admission.

Give every coordination/delta node a common `SwarmRecord` label in addition to
its type label. Reuse the existing database setting
`db.nornic.embedding.labels.exclude` (or deployment configuration
`NORNICDB_EMBEDDING_LABELS_EXCLUDE`), preserving other exclusions and adding
`SwarmRecord`. This setting currently requires process restart; installation
must apply it before service readiness, not assume a live mutation took effect.
Read back the effective policy before admitting the first task. Code searches
must also filter to importer-owned canonical code labels/repository; excluding
embedding alone does not exclude BM25 results. This uses the existing policy
owner rather than inventing an unenforced `noEmbed` property.

When serving code context:

1. Return canonical matches tagged with indexed commit and generation.
2. For the requesting task, mask canonical symbols whose paths/symbols were
   changed or deleted; consult its local source references and lexical/structural
   delta metadata. New unembedded symbols are explicitly identified.
3. Include other tasks' claims and frozen pending-change references as coordination
   context, without merging their unreviewed source into canonical truth.
4. On hash/revision disagreement, require targeted source inspection and scope
   refresh. Never assume a release-branch body is the agent's local body.

Delta extraction is limited to changed files. Do not run a whole-repository
extractor in every worktree just to calculate a delta. Languages without a
supported symbol extractor use file-level deltas and claims with a clear
capability flag. Source spans are revision-specific; ambiguous renames use
old/new file claims rather than guessed symbol identity.

If the canonical release advances beyond a task's base, compare the Git diff
between those commits and its declared source/dependency inputs. Affected tasks
must rebase/reconcile and invalidate relevant evidence. Unaffected tasks may
continue with an explicit unchanged-input proof. If that proof is incomplete,
block the affected transition and refresh. No full historical embedded corpus
is retained for each active worktree; Git holds source history.

Task deltas are retired after merge/closure and retention. Pending changes remain
until superseded, merged or explicitly abandoned. Query embeddings, if needed,
go through the one existing server provider and budget; no agent loads its own
embedding model for this integration.

## 4. Worktree and dependency reuse

Create linked Git worktrees from a pinned release commit. Git already shares
its object store. Never copy the entire repository, its vector DB, models,
dependency caches or original checkout's build output for each worker.

Dependency sharing is allowed only after comparing lockfiles/manifests,
toolchain/runtime versions, OS/architecture/ABI, relevant build flags and
installation configuration. Record that fingerprint in workspace readiness.
Resolve linked paths against the original tooling folder; never follow links
into another worker's writable source. Prefer a stable versioned shared cache
when the original folder changes often.

| Resource | Default sharing contract |
| --- | --- |
| Git objects | Native linked-worktree sharing; index and working tree remain private |
| Go module cache | Existing module cache; use its supported locking/integrity behavior |
| Go build cache | Existing toolchain-managed cache; build outputs/temp directories private |
| JS package store | Reuse a package-manager content store; link installed dependencies only for matching fingerprint and no consumer mutation |
| Python environment | Reuse original environment only when immutable and fingerprint-compatible; otherwise provision a scoped environment from shared download cache |
| Models and native libraries | One server model owner; shared immutable compatible library/model artifacts |
| Test DBs, coverage, generated sources, build outputs | Private paths, isolated ports and deterministic per-task fixtures |

Symlinks do not enforce read-only access. The executor must make shared dependency
targets read-only to workers, or use a storage primitive with equivalent
isolation. Never hardlink writable source files. Installers, postinstall scripts,
package upgrades and native rebuilds go through the dependency provisioner:
serialize changes to the selected cache, publish a new fingerprint, and attach
it only after validation. A worker that changes dependency manifests gets a
compatible isolated installation rather than mutating siblings' dependencies.
If the OS cannot provide the claimed protection, report unsupported sharing and
use private dependencies within the resource budget; don't label links isolated.

Explicit shared-workspace exceptions are limited to:

1. A task whose required tool cannot operate in a linked worktree.
2. A maintainer-configured integration/reproduction task requiring one checkout.

Each exception records a reason, acquires a workspace-wide edit claim, drains
other writers, and has a single executing session. Read-only reviewers may
inspect the frozen tree. Dependency-cache sharing is not a shared-source-workspace
exception. If a case is not one of these, use separate worktrees.

## 5. Resource admission

Reuse existing memory/embedding policy owners and the planned dynamic-memory
work where available; do not create a second process memory monitor. The swarm
scheduler consumes host telemetry and grants reservations for extraction,
embedding, builds/tests and benchmarks. This spec does not assert that the
separate dynamic-memory proposal is already implemented on the release branch.

Conservative initial policy:

- One canonical extraction/import job per project and one embedding batch in
  flight for the code corpus. Existing provider/model admission remains stricter
  when appropriate; no additional model/context per agent.
- Default two active coding workers and one heavy build/test job per host;
  configurable within the host budget. Queued workers keep task state without
  holding edit claims indefinitely. At admission, acquire compute reservation
  and claims without unbounded hold-and-wait.
- Derive job memory reservations from measured model context/batch settings,
  extraction working sets and test workloads. Include native memory and required
  OS/database headroom. A fixed percentage of Go heap is not a total-memory bound.
- Before a cold workload has an estimate, admit only one heavy job under explicit
  configured limits and measure it; do not infer that unknown means zero cost.
- Bound ingestion queues by bytes and items, stream extraction/import data,
  use backpressure, and reuse the importer's bounded batches. Never materialize
  the full repository JSON once per agent.
- Pause embedding and extraction admission under pressure. Finish or cancel
  admitted work through its existing owner. Reserve enough capacity for claim
  renewals, releases, durable receipts and user-facing progress.
- Benchmarks request an exclusive heavy-work slot and pause competing embedding
  work when needed for comparable measurements. Record actual background load.
  Pausing admission does not pretend an already loaded model has been freed.

Memory budgeting is cooperative unless the OS enforces a hard limit. Report
RSS/native/provider metrics available on the host; never promise an OOM-proof
process. Low disk space prevents new worktrees/builds and pauses new ingestion;
preserve dirty work and do not auto-delete user files. Worktree cleanup uses
task ownership and retention, not a broad recursive removal of the tooling root.

## 6. Success measurements

For N workers against an unchanged release corpus:

- Exactly one canonical ingestion owner and one shared vector corpus exist.
- Starting additional worktrees creates zero canonical embedding jobs.
- Re-ingesting an unchanged commit creates zero content updates/embedding jobs.
- Changing one canonical symbol re-embeds only content-policy-affected nodes;
  index/ref metadata changes must not invalidate unrelated code vectors.
- Coordination and delta nodes create zero embedding jobs and zero semantic
  search pollution, even when they contain long text properties.
- Memory growth is measured for sessions, deltas and active jobs separately
  from the already resident canonical graph/model. Claim no zero-cost scaling.
- A release update during active tasks produces truthful lag and deterministic
  affected-task invalidation, without replacing the corpus with any worker branch.

See [verification.md](verification.md) for fixtures and workload reporting.
