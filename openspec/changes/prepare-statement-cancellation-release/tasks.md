# NOR-20 / PR #1615: cancellation and transaction-read readiness

Target: `release/v1.4.4`, last verified remote head
`35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`.
Local rebased checkpoint: `b72010bda7dc25af19df8240fab491099405650a`.
Superseded published head: `259a3b5cb8abd354561c315447c8121608287588`.
The final qualified tree and publication evidence are recorded below and on
PR #1615. Maintainer-selected gate: **make test**; duplicate review uses the
complete known inventory this round. Do not merge automatically.

Evidence root: `~/.local/share/nornicdb-pr-readiness/pr1615/`.
NOR-20 now contains 27 acceptance criteria and a 29-case matrix. Do not silently
reduce that population or mark the family complete based on helper-only proof.

## Approved contracts

- [x] Preserve recursive scannerless parsing, HTTP/Bolt syntax and existing
  Badger keys, payloads, WAL format and storage version. No storage migration.
- [x] Migrate fn.Func/EvaluateFunction, loader I/O, Louvain and bulk storage Go
  APIs to explicit context; use canonical callers and document rebuilds.
- [x] Use JSONItems as the canonical JSON-row loader and Binary for both byte
  result names; retain Stream as the Go reader API.
- [x] Use immutable backend-bound APOC helper instances; retire storage globals
  and real forwarding aliases while preserving pure helpers.
- [x] Retire the empty-neighbor Go PageRank stub and bundled registration;
  preserve the supported native Cypher implementation.
- [x] Define the HTTP workload budget as 100 ms from actual caller cancellation
  or deadline until backend work retires; total request duration is separate.
  Neo4j's configured timeout/periodic monitoring is not a universal 100 ms promise.
- [x] Leave the ANTLR runtime unchanged; use repository grammar and pinned
  generator. Explicitly defer the independently tracked Unicode casing issue
  NOR-187 and do not add unsupported GDS features.
- [x] Preserve accepted-write durability and the separately owned terminal-outcome
  contract. PR #1684 remains open (latest observed head efb80d854); no duplicate terminal owner is introduced here.

## Integrated component corrections

- [x] Reproduce explicit-transaction read costs using the supplied recipe and
  matched local historical binaries; distinguish retained reads from the actual
  BEGIN/empty-count/COMMIT workload. Allocation profiles identify global cache
  recreation, not graph copying, as the dominant read-only commit cost.
- [x] Keep caches for a proven empty mutation scope; retain real/unknown-write
  invalidation, snapshots, detached ownership and publication ordering.
  Storage/Cypher/Bolt/HTTP focused controls pass. Reject per-entity Grow changes
  that increased memory. See tx-perf and tx-perf-1548 evidence.
- [x] Re-measure five lifecycle controls on integrated source. Three paired rounds:
  read-only sequence 164.38 to 45.79 us and 2,548,111 to 14,458 B; pre-#1548
  baseline 44.80 us / 14,269 B. Fresh-query median 551.23 us versus baseline
  569.98 us. Full matrix and limits are in the durable-cache qualification doc.
- [x] Correct materialized projection/sort/group/value cancellation and the
  cancellation-triggered WITH nil-map panic; preserve positive results, stable
  ordering and failure identities. Component public two-parser race proof retained.
- [x] Correct procedure populations, fulltext/vector/temporal work and approved
  Louvain context APIs; retain unsupported-name limitations and actual work bounds.
- [x] Route native and real bundled-plugin I/O through one typed capability;
  default-deny tests prove zero file/dial activity. Real DNS/HTTP cancellation,
  address checks, file-root behavior and healthy formats are covered separately.
- [x] Integrate context-first bulk reads through immediate snapshot capabilities,
  namespaced/transaction/managed wrappers and actual candidate enumeration. D09
  actual-cancel regressions cover continued backend work, not merely response return.
- [x] Validate large HTTP cancellation, rebuilt plugin policy/context, Binary byte
  results over HTTP/Bolt and successful controls. Do not claim unexecuted mid-body
  wire or external-reference cases passed.
- [x] Correct qualified composite reads and typed namespace projections, including
  unscoped/already-scoped bypasses. Preserve source-owned records, partial-error
  conventions and exact snapshot versions. Parent combined race checks pass.
- [x] Correct Fabric USE/composite LOAD CSV in repository grammar/semantic
  adaptation. Always-on both-parser structure/forwarding controls pass; pinned
  ANTLR 4.13.1 replay yields eight byte-identical generated files. Optional Neo4j
  differential remains skipped without a reference URI.
- [x] Integrate immutable backend-bound modules and registry replacement, retained
  handler/cache isolation, restored pure meta.Functions and one metadata alias
  owner. Full current APOC suite and focused public binding races pass.
- [x] Reproduce and correct mixed-criteria Any and list/map comparison panics;
  preserve safe scalar semantics, canonical Suggest aliases and empty results.
  Search traversal refinement is integrated; final full qualification remains below.
- [x] Share constraint pending-adjacency ownership. Repeated 2,048-edge bookkeeping
  medians improve about 342 to 144 us, 681 to 232 KB and 4,132 to 2,051 allocations.
  This is an amortized workload result, not deterministic-order or first-read proof.
- [x] Share cardinality policy while retaining distinct native/staged authority;
  remove the namespace-prefix scratch allocation. Parent race passes (3.899 s);
  production-route allocations equal the matched seed baseline.
- [x] Share bounded related-node traversal across all three consumers. Preserve
  depth, identity, visibility, missing-node and ordinary partial-error contracts;
  stop canceled rejected/duplicate/unusable-edge work. Parent core/public/security
  inventory races pass; six matched workloads use fewer allocations and improve
  median time. Worker final JSON failure did not lose archived edits or replay actions.
- [x] Fix canceled partial-search cache publication in canonical Put, migrate all
  callers and retain complete retry results; no publication forwarding wrapper.
- [x] Correct executable-input provenance and CSV source inventory failures with
  preserved fail-first negative cases. Parent current focused Cypher/search/storage
  races pass (17.378 s / 1.886 s / 4.104 s); no baseline waiver.
- [x] Consolidate duplicate test kernels while preserving original case names,
  populations, assertions, errors and ownership. Additional metric/restart, MVCC
  capability and snapshot-version tables pass independent race checks. Register
  snapshot cleanup before assertions; capture explicit read versions after seeding.
- [x] Share node/relationship property-map rename transition; compiler confirms
  both call sites inline. Full APOC refactor race passes (1.255 s).
- [x] Classify only new request-local census scratch fields, rename operation
  selectors that misleadingly looked like physical key prefixes, regenerate the
  source-derived census and pass fresh normal/order checks (7.523 s). No key,
  analyzer, ownership rule or duplicate-checker changes.
- [x] Keep the coverage file below 2,500 lines by moving three cohesive helper
  test bodies unchanged; focused race passes after the import correction.

## Final readiness work

- [x] Integrate the fused range-function search owner, preserve every test and
  compare against independent correct loops. Allocation counts match; 51/52
  operation medians are within 5% or faster, with the remaining case varying
  around parity in longer paired measurements. Old incomplete-code correctness
  overhead is disclosed separately.
- [x] Finish public/owner search cancellation and backend-binding component races.
- [x] Fix all seven first-full-gate failures: fixture ownership, exact numeric
  owner inventory, LOAD CSV row effects/header metadata/grammar census,
  GraphBuilder actual cancellation and generated storage census. Preserve red
  logs and run focused green assertions; no baseline waiver.
- [x] Consolidate remaining test kernels with exact original populations,
  including lazy read-failure gates, selected nil first-call behavior, snapshot
  history assertions, directionality, checkpoint contexts and stream kernels.
  Independent AST proof preserves all 52 composite test bodies except exact
  approved fixture-constructor substitutions.
- [x] Retire the approved GraphSample Go stub, registration and stale classifier
  entry; retain the canonical Graph owner and document the explicit API removal.
  Current catalog is 981 / 117 procedure-only / 814 scalar plugin exports.
- [x] Reconcile all 174 known candidates against actual receiver/name/body hashes,
  shared kernels and concrete contract/measurement evidence. Retain the incomplete
  300-second advisory run honestly. Maintainer explicitly requested no further
  checker run this round; no checker or persistent hook changes.
- [x] Correct the final exposed latest-visible visitor cancellation defect with
  one typed bounded owner; retire the identical point-read test double and
  migrate its APOC consumer directly to the existing owner.
- [x] Recheck release and PR #1684 status; rebase if necessary, never merge. Keep
  that separate terminal-outcome dependency explicit until actual integration proof.
- [x] Run make test on frozen integrated source, let it complete and fix every
  discovered regression, including failures predating this PR. Preserve failed,
  interrupted, cached and skipped evidence honestly.
- [x] Commit and push the qualified source with the exact remote-head lease;
  update PR/Linear AC maps, results and limitations and mark ready for review.
- [x] Complete cleanup skill after qualification: retire completed task-owned
  worktrees/branches/temp files and images, preserve required evidence, and clear
  the shared Go cache only when no active Go work depends on it.

## Current external state and cleanup

Release graph refresh completed at release35e3: 66,286 nodes and 2,882,068 edges.
Dirty source is not indexed; actual checkout bodies are authoritative.
The final private advisory remains draft (fixed 1.4.4, affected below 1.4.4), with
no publication or CVE assignment claimed.

Completed cardinality/read-projection checkouts were archived and byte-verified
before removal; ignored library/UI files remain identical in primary. Active
worker/primary/performance checkouts remain preserved. After the final test
processes retire the shared Go cache is cleaned, with module downloads retained.
Docker is unavailable, so image cleanup is deferred. No remote branch is deleted;
merge policy, duplicate analyzer and persistent hooks stay unchanged.

## Final gate

`make test` passed (exit 0) on tree
`e258ab6b433f6784b7bcf0aa2abf9b5bece827b9`, base
`35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`: 140 packages,
136 cached and four fresh, 608.65 seconds. The immediately preceding full
code run passed 139 packages and failed only the generated constructor
inventory; the sole subsequent input change adds exactly the two reviewed
`relatedgraph.Fanout` allocation-path rows. Normal inventory assertions pass
in the final gate; no rule, threshold, assertion or test exclusion changed.
Eight current public HTTP cancellation controls measured 18.02–32.25 ms to
backend retirement without race instrumentation.

The 174 known findings are fully dispositioned; no new duplicate check is run
by maintainer instruction. The attempted 300-second comparison is incomplete,
not clean. No checker or persistent hook setting changes are included.
The final commit adds only Markdown qualification notes after the tested tree;
code, tests and generated inventory must match that tree exactly.

Cleanup archives preserve eleven completed worker checkouts and four obsolete
local branches were removed. Active/shared/pinned checkouts and evidence stay.
Shared Go-cache cleanup occurs only after all test/build processes retire;
Docker image cleanup is deferred because the daemon is unavailable. No module
download cache is deleted. PR #1684 remains separately owned and open; this
PR does not claim its terminal-outcome/retry qualification or merge it.
