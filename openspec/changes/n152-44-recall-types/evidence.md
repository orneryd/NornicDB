# N152-44 verification

Release base: `e8912f801ffece3356ca8ca8d94ba0df61c7ee3d`.
Runtime and test source checked at `1fa75c101b5dd9ef8ba6ace8fa256a55aa5c70f7`;
the subsequent evidence commit changes only OpenSpec documentation.

## Reproduction and delegated work

Parent independently ran the original ticket regression on base
`1a6eb031471688a71966a68147740ead6c43d6cb`: the 24 union leaves failed,
with the existing type, limit, depth and schema controls preserved
(`/tmp/n152-next-baseline.log`).

DeepSeek job `73603fd454244993b665d7f3e957a795` completed in 619.324 seconds,
including queue time. It reproduced the failure, changed the canonical recall
filter and depth prose, and passed the original regression, full MCP package
and MCP vet checks. The parent reviewed the actual diff and added public-route
controls for duplicate requested labels, multi-label nodes, exact-case matching,
combined tags/date filters and limit eligibility. Those controls passed before
rebasing (0.765s), then passed as part of the complete package race run below.

## Parent checks on the updated release base

- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./pkg/mcp -count=1 -timeout=5m`: PASS, 17.735s package runtime; `/tmp/n152-44-race.log`.
- `make verify-list BASE=origin/release/v1.5.2 FETCH=0`: PASS; selects localization, UI, native libraries, build, test groups, allocations, conformance and differential. Docs is path-filtered out.
- `make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization`: PASS selected localization checks; `/tmp/n152-44-verify.log`. All other steps were unselected, not passed.
- `git diff --check`: PASS. Linker emitted the existing duplicate `-lobjc` warning.

## Duplicate review

The existing staged advisory checker ran with the configured code-intelligence
Python interpreter. Its graph request timed out (`/tmp/n152-44-duplicate.log`).
The worker's graph baseline and searches also timed out. Neither is a clean
duplicate verdict; graph candidate analysis remains pending.

Current-source comparison: `handleRecall` remains the sole recall type-filter
owner and reuses `containsLabel`. Search's `wantedTypesLower`, `nodeMatchesType`
and `indexedTypeAdmitter` admit case-insensitive labels and the `type` property
using search metadata; using them would change recall's exact-case, label-only
contract. `ExtractResourceType` selects one telemetry/audit classification,
not a result filter. `handleDiscover` already forwards the complete type list.
No parallel type matcher or shared abstraction was added. These decisions are
source-reviewed; unavailable indexed candidates remain unexamined.

## Limits and disposition

Draft review candidate, not full release qualification. Full repository tests,
native inference/UI build, allocation ratchet, conformance, differential,
stress and performance qualification were not run. Public pinned regression
fixtures use the default database; existing database routing controls are in
the package race run. Depth fixtures use keyword search. No broader tenant or
retrieval-quality claim is made. Keep the worktree, branch and logs for pending
verification; shared caches and other active work are preserved.
