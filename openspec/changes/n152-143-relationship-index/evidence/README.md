# N152-143 evidence: provenance and reproduction

This directory preserves the NOR-42 / N152-143 release-tracking ticket exactly as
recorded, and the fixtures it carries, for the bounded investigation of the
ordinary relationship index declaration.

The `fixtures/` and `ticket/` subdirectories preserve the original ticket bytes.
The initial worker investigation ran no builds/tests. The parent-approved Codex
continuation subsequently ran original RED, implemented canonical owner reuse,
and completed focused normal/race/vet, scoped verification and matched benchmarks.
See `verification.md` and `benchmark.md` for current evidence and explicit limits.
Historical findings below describe the original investigation, not the final
implementation state.

## Provenance

| Field | Value |
| --- | --- |
| Authorized source | `/tmp/n152-143-ticket.json` (Plane work-item payload, read with `workspace_command`) |
| Issue id | `b486164a-6189-4d13-b315-de618f17ee9a` |
| Ticket | `NOR-42 · Use relationship equality indexes consistently` |
| Sequence | 143 (release tracking card for N151-39) |
| Created / updated | 2026-11-18T16:53:33.241502Z / 2026-11-19T19:01:26.724179Z |
| Priority / state | medium / Ready (1.5.2) |
| Project / workspace | `02564fe1-72c5-416b-83e6-a13d299d12be` / `a7d5921b-62f0-4df2-9b56-fbc57b3625a5` |
| Ticket bytes / sha256 | see `MANIFEST.json` (`ticket_bytes`, `ticket_sha256`) |
| Ticket JSON copy | `ticket/n152-143-ticket.json` (byte-identical copy of the source file) |
| Source revision in the ticket | `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5`; production files "identical to v1.5.1 commit `cb0a574f75b183dfebfba6c4726b72244f7f374b`" |
| This checkout | `fix/n152-143-relationship-index` at `f77ec7706de099588ac232b32b6908860511d3da` (tracked tree clean at investigation start; see "Working-tree files observed during the session") |

## Extraction

`extract_ticket_fixture.py` reads the ticket JSON, copies it unchanged, and
extracts every `<pre>` fixture in issue order: HTML-unescaped, with only its
single `<code>…</code>` wrapper element removed, written as UTF-8 bytes. Each
fixture's label is the last non-empty text line the issue prints before it, so
the mapping is derived from the ticket rather than asserted by hand.
`fixtures/MANIFEST.json` records, per fixture: label, evidence path, byte count,
sha256, whether the `<code>` wrapper was removed, and the exact `description_html`
span it came from, together with the ticket and description hashes.

Extracted fixtures (hashes are in `MANIFEST.json`):

1. `fixtures/testdata/repo/pkg/cypher/relationship_property_equality_scope_test.go` — the
   canonical accepted-slice controls plus the fail-first
   `TestNOR42OnlineOrdinaryRelationshipIndexSuppliesEqualityLookup`.
2. `fixtures/testdata/repo/pkg/storage/relationship_property_equality_surrogate_test.go` —
   escaped surrogate property-name isolation for the lookup.
3. `fixtures/tmp/nor42-parent-checks.sh`, `fixtures/tmp/nor42-linux-checks.sh` —
   the exact focused selectors (resources `GOMAXPROCS=2 GOGC=50
   GOMEMLIMIT=1500MiB`, `-tags=noui,nolocalllm -p=1 -timeout=180s`).
4. `fixtures/tmp/nor42-parent-macos.log.txt`, `fixtures/tmp/nor42-parent-defect-macos.log.txt`,
   `fixtures/tmp/nor42-parent-linux.log.txt` — the captured PASS/FAIL runs.

Reproduce the extraction (rewrites the same files deterministically):

```sh
python3 openspec/changes/n152-143-relationship-index/evidence/extract_ticket_fixture.py
```

## What the preserved runs recorded

- Constraint-owned UNIQUE control, both parsers, auto-commit and explicit
  transaction: `constrained reads: 2 at 8 unrelated relationships, 2 at 512;
  complete scan: 11 and 515` (`nor42-parent-macos.log.txt:110-117`,
  `nor42-parent-linux.log.txt:111-118`).
- Ordinary (non-unique) relationship `CREATE INDEX`: `ordinary index reads: 11
  at 8 unrelated relationships, 515 at 512; complete scan: 11 and 515` — i.e. the
  declaration changes nothing (`nor42-parent-macos.log.txt:127-129`,
  `nor42-parent-linux.log.txt:128-130`).
- Fail-first contract RED on the ticket's source in both parsers:
  `expected: 11 / actual: 515`
  (`nor42-parent-defect-macos.log.txt`, `nor42-parent-linux.log.txt:136-153`).
- The declared index is reported `ONLINE` with `entityType RELATIONSHIP` and no
  `owningConstraint`; the constraint-owned row reports `owningConstraint`
  (`relationship_property_equality_scope_test.go`, the
  `...IsDeclaredNotOwned` control, passing in both logs).

These numbers are read counts from `PROFILE`'s plan (`TotalDBHits`), not
durations. No performance, throughput, memory or CPU claim is made here.

## Fixture files are not in any revision

`git log --all -- pkg/cypher/relationship_property_equality_scope_test.go
pkg/storage/relationship_property_equality_surrogate_test.go` returns nothing:
the two fixture test files exist only as ticket text (and in the parent's
temporary worktrees recorded in the logs: `/workspace` and
`~/.codex/worktrees/nor42-v151-rel-equality/NornicDB`). Their dependencies do
exist at HEAD: `relationshipEqualityAccessQuery`, `relationshipEqualityParsers`,
`beginRelationshipEqualityTransaction`
(`pkg/cypher/relationship_property_equality_access_test.go:18,30,105`),
`relationshipEqualityAllocationSmallPopulation`/`LargePopulation`
(`pkg/cypher/relationship_property_equality_allocation_test.go:16-17`), and
`rowMap` (`pkg/cypher/bug_optional_match_function_projection_test.go:58`).

## Working-tree files observed during the session

`git status --porcelain` was empty apart from the branch line when this lane started.
While it ran, two untracked test files appeared in the worktree (mtimes
2026-11-21T19:40:08, not written by this lane, not present in any revision):

- `pkg/cypher/relationship_property_equality_scope_test.go`, 6282 bytes,
  sha256 `832cbbf751eefc1d449966767fbd9ff2abf1a0b2102c89d69ff8818824173133`
- `pkg/storage/relationship_property_equality_surrogate_test.go`, 3351 bytes,
  sha256 `1b6afaee1813f0a4d1bb350cc23293780e3a3a56e1ea976cc68d82adfc2af1c0`

Neither was byte-identical to the ticket fixtures. The parent identified these
as the rejected Radeon copy attempt, preserved them outside the checkout at
`/tmp/n152-143-rejected-radeon-copy`, and restored the exact originals before the
current RED. The current production tests contain the approved fixed assertions;
unchanged originals remain under `fixtures/testdata/repo` with verified hashes.

## Retrieval limitations

- The read-only NornicDB code graph and docs MCP servers were unavailable during
  this investigation: both `nornicdb` (`https://radeon7.tail2f19fb.ts.net/mcp/nornicdbcode`)
  and `nornicdb_docs` failed at initialize with
  `http/request failed: error sending request`, and discovery returned zero tools
  with that catalog error. Graph retrieval is therefore **missing, not empty**;
  every source location in `owner-map.md` was read from this checkout instead.
- The initial worker did not run the defect. The subsequent Codex continuation
  reproduced the original RED on the current base before production changes.
  Its exact source hashes, read-count controls, passing correction and remaining
  verification limits are recorded in `verification.md`.
