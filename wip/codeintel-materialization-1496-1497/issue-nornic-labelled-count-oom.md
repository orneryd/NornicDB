# Labelled relationship count exhausts 5 GiB and kills NornicDB

Draft for the orchestrator; do not post. Measured 2026-10-18 on pinned v1.4.3
(`f2ddd27cfae3788076778a0a0d48c827df046a3b`), image digest
`sha256:b8fa89956239e6d20d0738a94d4e7c74e59d377814bb0fdf06b7a89f90221d73`.

## Minimal executable reproduction

Run `bash sig/codeintel.sh restart-probe 4096`. It uses only our TrueNAS API,
an isolated temporary app, and directories inside our code-intel dataset.
Source: `sig/codeintel/restart-probe.py`; controller: `sig/codeintel.mjs`.
Raw receipt: `sig/codeintel/restart-probe-4096.json`.

Create 54,000 `:CodeIntel:CodeIntelFunction` nodes with unique indexed `_ci_key`,
`id`, `repo`, and a 4096-byte `body` (221,184,000 body bytes). Create 1,300,000
directed `CALLS {props_hash:'fixed'}` relationships, 24-ish edges per node,
using 5000-row UNWIND transactions and four writers. See the source for exact
endpoint formula. Edge load: **77.748474 s**, approximately **16,720 rows/s**.
Server cap 5 GiB; `GOMEMLIMIT=4GiB`; embeddings off; CPU uncapped/lowest priority.

```cypher
MATCH ()-[r]->() RETURN count(r) AS n
```

Returns `1300000` in **0.00272655 s**. Then:

```cypher
MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r) AS n
```

After **78.026297 s**, the Bolt connection fails and the supervising process
records **exit 137**. Cgroup `memory.max=5368709120`,
`memory.peak=5369475072`, `memory.events: max 12293, oom 1, oom_kill 1`.
This establishes a kernel OOM kill; no Go fatal/panic is in the captured log.
A 1024-byte-body control with the same counts survives: labelled count 14.751 s,
unlabelled 5.61 ms; peak 4,538,277,888 bytes, no OOM. See
`sig/codeintel/restart-probe-latest.json` for that control.

## Plan and source

An EXPLAIN-only check on the full develop graph with the same server produced
`NodeByLabelScan(CodeIntel) → Expand → EagerAggregation(count(r)) → Limit`.
The transport appended `LIMIT 2` after the aggregation; it does not bound rows
consumed by the aggregation. Receipt is the CLI result for
`EXPLAIN MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r) AS n` in
`sig/codeintel/results/`. The isolated fatal run has no completed PROFILE.
Do not interpret estimated plan rows as actual materialized rows or physical reads.

`pkg/cypher/count_store.go:relationshipCountFromStore` declines its counter path
when both endpoints are labelled. The fallback's memory use is the bug; merely
using the unlabelled total for arbitrary differently labelled endpoints would
produce incorrect answers. Expected: stream/count within bounded memory, or
fail the query gracefully before killing the database.

## Branch status

Runtime reproduced on v1.4.3 only. Source inspected at develop
`16923aef778025fab3490f6a57549763b536f3d5`, release/v1.4.4
`24d08776cdb9cb624da1073007da437306518426`, and RC
`9f00d5456ea8dca4e64306c861f91fa325ff9b51`. `count_store.go` is byte-identical
in all four (`b64b78fdbd8ce64bf3bea7b4fb74cfd2a19fa507`). The rejection remains;
separately built develop/release branch runtime equivalence is not measured.

**RC runtime comparison now measured:** exact current RC
`39e3f7647e034ebe7bf23b067cd161a915700b98`, built centrally with Go 1.26.9,
same 54k/1.3M/4096-byte shape and 5 GiB cap, also dies: **76.585732 s**, sampled
5,368,709,120 bytes, supervisor exit **137**, `oom_kill` increments 0 → 1.
Plan remains NodeByLabelScan → Expand → EagerAggregation. Unlabelled count
returns 1,300,000 in 0.006458 s. RC edge load 69.356629 s. Receipt:
`sig/codeintel/compare-repros-39e3f7647e034ebe7bf23b067cd161a915700b98.json`.
The RC's existing release fixes do **not** resolve this reproduction. Both
instances are separate scratch apps; the live 1.4.3 service was not restarted.

The original release-graph restart at ~13:15 UTC is consistent with this failure
and was not an app update; its removed container exit status is unavailable.
See `issue-codeintel-restart.md` for the exact mutation history and that distinction.
