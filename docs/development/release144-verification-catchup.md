# v1.4.4 verification catch-up

This work owns all remaining failures from full `make verify` on
`release/v1.4.4`, the entire nine-finding PR #1638 duplicate inventory, and the
Northwind performance investigation. Tracking: [NOR-313](https://linear.app/nornicdb/issue/NOR-313)
and [PR #1696](https://github.com/orneryd/NornicDB/pull/1696).
Planned availability is v1.4.4; readiness does not mean merged or released.

This document records implementation checkpoints. The live checklists and final
checked head/base receipts are maintained on the PR and Linear ticket.

## Implemented and qualified checkpoint

Head `89e2ecb8e1af9bbdc43c5d61e6748bfb01b71048` was rebased onto release
`f0c932b902b4112114eaeb1fd8a160a6b2bb05cc` without a merge commit. Full
`make test` and `make verify ALL=1 BASE=origin/release/v1.4.4 FETCH=0` passed.
The latter took 54m17s. All selected stages passed; Linux-amd64 llama library
qualification is N/A on this Darwin arm64 machine. The unchanged allocation
ratchet also passed against original main `f2ddd27cfae3788076778a0a0d48c827df046a3b`,
without exceptions or threshold changes. A separate Node20 UI install/build passed.

| What we fixed | How | Outcome |
|---|---|---|
| Scalar allocation regressions | Share RETURN plans/immutable parameter rows and avoid redundant source/clock allocation | Fixed-count medians: internal 995 B/op /14 allocations, public 2,112 B/op /32; original-main ceilings pass |
| Native node allocation | Approved typed runtime metadata API with immutable records and bounded metadata-only reuse | Cache hit 496 B/op /4 allocations versus 640 B/op /4 |
| Storage and replication contracts | Preserve native/transport schemas and the historical gob reader's exported field schema | Frozen payload, historical snapshot, backup/restore and integrated storage/replication tests pass; Badger keys, stored payload contracts, WAL and storage version are unchanged |
| Integrated test regressions | Correct fixture ownership, provenance guard, generated census and reviewed lexical population | Full tests and all coverage groups pass without retries at the qualified checkpoint |
| Duplicate ownership | Converge database info, index creation, continuation type membership and test mechanics | Complete original-nine and subsequent 25-candidate dispositions; follow-up sweep has three retained test-contract candidates |
| Northwind comparison | Explicit config and effective 100k native cache prevent accidental home-config decay changes | Matching counts/hashes; controlled ingest medians improve in graph-only and search-enabled modes |

See [Northwind measurements and limits](../performance/release144-verification-convergence.md),
[original duplicate dispositions](https://github.com/orneryd/NornicDB/blob/89e2ecb8e1af9bbdc43c5d61e6748bfb01b71048/openspec/changes/release144-verification-convergence/duplicate-dispositions.md)
and [follow-up dispositions](https://github.com/orneryd/NornicDB/blob/89e2ecb8e1af9bbdc43c5d61e6748bfb01b71048/openspec/changes/release144-verification-convergence/followup-advisory-dispositions.md).
The report preserves every measured outlier and the higher peak RSS at GOGC=400;
it does not claim a five-second search-enabled ingest or universal memory improvement.
Earlier cache-capacity comparisons remain invalid and are excluded.

## Cache-hit timing follow-up

A longer isolated check exposed a ~6.5% cache-hit timing regression at the
qualified checkpoint, despite the byte reduction. CPU profiling localized
redundant complete runtime-state copying/comparison when the owner and ID were
already correct. A direct pair check avoids only that no-op; unbound/different
pairs retain the original state-binding path, including metadata and identity.
Six two-second samples per binary measured approximately 325.2 versus release
325.55 ns/op, within observed variation, while retaining 496 B/op /4 allocations.
Focused metadata tests pass. Fresh integrated qualification of that follow-up
is recorded on the PR after its commit; the earlier full pass is not represented
as a pass on subsequently changed source.

The duplicate checker stays advisory. Its graph baseline is older than the
rebased release, so current-source review accompanies every disposition. Earlier
failed/incomplete runs remain recorded rather than being relabeled as passing.
