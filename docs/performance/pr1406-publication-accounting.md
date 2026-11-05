# PR #1406 local performance evidence

On an Apple M2 Max, Go 1.26.9 darwin/arm64, `CGO_ENABLED=1`, `nolocalllm`, and `GOMAXPROCS=4`, the candidate at staged source tree `8e63e98e56538c99d0de640c9dfa3ffdaa7b4480` was compared with main `f2ddd27cfae3788076778a0a0d48c827df046a3b` and release `b1b018d3f407eb7ec9d4b3038667be5dae58a09e`.

These retained results predate the follow-up checker and DROP/Restore recovery corrections. They do not qualify the final integrated source; matched remeasurement is pending.

## Concurrent UNIQUE commit workload

`BenchmarkBadgerUniqueConstraintCommit_DisjointFunctionBatches` uses eight writers and 100 disjoint Function keys per writer. Engine setup and transaction staging are outside the timed commit region. All arms use the same fixture and data; main lacks only the test-lifetime ownership wrapper used before timing by release/candidate. This measures the commit region, not end-to-end Cypher.

Six balanced order permutations give six samples per arm, 1000 iterations each. Each arm appears twice in each order position. The first sample of each arm includes CPU and memory profiles; the remaining five are unprofiled. No concurrent Go or inference job ran during measurement. Every sample exited zero. Medians below include all six samples; no statistical significance claim is made. The user interrupted execution after 14 completed samples. Those samples were preserved; the unfinished fifteenth sample was retained separately as incomplete, then the remaining four samples ran with verified identical source and binary hashes. The pause is a timing limitation, not an excluded completed result.

| Source | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| main | 7,735,375.0 | 20,635,324.5 | 206,233.0 |
| release/v1.4.4 | 8,167,624.5 | 20,761,036.5 | 208,075.0 |
| candidate | 7,934,296.5 | 21,401,182.0 | 209,947.0 |

| Candidate change | Time | Bytes | Allocations |
|---|---:|---:|---:|
| versus main | +2.57% | +3.71% | +1.80% |
| versus release | -2.86% | +3.08% | +0.90% |

Raw samples (ns/op, B/op, allocs/op), in round order:

| Round | main | release | candidate |
|---|---|---|---|
| 1 | 8505652, 20375181, 206117 | 8665767, 20531338, 207593 | 8259041, 20898416, 205536 |
| 2 | 8011289, 20043754, 205162 | 8217634, 20661345, 208557 | 8161724, 21215011, 209741 |
| 3 | 7459461, 20742958, 207128 | 8209462, 20785912, 205041 | 7724185, 21673950, 210618 |
| 4 | 7300992, 20748907, 204096 | 7501596, 21610373, 210642 | 7636878, 21917097, 210555 |
| 5 | 7372941, 20996042, 207646 | 8023117, 20736161, 205225 | 7635731, 21587353, 210153 |
| 6 | 8090868, 20527691, 206349 | 8125787, 21229390, 210280 | 8144408, 21066605, 209144 |

Reproduce each sample from the corresponding checkout, built with the configuration above:

```sh
go test -c -tags=nolocalllm -o storage.test ./pkg/storage
GOMAXPROCS=4 ./storage.test -test.run='^$' \
  -test.bench='^BenchmarkBadgerUniqueConstraintCommit_DisjointFunctionBatches$' \
  -test.benchtime=1000x -test.benchmem -test.count=1
```

For the first sample add `-test.cpuprofile=cpu.pprof -test.memprofile=mem.pprof`. Retained receipts record the exact binary SHA256, build metadata, commands, host load, order and exit status. The first full comparison found a timing regression; it was retained rather than accepted. CPU/allocation investigation identified repeated entity-authority projections in constraint-only helpers. Those duplicates were removed while retaining the earlier canonical entity/endpoint owner. Fail-first hold-count tests and the existing temporal/UNIQUE/race controls establish that the accepted native owner retains its real reentrant hold. A separate matched 500-iteration before/after control measured median time −2.60% and bytes −3.32%; it is not pooled with these 1000-iteration samples.

## Count-only accounting

Profiling first-use managed node admission with 64 stored nodes and 16,384-float embeddings found eager snapshot hydration. Using identity entries for counts, with explicit complete same-snapshot reads for bytes, reduced the measured first-admission allocation from 29,850 KB / 6,448 allocations to 30.184 KB / 510 allocations. The no-vector candidate used 30.152 KB / 510 allocations. Count-only usage no longer scales with retained vector length. The older pre-integration implementation used 10.126 KB / 218 allocations without the complete new accounting contract; this is not an equivalent-feature speed comparison.

Steady initialized count-only managed updates retain existing vectors when the submitted properties have the same source. Projecting scalar deltas from private prepared pairs avoids full-image copies: the 16K-vector local control saved approximately 133,065 B and 15 allocations per update (3.457 MB / 884 allocations to 3.324 MB / 869 allocations). The full-copy API remains private-copy safe, and byte-policy upgrades still request complete images. These steady-update diagnostic profiles used `GOMAXPROCS=12`, whereas the paired UNIQUE and first-admission controls used four. Timings for these diagnostic profiles ran alongside other work and are exploratory. The pre-integration direct path cleared nil vectors, so its vector timings are not an equivalent-outcome baseline.

## Limits and artifacts

The historical TrueNAS UniqueDisjointBatches fixture and results are unavailable. These workload-specific measurements fulfill the maintainer-approved best-effort investigation; they do not reproduce or establish elimination of its historical +28.3% time, +37.4% memory or +21.4% allocation figures. Other #1655 / NOR-166 acceptance criteria remain there. The tests do not establish all-workload speed or zero allocation for managed writes.

Evidence directory: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1532`; final raw samples/profiles and binary receipts are in `candidate-perf/`, the superseded pre-checker comparison in `candidate-perf/archive-before-checker/`, the rejected earlier comparison in `candidate-perf/archive-duplicate-authority/`, and count-only profiles in `count-profile-root/` and `pr1406-count-update/`. The receipts identify the measured source and binary hashes; this report does not assert final-source qualification.
