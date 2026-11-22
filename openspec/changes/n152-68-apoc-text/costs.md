# N152-68 conversion costs

Draft correctness review only. Performance acceptance remains open, especially Camel/Clean. Old Base64 is a placeholder; faster wrong outputs are not valid semantic alternatives. No cost-neutral or release-ready verdict.

Matched exact a5b7980e8 baseline and candidate use identical ordered benchmark body SHA2569448cabf053aba54bcdb1546335fd0c6850d60b5e34f5755138b82316ee08a82, Go1.27.4 darwin/arm64, GOMAXPROCS2/GOMEMLIMIT1500MiB, five100ms samples each. Parent and sibling agents held Go during the quiet window; CPU profiles captured on both. Fixture repeats `Hello É ä λ-42! `1 or64 times.

| Cell | Baseline median ns (range) | Candidate median ns (range) | B/op before→after | allocs/op before→after |
|---|---:|---:|---:|---:|
| Clean/1 | 115.6 (114.6–138.5) | 753.2 (740.2–851.5) | 88→1040 | 2→11 |
| Camel/1 | 453.5 (449.2–460.8) | 1206 (1200–1221) | 296→1664 | 14→12 |
| UpperCamel/1 | 496.8 (492–501.7) | 1234 (1232–1238) | 312→1680 | 15→13 |
| URL/1 | 61.63 (61.33–62.25) | 154 (151–155.2) | 24→48 | 1→1 |
| Base64/1 | 1.937 (1.911–1.943) | 56.21 (55.95–56.52) | 0→32 | 0→1 |
| Index/1 | 6.747 (6.741–6.767) | 35.9 (35.88–36.16) | 0→0 | 0→0 |
| Clean/64 | 6413 (6401–6490) | 21664 (21554–21675) | 13312→11984 | 5→20 |
| Camel/64 | 29340 (29026–29413) | 65951 (65555–66701) | 26792→106234 | 719→463 |
| UpperCamel/64 | 29314 (29090–29779) | 65901 (65448–67217) | 26808→107128 | 720→464 |
| URL/64 | 3289 (3279–3302) | 6089 (6075–6100) | 1280→4608 | 1→2 |
| Base64/64 | 1.912 (1.909–1.913) | 3256 (3185–3409) | 0→3584 | 0→2 |
| Index/64 | 6.464 (6.45–6.466) | 36.59 (36.28–37.2) | 0→0 | 0→0 |

The candidate reuses the canonical lowercase owner per word, paying caser/transform allocation costs; this is not permission to introduce another mapper. Detailed CPU samples are retained in the task archive. A bounded follow-up may evaluate batching case mapping without altering per-word semantics, after parent review and differential proof. No optimization was applied after measurement.

Exact commands for each preserved binary: `GOMAXPROCS=2 GOMEMLIMIT=1500MiB <binary> -test.run ^$ -test.bench ^BenchmarkN68NonRegex$ -test.benchmem -test.benchtime=100ms -test.count=5 -test.cpuprofile=<profile>`. Raw logs, binaries, profiles and parsed samples: ~/.codex/artifacts/n152-68-20261121.
