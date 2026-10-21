# Application admission recovery and default-sizing investigation

PR #1618 was taken over to finish the measured availability defect and release
verification. The author's commits through `6b9f39d90c38d4158b3d51e8f7e87e9dcc62c517`
are retained. Base: `release/v1.4.4@d854845e9b08e510b56587fddf49694b208e6a3b`.

## Mechanism, measured before the fix

The unchanged head was rebuilt with `docker/Dockerfile.amd64-cpu`,
`HEADLESS=true`, its APOC plugin, and the existing neutral 3,000 Occurrence / five
Version fixture and ordered-status query. Each fresh container had 8 GiB,
no swap, four CPUs, no restart, and an in-cgroup HTTP/Bolt client capped at
128 MiB. Only one server or dedicated builder ran at a time.

Four configured HTTP copies all rejected with TransactionOutOfMemoryError in
13.93–14.56 s. Peak cgroup usage was 7.086 GiB, with no OOM event or restart.
Small HTTP and Bolt queries then rejected throughout the 15-second diagnostic
window; liveness stayed 200. This repeats the previously recorded 120-second
lockout, with additional measurements:

| Measurement | At query completion | After natural collection, before manual GC |
|---|---:|---:|
| HeapAlloc | 7,179,167,768 B | 213,647,304 B |
| HeapInuse | 7,408,828,416 B | 226,123,776 B |
| HeapIdle | 5,423,104 B | 7,188,455,424 B |
| HeapReleased | 5,423,104 B | 1,790,386,176 B |
| Completed GC cycles | 43 | 45 |
| Forced GC cycles | 0 | 0 |

At that second observation `memory.current` was 5,822,484,480 B (5.423 GiB),
including 5,669,609,472 B anonymous memory and only 133,840,896 B file cache.
About 5.03 GiB of the idle Go heap had not been released to the OS. The live
heap had already fallen to about 204 MiB. This is not a leaked transaction
reservation or simply the absence of a completed GC: resident idle pages keep
the physical sample above the 6 GiB policy's 5.4 GiB admission boundary.
An explicit GC was followed by success on both protocols at five seconds.

The correction therefore returns unused heap pages from the existing shared
sampler, then remeasures physical usage. It does not subtract guessed garbage
from a cgroup reading. Reclamation is single-flight within the policy, at least
`memory.recovery_interval_ms` apart (default 5000 ms), with one initial allowance.
Subsequent attempts require an admitted lease release or
`memory.recovery_rearm_ms` since the previous attempt started (default 30000 ms);
the latter permits progress after unleased allocations. Rejected requests never
collect or shorten these intervals, and each attempt requires fresh physical
pressure.
Real live, external or ancestor pressure continues to reject; failed telemetry
continues to fail closed. Oversized forecasts alone do not trigger collection.

## Why admission defaults remain unlimited

The same server image and fixture were used to enable tracking with a permissive
64 GiB aggregate limit and sample `SHOW TRANSACTIONS` every 200 ms. One successful
copy reached a sampled estimate of 2,636,700,000 B; two reached 5,271,910,856 B
(4.910 GiB), with a 7.270 GiB cgroup peak. Sampling is a lower bound on the exact
peak. This estimate differs from physical heap and includes retained intermediate
rows, lists and projections.

| Candidate in an 8 GiB cgroup | Copies | Outcome | Peak GiB |
|---|---:|---|---:|
| 5 GiB transaction pool (62.5% of cgroup) | 2 | Both return 3,000 rows | 7.282 |
| Same | 3 | OOM-killed, exit 137 | 8.000 |
| Same | 4 | OOM-killed, exit 137 | 8.000 |
| 2 GiB transaction pool (25%) | 1 | Pool memory error; no OOM | 3.700 |
| Same | 2 | Both pool memory errors; no OOM | 6.254 |
| Application maximum at 85% of cgroup, no transaction cap | 2 | Both return 3,000 rows; immediate small queries reject | 7.285 |
| Same | 3 | OOM-killed, exit 137 | 8.000 |
| Same | 4 | OOM-killed, exit 137 | 8.000 |

The three-copy OOM run's last sampled aggregate estimate was 4,699,080,000 B,
below the estimate needed by the successful two-copy run. Cooperative estimates
and physical allocation peaks are not interchangeable. The tested protective
pool also rejects work that currently succeeds; an
entry-time application check alone cannot stop work already admitted together.
These measurements do not establish a universal numeric default. No new
admission default is selected, and the existing 85% Go target is unchanged.
The configured 512 MiB / 2 GiB / 6 GiB transaction/pool/application profile is an
explicit rejection profile, not a compatibility-preserving default.

The [consumer audit](admission-consumer-audit.md) records the shared owners and
existing coverage boundaries. Final-head lab receipts and container cohort
results are reported with the takeover push comment; intermediate and fail-first
results are distinguished there.
