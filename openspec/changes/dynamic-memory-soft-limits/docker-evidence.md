# Docker evidence (draft implementation)

Source: `a6a23bfcf` (combined PR code head). Documentation-only correction
`693d8f1f1` followed the image build. Host: Apple M2 Max, Docker Engine
29.1.2, Linux arm64 containers. Image: `nornicdb-memory-a6a:local`, digest
`sha256:8eb19ace3fd32d84abe92f6abdb5904b8e508f85351d00256291e231cd6c32cd`.
The image was built from this source with Go 1.27.1 and
`go build -tags noui,nolocalllm ./cmd/nornicdb`; embeddings, vector search,
and MCP were disabled for this focused test. Both containers used
`--memory=1g --memory-swap=1g` and `serve --no-auth` on loopback-only host
ports. This proves the tested behavior at 1 GiB; it is **not** a reproduction
of the reported 8 GiB/7168 MiB workload.

| Case | Configuration and action | Observed result |
|---|---|---|
| Transaction limit | Go target 768 MiB, application soft max 768 MiB, transaction max 4 MiB. HTTP `RETURN 1`; `UNWIND range(1, 1000000) AS x RETURN size(collect(x)) AS n`; `RETURN 2`. | HTTP 200 success; HTTP 200 with `Neo.ClientError.General.TransactionOutOfMemoryError` at 4.0 MiB; HTTP 200 success. Container remained running; `memory.events` had `oom_kill 0`. |
| Application admission | Go target 768 MiB, application soft max 256 MiB. Write 100 MiB of zeroes to a temporary file inside the same cgroup, then request `RETURN 1`. Remove the file and request `RETURN 2`. | `memory.current` rose from 41,480,192 to 149,762,048 bytes. New transaction returned HTTP 503 with `Neo.TransientError.General.MemoryPoolOutOfMemoryError`. After removal and a sample interval, usage fell to 41,623,552 bytes and the request returned HTTP 200 with row `2`. Container remained running; `memory.events` had `oom_kill 0`. |

The application test uses file-cache pressure to exercise cgroup sampling and
admission. It does not establish cancellation of already-running transactions,
native/model work, index construction, async-buffer retention, or commit-fence
behavior. The full OOM reproduction, all transport and storage paths, and the
performance gate in `tasks.md` remain open before readiness.

## Exact-head retest after Bolt admission and result ownership

Source `70a7760c20159ea5a51b695ece786d7b4f619ebf`, image
`nornicdb-memory-70a:local` with digest
`sha256:27fc12a1c21670488c22e50819526c6c97ec147fcddec1f888c9dddb8c75bf8a`.
The same Go 1.27.1 Linux arm64 Dockerfile and `noui,nolocalllm` tags were used.
Each case ran in a fresh `--memory=1g --memory-swap=1g` container with no
authentication and no persisted host data.

| Case | Configuration and action | Observed result |
|---|---|---|
| Application admission and recovery | Go target 768 MiB; application soft max 384 MiB. Query `RETURN 1`; write 300 MiB into a file inside the cgroup; query `RETURN 2`; remove the file; query `RETURN 3`. | Cgroup usage was 219,672,576 bytes before pressure, 543,105,024 under pressure, and 219,639,808 after removal. Queries returned HTTP 200, then HTTP 503 with `Neo.TransientError.General.MemoryPoolOutOfMemoryError`, then HTTP 200. Container stayed running; `oom_kill 0`. |
| Transaction cap and recovery | Go target 768 MiB; application soft max 768 MiB; transaction max 4 MiB. Query `RETURN 1`; run `UNWIND range(1, 1000000) AS x RETURN size(collect(x)) AS n`; query `RETURN 2`. | First and last queries succeeded. The collect query returned a Neo4j error payload with `Neo.ClientError.General.TransactionOutOfMemoryError` at 4.0 MiB (the HTTP transaction endpoint itself returned 200, as it does for Cypher statement errors). Container stayed running; `oom_kill 0`. |

The application headroom rule reserves at least 128 MiB below the configured
soft max. One earlier 256 MiB trial started with 218 MiB of observed cgroup
usage and therefore rejected even an ordinary query; a separate 384 MiB trial
with only 100 MiB of file pressure remained below its effective threshold.
Those trials show why an operator must size the soft max against the measured
baseline and expected overlap. They are not evidence of a hard OOM guarantee.
The original 8 GiB failing workload, async write buffer, long-lived embedded
results, and full restart/durability scenarios remain unverified.

## Exact-head smoke retest after buffered-commit admission

Source `ddd6d5ea30033d8b50941a49ab8206cf958b593d`, Linux arm64 image
`nornicdb-memory-ddd:local`, digest
`sha256:9ea0a6d704729107223e403016a10f3d6164b6cd1b2cb8e8c04eaac6ceeeb0e4`.
The image was built with the same Go 1.27.1 Dockerfile and `noui,nolocalllm`
tags. Fresh containers used `--memory=1g --memory-swap=1g`.

| Case | Observation |
|---|---|
| Application pressure/recovery, 384 MiB soft max | `memory.current` was 42,659,840 bytes, rose to 366,694,400 after writing a 300 MiB file inside the cgroup, and fell to 42,991,616 after deletion. `RETURN 1`, `RETURN 2`, `RETURN 3` returned HTTP 200, HTTP 503 with `Neo.TransientError.General.MemoryPoolOutOfMemoryError`, HTTP 200. Container stayed running; `oom_kill 0`. |
| Transaction cap, 4 MiB | `RETURN 1` succeeded, a 1,000,000-row `collect` returned `Neo.ClientError.General.TransactionOutOfMemoryError` at 4.0 MiB, and `RETURN 2` succeeded. Container stayed running; `oom_kill 0`. |

These smoke tests cover the previously tested HTTP paths on the new head.
Buffered-commit admission itself is covered by focused storage tests for
pre-WAL rejection, reservation release after flush, and retention through a
failed replay followed by success. Large recovery sets and the original 8 GiB
case remain unverified.

One additional fresh 1 GiB container used the same image with a 384 MiB
application soft max, `NORNICDB_ASYNC_WRITES_ENABLED=true`, and a one-hour
flush interval. The server reported write-behind mode enabled. An HTTP create
returned 202, and the node was visible before flush. After `docker kill -s
KILL`, a new container opened the same data directory and logged
`Recovered 1 acknowledged write-behind commits from the WAL`; the node was
still queryable. Both containers had `oom_kill 0`. This exercises accepted
commit recovery, not write-behind pressure rejection or a large recovery set.

A separate write-behind container used a 180 MiB application soft max, the
same one-hour flush interval and a persisted data directory. A request to
create a node with a 2 MiB string returned a Cypher error payload containing
`Neo.TransientError.General.MemoryPoolOutOfMemoryError` (the transaction HTTP
endpoint itself returned 200 for this statement error). Cgroup usage was
43,233,280 bytes before the request and 63,954,944 afterward; the container
remained running with `oom_kill 0`. At that pressure, a subsequent read was
also rejected with HTTP 503. After restarting on the same data directory with
a 384 MiB soft max, `MATCH (n:MemoryProof) RETURN count(n)` returned 0 and
`oom_kill` remained 0. The storage regression separately establishes that the
rejected buffered write did not append an `OpBufferedCommit` WAL record.

### Bounded WAL recovery smoke — source `1eccf2f9e`

Image `nornicdb-memory-recovery:local`, manifest list
`sha256:9b7c6b73cc692c5af57130d91a08c2d88a03cfab31db260a2c34a02e944b7d77`.
Linux arm64, 1 GiB Docker memory/swap limit, Go GC target 768 MiB,
application soft maximum 768 MiB, async writes enabled, flush interval one hour.

Thirty-two separate HTTP CREATE calls each stored a 256 KiB string and returned
HTTP 202 without errors (8 MiB total string payload). The process was SIGKILLed
before scheduled flushing. Restarting with the same volume reported recovery of
32 acknowledged write-behind commits. `MATCH (n:MemoryProof) RETURN count(n)`
returned 32 with HTTP 200 and no errors. Both container runs recorded
`memory.events: oom_kill 0`; the restarted server remained running.

This controlled smoke exercises multiple estimated recovery batches and crash
replay. It does not reproduce the original 8 GiB workload or prove a hard process
memory bound. The maintainer requested that original reproduction from @mm0nst3r.
Local transcript: `/tmp/nornic-memory-recovery-smoke.log`.
