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
