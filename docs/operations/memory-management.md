# Memory accounting and limits

Choose a process/container budget, then tune storage and query work within it. No NornicDB setting makes arbitrary workloads immune to out-of-memory termination.

## Accounting boundaries

| Control or measurement | What it covers | Limitation |
| --- | --- | --- |
| Container/OS hard limit | Memory charged by the OS to a process or cgroup. | The kernel can terminate the process. Shared cgroups include other processes and file cache. |
| Go runtime target | Go-managed heap, stacks, and runtime memory through a soft GC target. | Not a total RSS cap; native models, GPU memory, mappings, and kernel charges need separate headroom. |
| Application soft maximum | Sampled Linux cgroup usage plus reservations for admitted work. | Does not immediately account for every allocation or preempt all accepted work. |
| Transaction estimates | Tracked query structures, staged writes, and shared transaction pools. | Not the process's physical memory, all caches, or every operator allocation. |
| Cache/buffer options | Particular entry counts, Badger arenas/caches, generations, or indexes. | Not one combined application byte cap. Entry and operation counts are not bytes. |
| Database storage-byte quotas | Persisted graph storage accounting. | Not RAM. Storage-size calculation is separate from startup recovery and query memory. |

Read Go heap, RSS, cgroup usage/limits, native allocations, and GPU allocation separately. A low live heap can coexist with high RSS/cgroup usage because idle heap pages, mappings, file cache, or other processes remain charged. Do not sum overlapping measurements as independent allocations.

## Go runtime target

The canonical settings accept a non-negative **integer number of bytes**:

| Surface | Setting |
| --- | --- |
| YAML | `memory.go_runtime_limit_bytes: "6442450944"` |
| Environment | `NORNICDB_GO_MEMORY_LIMIT_BYTES=6442450944` |
| CLI | `--go-memory-limit-bytes=6442450944` |

Unset, the server uses 85% of the tightest detected finite cgroup limit, unless an existing `GOMEMLIMIT` fits. A configured target greater than the cgroup ceiling is lowered to that 85% value with a warning. Explicit `0` disables NornicDB automatic sizing and skips applying a new Go target. It does not clear an already active `GOMEMLIMIT` or a target set by the host application. Without a usable cgroup limit, automatic resolution preserves an existing runtime target or leaves it unlimited.

Canonical byte settings do not accept `auto` or a `g` suffix. Deprecated `NORNICDB_MEMORY_LIMIT`, YAML `memory.runtime_limit`, and `--memory-limit` accept integer **MiB** or `auto`. Legacy `6144` and canonical `6442450944` both describe 6 GiB. Conflicting aliases at the same configuration surface fail startup; canonical wins when both are equal. Do not copy MiB values into a byte setting.

`serve` resolves and applies runtime memory before storage opens. Embedded applications must call `cfg.Memory.ResolveRuntimeLimit()` and `cfg.Memory.ApplyRuntimeMemory()` for the same runtime policy. GC controls are process-wide; another DB does not create its own heap or runtime limit.

## Admission and transaction caps

```yaml
# Example budgets for an 8 GiB container; qualify against your workload.
memory:
  go_runtime_limit_bytes: "6442450944"
  application_soft_max: "6g"
  transaction_total_max: "2g"
  transaction_max: "512m"
  recovery_interval_ms: 5000
  recovery_rearm_ms: 30000
```

Admission and transaction settings use bytes with binary `k`, `m`, `g`, or `t` suffixes. They default to `0`: disabled/unlimited. Automatic runtime sizing does not enable them.

| Scope | Setting |
| --- | --- |
| One transaction | Default `memory.transaction_max` / `NORNICDB_MEMORY_TRANSACTION_MAX`; database override `db.memory.transaction.max`. |
| Database transaction pool | Database override `db.memory.transaction.total.max`. |
| Process transaction pool | `memory.transaction_total_max` / `NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX` (`dbms.memory.transaction.total.max`). |
| Application admission | `memory.application_soft_max` / `NORNICDB_MEMORY_APPLICATION_SOFT_MAX`. |

Database transaction overrides are dynamic for subsequent statements. Runtime/application settings and the process transaction-pool limit require restart/reconfiguration of the owner. Running statements retain captured ceilings and shared retained charges. See [Transaction accounting, errors, and transport admission](transaction-memory-limits.md) for the detailed contract.

### Application pressure

When enabled, the DB policy samples Linux cgroup usage every 250 ms. Samples older than two seconds or unavailable samples reject admission. Headroom is the greater of 128 MiB or 10% of the application maximum, with measured ancestor capacity also enforced. On a host without a usable cgroup usage reading, the policy does not substitute RSS; enabled admission can remain unavailable. Docker Desktop's Linux containers have a different accounting boundary from their Windows/macOS host.

Request metadata/body forecasts and buffered-commit forecasts belong to the shared policy until their work ends. Retries do not free accepted work or bypass stale readings. Cooperative reservations do not impose a hard memory ceiling.

After pressure rejection, the sampler can collect and return idle Go heap pages. Starts are separated by `recovery_interval_ms` (default 5000); another attempt requires a released admitted lease or the quiet rearm interval (default 30000). Rearm must be at least the minimum interval. Rejected retries do not force collections. Native memory and file cache can keep admission closed after reclamation.

## Storage and search working sets

- [Low-memory mode](low-memory-mode.md) selects smaller Badger arenas/caches. It does not enable admission or change WAL sync policy.
- Keep async writes off unless buffered throughput is needed. The generation operation cap is not a byte cap; large properties/vectors can dominate memory. Application admission reserves forecasts for retained buffered commits when enabled.
- Accepted-WAL recovery targets 8 MiB of retained payload per application batch. An oversized commit is processed alone; decoding and other state can exceed that target.
- Lazy search warming defers load/build until first use. It does not unload indexes after requests or reduce the eventual warmed working set.
- Streaming snapshots/backups avoid whole-corpus materialization, but buffers, dictionaries, indexes, and concurrent queries still need headroom.
- Native model weights, inference contexts, GPU offload, and drivers need separate budgets. Use external inference or smaller models when needed.

## Diagnose before raising caps

```bash
docker stats --no-stream nornicdb
docker inspect --format '{{.State.OOMKilled}} {{.State.ExitCode}}' nornicdb
```

Exit 137 means SIGKILL; it does not alone prove an OOM kill. Exit 143 indicates SIGTERM. Check container state, orchestrator events, cgroup `memory.events`, and host/kernel logs. On cgroup v2 inspect `memory.current`, `memory.max`, and `memory.stat` in the relevant cgroup.

Reduce concurrency and result sizes, bound traversals, tune the measured cache/model working set, then retest. Independent limits can still compete for the same physical budget. Do not disable WAL or delete recovery files to reduce memory use.

## Implementation reference

[Runtime limit resolution](../../pkg/config/cgroup_memory.go), [units and aliases](../../pkg/config/config.go), [CLI runtime/low-memory setup](../../cmd/nornicdb/main.go), [DB policy ownership](../../pkg/nornicdb/db.go), [sampling/reservations](../../pkg/memory/policy.go), and [pressure reclamation](../../pkg/memory/policy_recovery.go).
