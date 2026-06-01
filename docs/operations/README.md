# Operations Guide

This section is the canonical runbook set for deploying, operating, and recovering NornicDB.

## Start Here

- Production setup: [deployment.md](deployment.md)
- Runtime configuration: [configuration.md](configuration.md)
- Environment variables: [environment-variables.md](environment-variables.md)
- Retention policy setup: [../user-guides/retention-policies.md](../user-guides/retention-policies.md)
- Symptom-based triage: [troubleshooting.md](troubleshooting.md)
- Global issue map: [../issues-index.md](../issues-index.md)

## Common Tasks

- Deploy on Docker/Kubernetes: [docker.md](docker.md)
- Monitor health/metrics: [monitoring.md](monitoring.md)
- Backup and restore: [backup-restore.md](backup-restore.md)
- Control WAL growth and recovery behavior: [wal-compaction.md](wal-compaction.md)
- Tune durability/performance: [durability.md](durability.md)
- Run low-memory deployments: [low-memory-mode.md](low-memory-mode.md)
- Scale capacity: [scaling.md](scaling.md)
- Secure cluster communication: [cluster-security.md](cluster-security.md)

## Related Operational Topics

- MVCC retention and historical reads: [../user-guides/historical-reads-mvcc-retention.md](../user-guides/historical-reads-mvcc-retention.md)
- Storage encoding format and migration details: [storage-serialization.md](storage-serialization.md)
- CLI workflows: [cli-commands.md](cli-commands.md)
- Offline admin import workflow: [admin-tool.md](admin-tool.md)

## Incident Routing

- Service down / cannot connect: [troubleshooting.md#connection-issues](troubleshooting.md#connection-issues)
- Auth errors (401/403): [troubleshooting.md#authentication-issues](troubleshooting.md#authentication-issues)
- Slow queries or high CPU: [troubleshooting.md#performance-issues](troubleshooting.md#performance-issues)
- OOM or memory pressure: [troubleshooting.md#high-memory-usage](troubleshooting.md#high-memory-usage)
- Crash recovery or corruption: [troubleshooting.md#data-integrity-recovery](troubleshooting.md#data-integrity-recovery)
