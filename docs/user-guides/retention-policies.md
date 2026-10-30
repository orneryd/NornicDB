# Retention Policies

NornicDB supports runtime retention policy enforcement, but it is disabled by default and must be explicitly enabled.

## Default Behavior

By default:

- `compliance.retention_enabled` is `false`
- no retention manager is created
- no retention sweep background worker starts
- no retention policy file is saved on shutdown
- retention admin endpoints return `503 Service Unavailable`

This keeps retention fully opt-in for deployments that do not want automatic lifecycle enforcement.

## Enable Retention

YAML:

```yaml
compliance:
  retention_enabled: true
  retention_policy_days: 30
  retention_auto_delete: false

retention:
  sweep_interval: 3600
  default_policies: false
  excluded_labels: ["AuditLog", "System"]
  max_sweep_records: 50000
```

`sweep_interval` is an integer number of seconds. Shorthand duration forms such as `"1h"` and `"30m"` are not accepted.

Environment variables:

```bash
export NORNICDB_RETENTION_ENABLED=true
export NORNICDB_RETENTION_POLICY_DAYS=30
export NORNICDB_RETENTION_AUTO_DELETE=false
```

## Behavior When Enabled

When enabled, NornicDB:

- creates a retention manager during DB startup
- loads persisted policies and optional built-in defaults
- starts a cancellable retention sweep worker tied to DB shutdown
- persists retention policy state on shutdown
- exposes retention admin endpoints for policies, legal holds, erasure requests, manual sweeps, and status

## Retention Controls

Use `retention.excluded_labels` to exempt labels globally. Exclusions win over delete decisions during sweep processing.

Use `retention.default_policies` to load built-in compliance policies at startup.

Use `retention.policies` for inline custom policies when you need category-specific durations or archive paths.

## Ebbinghaus-Roynard, As-Is

If you want a practical four-tier memory model today, use retention as a coarse lifecycle tool:

- keep durable knowledge labels out of retention entirely
- keep durable wisdom or rule labels out of retention entirely
- apply finite retention only to memory-like labels
- optionally relabel promoted memories into an excluded durable label

Recommended label mapping:

- `KnowledgeFact`: excluded from retention
- `WisdomDirective`: excluded from retention
- `MemoryEpisode`: retained for a bounded period
- `ConsolidatedMemory`: excluded from retention after promotion or review

Example:

```yaml
compliance:
  retention_enabled: true
  retention_auto_delete: false

retention:
  sweep_interval: 3600
  excluded_labels:
    - KnowledgeFact
    - WisdomDirective
    - ConsolidatedMemory
    - System
    - AuditLog
  policies:
    - id: memory-episode-30d
      name: Memory Episodes 30 Days
      category: USER
      retention_days: 30
      archive_before_delete: true
      archive_path: /archive/memory
      active: true
```

How to use it:

- store durable facts as `KnowledgeFact`
- store long-lived directives as `WisdomDirective`
- store ephemeral sessions or observations as `MemoryEpisode`
- when a memory becomes durable, relabel or copy it into `ConsolidatedMemory` or `KnowledgeFact`

Current caveats:

- this is whole-node retention, not true score-based decay
- edges are not retained independently yet
- properties do not have separate retention behavior
- there is no access-driven promotion or automatic consolidation in retention itself
- retention uses category-based expiry plus label exclusions, so this is an operational workaround, not the full declarative knowledge-layer design

## GDPR Integration

Retention integrates with GDPR delete handling:

- legal holds block erasure
- erasure requests are tracked through the retention manager
- subject matching is configurable through compliance subject-selector settings

## Admin API Availability

Retention admin endpoints are mounted with the rest of the server routes, but they only become operational when retention is enabled. If retention is disabled, they return `503 Service Unavailable` instead of pretending retention is configured.

See [Configuration](../operations/configuration.md#retention-policies-opt-in) and [Environment Variables](../operations/environment-variables.md).