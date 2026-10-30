# MVCC Lifecycle Admin API

NornicDB exposes an admin-only HTTP surface for inspecting and controlling MVCC lifecycle work on a per-database basis.

Use this API when you need to:

- inspect lifecycle pressure, debt, reader count, and last-run activity
- pause or resume automatic lifecycle work
- trigger a one-off prune run
- disable automatic runs entirely and operate in manual-only mode
- change the automatic lifecycle cadence without restarting the server

This guide covers:

- who can call the API
- the supported endpoints
- example requests and responses
- how automatic scheduling behaves
- operational guidance for using manual and automatic modes safely

## Access Requirements

These routes are registered under the admin router and require `auth.PermAdmin` when security is enabled.

In normal deployments, that means a caller needs the built-in `admin` role or another role that has equivalent admin permission.

Routes:

- `GET /admin/databases/{db}/mvcc`
- `GET /admin/databases/{db}/mvcc/status`
- `POST /admin/databases/{db}/mvcc/pause`
- `POST /admin/databases/{db}/mvcc/resume`
- `POST /admin/databases/{db}/mvcc/prune`
- `POST /admin/databases/{db}/mvcc/schedule`

Notes:

- the `system` database is not supported on this route family
- composite databases do not support MVCC lifecycle controls
- if a database does not expose lifecycle support, the status route returns `{"enabled": false, "database": "..."}`
- when authentication is disabled globally, the server bypasses permission checks

## Operator UI

The admin UI exposes the same lifecycle control plane without requiring direct API calls.

Navigation path:

1. Sign in as an admin user.
2. Open `Security`.
3. Select `MVCC Lifecycle`.

The page only allows standard databases.

- `system` is excluded
- composite databases are excluded
- the database selector switches the panel to another supported database in place

The page is organized into the following areas:

1. Top summary cards
   Pressure band, runtime mode, current interval, debt size, active readers, and oldest reader age.

2. Runtime controls
   Update the lifecycle interval, switch to manual-only mode with `0s`, pause or resume automatic runs, and trigger an immediate prune run.

3. Rollups and pressure panel
   Shows short-window lifecycle activity so operators can judge whether debt is falling or building.

4. Top debt keys table
   Lists the logical keys contributing the most compaction debt. This is the fastest way to confirm whether churn is concentrated in a small part of the dataset.

5. Per-namespace summary
   Shows whether one namespace is dominating debt or prune work.

6. Active readers table
   Highlights readers that may be pinning old versions behind the floor.

Every destructive lifecycle action in the UI is confirmation-gated.

Recommended operator workflow in the UI:

1. Select the target database.
2. Check `pressure band`, `pinned bytes`, and `oldest reader age` first.
3. Review `top debt keys` and `per-namespace` summaries to see where debt is coming from.
4. If the system is stable but debt is climbing, widen or narrow the interval based on workload pattern.
5. If ingest is active or latency is sensitive, switch to `0s` for manual-only mode and run `prune now` later during a quieter window.
6. Use `pause` only when you want lifecycle work completely quiet without changing the configured interval.

## Choosing A Default Interval

The lifecycle interval is a performance dial, not a correctness dial.

Snapshot correctness and retained-floor safety do not depend on whether the automatic loop runs every `30s`, every `5m`, or in manual-only mode. The interval changes:

- how quickly debt is reduced
- how much stale MVCC history can accumulate between runs
- how quickly pressure metrics respond to churn
- how much steady background work you accept during normal operation

Practical default guidance:

1. Most workloads: start at `2m` to `5m`
   This is the best default tradeoff for mixed read/write systems because background maintenance stays active without waking too aggressively.

2. High-write or high-churn workloads: use `30s` to `2m`
   Use this when update or delete churn is continuous and you want debt to stay flatter. This reduces backlog growth but increases steady background maintenance frequency.

3. Read-heavy or lightly changing workloads: use `15m` to `1h`
   If writes are infrequent, a short cadence adds little value. A slower interval usually improves efficiency with no correctness downside.

4. Bulk ingest or latency-sensitive maintenance windows: use `0s`
   Manual-only mode is the safest option when you want full control over when compaction work happens.

For most production deployments, `30s` is more aggressive than necessary as a default. It is reasonable for smaller, churn-heavy systems, but it is not the best general-purpose default if the goal is performance stability across mixed workloads.

For most production deployments, a better starting default is:

- `2m` if you expect steady write traffic
- `5m` if the workload mix is broader and you want lower background maintenance frequency

Avoid daily scheduling as a default for normal databases. A `24h` cadence is usually too slow because:

- compaction debt can accumulate for too long
- pinned-byte pressure becomes less responsive
- recovery from churn shifts from steady maintenance to large catch-up runs

Daily or near-daily runs are only sensible for tiny, low-churn datasets where storage growth and pinned-history buildup are negligible.

## Authentication Example

Bearer token example:

```bash
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:7474/admin/databases/nornic/mvcc
```

Basic auth also works:

```bash
curl -sS \
  -u admin:password123 \
  http://localhost:7474/admin/databases/nornic/mvcc
```

## Status Endpoint

`GET /admin/databases/{db}/mvcc`

`GET /admin/databases/{db}/mvcc/status`

Both routes return the same lifecycle snapshot for the target database.

Example:

```bash
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:7474/admin/databases/nornic/mvcc | jq
```

Example response:

```json
{
  "database": "nornic",
  "enabled": true,
  "running": true,
  "paused": false,
  "automatic": true,
  "cycle_interval": "2m0s",
  "pressure_band": "normal",
  "emergency_mode": false,
  "mvcc_active_snapshot_readers": 0,
  "mvcc_oldest_reader_age_seconds": 0,
  "mvcc_bytes_pinned_by_oldest_reader": 0,
  "mvcc_compaction_debt_bytes": 8192,
  "mvcc_compaction_debt_keys": 3,
  "mvcc_prunable_bytes_total": 8192,
  "mvcc_pruned_bytes_total": 16384,
  "mvcc_tombstone_chain_max_depth": 4,
  "mvcc_floor_lag_versions": 2,
  "mvcc_prune_run_duration_seconds": 0.012,
  "mvcc_prune_run_keys_scanned_total": 9,
  "mvcc_prune_stale_plan_skips_total": 0,
  "last_run": {
    "keys_processed": 3,
    "versions_deleted": 12,
    "bytes_freed": 8192,
    "fence_mismatches": 0,
    "hot_contention_keys": 0
  },
  "per_namespace": {
    "nornic": {
      "compaction_debt_bytes": 8192,
      "compaction_debt_keys": 3,
      "prunable_bytes_total": 8192,
      "pruned_bytes_total": 16384
    }
  },
  "readers": []
}
```

Important fields:

- `enabled`: lifecycle support exists and is enabled in the backend
- `running`: background loop is currently active
- `paused`: automatic loop is paused but manual operations still work
- `automatic`: automatic cadence is enabled; `false` means manual-only mode
- `cycle_interval`: current automatic cadence as a Go duration string
- `pressure_band`: current lifecycle pressure band such as `normal`, `high`, or `critical`
- `mvcc_active_snapshot_readers`: currently registered snapshot readers
- `mvcc_bytes_pinned_by_oldest_reader`: bytes currently pinned behind the oldest reader
- `mvcc_compaction_debt_bytes` and `mvcc_compaction_debt_keys`: estimated prune debt
- `mvcc_prune_stale_plan_skips_total`: keys skipped due to fence mismatch and replan behavior
- `last_run`: summary of the most recent successful automatic or manual prune cycle

## Pause Automatic Lifecycle Work

`POST /admin/databases/{db}/mvcc/pause`

This pauses automatic background lifecycle cycles. It does not disable manual operations.

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:7474/admin/databases/nornic/mvcc/pause
```

Example response:

```json
{
  "status": "lifecycle paused",
  "database": "nornic"
}
```

Use pause when:

- you are diagnosing write latency and want lifecycle work completely quiet
- you are about to run a controlled manual prune window
- you want UI or operator automation to explicitly own maintenance timing

## Resume Automatic Lifecycle Work

`POST /admin/databases/{db}/mvcc/resume`

This resumes automatic lifecycle cycles after a pause.

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:7474/admin/databases/nornic/mvcc/resume
```

Example response:

```json
{
  "status": "lifecycle resumed",
  "database": "nornic"
}
```

## Trigger A One-Off Prune Run

`POST /admin/databases/{db}/mvcc/prune`

This kicks off an immediate lifecycle prune cycle using the database's configured lifecycle policy.

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:7474/admin/databases/nornic/mvcc/prune
```

Example response:

```json
{
  "status": "prune triggered",
  "database": "nornic"
}
```

Use prune-now when:

- you switched a database to manual-only mode and want maintenance on demand
- you just finished a write-heavy ingest window
- you are testing retention behavior under controlled load

## Change The Automatic Schedule

`POST /admin/databases/{db}/mvcc/schedule`

Request body:

```json
{
  "interval": "2m"
}
```

The interval uses Go duration syntax, for example:

- `30s`
- `2m`
- `15m`
- `1h`
- `0s`

`0s` has a special meaning: it disables automatic runs and leaves lifecycle in manual-only mode.

Example:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"interval":"0s"}' \
  http://localhost:7474/admin/databases/nornic/mvcc/schedule | jq
```

Example manual-only response:

```json
{
  "database": "nornic",
  "enabled": true,
  "running": false,
  "paused": false,
  "automatic": false,
  "cycle_interval": "0s"
}
```

Switch back to automatic mode:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"interval":"5m"}' \
  http://localhost:7474/admin/databases/nornic/mvcc/schedule | jq
```

Example automatic response:

```json
{
  "database": "nornic",
  "enabled": true,
  "running": true,
  "paused": false,
  "automatic": true,
  "cycle_interval": "5m0s"
}
```

## How Operators Typically Use It

Common operating patterns:

1. Default scheduled mode
   Run with a moderate cadence such as `2m` or `5m` and monitor debt and pinned bytes.

2. Manual-only during heavy ingest
   Set `interval` to `0s`, let the ingest finish, then call `POST /mvcc/prune` during a quieter window.

3. Pause for incident isolation
   Use `pause` to stop automatic maintenance without changing the configured schedule, then `resume` when the incident window closes.

4. UI-driven maintenance
   Keep the schedule in manual-only mode and let the UI expose explicit "run now" and "resume automatic runs" controls for operators.

## Error Cases

Expected errors:

- `405 Method Not Allowed` when using the wrong HTTP method, such as `GET /mvcc/prune`
- `400 Bad Request` for malformed JSON or invalid schedule durations
- `400 Bad Request` for unsupported databases such as `system` or composite databases
- `404 Not Found` when the target database does not exist
- `200 OK` with `enabled: false` when the database exists but does not expose lifecycle support

## Relationship To Retention Settings

The lifecycle admin API controls when lifecycle work runs, not what retention policy means.

Retention settings still come from database configuration, such as:

- MVCC retention max versions
- MVCC retention TTL
- MVCC lifecycle interval and chain cap defaults

Use this admin API to operate the lifecycle manager. Use configuration to define the retention policy it enforces.

Related guides:

- [Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md)
- [Operations Configuration](../operations/configuration.md)
