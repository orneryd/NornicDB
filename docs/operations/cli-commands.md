# CLI Commands

**Command-line interface for managing NornicDB databases.**

## Overview

NornicDB ships a single CLI binary (`nornicdb`) for starting the server and running a small set of database-management subcommands. Most subcommands accept the `--data-dir` flag to point at the database location; default is `./data`.

## Installation

The CLI is built from source:

```bash
go build -o nornicdb ./cmd/nornicdb
# or install globally
go install ./cmd/nornicdb
```

## Available Commands

The CLI surface is intentionally small. The full set is:

- `nornicdb version` — print build version.
- `nornicdb serve` — start the server.
- `nornicdb init` — initialize a new data directory.
- `nornicdb shell` — interactive Cypher query shell.
- `nornicdb decay suppress` — re-evaluate suppression status.
- `nornicdb decay stats` — print decay statistics.

For migration, backup, and import operations, use the HTTP/Bolt APIs, the runnable scripts under [`scripts/migration/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration), or the offline admin guide at [admin-tool.md](admin-tool.md). The main `nornicdb` server CLI does not have general-purpose `import` or `export` subcommands; those live under `nornicdb-admin`.

---

### `nornicdb serve`

Start the NornicDB server with Bolt protocol and HTTP API.

```bash
nornicdb serve \
  --data-dir ./data \
  --bolt-port 7687 \
  --http-port 7474
```

**Common flags:**

- `--data-dir`: database directory (default: `./data`; env: `NORNICDB_DATA_DIR`).
- `--bolt-port`: Bolt protocol port (default: `7687`; env: `NORNICDB_BOLT_PORT`).
- `--http-port`: HTTP API port (default: `7474`; env: `NORNICDB_HTTP_PORT`).
- `--address`: bind address (default: `0.0.0.0`).
- `--no-auth`: disable authentication (development only).
- `--headless`: disable web UI.

**Example:**

```bash
nornicdb serve --data-dir /var/lib/nornicdb --bolt-port 7687
```

---

### `nornicdb init`

Initialize a new NornicDB data directory.

```bash
nornicdb init --data-dir ./mydb
```

Creates the directory layout the server expects on first boot.

---

### `nornicdb shell`

Interactive Cypher query shell.

```bash
nornicdb shell --data-dir ./data
```

**Features:**

- Interactive prompt: `nornicdb>`.
- Execute Cypher queries directly.
- Tabular result display.
- Exit with `exit`, `quit`, or Ctrl+D.

**Example session:**

```text
$ nornicdb shell --data-dir ./data
📂 Opening database at ./data...
✅ Connected to NornicDB
Type 'exit' or Ctrl+D to quit
Enter Cypher queries (end with semicolon or newline):

nornicdb> MATCH (n) RETURN count(n) AS total
total
---
42

(1 row(s))

nornicdb> exit
👋 Goodbye!
```

**Flags:**

- `--data-dir`: database directory (required).

---

### `nornicdb decay suppress`

Suppress nodes whose decay scores fall below the visibility threshold. Sets `suppressed: true` and `suppressed_at` on each affected node; suppressed nodes are hidden from normal queries but remain in storage and can be revealed with the `reveal()` Cypher function.

```bash
nornicdb decay suppress --data-dir ./data --threshold 0.10
```

**Flags:**

- `--data-dir`: database directory (default: `./data`).
- `--threshold`: visibility threshold (default: `0.05`).

**Example:**

```text
$ nornicdb decay suppress --data-dir ./data --threshold 0.10
📂 Opening database at ./data...
📊 Loading nodes...
📦 Suppressing nodes with decay score < 0.10...
✅ Suppressed 127 nodes
```

To inspect suppressed nodes from Cypher:

```cypher
MATCH (n) WHERE reveal(n) AND n.suppressed = true
RETURN n.id, n.suppressed_at, decayScore(n)
ORDER BY decayScore(n);
```

---

### `nornicdb decay stats`

Display decay statistics.

```bash
nornicdb decay stats --data-dir ./data
```

**Flags:**

- `--data-dir`: database directory (default: `./data`).

**Example output:**

```text
$ nornicdb decay stats --data-dir ./data
📂 Opening database at ./data...
📊 Decay Statistics:
  Total nodes: 10,000
  High (>0.5): 6,500
  Medium (0.1-0.5): 2,800
  Low (<0.1): 573
  Suppressed: 127
  No profile: 1,200 (score: 1.0)
```

---

### `nornicdb version`

Print version information.

```bash
nornicdb version
```

---

## Knowledge-Layer Scoring

Decay behavior, visibility thresholds, and promotion tiers are authored through Cypher DDL (`CREATE DECAY PROFILE`, `CREATE PROMOTION POLICY`). The CLI subcommands above are operational helpers; the _configuration_ surface is Cypher. See:

- [Decay Profiles](../user-guides/decay-profiles.md) — defining decay behavior.
- [Promotion Policies](../user-guides/promotion-policies.md) — boosting scores.
- [Visibility Suppression and Deindex](../user-guides/visibility-suppression-deindex.md) — suppression and deindex behavior.
- [Knowledge-Layer Policies](../user-guides/knowledge-layer-policies.md) — system overview.

## Environment Variables

The most common server flags also have environment-variable counterparts:

- `NORNICDB_DATA_DIR` — default data directory.
- `NORNICDB_BOLT_PORT` — default Bolt port.
- `NORNICDB_HTTP_PORT` — default HTTP port.

```bash
export NORNICDB_DATA_DIR=/var/lib/nornicdb
nornicdb shell  # uses /var/lib/nornicdb automatically
```

For the full configuration surface, see [Environment Variables Reference](environment-variables.md) and [Configuration](configuration.md).

## Troubleshooting

**Database not found:**

```bash
ls -la /path/to/data
nornicdb init --data-dir /path/to/data   # initialize if needed
```

**Permission errors:**

```bash
chmod 755 /path/to/data
chown -R $USER:$USER /path/to/data
```

## Related Documentation

- [Memory Decay Feature](../features/memory-decay.md) — detailed decay system documentation.
- [Operations Guide](README.md) — production deployment and maintenance.
- [Backup & Restore](backup-restore.md) — data protection strategies.
- [Monitoring](monitoring.md) — health checks and metrics.
- [nornicdb-admin Guide](admin-tool.md) — offline bulk import, `--from-path`, and Neo4j-compatible export workflows.
