# CLI Commands

**Command-line interface for managing NornicDB databases.**

## Overview

NornicDB provides a comprehensive CLI tool (`nornicdb`) for database management, query execution, and maintenance operations. All commands support the `--data-dir` flag to specify the database location.

## Installation

The CLI is included when you build NornicDB from source:

```bash
# Build the binary
go build -o nornicdb ./cmd/nornicdb

# Or install globally
go install ./cmd/nornicdb
```

## Available Commands

### Server Commands

#### `nornicdb serve`

Start the NornicDB server with Bolt protocol and HTTP API.

```bash
nornicdb serve \
  --data-dir ./data \
  --bolt-port 7687 \
  --http-port 7474
```

**Common Flags:**
- `--data-dir`: Database directory (default: `./data`)
- `--bolt-port`: Bolt protocol port (default: `7687`)
- `--http-port`: HTTP API port (default: `7474`)
- `--address`: Bind address (default: `0.0.0.0`)
- `--no-auth`: Disable authentication (development only)
- `--headless`: Disable web UI

**Example:**
```bash
# Start server with custom data directory
nornicdb serve --data-dir /var/lib/nornicdb --bolt-port 7687
```

#### `nornicdb init`

Initialize a new NornicDB database.

```bash
nornicdb init --data-dir ./mydb
```

Creates the database directory structure and configuration files.

#### `nornicdb import`

Import data from a Neo4j export directory.

```bash
nornicdb import /path/to/export --data-dir ./data
```

**Flags:**
- `--data-dir`: Target database directory
- `--embedding-url`: Embedding API URL (default: `http://localhost:11434`)

**Example:**
```bash
# Import Neo4j export
nornicdb import ./neo4j-export --data-dir ./data
```

### Interactive Shell

#### `nornicdb shell`

Interactive Cypher query shell for executing queries directly.

```bash
nornicdb shell --data-dir ./data
```

**Features:**
- Interactive prompt: `nornicdb>`
- Execute Cypher queries directly
- Tabular result display
- Exit with `exit`, `quit`, or Ctrl+D

**Example Session:**
```bash
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

nornicdb> MATCH (p:Person) RETURN p.name, p.age LIMIT 5
p.name | p.age
---
Alice  | 30
Bob    | 25
Charlie| 35

(3 row(s))

nornicdb> exit
👋 Goodbye!
```

**Flags:**
- `--data-dir`: Database directory (required)

### Knowledge-Layer Scoring Commands

NornicDB uses a profile-driven knowledge-layer scoring system. Decay profiles and promotion policies are defined via Cypher DDL, and nodes/edges are scored based on their bound profile. These commands help manage scoring and visibility.

#### `nornicdb decay recalculate`

Recalculate decay scores for all nodes in the database using their bound decay profiles.

```bash
nornicdb decay recalculate --data-dir ./data
```

**What it does:**
- Loads all nodes from storage
- Resolves each node's decay profile via its labels
- Recalculates scores using the profile's decay function and half-life
- Updates nodes with new scores in memory-efficient chunks

**Example:**
```bash
$ nornicdb decay recalculate --data-dir ./data
📂 Opening database at ./data...
📊 Loading nodes...
🔄 Recalculating decay scores for 10,000 nodes...
   Processed 10000/10000 nodes...
✅ Recalculated decay scores: 3,245 nodes updated
```

**Flags:**
- `--data-dir`: Database directory (required)

#### `nornicdb suppress`

Suppress nodes with decay scores below the visibility threshold.

```bash
nornicdb suppress --data-dir ./data --threshold 0.10
```

**What it does:**
- Loads all nodes and calculates current decay scores
- Identifies nodes scoring below the threshold
- Marks suppressed nodes with `suppressed: true` and `suppressed_at` properties
- Suppressed nodes are hidden from normal queries but remain in storage

**Example:**
```bash
$ nornicdb suppress --data-dir ./data --threshold 0.10
📂 Opening database at ./data...
📊 Loading nodes...
📦 Suppressing nodes with decay score < 0.10...
✅ Suppressed 127 nodes (decay score < 0.10)
```

**Flags:**
- `--data-dir`: Database directory (required)
- `--threshold`: Visibility threshold (default: `0.10`)

**Querying suppressed nodes:**
```cypher
MATCH (n)
CALL reveal(n)
WHERE n.suppressed = true
RETURN n.id, n.suppressed_at, decayScore(n)
ORDER BY decayScore(n)
```

#### `nornicdb decay stats`

Display decay statistics for all nodes.

```bash
nornicdb decay stats --data-dir ./data
```

**What it shows:**
- Total node count
- Score distribution (high/medium/low/suppressed)
- Nodes with no decay profile bound
- Average decay score

**Example:**
```bash
$ nornicdb decay stats --data-dir ./data
📂 Opening database at ./data...
📊 Loading nodes...
📊 Decay Statistics:
  Total nodes: 10,000
  High (>0.5): 6,500 (avg: 0.78)
  Medium (0.1-0.5): 2,800 (avg: 0.28)
  Low (<0.1): 573 (avg: 0.04)
  Suppressed: 127
  No profile: 1,200 (score: 1.0)
  Average decay score: 0.65
```

**Flags:**
- `--data-dir`: Database directory (required)

### Utility Commands

#### `nornicdb version`

Display version information.

```bash
nornicdb version
```

**Output:**
```
NornicDB v1.0.0 (abc1234) built 2024-12-20T10:30:00Z
```

## Knowledge-Layer Scoring

NornicDB implements a profile-driven decay and promotion scoring system for knowledge graphs. Decay behavior, visibility thresholds, and promotion tiers are authored through Cypher DDL. See the user guides for full details:

- **[Decay Profiles](../user-guides/decay-profiles.md)** — Defining decay behavior
- **[Promotion Policies](../user-guides/promotion-policies.md)** — Boosting scores
- **[Visibility Suppression](../user-guides/visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Knowledge-Layer Policies](../user-guides/knowledge-layer-policies.md)** — System overview

## Environment Variables

All commands respect environment variables:

- `NORNICDB_DATA_DIR`: Default data directory
- `NORNICDB_BOLT_PORT`: Default Bolt port
- `NORNICDB_HTTP_PORT`: Default HTTP port

**Example:**
```bash
export NORNICDB_DATA_DIR=/var/lib/nornicdb
nornicdb shell  # Uses /var/lib/nornicdb automatically
```

## Best Practices

### Regular Maintenance

1. **Weekly decay recalculation:**
   ```bash
   nornicdb decay recalculate --data-dir ./data
   ```

2. **Monthly archival:**
   ```bash
   nornicdb decay archive --data-dir ./data --threshold 0.05
   ```

3. **Monitor decay health:**
   ```bash
   nornicdb decay stats --data-dir ./data
   ```

### Performance Tips

- Use `--data-dir` to specify database location explicitly
- Recalculate decay during low-traffic periods
- Archive operations are read-only (safe to run anytime)
- Stats command is fast (read-only, no updates)

### Troubleshooting

**Database not found:**
```bash
# Ensure data directory exists
ls -la /path/to/data

# Initialize if needed
nornicdb init --data-dir /path/to/data
```

**Permission errors:**
```bash
# Check directory permissions
chmod 755 /path/to/data

# Ensure user has write access
chown -R $USER:$USER /path/to/data
```

## Related Documentation

- **[Memory Decay Feature](../features/memory-decay.md)** - Detailed decay system documentation
- **[Operations Guide](README.md)** - Production deployment and maintenance
- **[Backup & Restore](backup-restore.md)** - Data protection strategies
- **[Monitoring](monitoring.md)** - Health checks and metrics

