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

### Memory Decay Commands

NornicDB implements a three-tier memory decay system (Episodic, Semantic, Procedural) that simulates how human memory works. These commands help manage decay scores and archived memories.

#### `nornicdb decay recalculate`

Recalculate decay scores for all nodes in the database.

```bash
nornicdb decay recalculate --data-dir ./data
```

**What it does:**
- Loads all nodes from storage
- Extracts memory tier from node properties (EPISODIC, SEMANTIC, PROCEDURAL)
- Recalculates decay scores using the decay algorithm
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

**When to use:**
- After bulk data imports
- When decay configuration changes
- Periodic maintenance (e.g., weekly)

**Flags:**
- `--data-dir`: Database directory (required)

#### `nornicdb decay archive`

Archive nodes with low decay scores (below threshold).

```bash
nornicdb decay archive --data-dir ./data --threshold 0.05
```

**What it does:**
- Loads all nodes and calculates current decay scores
- Identifies nodes with scores below the threshold
- Marks archived nodes with properties:
  - `archived: true`
  - `archived_at`: Timestamp
  - `archived_score`: Final decay score

**Example:**
```bash
$ nornicdb decay archive --data-dir ./data --threshold 0.05
📂 Opening database at ./data...
📊 Loading nodes...
📦 Archiving nodes with decay score < 0.05...
✅ Archived 127 nodes (decay score < 0.05)
```

**Flags:**
- `--data-dir`: Database directory (required)
- `--threshold`: Archive threshold (default: `0.05`)

**Querying archived nodes:**
```cypher
// Find archived nodes
MATCH (n)
WHERE n.archived = true
RETURN n.id, n.archived_at, n.archived_score
ORDER BY n.archived_score
```

#### `nornicdb decay stats`

Display decay statistics for all nodes.

```bash
nornicdb decay stats --data-dir ./data
```

**What it shows:**
- Total memory count
- Count and average decay score per tier (Episodic, Semantic, Procedural)
- Number of archived nodes
- Overall average decay score

**Example:**
```bash
$ nornicdb decay stats --data-dir ./data
📂 Opening database at ./data...
📊 Loading nodes...
📊 Decay Statistics:
  Total memories: 10,000
  Episodic: 2,500 (avg decay: 0.45)
  Semantic: 6,000 (avg decay: 0.72)
  Procedural: 1,500 (avg decay: 0.89)
  Archived: 127 (score < 0.05)
  Average decay score: 0.68
```

**Flags:**
- `--data-dir`: Database directory (required)

**Use cases:**
- Monitor memory health
- Understand decay patterns
- Plan archival operations

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

## Memory Decay System

NornicDB implements a cognitive science-based memory decay system with three tiers:

### Tier Types

1. **Episodic** (7-day half-life)
   - Short-term memories: chat context, temporary notes, session data
   - Fast decay, requires frequent access to maintain

2. **Semantic** (69-day half-life)
   - Medium-term memories: facts, concepts, user preferences
   - Moderate decay, more stable than episodic

3. **Procedural** (693-day half-life)
   - Long-term memories: skills, patterns, procedures
   - Very slow decay, almost permanent

### Decay Score Calculation

Decay scores (0.0-1.0) are calculated from:
- **Recency**: Time since last access (exponential decay)
- **Frequency**: Number of accesses (logarithmic growth)
- **Importance**: Manual weight or tier default

### Node Properties

Nodes with decay support should have:
- `tier`: Memory tier (`"EPISODIC"`, `"SEMANTIC"`, or `"PROCEDURAL"`)
- `decay_score`: Current decay score (0.0-1.0)
- `last_accessed`: Last access timestamp
- `access_count`: Total access count

**Example node:**
```cypher
CREATE (n:Memory {
  tier: "SEMANTIC",
  decay_score: 0.75,
  last_accessed: datetime(),
  access_count: 10,
  content: "PostgreSQL is our primary database"
})
```

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

