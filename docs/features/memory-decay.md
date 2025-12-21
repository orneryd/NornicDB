# Memory Decay

**Time-based importance scoring inspired by human memory.**

## Overview

NornicDB implements a memory decay system that naturally reduces the importance of older, unused information while preserving frequently accessed and important data.

## Memory Tiers

Inspired by cognitive science, memories are classified into three tiers:

| Tier | Half-Life | Use Case |
|------|-----------|----------|
| Episodic | ~7 days | Recent events, conversations |
| Semantic | ~69 days | Facts, knowledge, concepts |
| Procedural | ~693 days | Skills, habits, core knowledge |

## How It Works

### Decay Formula

Memory strength decays exponentially over time:

```
strength(t) = initial_strength × e^(-λt)
```

Where:
- `λ` = decay constant (varies by tier)
- `t` = time since last access

### Access Reinforcement

Each access reinforces the memory:

```go
// Memory is reinforced on access
memory := db.Recall(ctx, "mem-123")
// memory.DecayScore is increased
// memory.LastAccessed is updated
// memory.AccessCount is incremented
```

### Tier Promotion

Frequently accessed memories are promoted to more stable tiers:

```
Episodic → Semantic → Procedural
```

## Configuration

### Enable Memory Decay

```yaml
# nornicdb.yaml
decay:
  enabled: true
  recalculate_interval: 1h
  archive_threshold: 0.1  # Archive below 10% strength
```

### Code Configuration

```go
config := nornicdb.DefaultConfig()
config.DecayEnabled = true
config.DecayRecalculateInterval = time.Hour
config.DecayArchiveThreshold = 0.1

db, err := nornicdb.Open("/data", config)
```

## API Usage

### Store with Tier

```go
// Create episodic memory (fast decay)
memory := &Memory{
    Content: "User said hello today",
    Tier:    TierEpisodic,
}
db.Store(ctx, memory)

// Create semantic memory (slow decay)
// Note: TierSemantic is the DEFAULT if no tier is specified
memory := &Memory{
    Content: "User's favorite color is blue",
    Tier:    TierSemantic,  // Optional - this is the default
}
db.Store(ctx, memory)

// Create procedural memory (very slow decay)
memory := &Memory{
    Content: "User prefers dark mode",
    Tier:    TierProcedural,
}
db.Store(ctx, memory)
```

### Check Decay Score

```go
memory, err := db.Recall(ctx, "mem-123")
fmt.Printf("Decay score: %.2f%%\n", memory.DecayScore * 100)
// Decay score: 85.00%
```

### Query by Decay

```cypher
// Find strong memories
MATCH (m:Memory)
WHERE m.decay_score > 0.5
RETURN m ORDER BY m.decay_score DESC

// Find fading memories
MATCH (m:Memory)
WHERE m.decay_score < 0.2
RETURN m
```

## CLI Commands

NornicDB provides CLI commands for managing memory decay. See **[CLI Commands Guide](../operations/cli-commands.md)** for complete documentation.

### Decay Statistics

View aggregate statistics across all memories:

```bash
nornicdb decay stats --data-dir ./data
```

**Output:**
```
📂 Opening database at ./data...
📊 Loading nodes...
📊 Decay Statistics:
  Total memories: 15,234
  Episodic: 5,123 (avg decay: 0.45)
  Semantic: 8,456 (avg decay: 0.72)
  Procedural: 1,655 (avg decay: 0.89)
  Archived: 1,234 (score < 0.05)
  Average decay score: 0.68
```

### Recalculate Decay Scores

Recalculate decay scores for all nodes (useful after bulk imports or configuration changes):

```bash
nornicdb decay recalculate --data-dir ./data
```

**When to use:**
- After bulk data imports
- When decay configuration changes
- Periodic maintenance (e.g., weekly)

**Example:**
```bash
$ nornicdb decay recalculate --data-dir ./data
📂 Opening database at ./data...
📊 Loading nodes...
🔄 Recalculating decay scores for 15,234 nodes...
   Processed 10000/15234 nodes...
✅ Recalculated decay scores: 3,245 nodes updated
```

### Archive Low-Score Memories

Archive nodes with decay scores below a threshold:

```bash
nornicdb decay archive --data-dir ./data --threshold 0.05
```

**What it does:**
- Marks archived nodes with `archived: true`, `archived_at`, and `archived_score` properties
- Nodes remain in the database but are marked for archival
- Safe to run anytime (read-only operation)

**Example:**
```bash
$ nornicdb decay archive --data-dir ./data --threshold 0.05
📂 Opening database at ./data...
📊 Loading nodes...
📦 Archiving nodes with decay score < 0.05...
✅ Archived 1,234 nodes (decay score < 0.05)
```

**Query archived nodes:**
```cypher
// Find archived nodes
MATCH (n)
WHERE n.archived = true
RETURN n.id, n.archived_at, n.archived_score
ORDER BY n.archived_score
```

### Interactive Shell

Execute Cypher queries interactively:

```bash
nornicdb shell --data-dir ./data
```

**Example session:**
```bash
$ nornicdb shell --data-dir ./data
nornicdb> MATCH (m:Memory) WHERE m.decay_score < 0.1 RETURN count(m) AS weak
weak
---
1234

(1 row(s))
```

See **[CLI Commands Guide](../operations/cli-commands.md)** for complete CLI documentation.

## Archiving

### Automatic Archiving

Memories below the threshold can be archived using the CLI:

```bash
# Archive memories with score < 10%
nornicdb decay archive --data-dir ./data --threshold 0.1
```

Archived nodes are marked with properties but remain in the database for querying and potential restoration.

## Use Cases

### Conversational AI

```go
// Store conversation as episodic memory
memory := &Memory{
    Content: fmt.Sprintf("User: %s\nAssistant: %s", userMsg, response),
    Tier:    TierEpisodic,
    Tags:    []string{"conversation", sessionID},
}
db.Store(ctx, memory)

// Old conversations naturally fade
// Important topics get reinforced through re-access
```

### Knowledge Base

```go
// Store facts as semantic memory
memory := &Memory{
    Content: "The capital of France is Paris",
    Tier:    TierSemantic,
    Tags:    []string{"geography", "facts"},
}
db.Store(ctx, memory)
```

### User Preferences

```go
// Store preferences as procedural memory
memory := &Memory{
    Content: "User prefers formal communication style",
    Tier:    TierProcedural,
    Tags:    []string{"preferences", "communication"},
}
db.Store(ctx, memory)
```

## Integration with Search

Decay scores are used in search ranking:

```go
// Search considers decay in relevance
results, err := db.Remember(ctx, queryEmbedding, 10)
// Results are ranked by: similarity × decay_score
```

### Custom Weighting

```cypher
// Custom decay-aware query
MATCH (m:Memory)
WHERE m.content CONTAINS 'project'
RETURN m, m.decay_score * cosineSimilarity(m.embedding, $query) as score
ORDER BY score DESC
LIMIT 10
```

## Disable Decay

For use cases where decay isn't appropriate:

```yaml
decay:
  enabled: false
```

Or per-memory:

```go
memory := &Memory{
    Content: "Critical system information",
    Properties: map[string]any{
        "no_decay": true,
    },
}
```

## See Also

- **[CLI Commands](../operations/cli-commands.md)** - Complete CLI documentation for decay management
- **[Vector Search](../user-guides/vector-search.md)** - Search with decay
- **[GPU Acceleration](gpu-acceleration.md)** - Performance
- **[Architecture](../architecture/system-design.md)** - System design

