# Knowledge-Layer Scoring and Visibility

**Profile-driven decay and promotion scoring for knowledge graphs.**

## Overview

NornicDB implements a knowledge-layer scoring system that manages the lifecycle and visibility of nodes and edges through declarative decay profiles and promotion policies. Administrators define profiles via Cypher DDL and bind them to specific labels or edge types using `FOR` and `APPLY` blocks.

## Key Concepts

### Decay Profiles

A decay profile is a targeted binding that defines how a node or edge's visibility score decreases over time. Every decay profile must include `FOR` and `APPLY` clauses:

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}
```

| Directive | Description |
|-----------|-------------|
| `DECAY HALF LIFE <seconds>` | Time in seconds until score reaches 50% |
| `DECAY PROFILE '<name>'` | Reference a named parameter bundle |
| `DECAY VISIBILITY THRESHOLD <float>` | Score below which the entity is suppressed |
| `DECAY FLOOR <float>` | Minimum score (entity never falls below this) |
| `NO DECAY` | Entity never decays (score stays at 1.0) |

### Parameter Bundles

Reusable configuration objects with no `FOR` clause:

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.01
}
```

### Promotion Policies

Promotion policies boost a node's score when conditions are met. They contain `ON ACCESS` mutations and `WHEN` predicates:

```cypher
CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROFILE 'canonical_tier'
}
```

### Scoring Formula

The decay score at time `t`:

```
score(t) = max(scoreFloor, f(t) * promotionMultiplier)
```

Where `f(t)` is the decay function applied since the score anchor (creation time or version time).

## Configuration

### Enable Scoring

```yaml
# nornicdb.yaml
memory:
  decay_enabled: true
  visibility_threshold: 0.05
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_MEMORY_DECAY_ENABLED` | `false` | Enable the scoring system |
| `NORNICDB_VISIBILITY_THRESHOLD` | `0.05` | Default visibility threshold |
| `NORNICDB_DEFAULT_NODE_LABEL` | `Memory` | Default label for MCP store operations |

## Cypher DDL Examples

### Node-Level Decay with Property Rules

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}
```

### Edge-Level Decay

```cypher
CREATE DECAY PROFILE coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY HALF LIFE 1209600
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}
```

### No-Decay Profile

```cypher
CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}
```

### List and Drop

```cypher
SHOW DECAY PROFILES;
SHOW PROMOTION POLICIES;
DROP DECAY PROFILE session_record_retention;
```

## Querying with Decay

### Decay-Aware Queries

```cypher
-- Find strong nodes
MATCH (n:Document)
WHERE decayScore(n) > 0.5
RETURN n ORDER BY decayScore(n) DESC

-- Include suppressed nodes (bypass visibility)
MATCH (n:Document)
CALL reveal(n)
RETURN n, decayScore(n)
```

### Cypher Functions

| Function | Description |
|----------|-------------|
| `decayScore(entity)` | Current decay score (0.0-1.0) |
| `decay(entity)` | Full scoring resolution with metadata |
| `reveal(entity)` | Bypass visibility suppression |
| `policy(entity)` | Show which profile/policy applies |

## Visibility Suppression

Nodes and edges whose decay score drops below the visibility threshold are automatically suppressed. Suppressed entities:

- Are excluded from search results
- Are excluded from MATCH queries (unless `reveal()` is used)
- Retain their data and can be restored

### Deindex Cleanup

A background job periodically removes suppressed entities from secondary indexes (BM25, vector) to reclaim space. Primary data remains intact.

## Cypher Introspection

The supported operational surface for knowledge-layer scoring is Cypher, not a dedicated admin HTTP API.

```cypher
CALL nornicdb.knowledgepolicy.info();
CALL nornicdb.knowledgepolicy.profiles();
CALL nornicdb.knowledgepolicy.policies();
CALL nornicdb.knowledgepolicy.resolve('nornic:episode-1', '', '');
CALL nornicdb.knowledgepolicy.deindexStatus();

SHOW DECAY PROFILES;
SHOW PROMOTION PROFILES;
SHOW PROMOTION POLICIES;
```

Use these procedures to inspect effective policy resolution, catalog stored profiles and policies, and monitor deindex cleanup state.

## Disable Scoring

```yaml
memory:
  decay_enabled: false
```

When disabled, all entities score 1.0 and nothing is suppressed.

## See Also

- **[Decay Profiles Guide](../user-guides/decay-profiles.md)** — Authoring decay profiles
- **[Promotion Policies Guide](../user-guides/promotion-policies.md)** — Authoring promotion policies
- **[Visibility Suppression Guide](../user-guides/visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Ebbinghaus-Roynard Bootstrap](../user-guides/ebbinghaus-roynard-bootstrap.md)** — Complete ready-to-use scoring configuration
- **[CLI Commands](../operations/cli-commands.md)** — CLI decay management
