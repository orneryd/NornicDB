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

The decay pipeline evaluates two independent things at every read:

```
finalScore  = max(scoreFloor, capped_promoted_curve(t))
suppressed  = finalScore < visibilityThreshold      // strict less-than
```

`scoreFloor` is a **score clamp** — it pins the value the entity reports. `visibilityThreshold` is a **suppression cutoff** — it gates whether the entity is hidden from queries. They are independent levers; setting `scoreFloor` alone does **not** keep an entity visible unless the floor itself clears `visibilityThreshold`. See [`scoreFloor` vs `visibilityThreshold`](../user-guides/decay-profiles.md#scorefloor-vs-visibilitythreshold-they-are-independent) for the full disambiguation table and lifecycle examples.

#### Inverted Curves

A **negative `halfLifeSeconds`** flips the curve in place: `f(t)` becomes `1 - f(t, |halfLife|)`. The score grows from 0 toward the asymptote 1.0 instead of falling. Combined with `scoreFrom: 'LAST_ACCESSED'` this implements an idle-time consolidation curve — time without access strengthens the score and an access resets it. Combined with a promotion profile whose `multiplier < 1.0` you get the dual of the usual recency-bias caching: **frequently-accessed nodes and edges decay faster**, while **idle entries gain strength over time**. See [Inverted Decay](../user-guides/decay-profiles.md#inverted-decay-consolidation) for the full table and Cypher examples.

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

### Suppressing on demand

`kp.suppress(entity)` hides a specific node or relationship, and
`kp.unsuppress(entity)` shows it again. Use them when an earlier
reading is replaced, or a user excludes a link. They work whether or not decay
is enabled. A suppressed entity:

- isn't matched, traversed or counted;
- is no keyword or vector search hit;
- keeps its data, and is visible under `reveal()`.

A relationship's endpoints stay visible.

```cypher
MATCH (n:Reading {id: $old}) CALL kp.suppress(n);
// unsuppress needs reveal() to find the hidden entity
MATCH (n:Reading) WHERE reveal(n).id = $old CALL kp.unsuppress(n);
```

Inside an explicit transaction the change takes effect when the transaction
commits, and not at all if it rolls back. Deleting an entity clears its
suppression, so an entity created again with the same ID is visible. No label
or property hides data: a reserved label would change the meaning of ordinary
Cypher.

### Deindex Cleanup

A background job periodically removes suppressed entities from secondary indexes (BM25, vector) to reclaim space. Primary data remains intact.

## Cypher Introspection

The supported operational surface for knowledge-layer scoring is Cypher, not a dedicated admin HTTP API.

```cypher
CALL kp.info();
CALL kp.profiles();
CALL kp.policies();
CALL kp.resolve('nornic:episode-1', '', '');
CALL kp.deindexStatus();

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
