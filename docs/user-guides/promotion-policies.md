# Promotion Policies

Promotion policies boost an entity's decay score when specific conditions are met. They contain logic: `FOR` targets, `APPLY` blocks with `ON ACCESS` mutations, and `WHEN` predicates that select promotion profiles.

## Architecture

Promotion has two components:

1. **Promotion Profiles** — named parameter bundles declaring multiplier, score floor, score cap (no logic, no targets)
2. **Promotion Policies** — targeted bindings with `FOR`, `APPLY`, `ON ACCESS`, and `WHEN` clauses

## Creating a Promotion Profile

Promotion profiles contain no logic — they are parameter bundles referenced by name inside promotion policy `APPLY` blocks:

```cypher
CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
  multiplier: 1.5,
  scoreFloor: 0.3,
  scoreCap: 1.0
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `multiplier` | `1.0` | Score multiplier. `>1.0` boosts, `1.0` is neutral, `<1.0` dampens. A multiplier of `0.5` halves the decayed score; combined with an inverted decay profile (negative `halfLifeSeconds`) this implements "punish frequent access" semantics where hot nodes are demoted. See [Inverted Decay](decay-profiles.md#inverted-decay-consolidation). |
| `scoreFloor` | `0.0` | Minimum score after promotion |
| `scoreCap` | `1.0` | Maximum score after promotion |

## Creating a Promotion Policy

```cypher
CREATE PROMOTION POLICY <name>
FOR (<target>)
APPLY {
  ON ACCESS {
    <mutations>
  }

  WHEN <predicate>
    APPLY PROFILE '<profile_name>'
}
```

### Example: Basic Tiered Promotion

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

### Example: Negative Multiplier (Access Dampening)

A multiplier below 1.0 dampens the score for entities that hit a hot-path threshold. Pairs with an inverted decay profile to invert the recency bias: frequently-accessed entries are demoted, idle entries strengthen over time. See the full walkthrough in [Inverted Decay](decay-profiles.md#use-case-negative-promotion-combined-with-inverted-decay).

```cypher
CREATE PROMOTION PROFILE access_dampener OPTIONS {
  multiplier: 0.5,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY hot_path_dampening
FOR (n:Memory)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }
  WHEN n.accessCount >= 5
    APPLY PROFILE 'access_dampener'
}
```

### Example: Kalman-Filtered Behavioral Signals

```cypher
CREATE PROMOTION POLICY episodic_recall_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()

    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence

    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
    SET n._lastAgentId = $_agent
  }

  WHEN n.accessCount >= 5 AND n.confidenceScore >= 0.8
    APPLY PROFILE 'high_confidence_tier'

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}
```

## ON ACCESS Semantics

`ON ACCESS` mutations execute when the target entity is accessed and passes the suppression gate. Key rules:

- Mutations apply exclusively to the **accessMeta index**, never to the target node/edge itself
- Property reads (e.g., `n.accessCount`) resolve from accessMeta first, then fall back to stored properties
- Suppressed entities (score below visibility threshold) do not accumulate access state
- `WHEN` predicates read persisted + buffered accessMeta state from prior accesses

## WITH KALMAN

The `WITH KALMAN` clause applies Kalman filtering to smooth behavioral signals:

```cypher
WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence
```

| Parameter | Description |
|-----------|-------------|
| `q` | Process noise covariance (how much the true value changes between observations) |
| `r` | Measurement noise covariance (how noisy observations are) |

When `q` and `r` are omitted, defaults are used. Kalman filtering is appropriate for derived behavioral metrics (access rates, confidence scores) — not for ground-truth values.

## Query Context Variables

`ON ACCESS` blocks can access query context variables projected from request headers:

| Request Header | Available As | Purpose |
|---------------|-------------|---------|
| `X-Query-Session` | `$_session` | Same-session deduplication |
| `X-Query-Agent` | `$_agent` | Agent provenance tracking |

## Listing and Dropping

```cypher
SHOW PROMOTION POLICIES;
DROP PROMOTION POLICY session_record_tiering;
DROP PROMOTION PROFILE reinforced_tier;
```

Dropping a promotion profile that is still referenced by an active policy produces a validation error.

## See Also

- **[Knowledge-Layer Policies](knowledge-layer-policies.md)** — System overview
- **[Decay Profiles](decay-profiles.md)** — Defining decay behavior
- **[Visibility Suppression](visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Ebbinghaus-Roynard Bootstrap](ebbinghaus-roynard-bootstrap.md)** — Complete working example with promotion policies and Kalman filtering
