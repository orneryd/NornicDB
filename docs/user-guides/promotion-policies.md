# Promotion Policies

Promotion is the optional layer on top of decay. Decay handles time-based fade entirely on its own and **does not need promotion to function**. Promotion does two distinct things:

1. **Tracks access** via `ON ACCESS { ... }` — `SET` mutations write to per-entity access metadata (counters, last-access timestamps, smoothed signals). This is the only mechanism that updates access metadata.
2. **Changes scores conditionally** via `WHEN <predicate> APPLY PROFILE '<name>'` — when a predicate matches at score time, the named promotion profile's `multiplier`/`scoreFloor`/`scoreCap` apply to the decayed score.

The two responsibilities are independent:

- A policy with `ON ACCESS` and no `WHEN` clauses **only tracks**. It updates access metadata so a `LAST_ACCESSED`-anchored decay binding can use it. Scores are not promoted.
- A policy with `WHEN` and no `ON ACCESS` **only promotes**. It evaluates predicates against existing properties or pre-existing metadata at score time.
- A policy with both does both.

## Architecture

Promotion has two object kinds. One is an inert parameter package; the other carries a `FOR (...)` target that selects entities.

| Kind | DDL form | Has target? | What it does on its own |
|---|---|---|---|
| **Promotion profile** | `CREATE PROMOTION PROFILE <name> OPTIONS { ... }` | No | Nothing — names a `multiplier`/`scoreFloor`/`scoreCap` set |
| **Promotion policy** | `CREATE PROMOTION POLICY <name> FOR (...) APPLY { ON ACCESS { ... } WHEN ... APPLY PROFILE '...' }` | Yes (`FOR`) | Tracks access, applies promotion profiles when `WHEN` predicates match, or both |

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
| `scoreFloor` | `0.0` | Minimum score AFTER the multiplier is applied. **Different from the decay bundle's `scoreFloor`** — this one acts inside the promoted-curve computation (`max(promoFloor, base * multiplier)`), the bundle's floor acts as the final clamp on the result. Neither is a visibility cutoff; the suppression check is `visibilityThreshold` on the decay bundle. |
| `scoreCap` | `1.0` | Maximum score after promotion |

> The decay pipeline is `final = max(decayBundle.scoreFloor, min(promoProfile.scoreCap, max(promoProfile.scoreFloor, base * multiplier)))`, then `suppressed = final < decayBundle.visibilityThreshold`. The promotion floor only matters when the matching `WHEN` predicate fires; the decay floor matters always. Pick the right floor for the right job.

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

For repeatable bootstrap scripts, use `CREATE PROMOTION POLICY IF NOT EXISTS <name> FOR ... APPLY { ... }` (or place `IF NOT EXISTS` after the name). An existing policy is left unchanged; without the guard, a duplicate name returns an error.

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

`ON ACCESS` mutations execute when the target entity is read or traversed and passes the suppression gate.

What ON ACCESS *does*:

- **Trigger:** every read or traversal of an entity matching the policy's `FOR` clause fires the block. A query that doesn't touch the entity does not.
- **Target of writes:** the per-entity **access metadata** store, keyed by entity ID. This store is separate from `n.Properties`; the node payload is never mutated by `ON ACCESS`.
- **Read inside `SET`:** property reads (e.g., `n.accessCount`) resolve from access metadata first, then fall back to stored properties. You can also reference parameters (`$x`) and use Cypher expressions on the right-hand side.
- **Timing:** mutations are buffered by an access flusher and committed in batches. A read taken immediately after an access may not yet observe the new counter.
- **Predicate fairness:** suppressed entities (score below `visibilityThreshold`) do not accumulate access state, so policies cannot use ON ACCESS to "rescue" hidden items.
- **`WHEN` evaluation:** runs at score time against the persisted + buffered access metadata.

What ON ACCESS does **not** do:

- It does not trigger or "tick" decay. Decay runs on every read regardless of whether any `ON ACCESS` has fired.
- It does not mutate `n.Properties`. The stored payload is unchanged; use `policy(n)` or `kp.resolve(...)` to inspect access metadata.
- It does not return values. The block has no `RETURN`; it only mutates.

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
