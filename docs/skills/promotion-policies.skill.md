---
name: nornicdb-promotion-policies
description: Boost or dampen scores for NornicDB entities using promotion profiles and promotion policies — multipliers, ON ACCESS mutations, WHEN predicates, Kalman-smoothed behavioral signals. Use when writing CREATE/ALTER/DROP/SHOW PROMOTION PROFILE / POLICY in Cypher, configuring access counters, or shaping reinforcement / dampening behavior.
---

# Promotion Profiles & Policies (Cypher API)

Promotion is the optional second half of NornicDB's scoring pipeline. Decay handles time-based fade and runs entirely on its own (see `nornicdb-knowledge-policies` and `nornicdb-decay-tuning`). Promotion does two distinct things:

1. **Tracks access** — `ON ACCESS` mutations write to per-entity access metadata (counters, last-access timestamps, smoothed signals). This is the *only* mechanism that updates access metadata.
2. **Changes scores conditionally** — `WHEN <predicate> APPLY PROFILE '<name>'` causes a promotion profile's `multiplier`/`scoreFloor`/`scoreCap` to be applied to the decayed score when the predicate is true.

These two responsibilities are independent:

- A policy with `ON ACCESS` but no `WHEN` clauses **only tracks**. Scores are not promoted, but `LAST_ACCESSED` decay bindings on the same target can rely on the tracking.
- A policy with `WHEN` but no `ON ACCESS` **only promotes**. It evaluates predicates against existing properties / metadata at score time.
- A policy with both does both.

## Two objects

- **Promotion profile — `CREATE PROMOTION PROFILE <name> OPTIONS { ... }`**: a named, untargeted parameter set. Inert until referenced by a `WHEN ... APPLY PROFILE '<name>'` clause.
  ```cypher
  CREATE PROMOTION PROFILE <name> OPTIONS {
    multiplier: <float>,    -- < 1.0 dampens, > 1.0 boosts, 1.0 neutral
    scoreFloor: <float>,    -- promoted-score floor, applied before scoreCap
    scoreCap:   <float>,    -- promoted-score ceiling, applied after multiplier
    scope:      'NODE'      -- NODE | EDGE
  }
  ```
- **Promotion policy — `CREATE PROMOTION POLICY <name> FOR (...) APPLY { ON ACCESS { ... } WHEN ... APPLY PROFILE '...' }`**: targets entities with `FOR` and contains the access mutations and/or `WHEN` clauses. The `FOR` clause uses the same target shapes as decay bindings (label, multi-label, edge type, wildcard).

## How promotion enters the score

```
baseScore     = baseDecay(t)                                  // from the decay binding
promoted      = baseScore * multiplier                        // multiplier from the matched WHEN's profile
clampedPromo  = min(scoreCap, max(scoreFloor, promoted))      // promotion profile's floor/cap
finalScore    = max(decayBindingFloor, clampedPromo)          // decay binding's floor (independent)
suppressed    = finalScore < visibilityThreshold              // decay binding's threshold
```

Order:
1. Decay computes `baseScore` from the binding's parameters and the entity's anchor timestamp.
2. If a promotion policy targets this entity *and* a `WHEN` predicate matches, the predicate's profile contributes `multiplier`, `scoreFloor`, `scoreCap`. If no `WHEN` matches (or no policy targets the entity), `multiplier = 1.0` and the promotion floor/cap don't apply.
3. The decay binding's `scoreFloor` clamps the result.
4. The decay binding's `visibilityThreshold` is the suppression cutoff.

This is why `multiplier > 1` paired with `scoreCap < 1` is meaningful: the multiplier lifts the hot path, the cap stops it from dominating ranking.

## ON ACCESS mutations

```cypher
CREATE PROMOTION POLICY episode_reinforcement
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount    = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    SET n.totalDuration  = coalesce(n.totalDuration, 0) + $duration
  }
  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_episode'
}
```

What ON ACCESS actually does, precisely:

- **Trigger:** every read or traversal of an entity that matches the policy's `FOR` clause fires the block. (A query that doesn't touch the entity does not.)
- **Target of the writes:** access metadata — a per-entity store keyed by entity ID, separate from `n.Properties`. Reading `n.accessCount` inside `SET` expressions reads from this same metadata store. The node's stored properties are never mutated by `ON ACCESS`.
- **Inspecting the writes:** `policy(n)` returns the access metadata; `nornicdb.knowledgepolicy.resolve(...)` includes effective values; a plain `MATCH (n) RETURN n` shows unchanged stored properties.
- **Timing:** mutations are buffered by an access flusher and committed in batches. Reads immediately following an access may not yet observe the new counter.
- **Read inside SET:** the right-hand side can read existing access metadata (`n.accessCount`, `n.lastAccessedAt`, ...), the entity's stored properties, and query parameters (`$x`) via the bind variable.
- **One mutation per `SET`.** Use multiple `SET` lines instead of comma-separated assignments.

What ON ACCESS does **not** do:

- It does not trigger or "tick" decay. Decay runs on every read regardless. ON ACCESS only updates the metadata that `LAST_ACCESSED`-anchored decay reads.
- It does not change `n.Properties`. The node payload is untouched.
- It does not return values. `SET` mutates; the policy block has no `RETURN`.

## Kalman-smoothed signals

Behavioral signals (clicks, dwell time, vote counts) are noisy and induce sycophantic feedback if applied raw. Wrap a SET with `WITH KALMAN { ... }` to smooth it:

```cypher
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1

    WITH KALMAN { q: 0.1, r: 88.0, varianceScale: 10.0, windowSize: 32 }
      SET n.relevance = $observation
  }
  WHEN n.relevance > 0.7
    APPLY PROFILE 'high_relevance'
}
```

Defaults applied when a key is omitted:
- `q = 0.1` — process noise (how fast the underlying value drifts)
- `r = 88.0` — measurement noise
- `varianceScale = 10.0` — auto-R sensitivity
- `windowSize = 32` — rolling window for variance estimation

Setting `r` explicitly switches the filter from auto-R mode to manual mode. Auto mode is recommended unless you have a known sensor model.

## WHEN predicates

`WHEN <expr> APPLY PROFILE '<name>'` runs after ON ACCESS mutations are flushed, so predicates can reference the freshly-updated access fields:

```cypher
WHEN n.accessCount >= 3                APPLY PROFILE 'reinforced'
WHEN n.lastAccessedAt > timestamp() - 3600000  APPLY PROFILE 'hot'
WHEN n.relevance > 0.7 AND n.tenantId = $tenant  APPLY PROFILE 'tenant_high'
```

Multiple `WHEN` clauses are evaluated in declaration order; the first matching profile wins.

## Targets

Promotion policies use the same `FOR` shapes as decay bindings:

```cypher
FOR (n:Label)                    -- single label
FOR (n:Label1:Label2)            -- all labels must match
FOR ()-[r:CO_ACCESSED]-()        -- edge type
FOR ()                           -- wildcard (catches everything else)
```

## Common patterns

### Reinforce after N accesses

```cypher
CREATE PROMOTION PROFILE reinforced OPTIONS { multiplier: 1.5, scoreFloor: 0.20, scoreCap: 0.95 }

CREATE PROMOTION POLICY reinforce_after_three
FOR (n:Memory)
APPLY {
  ON ACCESS { SET n.accessCount = coalesce(n.accessCount, 0) + 1 }
  WHEN n.accessCount >= 3 APPLY PROFILE 'reinforced'
}
```

### Dampen frequently-accessed (anti-sycophancy)

```cypher
CREATE PROMOTION PROFILE access_dampener OPTIONS { multiplier: 0.5, scoreFloor: 0.0, scoreCap: 1.0 }

CREATE PROMOTION POLICY hot_path_dampening
FOR (n:Memory)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }
  WHEN n.accessCount >= 5 APPLY PROFILE 'access_dampener'
}
```

Combined with an inverted decay profile (`halfLifeSeconds: -86400, scoreFrom: 'LAST_ACCESSED'`), this implements interference-driven forgetting: idle entries strengthen, frequently-accessed entries get pinned at half-strength.

### Recency-weighted edge boost

```cypher
CREATE PROMOTION PROFILE fresh_edge OPTIONS { multiplier: 2.0, scoreCap: 1.0, scope: 'EDGE' }

CREATE PROMOTION POLICY fresh_co_access
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()
  }
  WHEN r.lastTraversedAt > timestamp() - 86400000  -- last 24h
    APPLY PROFILE 'fresh_edge'
}
```

### Tenant-gated boost

```cypher
CREATE PROMOTION POLICY tenant_priority
FOR (n:Document)
APPLY {
  WHEN n.tenantId = $priorityTenant AND n.kind = 'high_value'
    APPLY PROFILE 'reinforced'
}
```

No ON ACCESS — pure score-time predicate.

## Diagnostics

```cypher
SHOW PROMOTION PROFILES
SHOW PROMOTION POLICIES

-- Combined catalog including profile vs policy rows
CALL nornicdb.knowledgepolicy.policies()

-- Effective resolution for an entity. Reports the resolved decay binding,
-- the targeted promotion policy, and the promotion profile NornicDB would
-- pick for this entity if every WHEN matched. Note: resolve() runs as a
-- dry-run with empty access metadata, so WHEN predicates that depend on
-- access counts may not reflect the live decision.
CALL nornicdb.knowledgepolicy.resolve('nornic:abc-123', '', '')

-- Inspect access metadata
MATCH (n:Memory {id: $id}) RETURN policy(n)
```

## Lifecycle

```cypher
ALTER PROMOTION PROFILE reinforced SET OPTIONS { multiplier: 1.75 }
ALTER PROMOTION POLICY reinforce_after_three DISABLE
ALTER PROMOTION POLICY reinforce_after_three ENABLE
ALTER PROMOTION POLICY reinforce_after_three SET OPTIONS { enabled: true } DISABLE
DROP   PROMOTION POLICY IF EXISTS reinforce_after_three
DROP   PROMOTION PROFILE IF EXISTS reinforced
```

`SET OPTIONS` accepts only the boolean `enabled` option. A terminal `ENABLE` or
`DISABLE` overrides a valid `enabled` option; unknown options, invalid types,
misspelled clauses, incomplete blocks, and trailing text are rejected without
changing the policy. CREATE and ALTER policy definitions also reject text after
their target or APPLY block.

Native decay and promotion commands use the same DDL handler with either
`NORNICDB_PARSER=nornic` or `NORNICDB_PARSER=antlr`.

CREATE and ALTER promotion profiles share case-insensitive option keys and the
same field-specific decoding. Numeric fields remain numeric for literal `0` and
`1`; the `enabled` option accepts boolean literals and the native `0`/`1` forms.
Unknown options, invalid values, and wrongly typed values are rejected without
changing the stored profile. `scope` can be set during creation or alteration.

Profile and policy statements accept one optional trailing semicolon. Additional
text after an OPTIONS or APPLY block is rejected rather than silently ignored.

CREATE PROMOTION POLICY requires an explicit `FOR` target pattern. Use `FOR (n)`
for an unlabeled-node target; node and relationship targets remain supported.
Native CREATE declarations are schema operations, not graph entity writes.

Native profile/policy changes are transaction-local. SHOW statements in the same
transaction see staged definitions; other transactions see committed definitions.
COMMIT publishes the persisted native snapshot and ROLLBACK discards it. Concurrent
changes to the same database's schema can conflict instead of overwriting each
other. These metadata-only mutations do not increase entity-operation counters.

Drop the policy before the profile if both are going away — dropping a profile that policies still reference produces a validation error.

## Gotchas

- **`ON ACCESS` mutations are eventually-consistent.** Buffered in an access flusher and committed in batches. A read taken immediately after an access may not yet observe the new counter. Don't rely on it being synchronous.
- **A promotion policy is not required for decay to work.** Create promotion only to (a) update `lastAccessedAt` for `LAST_ACCESSED` decay, (b) track behavioral signals, or (c) conditionally apply a profile.
- **`ON ACCESS` writes go to access metadata, never to `n.Properties`.** Inspect with `policy(n)` or `nornicdb.knowledgepolicy.resolve(...)`.
- **`WITH KALMAN` only smooths a single SET expression.** Chain multiple Kalman blocks if you have multiple noisy fields.
- **`multiplier: 0.0`** collapses the promoted score to the promotion `scoreFloor`. To "disable" a profile, `ALTER` it (e.g. set `multiplier: 1.0`) or drop the policy that references it.
- **`scoreCap < scoreFloor` on a promotion profile** is a misconfiguration. Clamping order is floor-then-cap, so the cap wins and the floor becomes inert.
- **A `WHEN` predicate that references an undefined property** returns null and is treated as not matching. Use `coalesce(n.foo, 0)` to be explicit.
- **Multiple `WHEN` clauses are tried in declaration order; first match wins.** Order from most-specific to most-general.
- **`ALTER PROMOTION POLICY ... ENABLE / DISABLE`** is a runtime toggle. A disabled policy still appears in `SHOW PROMOTION POLICIES` but neither tracks access nor promotes scores.
