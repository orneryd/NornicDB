# Decay Profiles

Decay profiles define how a node or edge's visibility score decreases over time. Every decay profile must include `FOR` and `APPLY` clauses that bind it to a specific target.

## Creating a Targeted Decay Profile

```cypher
CREATE DECAY PROFILE <name>
FOR (<target>)
APPLY {
  <directives>
}
```

### Targets

```cypher
-- Node by label
FOR (n:SessionRecord)

-- Edge by type
FOR ()-[r:CO_ACCESSED]-()

-- Multi-label node
FOR (n:SessionRecord:MemoryEpisode)
```

### APPLY Directives

| Directive | Description |
|-----------|-------------|
| `DECAY HALF LIFE <seconds>` | Time in seconds until score reaches 50% |
| `DECAY PROFILE '<bundle_name>'` | Reference a named parameter bundle |
| `DECAY VISIBILITY THRESHOLD <float>` | Score below which the entity is suppressed (default: 0.05) |
| `DECAY FLOOR <float>` | Minimum score (entity never falls below this) |
| `NO DECAY` | Entity never decays (score stays at 1.0) |

### Property-Level Rules

Properties can have their own decay directives inside the `APPLY` block:

```cypher
n.propertyName DECAY HALF LIFE <seconds>
n.propertyName DECAY PROFILE '<bundle_name>'
n.propertyName DECAY FLOOR <float>
n.propertyName NO DECAY
```

## Creating a Parameter Bundle

Parameter bundles are reusable configuration objects with no `FOR` clause. They declare values only and are referenced by name inside targeted profiles:

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.01
}
```

### Bundle Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `halfLifeSeconds` | int | Time in seconds until score reaches 50%. **Negative values invert the curve** — see [Inverted Decay](#inverted-decay-consolidation). |
| `function` | string | `exponential`, `linear`, `step`, or `none` |
| `visibilityThreshold` | float | Score below which the entity is suppressed |
| `scoreFloor` | float | Minimum score |
| `scoreFrom` | string | `CREATED`, `VERSION`, `CUSTOM`, or `LAST_ACCESSED` |
| `scoreFromProperty` | string | Property name when scoreFrom is `CUSTOM` |

### Decay Functions

**Exponential** — Natural decay curve:
```
score(t) = e^(-ln(2)/halfLife * t)
```

**Linear** — Steady decrease to zero:
```
score(t) = max(0, 1 - t/(2 * halfLife))
```

**Step** — Binary: full score, then zero:
```
score(t) = 1.0 if t < halfLife, else 0.0
```

**None** — No decay, score stays at 1.0.

### ScoreFrom Modes

| Mode | Behavior |
|------|----------|
| `CREATED` | Decay starts from the entity's creation timestamp |
| `VERSION` | Decay restarts from the most recent update timestamp |
| `CUSTOM` | Decay starts from a custom property |
| `LAST_ACCESSED` | Decay starts from the entity's last access timestamp; falls back to `CREATED` until an access is recorded. Pairs with a negative `halfLifeSeconds` to model Ebbinghaus-Roynard consolidation, where time-since-last-access strengthens the score and an access resets it. |

## Inverted Decay (Consolidation)

A **negative `halfLifeSeconds`** flips the chosen function family in place: the compiled score becomes `1 - f(age, |halfLife|)` instead of `f(age, halfLife)`. The dispatch is purely a curve property — it composes with every function (`exponential`, `linear`, `step`) and with every `scoreFrom` anchor.

| `halfLifeSeconds` sign | Curve shape | Score at age 0 | Asymptote at large age |
|------------------------|-------------|----------------|-----------------------|
| Positive | Forgetting (Ebbinghaus) — strong now, fades over time | `1.0` | `0.0` |
| Negative | Consolidation (Roynard) — weak now, strengthens over time | `0.0` | `1.0` |

### Use Case: Ebbinghaus-Roynard Consolidation

Combine a negative half-life with `scoreFrom: 'LAST_ACCESSED'` to invert the cognitive model: the entity gains visibility while idle and resets on every access.

```cypher
CREATE DECAY PROFILE consolidation_curve OPTIONS {
  halfLifeSeconds: -86400,        -- one-day inversion (negative)
  function: 'exponential',
  scoreFrom: 'LAST_ACCESSED',
  visibilityThreshold: 0.10,
  scoreFloor: 0.0
}
```

A node bound to `consolidation_curve`:

- Scores `0.0` immediately after access — suppressed unless `reveal()` is used.
- Strengthens monotonically as time passes without access, reaching `0.5` at one day idle and approaching `1.0` over a week.
- Drops back to `0.0` the instant an access is recorded (the `LAST_ACCESSED` anchor is updated to the access time, so the new "age" is 0).

### Floor Selection on Inverted Curves

Inverted curves score **0.0 immediately after access** because the curve `1 - f(0, |H|)` evaluates to `1 - 1 = 0`. Without intervention, an entity disappears the instant it's accessed (since 0.0 is below any positive `visibilityThreshold`). The `scoreFloor` parameter is the lever that prevents this. It's the **last clamp** in the final-score pipeline — applied after the curve, the promotion multiplier, and the promotion cap — so it overrides whatever the curve produces:

```
final = max(scoreFloor, min(scoreCap, max(promoFloor, base * multiplier)))
```

Suppression uses a strict less-than check (`finalScore < visibilityThreshold`), so the operator's job is to choose a floor relative to the threshold:

| `scoreFloor` value | At-access score | Visible at access? | Use case |
|--------------------|-----------------|--------------------|----------|
| `0.0` (default) | `0.0` | No — suppressed | Pure consolidation: hide hot entries until they cool. |
| `< visibilityThreshold` | `floor` | No — suppressed | Same as above; floor is a no-op for visibility but lifts deindex behavior. |
| `= visibilityThreshold` | `floor` | **Yes** — exactly at threshold | "Relevant but not boosted." Entity stays in result sets, ranked at the bottom. |
| `> visibilityThreshold` | `floor` | **Yes** — with headroom | "Always visible with priority over thresholded entries." |

Important: the floor is a no-op once the curve has grown above it. At one half-life the inverted exponential reaches 0.5 — far above any sane floor — so the floor only matters in the moments right after access. The consolidation gradient between idle entries is unaffected.

```cypher
-- Entity stays visible right after access; consolidates above the floor
-- as it idles. Pick scoreFloor: visibilityThreshold for "visible but
-- ranked lowest after access" — the most common Roynard configuration.
CREATE DECAY PROFILE relevant_consolidation OPTIONS {
  halfLifeSeconds: -86400,
  function: 'exponential',
  scoreFrom: 'LAST_ACCESSED',
  visibilityThreshold: 0.10,
  scoreFloor: 0.10
}
```

After a fresh access this profile reports `final = 0.10` (clamped by floor); at one day idle it reports `0.5` (curve overrides floor); at one week idle it reports `~0.99`. A subsequent access drops it back to `0.10` — never below — so the entity is never deindexed even though its score continually resets.

### Use Case: Negative Promotion Combined With Inverted Decay

A `multiplier < 1.0` on a promotion profile **dampens** rather than boosts. Pair it with the inverted curve above to build a "punish frequent access" model that fits Roynard's interference-driven forgetting:

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

Combined behavior:

| Scenario | Decay (inverted) contribution | Promotion contribution | Net |
|----------|-------------------------------|------------------------|-----|
| Just accessed, low access count | `≈0.0` | `1.0` | suppressed |
| Idle for a day, low access count | `≈0.5` | `1.0` | visible |
| Idle for a day, accessCount ≥ 5 | `≈0.5` | `0.5` | borderline-suppressed |
| Idle for a week, accessCount ≥ 5 | `≈0.99` | `0.5` | visible but dampened |

The result is the inverse of the classic graph-DB heuristic: **frequently-accessed nodes and edges decay faster** (each access resets the consolidation clock and the dampener pins the multiplier), while **idle entries gain strength over time**. This matches Roynard's argument that consolidation requires time without retrieval, and is the dual of the recency-bias most caches encode.

> Operational note: inverted profiles bypass the threshold-age fast-path used to skip-scan obviously-visible entries. Read paths score every binding hit, which costs slightly more CPU per read. Reserve inversion for label sets where the consolidation semantics are actually wanted.

## Examples

### Node-Level with Property Rules

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

### Edge-Level Custom Rate

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

### No-Decay (Canonical Links)

```cypher
CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}
```

### Edge with Visibility Override

```cypher
CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY HALF LIFE 604800
  r.confidence DECAY HALF LIFE 86400
  r.confidence DECAY FLOOR 0.25
}
```

## Resolution Priority

When multiple profiles could match a node:

1. Multi-label target (most labels) takes precedence over single-label
2. Exact label match takes precedence over wildcard
3. If two bindings have equal specificity, the resolver returns a diagnostic warning

## Listing and Dropping

```cypher
SHOW DECAY PROFILES;
DROP DECAY PROFILE session_record_retention;
```

Dropping a parameter bundle that is still referenced by an active binding produces a validation error.

## Inspecting Scores

```cypher
-- Current score
MATCH (n:SessionRecord {id: $id})
RETURN n.id, decayScore(n)

-- Full resolution (for debugging)
MATCH (n:SessionRecord {id: $id})
RETURN decay(n)

-- Which profile applies
MATCH (n:SessionRecord)
RETURN policy(n)
```

## See Also

- **[Knowledge-Layer Policies](knowledge-layer-policies.md)** — System overview
- **[Promotion Policies](promotion-policies.md)** — Boosting scores
- **[Visibility Suppression](visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Ebbinghaus-Roynard Bootstrap](ebbinghaus-roynard-bootstrap.md)** — Complete working example with all profile types
