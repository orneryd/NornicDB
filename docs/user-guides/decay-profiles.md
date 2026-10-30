# Decay Profiles

NornicDB's decay surface uses **two distinct objects**, both spelled `CREATE DECAY PROFILE`. The parser distinguishes them by what follows the name:

| Form | Object kind | Has target? | What it does on its own |
|---|---|---|---|
| `CREATE DECAY PROFILE <name> OPTIONS { ... }` | **Decay bundle** | No | Nothing — names a reusable parameter set |
| `CREATE DECAY PROFILE <name> FOR (...) APPLY { ... }` | **Decay binding** | Yes (`FOR`) | Activates decay scoring for matched entities; references a bundle and/or sets parameters inline |

A **bundle** is a parameter package. It never selects entities, never runs code, and never changes any score on its own. A **binding** has the `FOR (...)` clause that targets entities and the `APPLY { ... }` block that either references a bundle by name (`DECAY PROFILE 'bundle_name'`) or sets parameters via inline directives.

Decay is **pure time math, evaluated on every read** — a binding alone is sufficient. It does not need a promotion policy or `ON ACCESS` to function. (`ON ACCESS` belongs to promotion policies, not decay bindings.)

## Creating a Targeted Decay Binding

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

Parameter bundles are reusable configuration objects with **no target** — no `FOR` clause, no effect on any entity until a binding references them. A bundle is purely a named parameter package:

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.01
}
```

The bundle does nothing on its own. Decay scoring activates only when a binding references it:

```cypher
CREATE DECAY PROFILE memory_binding
FOR (n:Memory)
APPLY { DECAY PROFILE 'working_memory' }
```

### Bundle Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `halfLifeSeconds` | int | Time in seconds until score reaches 50%. **Negative values invert the curve** — see [Inverted Decay](#inverted-decay-consolidation). |
| `function` | string | `exponential`, `linear`, `step`, or `none` |
| `visibilityThreshold` | float | **Suppression cutoff.** If `finalScore < visibilityThreshold` the entity is hidden from queries and eligible for deindex. Boolean gate; does not change the score itself. |
| `scoreFloor` | float | **Score clamp.** The reported score can never be lower than `scoreFloor`, no matter what the decay curve and promotion multiplier compute. Pure arithmetic; the last `max()` in the score pipeline. |
| `scoreFrom` | string | `CREATED`, `VERSION`, `CUSTOM`, or `LAST_ACCESSED` |
| `scoreFromProperty` | string | Property name when scoreFrom is `CUSTOM` |

#### `scoreFloor` vs `visibilityThreshold` — They Are Independent

The two parameters do different jobs and can be set independently. The pipeline is:

```
finalScore  = max(scoreFloor, capped_promoted_score)
suppressed  = finalScore < visibilityThreshold        // strict less-than
```

| | `scoreFloor` | `visibilityThreshold` |
|---|---|---|
| **What it controls** | The score *value* | Whether the entity is *visible* |
| **Where it acts** | Last clamp in `computeFinalScore` | Boolean gate after the score is computed |
| **Effect on score** | Pins it upward | None — it just compares |
| **Effect on visibility** | Indirect — only matters when the floor lifts the score above the threshold | Direct — the cutoff itself |

Three concrete configurations show how they compose. Assume a forward exponential profile that has decayed to `0.02` (well below threshold):

| `scoreFloor` | `visibilityThreshold` | finalScore | Visible? | Why |
|---|---|---|---|---|
| `0.0` | `0.10` | `0.02` | No | Curve dropped to 0.02; floor doesn't lift; 0.02 < 0.10 → suppressed. |
| `0.05` | `0.10` | `0.05` | No | Floor raised score to 0.05; **but 0.05 < 0.10**, still suppressed. |
| `0.10` | `0.10` | `0.10` | **Yes** | Floor raised score to threshold; strict `<` means equality passes. |
| `0.30` | `0.10` | `0.30` | **Yes** (with headroom) | Floor pins score above the threshold; entity ranks above thresholded peers. |

Key takeaway: setting `scoreFloor` alone does **not** make an entity stay visible. It only makes the entity stay visible **if the floor is high enough to clear `visibilityThreshold`**.

There's also a third use of a non-zero floor that has nothing to do with visibility: some downstream code (suppression sweepers, tombstone cleanup, ranking layers) treats a strict-zero score differently from a small non-zero score. A `scoreFloor: 0.05` with `visibilityThreshold: 0.10` produces an entity that is *suppressed but not at strict zero* — useful when you want gradual deindex pressure without permanent collapse.

#### Forward-Decay Lifecycle Example

The two levers also matter on the standard (non-inverted) decay curve. Consider a `Document` that decays exponentially with a 7-day half-life and the bundle:

```cypher
CREATE DECAY PROFILE doc_retention OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.05
}
```

| Time since anchor | Curve output | After `max(floor, curve)` | Visible? | Why |
|---|---|---|---|---|
| `0` (just created) | `1.000` | `1.000` | Yes | curve well above threshold; floor inactive |
| `1 half-life` (7d) | `0.500` | `0.500` | Yes | curve still above threshold |
| `2 half-lives` (14d) | `0.250` | `0.250` | Yes | curve above threshold |
| `~3.32 half-lives` (~23d) | `0.100` | `0.100` | **Yes** (border) | exactly at threshold; strict `<` keeps it visible |
| `4 half-lives` (28d) | `0.0625` | `0.0625` | No | below threshold; floor inactive (curve > floor) |
| `~4.32 half-lives` (~30d) | `0.050` | `0.050` | No | curve == floor; floor takes over from here |
| `10 half-lives` (70d) | `0.001` | `0.050` | No | floor pinned the score; still suppressed |

Two things to notice in the forward-decay case:

1. **The floor only activates once the curve has dropped below `0.05`** (around 4.32 half-lives). Before that, the curve is the binding constraint and the floor is invisible.
2. **The floor doesn't make the entity visible** — `0.05 < 0.10` is still suppressed. The entity stays hidden but its score never collapses to zero. Move it back to visible by promoting (`multiplier > 1` lifts the score above threshold) or by raising the floor to `0.10` if you want unconditional visibility.

To make the forward-decay entity become visible again after time, raise the floor to match the threshold:

```cypher
-- Forward decay, but the entity stays at 0.10 (visible) forever even after
-- the curve hits zero. Use this when "old but never deleted" matters.
CREATE DECAY PROFILE doc_persistent OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.10
}
```

This gives a curve that fades from `1.0` to `0.10` over time, then plateaus at `0.10` forever — visible but ranked at the bottom.

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
| `LAST_ACCESSED` | Decay starts from the entity's last access timestamp; falls back to `CREATED` until an access is recorded. Pairs with a negative `halfLifeSeconds` to model an idle-time consolidation curve, where time-since-last-access strengthens the score and an access resets it. |

## Inverted Decay (Consolidation)

A **negative `halfLifeSeconds`** flips the chosen function family in place: the compiled score becomes `1 - f(age, |halfLife|)` instead of `f(age, halfLife)`. The dispatch is purely a curve property — it composes with every function (`exponential`, `linear`, `step`) and with every `scoreFrom` anchor.

| `halfLifeSeconds` sign | Curve shape | Score at age 0 | Asymptote at large age |
|------------------------|-------------|----------------|-----------------------|
| Positive | Forgetting (Ebbinghaus) — strong now, fades over time | `1.0` | `0.0` |
| Negative | Inverted consolidation — weak now, strengthens over time | `0.0` | `1.0` |

### Use Case: Idle-Time Consolidation

Combine a negative half-life with `scoreFrom: 'LAST_ACCESSED'` to invert the cognitive model: the entity gains visibility while idle and resets on every access. The two levers do the same independent jobs as in the forward case (`scoreFloor` clamps the score, `visibilityThreshold` checks for suppression) but their interaction is more visible because the inverted curve evaluates to **exactly 0.0 right after access**. Without a positive `scoreFloor`, the post-access score sits at `0.0 < 0.10`, the entity is suppressed and deindexed every time it's read, and the consolidation curve never gets a chance to run.

```cypher
CREATE DECAY PROFILE consolidation_curve OPTIONS {
  halfLifeSeconds: -86400,        -- one-day inversion (negative)
  function: 'exponential',
  scoreFrom: 'LAST_ACCESSED',
  visibilityThreshold: 0.10,
  scoreFloor: 0.10                -- floor == threshold → barely visible at access
}
```

A node bound to `consolidation_curve`:

| Time since last access | Curve output | After `max(floor, curve)` | Visible? |
|---|---|---|---|
| `0` (just accessed) | `0.000` | `0.100` (floor) | **Yes** (at threshold; strict `<` passes) |
| `1 half-life` (24h) | `0.500` | `0.500` | Yes (curve overrides floor) |
| `7 half-lives` (1w) | `0.992` | `0.992` | Yes |
| Then accessed again | `0.000` | `0.100` (floor) | **Yes** (resets to the floor, not zero) |

Notice the floor only acts during the brief post-access window when the raw curve is below `0.10`. Once the consolidation curve climbs past the floor it takes over and the floor is invisible — so the consolidation gradient between idle entries (0.5 vs 0.99) is preserved exactly as it would be without the floor.

Compare with `scoreFloor: 0.0` (the default):

| Time since last access | Curve output | After `max(0, curve)` | Visible? |
|---|---|---|---|
| `0` (just accessed) | `0.000` | `0.000` | **No** — `0.0 < 0.10` |
| `~3.32 hours` (when curve hits 0.10) | `0.100` | `0.100` | Yes |
| `1 half-life` (24h) | `0.500` | `0.500` | Yes |

That `scoreFloor: 0.0` configuration is sometimes what you want — it produces a "cooldown" memory that disappears for the first ~3.3 hours after each access and only re-appears after enough idle time has passed for the curve to lift it back over `visibilityThreshold`. But it is **not** the default consolidation behavior most operators expect; choose it deliberately, not by accident.

### Use Case: Negative Promotion Combined With Inverted Decay

A `multiplier < 1.0` on a promotion profile **dampens** rather than boosts. Pair it with the inverted curve above to build a "punish frequent access" model in which interference (each retrieval) erodes the consolidated score:

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

The result is the inverse of the classic graph-DB heuristic: **frequently-accessed nodes and edges decay faster** (each access resets the consolidation clock and the dampener pins the multiplier), while **idle entries gain strength over time**. The model encodes the idea that consolidation requires time without retrieval, and is the dual of the recency-bias most caches use.

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
  DECAY HALF LIFE 1209600                  -- 14-day forgetting curve on the edge
  DECAY VISIBILITY THRESHOLD 0.10          -- edge hides when finalScore < 0.10
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15           -- score clamp on the property only
  r.externalId NO DECAY
}
```

`DECAY FLOOR 0.15` on `r.signalScore` is a **score clamp** — the property's reported score never drops below `0.15`. Because the floor is above the edge's `0.10` threshold, the property also stays visible forever (when read on a non-suppressed edge). If the edge itself drops below `0.10` it suppresses regardless of property floors; the floor only protects the property's *value*, not the parent edge's visibility.

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
  DECAY VISIBILITY THRESHOLD 0.10           -- edge hides when finalScore < 0.10
  r.confidence DECAY HALF LIFE 86400        -- 1-day fade on the property
  r.confidence DECAY FLOOR 0.25             -- but its score never drops below 0.25
}
```

`r.confidence` decays fast (1-day half-life) but its score is clamped at `0.25` — well above the edge's `0.10` threshold, so the property stays visible on every non-suppressed edge. The threshold and the floor are doing different jobs: the threshold gates the *edge*'s visibility, the floor pins the *property*'s minimum score.

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
