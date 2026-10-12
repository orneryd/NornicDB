---
name: nornicdb-knowledge-policies
description: Author and use NornicDB knowledge-layer policies (decay profiles, parameter bundles, promotion profiles, promotion policies) from Cypher. Use when writing CREATE/ALTER/DROP/SHOW DECAY PROFILE / PROMOTION PROFILE / PROMOTION POLICY, calling kp.* procedures, or reading entity scores via decayScore() / decay() / policy() / reveal() in NornicDB.
---

# NornicDB Knowledge-Layer Policies (Cypher API)

NornicDB scores every node and edge through a Cypher-controlled pipeline. The mental model is Neo4j Cypher: `CREATE`/`ALTER`/`DROP`/`SHOW` statements plus `CALL` procedures.

## The four object kinds (and exactly what each one does)

There is **one targeting mechanism** (`FOR (...)`) used in two places. Object kinds:

| Kind                                                                                                                       | Carries a target? | What it does                                                                                                                                                                                       | Effect on scores by itself                                           |
| -------------------------------------------------------------------------------------------------------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| **Decay bundle** — `CREATE DECAY PROFILE <name> OPTIONS { ... }`                                                           | No                | Names a reusable parameter set: `halfLifeSeconds`, `function`, `visibilityThreshold`, `scoreFloor`, `scope`, `scoreFrom`, `scoreFromProperty`                                                      | None. A bundle on its own is inert.                                  |
| **Decay binding** — `CREATE DECAY PROFILE <name> FOR (...) APPLY { ... }`                                                  | Yes (`FOR`)       | Attaches decay math to a label / multi-label / edge type / wildcard. The `APPLY` block either references a bundle (`DECAY PROFILE 'name'`) or sets parameters inline, plus per-property overrides. | Decay is active for entities matching `FOR`.                         |
| **Promotion profile** — `CREATE PROMOTION PROFILE <name> OPTIONS { ... }`                                                  | No                | Names a reusable boost: `multiplier`, `scoreFloor`, `scoreCap`, `scope`.                                                                                                                           | None on its own.                                                     |
| **Promotion policy** — `CREATE PROMOTION POLICY <name> FOR (...) APPLY { ON ACCESS { ... } WHEN ... APPLY PROFILE '...' }` | Yes (`FOR`)       | Attaches access-counter mutations and conditional boosts to a target. ON ACCESS mutations write to access metadata; WHEN clauses select which promotion profile applies.                           | Mutates access metadata; promotion math applies when a WHEN matches. |

So:

- **Bundles and profiles are inert parameter packages.** They never select entities, never run code on their own, and never change scores until a binding or policy references them.
- **Bindings and policies are the only objects that reference entities.** Both use the same `FOR (...)` target syntax.
- Yes, the keyword `CREATE DECAY PROFILE` has two shapes — the parser chooses bundle vs binding based on whether `OPTIONS` or `FOR` follows the name. Same keyword, two distinct objects.

## How decay runs (no event needed)

Decay is **pure time math, evaluated on every read**. Nothing has to "tick" or fire. When a query touches an entity:

1. The resolver looks up the most specific decay binding that matches the entity's labels (or edge type).
2. If no binding matches → score is `1.0`, never suppressed.
3. If a binding matches → the scorer reads:
   - the binding's compiled parameters (half-life, function, threshold, floor, scoreFrom),
   - the entity's anchor timestamp (selected by `scoreFrom`),
   - the entity's access metadata (only used when `scoreFrom: 'LAST_ACCESSED'`),
     and computes the score from those values right then.

A decay binding does **not** need a promotion policy to function. `CREATE DECAY PROFILE doc_binding FOR (n:Document) APPLY { DECAY PROFILE 'doc_retention' }` with no promotion policy in the catalog produces a fully-working forgetting curve on `:Document`. ON ACCESS is unrelated to making decay run.

### Anchor timestamps (`scoreFrom`)

| Mode                | Anchor used as `t = 0` | Where it comes from                                                                          |
| ------------------- | ---------------------- | -------------------------------------------------------------------------------------------- |
| `CREATED` (default) | entity creation time   | `node.CreatedAt` / `edge.CreatedAt`                                                          |
| `VERSION`           | last property update   | `node.UpdatedAt`                                                                             |
| `CUSTOM`            | a property             | the property named in `scoreFromProperty` (must hold a timestamp)                            |
| `LAST_ACCESSED`     | last access            | access metadata's `lastAccessedAt`; falls back to `CreatedAt` until first access is recorded |

`LAST_ACCESSED` decay only **reads** access metadata. To make `LAST_ACCESSED` actually reset on each access, you also need a promotion policy on the same target whose `ON ACCESS` writes `lastAccessedAt` (see "Combining decay with access tracking" below).

## Score formula

For an entity that matches a decay binding:

```
t            = now - anchor                                  // anchor selected by scoreFrom
baseScore    = f(t, halfLifeSeconds)                         // by binding's `function`
multiplier   = (matching promotion WHEN clause) ? profile.multiplier : 1.0
promoted     = baseScore * multiplier
clampedPromo = min(promoCap,    max(promoFloor, promoted))   // promotion profile's floor/cap (only when promoted)
finalScore   = max(decayFloor,  clampedPromo)                // decay binding's floor
suppressed   = finalScore < visibilityThreshold              // strict less-than
```

`baseDecay(t) = f(t, halfLifeSeconds)`:

- `exponential` — `e^(-ln(2)/halfLife * t)`
- `linear` — `max(0, 1 - t/(2 * halfLife))` (0.5 at one half-life, 0.0 at two)
- `step` — `1.0` if `t < halfLife`, else `0.0`
- `none` — always `1.0`

Two specific things that catch people:

- A **negative `halfLifeSeconds`** inverts the curve: the score becomes `1 - f(age, |halfLife|)`. Combined with `scoreFrom: 'LAST_ACCESSED'` and a promotion policy that updates `lastAccessedAt`, this produces an idle-time consolidation curve: 0 right after access, climbs toward 1 with idle time, resets on each access.
- `scoreFloor` and `visibilityThreshold` are **independent**. The floor clamps the score value upward; the threshold is a strict-less-than gate. A floor only keeps an entity visible when `scoreFloor >= visibilityThreshold`. Otherwise the floor pins the score above zero **while it stays suppressed**.

## Binding resolution (which decay binding wins)

When multiple decay bindings could match an entity:

1. Multi-label binding (most labels matched) wins over fewer-label.
2. Exact-label binding wins over wildcard `FOR ()`.
3. Two bindings against the same target with different `Order` values: the lower `Order` wins. Two bindings against the same target with the same `Order` cause the binding-table build to return a conflict error, so this state is rejected at DDL time rather than silently picked.
4. No binding matches → score is `1.0`, suppression never applies.

Promotion-policy resolution uses the same priority rules.

When decay is globally disabled, every entity scores `1.0` regardless of bindings. Verify with `CALL kp.info()`.

## DDL reference

### Decay bundle — `CREATE DECAY PROFILE <name> OPTIONS { ... }`

A bundle is a named parameter set with **no target**. It never affects any entity until a binding references it. Bundles cannot reference other bundles.

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,            -- negative inverts the curve
  function: 'exponential',            -- exponential | linear | step | none
  visibilityThreshold: 0.10,          -- suppression cutoff
  scoreFloor: 0.05,                   -- score clamp; independent of threshold
  scope: 'NODE',                      -- NODE | EDGE
  scoreFrom: 'CREATED',               -- CREATED | VERSION | CUSTOM | LAST_ACCESSED
  scoreFromProperty: 'reviewedAt',    -- required when scoreFrom: 'CUSTOM'
  decayEnabled: true,                 -- bundle-level on/off
  enabled: true
}
```

Effect of this statement: a row appears in `kp.profiles()` with `kind='bundle'`. No entity's score has changed.

### Decay binding — `CREATE DECAY PROFILE <name> FOR (...) APPLY { ... }`

A binding is the **only** object that selects entities for decay. The same `CREATE DECAY PROFILE` keyword is used; the parser distinguishes by whether `OPTIONS` or `FOR` follows the name. A binding has no parameter values of its own beyond the optional overrides — it must either reference a bundle or set parameters via `DECAY HALF LIFE` / `DECAY VISIBILITY THRESHOLD` / `DECAY FLOOR` directives in the `APPLY` block.

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)                              -- target: see "Target shapes" below
APPLY {
  DECAY PROFILE 'working_memory'                   -- reference a bundle (most common)
  DECAY HALF LIFE 86400                            -- override the bundle's half-life
  DECAY VISIBILITY THRESHOLD 0.10                  -- override the bundle's threshold
  DECAY FLOOR 0.05                                 -- override the bundle's floor
  -- NO DECAY                                      -- whole entity stays at score 1.0
  n.tenantId NO DECAY                              -- per-property: never decays
  n.summary DECAY PROFILE 'session_summary'        -- per-property: different bundle
  n.lastConversationSummary DECAY HALF LIFE 2592000-- per-property: inline override
  n.confidence DECAY FLOOR 0.25                    -- per-property: clamp score
}
```

Effect of this statement: decay is now active for every `:SessionRecord` node. Reads of that label go through the scorer using the resolved parameters. A row appears in `kp.profiles()` with `kind='binding'`.

### Target shapes (`FOR` clause)

The same target syntax is used by decay bindings and promotion policies:

```cypher
FOR (n:SessionRecord)                  -- single label
FOR (n:KnowledgeFact:MemoryEpisode)    -- multi-label: entity must carry every listed label
FOR ()-[r:CO_ACCESSED]-()              -- edge type
FOR ()                                 -- wildcard: matches anything no more specific binding caught
```

Property directives inside `APPLY` must use the bind variable from the pattern: `n.foo` for nodes, `r.foo` for edges.

### Promotion profile — `CREATE PROMOTION PROFILE <name> OPTIONS { ... }`

A promotion profile is a named boost with **no target**. It is inert until a promotion policy's `WHEN` clause references it. Effect of this statement alone: nothing scores differently yet.

```cypher
CREATE PROMOTION PROFILE reinforced_episode OPTIONS {
  multiplier: 1.25,                   -- < 1.0 dampens, > 1.0 boosts; 1.0 = neutral
  scoreFloor: 0.25,                   -- promoted-score floor (clamped before scoreCap)
  scoreCap:   0.95,                   -- promoted-score cap (clamped after multiplier)
  scope:      'NODE'                  -- NODE | EDGE
}

ALTER PROMOTION PROFILE reinforced_episode SET OPTIONS { multiplier: 1.5 }
DROP PROMOTION PROFILE IF EXISTS reinforced_episode
SHOW PROMOTION PROFILES
```

### Promotion policy — `CREATE PROMOTION POLICY <name> FOR (...) APPLY { ... }`

A promotion policy targets entities with `FOR` and does two distinct things in its `APPLY` block:

1. **`ON ACCESS { ... }`** — runs on every read/traversal of a matched entity. Each `SET` mutates the entity's **access metadata** (a separate per-entity store keyed by entity ID). Mutations are buffered and flushed in batches. They never touch `n.Properties`.
2. **`WHEN <predicate> APPLY PROFILE '<promotion_profile_name>'`** — evaluated when the scorer runs. The first matching `WHEN` chooses which promotion profile's `multiplier` / `scoreFloor` / `scoreCap` apply. If no `WHEN` matches, no promotion is applied.

```cypher
CREATE PROMOTION POLICY episode_reinforcement
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount    = coalesce(n.accessCount, 0) + 1   -- writes to access metadata
    SET n.lastAccessedAt = timestamp()                      -- writes to access metadata

    -- Optional: smooth a noisy behavioral signal with a Kalman filter.
    WITH KALMAN { q: 0.1, r: 88.0, varianceScale: 10.0, windowSize: 32 }
      SET n.behavioralScore = $observation
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_episode'

  WHEN n.lastAccessedAt > timestamp() - 3600000
    APPLY PROFILE 'hot_episode'
}

ALTER PROMOTION POLICY episode_reinforcement DISABLE   -- or ENABLE
ALTER PROMOTION POLICY episode_reinforcement
FOR (n:ReviewedEpisode)
APPLY {
  ON ACCESS { SET n.lastAccessedAt = timestamp() }
  WHEN n.accessCount >= 5 APPLY PROFILE 'reinforced_episode'
}
DROP   PROMOTION POLICY IF EXISTS episode_reinforcement
SHOW PROMOTION POLICIES
```

`ALTER PROMOTION POLICY … FOR … APPLY` atomically replaces the target, `WHEN` predicates, and `ON ACCESS` block while preserving the policy's enabled state. Use `ALTER PROMOTION POLICY … ENABLE|DISABLE` for enablement, and `ALTER PROMOTION PROFILE … SET OPTIONS { multiplier, scoreFloor, scoreCap, enabled }` to change the referenced math.

ON ACCESS rules:

- Each `SET` is a mutation against access metadata. Inspect the result with `policy(n)` or `kp.resolve(...)` — `n.Properties` is unchanged.
- Mutations are eventually-consistent: a read immediately after access may see the previous values until the access flusher commits.
- `WITH KALMAN` defaults when keys are omitted: `q=0.1`, `r=88.0`, `varianceScale=10.0`, `windowSize=32`. Setting `r` explicitly switches the filter from auto-R mode to manual mode.
- `WHEN` predicates are evaluated against the freshly-flushed metadata; declaration order decides priority — first match wins.

### Combining decay with access tracking

Decay and promotion are independent. Two common combinations:

- **Forgetting curve, no access tracking.** A decay binding alone is enough. Use `scoreFrom: 'CREATED'` (default) so the anchor never moves.
- **Consolidation curve that resets on access.** You need both:

  ```cypher
  -- 1. Decay binding with LAST_ACCESSED scoreFrom (reads access metadata)
  CREATE DECAY PROFILE consolidation OPTIONS {
    halfLifeSeconds: -86400, function: 'exponential',
    scoreFrom: 'LAST_ACCESSED',
    visibilityThreshold: 0.10, scoreFloor: 0.10
  }
  CREATE DECAY PROFILE memory_decay
  FOR (n:Memory) APPLY { DECAY PROFILE 'consolidation' }

  -- 2. Promotion policy whose ON ACCESS writes lastAccessedAt
  CREATE PROMOTION POLICY memory_access_tracking
  FOR (n:Memory) APPLY {
    ON ACCESS { SET n.lastAccessedAt = timestamp() }
  }
  ```

  Without the promotion policy's ON ACCESS, `lastAccessedAt` would never update and the decay binding would behave as if `scoreFrom: 'CREATED'`.

### Alter / drop / list (decay)

```cypher
ALTER DECAY PROFILE working_memory SET OPTIONS { halfLifeSeconds: 1209600 }
ALTER DECAY PROFILE session_record_retention
FOR (n:SessionRecord:Reviewed)
APPLY {
  DECAY PROFILE 'working_memory'
  n.tenantId NO DECAY
}
DROP  DECAY PROFILE IF EXISTS session_record_retention
SHOW  DECAY PROFILES
```

`ALTER DECAY PROFILE … SET OPTIONS` updates a bundle. `ALTER DECAY PROFILE … FOR … APPLY` atomically replaces a binding's target and complete apply block while preserving its resolution order.

## Diagnostics & inspection

```cypher
CALL kp.info()                       -- catalog counts + enabled flag
CALL kp.profiles()                   -- bundles and bindings, one row each
CALL kp.policies()                   -- promotion profiles and policies
CALL kp.deindexStatus()              -- pending suppression cleanup work

-- Resolve effective policy for an entity, label set, or edge type (any one is enough):
CALL kp.resolve('nornic:abc-123', '', '')         -- by ID
CALL kp.resolve('', 'MemoryEpisode,Session', '')  -- by labels (CSV)
CALL kp.resolve('', '', 'CO_ACCESSED')            -- by edge type
```

`resolve(...)` returns columns:
`TargetID, TargetScope, ResolvedDecayProfileID, ResolvedScoreFrom, ResolutionSourceChain, AppliedDecayProfileNames, AppliedPromotionPolicyName, AppliedPromotionProfileName, EffectiveRate, EffectiveThreshold, EffectiveMultiplier, BaseScore, FinalScore, NoDecay, SuppressionEligible, Explanation`.

## Reading scores from queries

```cypher
-- Live score (float in [0, 1] after clamping)
MATCH (n:SessionRecord) RETURN n.id, decayScore(n) ORDER BY decayScore(n) DESC LIMIT 10

-- Per-property score (uses the property override if defined)
MATCH (n:Document) RETURN decayScore(n, {property: 'summary'})

-- Full resolution map (for debugging)
MATCH (n:Document {id: $id}) RETURN decay(n)

-- Access metadata (counters, last access, etc.) for an entity
MATCH (n:MemoryEpisode {id: $id}) RETURN policy(n)

-- See suppressed entities by bypassing the visibility gate (read-only)
MATCH (n:Document) RETURN reveal(n) AS node, decayScore(n) AS score
```

`reveal()` is a query-scope flag that bypasses suppression for the duration of the query. It does not change scores; suppressed entities simply become readable.

## Authoring workflow

1. **Bundle first.** Create the parameter set (`OPTIONS { ... }`). Nothing scores differently yet — confirm with `SHOW DECAY PROFILES` (the bundle appears with `kind='bundle'`).
2. **Binding next.** `CREATE DECAY PROFILE <bindingName> FOR (...) APPLY { DECAY PROFILE '<bundleName>' ... }`. Decay is now active for matched entities. Confirm with `CALL kp.resolve('', '<labels>', '')` — `ResolvedDecayProfileID` should be the bundle name; `EffectiveRate`, `EffectiveThreshold`, and `EffectiveMultiplier` should match what you intended. (For the floor value, read `floor` from `decay(n)` on a real entity — the `resolve()` projection does not include the floor directly.)
3. **Promotion only when needed.** Create promotion profiles (`OPTIONS`) and a promotion policy (`FOR ... APPLY { ... }`). Even a policy with only `ON ACCESS` and no `WHEN` is useful — it lets a `LAST_ACCESSED` decay binding work correctly.
4. **Tune with `ALTER`.** `ALTER DECAY PROFILE <bundleName> SET OPTIONS { ... }` propagates math changes to every referencing binding. Use `ALTER DECAY PROFILE <bindingName> FOR (...) APPLY { ... }` when the target or binding overrides change.
5. **Validate again** after every change: `SHOW DECAY PROFILES`, `SHOW PROMOTION POLICIES`, `CALL kp.resolve(...)`, then `MATCH (n:Label) RETURN decay(n)` on a known entity.

## Tuning knobs (what to reach for first)

| Symptom                                      | First lever                                                                         |
| -------------------------------------------- | ----------------------------------------------------------------------------------- |
| Entities disappear too fast                  | Lengthen `halfLifeSeconds` on the bundle                                            |
| Entities never decay enough                  | Shorten `halfLifeSeconds`, or switch `function` from `none`/`step` to `exponential` |
| Old entries linger above zero                | Lower `scoreFloor` or remove it from the bundle/binding                             |
| Hot path scores too high                     | Lower the promotion `multiplier`, or add a `scoreCap` to the promotion profile      |
| Noisy behavioral signal causing oscillation  | Wrap the offending `SET` with `WITH KALMAN { ... }`                                 |
| Score is high enough but entity still hidden | Raise `scoreFloor` to `>= visibilityThreshold`, or lower `visibilityThreshold`      |
| `LAST_ACCESSED` decay never advances         | Missing promotion policy with `SET n.lastAccessedAt = timestamp()` in `ON ACCESS`   |

## Common gotchas

- **`CREATE DECAY PROFILE` has two shapes.** With `OPTIONS { ... }` it creates a **bundle** (parameters, no target, inert until referenced). With `FOR (...) APPLY { ... }` it creates a **binding** (target + parameter source). Same keyword, two distinct objects in the catalog.
- **Bundles never act on their own.** A bundle with no binding referencing it is documented in the catalog but changes no entity's score.
- **Bindings need parameters.** A binding with neither a `DECAY PROFILE '<bundle>'` reference nor inline directives in `APPLY` will use defaults (`function='exponential'` is not implied; the resolver may not produce a usable curve). Reference a bundle, or set parameters explicitly.
- **Property rules (`n.foo NO DECAY`) live in the binding's `APPLY` block** — they are not legal inside a bundle's `OPTIONS`.
- **`ON ACCESS` belongs to promotion policies only.** It is not a feature of decay bindings. Decay does not need or use `ON ACCESS` to function.
- **`ON ACCESS SET` writes to access metadata, not `n.Properties`.** Inspect with `policy(n)` or `kp.resolve(...)`. The node's stored properties are unchanged.
- **`multiplier < 1.0` is a valid promotion** — it dampens. Pair with inverted decay (`halfLifeSeconds: -...`) for a punish-frequent-access pattern.
- **A wildcard binding (`FOR ()`)** catches every entity that no more-specific binding matches. Useful for global defaults; easy to forget. Inspect with `SHOW DECAY PROFILES` and look for a binding with `target='*'`.
- **Negative half-lives bypass the threshold-age fast path.** Reads cost slightly more CPU. Reserve for label sets where consolidation is the actual model.
- **Binding and policy definition alters are complete replacements.** Include the full current `FOR` target and `APPLY` body; omitted directives are removed. Promotion policy enablement and decay binding order are preserved.
- **Drop order matters.** Dropping a bundle while a binding still references it returns a validation error. Drop the binding first, then the bundle.

## Worked example: minimum bootstrap

```cypher
-- 1. Bundle (no target — inert until referenced)
CREATE DECAY PROFILE memory_decay OPTIONS {
  halfLifeSeconds: 604800, function: 'exponential',
  visibilityThreshold: 0.10, scoreFloor: 0.05, scoreFrom: 'CREATED'
}

-- 2. Binding (target + parameter source — decay is now active for :Memory)
CREATE DECAY PROFILE memory_binding
FOR (n:Memory)
APPLY {
  DECAY PROFILE 'memory_decay'
  n.tenantId NO DECAY
}

-- 3. (Optional) Promotion math (no target — inert)
CREATE PROMOTION PROFILE reinforced OPTIONS {
  multiplier: 1.5, scoreFloor: 0.20, scoreCap: 0.95
}

-- 4. (Optional) Promotion policy (target + ON ACCESS + WHEN)
CREATE PROMOTION POLICY memory_reinforcement
FOR (n:Memory)
APPLY {
  ON ACCESS { SET n.accessCount = coalesce(n.accessCount, 0) + 1 }
  WHEN n.accessCount >= 3 APPLY PROFILE 'reinforced'
}

-- Verify
SHOW DECAY PROFILES                                            -- bundle + binding rows
SHOW PROMOTION PROFILES                                        -- promotion profile row
SHOW PROMOTION POLICIES                                        -- promotion policy row
CALL kp.resolve('', 'Memory', '')        -- effective config
MATCH (n:Memory) RETURN n.id, decay(n) LIMIT 5                 -- live scores
```

After step 2 alone (no promotion), `:Memory` nodes already have a working forgetting curve. Steps 3–4 add reinforcement on top of it.

For a complete production-shape configuration (Knowledge / Memory / Wisdom / Evidence layers), see `docs/user-guides/ebbinghaus-roynard-bootstrap.md`.
