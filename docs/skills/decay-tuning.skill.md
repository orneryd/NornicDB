---
name: nornicdb-decay-tuning
description: Tune NornicDB decay profiles via Cypher — pick half-life, function, threshold, floor, scoreFrom; diagnose suppression vs score-clamp confusion; build forgetting and consolidation curves. Use when adjusting halfLifeSeconds, visibilityThreshold, scoreFloor, function, scoreFrom, or building inverted-decay (idle-time consolidation) setups in NornicDB.
---

# Tuning NornicDB Decay Profiles

This skill is the playbook for picking and adjusting decay parameters. It assumes the vocabulary from the `nornicdb-knowledge-policies` skill: a **bundle** is a parameter set (`CREATE DECAY PROFILE <name> OPTIONS { ... }`, no target, inert), a **binding** attaches decay to entities (`CREATE DECAY PROFILE <name> FOR (...) APPLY { ... }`).

All tuning is done by either editing a bundle (`ALTER DECAY PROFILE <bundle> SET OPTIONS { ... }`) or by setting overrides inside a binding's `APPLY` block. Decay runs on every read regardless of whether any promotion policy exists — `ON ACCESS` is a promotion concern, not a decay one.

## The four levers

| Lever | What it controls | Independent of |
|---|---|---|
| `halfLifeSeconds` | Time to fall to 0.5 (negative inverts the curve) | All others |
| `function` | Curve shape: `exponential` / `linear` / `step` / `none` | Half-life value |
| `visibilityThreshold` | Boolean cutoff. `finalScore < threshold` ⇒ entity hidden | Score itself |
| `scoreFloor` | `max()` clamp on the reported score | Visibility |

`scoreFloor` and `visibilityThreshold` are the two parameters most people misuse. **A floor only makes something visible if `scoreFloor >= visibilityThreshold`.** Otherwise the floor just keeps the score off zero while it stays suppressed.

## Picking `halfLifeSeconds`

Start from "how long should this still be visible by default?" and divide by ~3.32 to get the threshold-crossing time at the default `0.10` threshold:

| Half-life | Crosses 0.10 (exp) at | Use for |
|---|---|---|
| 3600 s (1h) | ~3.3h | ephemeral working state, scratch |
| 86400 s (1d) | ~3.3d | sessions, short-lived signals |
| 604800 s (1w) | ~23d | typical document/episode memory |
| 2592000 s (30d) | ~3.3 months | reference material that ages slowly |
| `none` / `NO DECAY` | never | identifiers, canonical links |

For `linear` the same threshold is reached at `(1 - threshold) * 2 * halfLife`. For `step` everything is full-score until the half-life, then zero — useful only when "expires sharply" is the model.

## Picking `function`

- `exponential` — default. Smooth, well-understood.
- `linear` — when you need a predictable slope (`0.5` at 1× half-life, `0.0` at 2×). No long tail.
- `step` — for time-boxed validity (a token, a session window). Avoid if you want gradual ranking.
- `none` — for permanent records. Equivalent to `NO DECAY` but expressible as a bundle.

## `scoreFrom` modes

| Mode | t = 0 anchor |
|---|---|
| `CREATED` | `n.CreatedAt` (default) |
| `VERSION` | `n.UpdatedAt` (resets on every property update) |
| `CUSTOM` | `n.<scoreFromProperty>` (e.g. `reviewedAt`) |
| `LAST_ACCESSED` | last access metadata; falls back to created until first access |

`LAST_ACCESSED` paired with a **negative** `halfLifeSeconds` is the inverted (idle-time consolidation) curve: starts at 0 right after access, climbs toward 1 as the entity sits idle. The reset on access only works if **a promotion policy on the same target writes `lastAccessedAt`** (or `n.lastAccessedAt`) inside its `ON ACCESS` block. Without that policy, `lastAccessedAt` never updates and the binding behaves as if `scoreFrom: 'CREATED'`.

```cypher
-- Decay binding (reads access metadata)
CREATE DECAY PROFILE consolidation OPTIONS {
  halfLifeSeconds: -86400, function: 'exponential',
  scoreFrom: 'LAST_ACCESSED',
  visibilityThreshold: 0.10, scoreFloor: 0.10
}
CREATE DECAY PROFILE memory_decay FOR (n:Memory) APPLY { DECAY PROFILE 'consolidation' }

-- Promotion policy (writes access metadata so the decay anchor moves)
CREATE PROMOTION POLICY memory_access_tracking
FOR (n:Memory) APPLY { ON ACCESS { SET n.lastAccessedAt = timestamp() } }
```

## Forgetting curve cookbook (forward decay)

```cypher
CREATE DECAY PROFILE doc_retention OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.05
}
```

Lifecycle (assume threshold 0.10, floor 0.05):

| Age | curve | clamped | visible |
|---|---|---|---|
| 0 | 1.000 | 1.000 | yes |
| 1×HL (7d) | 0.500 | 0.500 | yes |
| 3.32×HL (~23d) | 0.100 | 0.100 | yes (strict `<`) |
| 4×HL (28d) | 0.063 | 0.063 | no |
| 4.32×HL (~30d) | 0.050 | 0.050 | no — floor takes over |
| 10×HL (70d) | 0.001 | 0.050 | no — pinned |

To make it stay visible forever after fading, set `scoreFloor: 0.10` (= threshold).

## Consolidation cookbook (inverted decay)

```cypher
CREATE DECAY PROFILE consolidation OPTIONS {
  halfLifeSeconds: -86400,           -- negative → invert
  function: 'exponential',
  scoreFrom: 'LAST_ACCESSED',
  visibilityThreshold: 0.10,
  scoreFloor: 0.10                   -- keep visible right after access
}
```

| Time since access | curve | clamped | visible |
|---|---|---|---|
| 0 (just accessed) | 0.000 | 0.100 (floor) | yes (strict `<` passes) |
| 1×HL (24h) | 0.500 | 0.500 | yes |
| 7×HL (1w) | 0.992 | 0.992 | yes |
| accessed again | 0.000 | 0.100 (floor) | yes — resets |

Without `scoreFloor: 0.10`, the entity disappears for ~3.3h after every access (cool-down memory). Pick deliberately, not by accident.

## Property-level rules

Inside a binding's `APPLY` block you can override decay per-property:

```cypher
CREATE DECAY PROFILE doc_binding
FOR (n:Document)
APPLY {
  DECAY PROFILE 'doc_retention'
  n.tenantId NO DECAY                     -- never decays
  n.summary DECAY PROFILE 'fast_summary'  -- different bundle
  n.confidence DECAY HALF LIFE 86400      -- inline override
  n.confidence DECAY FLOOR 0.25           -- score clamp on the property only
}
```

Property floor `> entity threshold` keeps the property's value visible *on a non-suppressed parent*. If the parent entity is below threshold, suppression wins regardless of property floor.

## When to use each

- "I want this to fade and disappear" → `function: 'exponential'`, threshold > 0, no floor.
- "I want this to fade but never disappear" → `scoreFloor >= visibilityThreshold`.
- "I want this to fade to a hidden non-zero state" → `0 < scoreFloor < visibilityThreshold`.
- "I want this to strengthen with time and reset on access" → negative `halfLifeSeconds` + `scoreFrom: 'LAST_ACCESSED'` **plus** a promotion policy on the same target that writes `lastAccessedAt` in its `ON ACCESS`.
- "I want a hard expiry boundary" → `function: 'step'`.
- "Some properties should be permanent" → property-level `NO DECAY`.

## Testing & validation loop

```cypher
-- 1. After CREATE, confirm the bundle/binding exists with the values you typed.
SHOW DECAY PROFILES

-- 2. Confirm the resolver picks your binding for the labels you care about.
CALL kp.resolve('', 'Document', '')

-- 3. Score real entities and inspect the resolution chain.
MATCH (n:Document {id: $id}) RETURN decay(n)

-- 4. Watch what suppression does in practice.
MATCH (n:Document) WHERE decayScore(n) < 0.10 RETURN count(n) AS hidden

-- 5. Adjust with ALTER, then redo step 3 — no re-create needed.
ALTER DECAY PROFILE doc_retention SET OPTIONS { halfLifeSeconds: 1209600 }
```

`decay(n)` returns `{score, policy, scope, function, visibilityThreshold, floor, applies, reason, scoreFrom}` — read `applies: false` plus `reason` to find unbound entities.

## Tuning patterns to reach for first

| Observation | Adjust |
|---|---|
| Recall drops too fast at a few days | Half-life ×2 on the bundle |
| Old items dominate ranking | Add `scoreFloor: 0` and lower `visibilityThreshold` instead of forcing visibility |
| Items disappear immediately on every access | Inverted curve without `scoreFloor >= visibilityThreshold`, **or** `scoreFrom: 'LAST_ACCESSED'` without a promotion policy that updates `lastAccessedAt` |
| Score histogram bimodal at 0.0 / 1.0 | `function: 'step'` is doing what you asked — switch to `'exponential'` if you wanted a gradient |
| Anchor never moves on `LAST_ACCESSED` decay | No promotion policy is writing `lastAccessedAt` in `ON ACCESS` — add one |

## Reasoning under disabled decay

If `kp.info()` returns `enabled: false`, every `decayScore()` returns `1.0` and no suppression happens regardless of bindings. Don't `ALTER` parameters to compensate — enable decay (`NORNICDB_MEMORY_DECAY_ENABLED=true` or `memory.decay_enabled: true` in YAML) instead.
