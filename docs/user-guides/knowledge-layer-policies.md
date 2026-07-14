# Knowledge-Layer Policies

NornicDB's knowledge-layer scoring system manages the lifecycle and visibility of graph entities through declarative parameter packages and targeted bindings. This guide covers the system architecture and how the pieces fit together.

## Architecture

There are **four distinct object kinds** in the catalog. Two are inert parameter packages (no target, no effect on their own); two carry a `FOR (...)` target that selects which entities they affect.

| Kind | DDL form | Has target? | What it does on its own |
|---|---|---|---|
| **Decay bundle** | `CREATE DECAY PROFILE <name> OPTIONS { ... }` | No | Nothing — names a parameter set |
| **Decay binding** | `CREATE DECAY PROFILE <name> FOR (...) APPLY { ... }` | Yes | Activates decay scoring for matched entities, using a referenced bundle and/or inline overrides |
| **Promotion profile** | `CREATE PROMOTION PROFILE <name> OPTIONS { ... }` | No | Nothing — names a multiplier/floor/cap set |
| **Promotion policy** | `CREATE PROMOTION POLICY <name> FOR (...) APPLY { ON ACCESS { ... } WHEN ... APPLY PROFILE '...' }` | Yes | Tracks access (via `ON ACCESS`) and/or applies promotion profiles when `WHEN` predicates match |

Two important consequences:

- **`CREATE DECAY PROFILE` is overloaded.** With `OPTIONS { ... }` it creates a bundle; with `FOR (...) APPLY { ... }` it creates a binding. Same keyword, two distinct objects in the catalog.
- **There is one targeting mechanism — the `FOR` clause.** It is used by decay bindings and promotion policies; it is not used by bundles or promotion profiles.

```
                    ┌─────────────┐
                    │   Entity    │
                    │ (Node/Edge) │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Resolver   │ ← Finds matching binding by label/edge type
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │                         │
       ┌──────▼──────┐          ┌──────▼──────┐
       │   Decay     │          │  Promotion  │
       │  Profile    │          │   Policy    │
       └──────┬──────┘          └──────┬──────┘
              │                         │
              └────────────┬────────────┘
                           │
                    ┌──────▼──────┐
                    │   Scorer    │ ← Computes final score
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ Visibility  │ ← Suppresses if below threshold
                    └─────────────┘
```

For a full, code-level flowchart of every stage between "entity enters the scorer" and "hand-off to the Visibility Layer" — including the resolver, decay curves, promotion, and the access-feedback loop — see [Knowledge-Policy Scoring Pipeline](../architecture/knowledge-policy-scoring-pipeline.md).

## Quick Start

### 1. Enable Scoring

```yaml
# nornicdb.yaml
memory:
  decay_enabled: true
```

### 2. Define a Parameter Bundle

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10
}
```

### 3. Create a Targeted Decay Profile

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.tenantId NO DECAY
}
```

### 4. Query with Scoring

```cypher
MATCH (n:SessionRecord)
WHERE decayScore(n) > 0.3
RETURN n ORDER BY decayScore(n) DESC
```

## How Scoring Works

Decay is **pure time math, evaluated on every read**. Nothing has to "tick" or fire — when a query touches an entity, the scorer computes the score on the spot from timestamps already on the entity. A decay binding is sufficient on its own; it does not need a promotion policy to function.

When an entity is queried, the scorer:

1. **Resolves** the most specific decay binding whose `FOR` clause matches the entity's labels or edge type. If no binding matches, the score is `1.0` and suppression never applies.
2. **Reads** the binding's compiled parameters (half-life, function, threshold, floor, scoreFrom) and the entity's anchor timestamp (selected by `scoreFrom`).
3. **Computes** the base decay score: `baseDecay(t)` where `t = now - anchor`.
4. **Looks for a promotion policy** targeting this entity. If one is configured and a `WHEN` predicate matches, the predicate's promotion profile contributes a `multiplier`, `scoreFloor`, and `scoreCap`. If no policy targets the entity (or no `WHEN` matches), `multiplier = 1.0` and the promotion floor/cap don't apply.
5. **Suppresses** if the final score is below the binding's `visibilityThreshold`.

### Score Formula

```
t            = now - anchor                                 // anchor selected by binding's scoreFrom
baseScore    = baseDecay(t)                                 // exponential / linear / step / none
multiplier   = (matched WHEN's promotion profile).multiplier if matched else 1.0
promoted     = baseScore * multiplier
clampedPromo = min(promoCap, max(promoFloor, promoted))     // promotion profile's floor/cap (only if matched)
finalScore   = max(decayBindingFloor, clampedPromo)         // decay binding's floor
suppressed   = finalScore < visibilityThreshold             // strict less-than
```

`scoreFloor` (on the decay binding) clamps the score value upward; `visibilityThreshold` is the boolean cutoff for suppression. They are independent — setting `scoreFloor` alone does not keep an entity visible unless the floor itself is at or above `visibilityThreshold`. See [`scoreFloor` vs `visibilityThreshold`](decay-profiles.md#scorefloor-vs-visibilitythreshold-they-are-independent) for lifecycle examples.

`baseDecay(t)`:
- **Exponential:** `e^(-ln(2)/halfLife * t)` where halfLife is in seconds
- **Linear:** `max(0, 1 - t/(2*halfLife))` (reaches 0.5 at one half-life, 0.0 at two half-lives)
- **Step:** `1.0` if `t < halfLife`, else `0.0`
- **None:** Always `1.0`

A **negative `halfLifeSeconds`** inverts the curve: the score becomes `1 - f(age, |halfLife|)`. Pair with `scoreFrom: 'LAST_ACCESSED'` for a consolidation curve. The reset on access only works if a promotion policy on the same target writes `lastAccessedAt` in its `ON ACCESS` block — see [Combining decay with access tracking](#combining-decay-with-access-tracking) below.

### scoreFrom Modes

| Mode | Anchor used as `t = 0` | Where it comes from |
|---|---|---|
| `CREATED` (default) | entity creation time | `node.CreatedAt` / `edge.CreatedAt` |
| `VERSION` | last property update | `node.UpdatedAt` |
| `CUSTOM` | a property (`scoreFromProperty`) | the property must hold a timestamp |
| `LAST_ACCESSED` | last access | access metadata's `lastAccessedAt`; falls back to `CreatedAt` until first access is recorded |

`LAST_ACCESSED` decay only **reads** access metadata. It does not write. To make it actually advance on each access, a promotion policy on the same target must update the metadata.

## Binding Resolution Order

When multiple decay bindings could match:

1. Multi-label target (most labels) takes precedence over single-label
2. Exact label match takes precedence over wildcard `FOR ()`
3. If two bindings have equal specificity, the resolver records a diagnostic; the first-registered binding is used
4. No binding → entity scores 1.0, suppression never applies

Promotion-policy resolution uses the same rules.

When decay is globally disabled (`memory.decay_enabled: false`), every entity scores `1.0` regardless of bindings. Confirm with `CALL nornicdb.knowledgepolicy.info()`.

## Combining decay with access tracking

Decay and promotion are independent objects with independent purposes:

| Want | Build |
|---|---|
| Forgetting curve, no access tracking needed | A decay bundle + a decay binding. No promotion policy required. |
| Consolidation curve that resets on each access | A decay bundle with `scoreFrom: 'LAST_ACCESSED'` + a decay binding **plus** a promotion policy on the same target whose `ON ACCESS { SET n.lastAccessedAt = timestamp() }` keeps the anchor moving. |
| Reinforcement after N accesses | A decay binding (any anchor) + a promotion profile with `multiplier > 1` + a promotion policy whose `ON ACCESS` increments a counter and whose `WHEN` references the matching profile. |
| Dampening for hot paths | Same as reinforcement but with `multiplier < 1`. |

Without a promotion policy, decay still runs — `ON ACCESS` is a feature of promotion only.

## Cypher Diagnostics

The knowledge-layer system is operated and inspected through Cypher.

### Catalog and Status

```cypher
CALL nornicdb.knowledgepolicy.info();
CALL nornicdb.knowledgepolicy.profiles();
CALL nornicdb.knowledgepolicy.policies();

SHOW DECAY PROFILES;
SHOW PROMOTION PROFILES;
SHOW PROMOTION POLICIES;
```

### Resolve an Effective Policy

```cypher
-- Resolve by entity ID
CALL nornicdb.knowledgepolicy.resolve('nornic:episode-1', '', '');

-- Resolve by label set
CALL nornicdb.knowledgepolicy.resolve('', 'MemoryEpisode,SessionRecord', '');

-- Resolve by edge type
CALL nornicdb.knowledgepolicy.resolve('', '', 'CO_ACCESSED');
```

### Deindex Queue Status

```cypher
CALL nornicdb.knowledgepolicy.deindexStatus();
```

These procedures are the supported diagnostics surface. The older `/admin/knowledge-policies/*` HTTP endpoints have been removed.

## Browser UI

The Knowledge Policies admin page is available at **Security > Knowledge Policies** in the browser UI. It provides:
- Overview of all profiles, bindings, and policies
- Interactive resolve tool for debugging scoring
- Deindex status monitoring

The UI uses the same Cypher DDL and knowledge-policy procedures shown above rather than a separate admin API.

Node detail views in the graph browser show decay metadata (score, suppression status, access count) when present.

## Authoring Workflow

The most reliable way to author knowledge policy is to treat it as a four-step workflow:

1. Create a reusable decay bundle with `OPTIONS { ... }`.
2. Bind that bundle to concrete node labels or edge types with `FOR ... APPLY { ... }`.
3. Add promotion profiles for the boost behavior you want.
4. Add promotion policies only after the base decay behavior resolves the right targets.

Example:

```cypher
CREATE DECAY PROFILE episode_decay OPTIONS {
       halfLifeSeconds: 3600,
       function: 'exponential',
       visibilityThreshold: 0.10,
       scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE episode_binding
FOR (n:MemoryEpisode)
APPLY {
       DECAY PROFILE 'episode_decay'
}

CREATE PROMOTION PROFILE reinforced_episode OPTIONS {
       multiplier: 1.25,
       scoreFloor: 0.25,
       scoreCap: 0.95,
       scope: 'NODE'
}

CREATE PROMOTION POLICY episode_reinforcement
FOR (n:MemoryEpisode)
APPLY {
       ON ACCESS {
              SET n.accessCount = coalesce(n.accessCount, 0) + 1
              SET n.lastAccessedAt = timestamp()
       }

       WHEN n.accessCount >= 3
              APPLY PROFILE 'reinforced_episode'
}
```

Immediately validate each step with:

```cypher
SHOW DECAY PROFILES;
SHOW PROMOTION POLICIES;
CALL nornicdb.knowledgepolicy.info();
CALL nornicdb.knowledgepolicy.resolve('', 'MemoryEpisode', '');
```

## Recommended Validation Loop

Use the same loop the codebase itself relies on in the Cypher e2e tests:

1. Create bundles and bindings.
2. Confirm the catalog row shapes with `SHOW ...` and `CALL nornicdb.knowledgepolicy.profiles()`.
3. Confirm the resolved effective policy with `CALL nornicdb.knowledgepolicy.resolve(...)`.
4. Query live entities with `decayScore()`, `decay()`, and `policy()`.
5. Only then add `ON ACCESS` mutations or Kalman-smoothed behavioral signals.

That mirrors the exercised paths in:

- `pkg/cypher/knowledgepolicy_procedure_e2e_test.go`
- `pkg/cypher/knowledgepolicy_functions_test.go`
- `pkg/knowledgepolicy/integration_test.go`

## What The Current Implementation Guarantees

The current code and tests establish a few important operator-facing contracts:

- If decay is disabled, entities return a neutral score of `1.0` and are not suppressed.
- If no binding matches a label or edge type, the entity also falls back to score `1.0`.
- Multi-label bindings are preferred over less specific bindings.
- Property overrides inherit the node or edge binding unless a property-specific rule overrides them.
- `NO DECAY` properties stay at score `1.0` and can block suppression eligibility for the containing entity.
- `ON ACCESS` state is written to access metadata, not directly to the node or edge payload.
- Suppressed entities are removed from normal visibility and are expected to feed deindex cleanup.

Those behaviors are covered in:

- `pkg/knowledgepolicy/resolver_test.go`
- `pkg/knowledgepolicy/scorer_test.go`
- `pkg/knowledgepolicy/scorer_property_visibility_test.go`
- `pkg/knowledgepolicy/access_flusher_on_access_test.go`

## Tuning And Testing

For a practical operator workflow covering policy design, profile selection, tuning knobs, observability, and regression tests, see:

- **[Knowledge Policy Tuning and Testing](knowledge-policy-tuning-testing.md)** — concrete authoring patterns, scenario playbooks, and test commands grounded in the current implementation

## See Also

- **[Decay Profiles](decay-profiles.md)** — Detailed profile authoring guide
- **[Promotion Policies](promotion-policies.md)** — Promotion policy authoring guide
- **[Visibility Suppression](visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Knowledge Policy Tuning and Testing](knowledge-policy-tuning-testing.md)** — How to create, tune, observe, and regression-test policies and profiles
- **[Ebbinghaus-Roynard Bootstrap](ebbinghaus-roynard-bootstrap.md)** — Complete ready-to-use configuration implementing the four-layer decomposition
- **[Feature Overview](../features/memory-decay.md)** — Feature summary
