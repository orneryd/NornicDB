# Knowledge-Layer Policies

NornicDB's knowledge-layer scoring system manages the lifecycle and visibility of graph entities through declarative profiles and policies. This guide covers the system architecture and how the pieces fit together.

## Architecture

The scoring system has three layers:

1. **Decay Profiles** — targeted bindings (`FOR` + `APPLY`) defining how scores decrease over time
2. **Parameter Bundles** — reusable configuration objects (no `FOR`) referenced by name inside profiles
3. **Promotion Policies** — targeted bindings with `ON ACCESS` mutations and `WHEN` predicates that boost scores

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

When an entity is queried, the scorer:

1. **Resolves** the decay profile by matching the entity's labels (or edge type) against bindings
2. **Calculates** the base decay score using the profile's function and half-life
3. **Applies** any matching promotion policy multiplier
4. **Determines** visibility: if the final score is below the threshold, the entity is suppressed

### Score Formula

```
finalScore = max(scoreFloor, baseDecay(t) * promotionMultiplier)
```

Where `baseDecay(t)` depends on the decay function:
- **Exponential:** `e^(-ln(2)/halfLife * t)` where halfLife is in seconds
- **Linear:** `max(0, 1 - t/fullLife)`
- **Step:** `1.0` if `t < halfLife`, else `0.0`
- **None:** Always `1.0`

## Binding Resolution Order

When multiple bindings could match:

1. Multi-label target (most labels) takes precedence over single-label
2. Exact label match takes precedence over wildcard
3. If two bindings have equal specificity, the resolver returns a diagnostic warning
4. No binding → entity scores 1.0 (no decay)

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
