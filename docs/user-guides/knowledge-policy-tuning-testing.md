# Knowledge Policy Tuning and Testing

This guide is for operators and contributors who need to do more than enable knowledge policy. It focuses on how to create bundles, bindings, and promotion policies; how to tune them against real query behavior; and how to validate them against the current implementation.

The examples and recommendations in this guide are grounded in the code paths exercised by:

- `pkg/cypher/knowledgepolicy_procedure_e2e_test.go`
- `pkg/cypher/knowledgepolicy_functions_test.go`
- `pkg/knowledgepolicy/integration_test.go`
- `pkg/knowledgepolicy/scorer_property_visibility_test.go`
- `pkg/knowledgepolicy/access_flusher_on_access_test.go`
- `pkg/knowledgepolicy/kalman_anti_sycophancy_test.go`

# Description

Design knowledge policy in this order:

1. Decide what should decay.
2. Decide what should never decay.
3. Decide what should become stronger with repeated or corroborated access.
4. Decide what score should hide the entity from normal reads.

In practice, that means:

- decay bundles define the math
- decay bindings decide the target
- property rules decide exceptions
- promotion profiles define boost parameters
- promotion policies decide when those boosts apply

## Authoring Checklist

Use this sequence every time you introduce a new policy family.

### 1. Start With A Bundle

Use `OPTIONS { ... }` bundles for reusable math only.

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.01,
  scoreFrom: 'CREATED'
}
```

Choose `scoreFrom` intentionally:

- `CREATED`: use when age should reflect original creation time.
- `VERSION`: use when updates should refresh the decay anchor.
- `CUSTOM`: use when a promotion or corroboration signal should control the anchor.

The scorer paths for these are covered in `pkg/knowledgepolicy/scorer_test.go`, especially the `ScoreFromVersion` and `ScoreFromCustom` cases.

### 2. Bind The Bundle To Real Targets

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY HALF LIFE 1209600
  n.tenantId NO DECAY
}
```

Use property rules only where the entity-level rule is not enough.

Current implementation details that matter here:

- unmatched labels fall back to neutral score `1.0`
- property rules inherit the parent binding unless overridden
- `NO DECAY` property rules stay at `1.0`
- `NO DECAY` properties can make the entity not suppression-eligible

Those behaviors are exercised in `pkg/knowledgepolicy/scorer_property_visibility_test.go`.

### 3. Add Promotion Profiles Before Promotion Policies

```cypher
CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
  multiplier: 1.25,
  scoreFloor: 0.25,
  scoreCap: 0.95,
  scope: 'NODE'
}
```

Then attach them with policy logic:

```cypher
CREATE PROMOTION POLICY session_record_reinforcement
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}
```

Important: `ON ACCESS` mutations write into access metadata, not the node or edge record itself. That behavior is validated in `pkg/knowledgepolicy/access_flusher_on_access_test.go`.

### 4. Validate Resolution Before Shipping

Run the same diagnostics the e2e tests use:

```cypher
CALL nornicdb.knowledgepolicy.info();
CALL nornicdb.knowledgepolicy.profiles();
CALL nornicdb.knowledgepolicy.policies();
CALL nornicdb.knowledgepolicy.resolve('', 'SessionRecord', '');

SHOW DECAY PROFILES;
SHOW PROMOTION POLICIES;
```

What to look for:

- the expected bundle and binding rows exist
- the target labels or edge type are what you intended
- the resolved profile name is the one you expected
- the effective threshold and multiplier match your design

## Scenario Playbooks

### Fast-Fading Episodes

Use this when short-lived conversational episodes should disappear quickly unless they are revisited.

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
```

Why this is a safe starting point:

- the codebase already validates that a recent `MemoryEpisode` stays visible
- a sufficiently old `MemoryEpisode` becomes suppression-eligible

See `pkg/cypher/knowledgepolicy_functions_test.go` and `pkg/knowledgepolicy/integration_test.go`.

### Durable Facts That Never Decay

Use this when facts should remain visible even when they are old.

```cypher
CREATE DECAY PROFILE canonical_fact OPTIONS {
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE fact_binding
FOR (n:KnowledgeFact)
APPLY {
  DECAY PROFILE 'canonical_fact'
}
```

The neutral no-decay behavior is covered by the `KnowledgeFact` paths in:

- `pkg/cypher/knowledgepolicy_functions_test.go`
- `pkg/knowledgepolicy/integration_test.go`

### Entity Decays, Identifier Does Not

Use property rules when most of the entity can fade but certain properties must remain stable for joins, tenancy, or provenance.

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  n.summary DECAY PROFILE 'session_summary'
  n.tenantId NO DECAY
}
```

This pattern is important because the implementation treats `NO DECAY` properties as stronger than the surrounding decaying entity for visibility calculations. That is not just documentation preference; it is exercised in `pkg/knowledgepolicy/scorer_property_visibility_test.go`.

### Updates Reset Freshness

Use `scoreFrom: 'VERSION'` when edits should refresh the score anchor.

```cypher
CREATE DECAY PROFILE mutable_document OPTIONS {
  halfLifeSeconds: 2592000,
  function: 'exponential',
  visibilityThreshold: 0.08,
  scoreFrom: 'VERSION'
}
```

This is appropriate for living summaries, drafts, or rolling state. The scorer behavior is covered in `TestScorer_ScoreFromVersion` in `pkg/knowledgepolicy/scorer_test.go`.

### Corroboration-Driven Recovery

Use `scoreFrom: 'CUSTOM'` plus on-access mutation logic when corroboration or explicit signal refresh should control decay.

```cypher
CREATE DECAY PROFILE evidence_decay OPTIONS {
  halfLifeSeconds: 3600,
  function: 'exponential',
  visibilityThreshold: 0.20,
  scoreFrom: 'CUSTOM',
  scoreFromProperty: 'lastCorroboratedAt'
}

CREATE PROMOTION PROFILE reinforced_evidence OPTIONS {
  multiplier: 1.25,
  scoreFloor: 0.25,
  scoreCap: 1.0,
  scope: 'NODE'
}
```

This is the same general shape exercised in `TestIntegration_SuppressionLayer_NoisyCorroborationSignals` in `pkg/knowledgepolicy/integration_test.go`.

### Noisy Behavioral Signals

If you use `WITH KALMAN`, tune it as a smoothing mechanism, not a truth generator.

```cypher
CREATE PROMOTION POLICY episodic_recall_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
  }
}
```

The current tests establish three practical rules:

- isolated spikes should be dampened
- sustained trends should still move the filtered value
- same-session repetition should not look like cross-session corroboration

Those are covered in:

- `pkg/knowledgepolicy/kalman_anti_sycophancy_test.go`
- `pkg/knowledgepolicy/kalman_multiagent_test.go`

## Tuning Knobs

### Half-Life

Adjust half-life first. It is the biggest lever.

- shorter half-life: stronger recency bias, faster suppression
- longer half-life: slower fade, larger visible working set

Use `exponential` when you want graceful fading, `step` when you want a hard cliff, and `none` when the entity should remain durable.

### Visibility Threshold

Adjust threshold second. Threshold decides when a score stops being visible, not how fast it drops.

- high threshold: more aggressive hiding
- low threshold: more entities remain query-visible

If many things are unexpectedly disappearing, lower the threshold before you start rewriting promotion logic.

### Score Floor

Use score floor when something should degrade in ranking but should not fully collapse.

Good uses:

- keep a confidence field queryable even when the entity is old
- prevent promoted entities from dropping to near-zero after a brief idle period

Property-level floor behavior is exercised in `TestPropertyVisibility_ScoreFloorOverride`.

### Promotion Multiplier, Floor, and Cap

Treat these as ranking controls layered on top of the base decay curve.

- `multiplier` increases or dampens the score
- `scoreFloor` guarantees a minimum promoted score
- `scoreCap` prevents runaway inflation

If promotion makes everything effectively permanent, lower the cap before lowering the multiplier.

## Testing Strategy

### Cypher Smoke Tests

For every new policy family, run a small manual script:

```cypher
SHOW DECAY PROFILES;
SHOW PROMOTION POLICIES;
CALL nornicdb.knowledgepolicy.resolve('', 'MemoryEpisode', '');

MATCH (n:MemoryEpisode)
RETURN n.id, decayScore(n), decay(n), policy(n)
ORDER BY decayScore(n) ASC
LIMIT 20;
```

This catches most authoring mistakes before they become runtime incidents.

### Focused Go Tests

Use these focused test slices depending on what changed:

```bash
go test ./pkg/cypher -run 'KnowledgePolicy|DecayScore|ShowDecayProfiles|ShowPromotionPolicies' -count=1
go test ./pkg/knowledgepolicy/... -count=1
```

For narrower validation:

```bash
go test ./pkg/knowledgepolicy -run 'TestIntegration_DDL_Score_Visibility|TestIntegration_SuppressionLayer_NoisyCorroborationSignals' -count=1
go test ./pkg/knowledgepolicy -run 'TestPropertyVisibility|TestAccessFlusher_AppliesOnAccessMutations|TestKalmanAntiSycophancy' -count=1
```

### Performance Checks

If you change heavily-accessed policies or add more on-access logic, run the hot-path benchmarks too:

```bash
go test ./pkg/knowledgepolicy -bench 'BenchmarkScoreNode|BenchmarkAccumulator|BenchmarkKalmanMutation' -benchmem -count=1
```

Relevant benchmark files:

- `pkg/knowledgepolicy/scorer_bench_test.go`
- `pkg/knowledgepolicy/access_accumulator_bench_test.go`
- `pkg/knowledgepolicy/kalman_accumulator_bench_test.go`

## Observability And Runtime Diagnostics

Use the observability surface when tuning in staging or production:

- `nornicdb_knowledge_policy_scored_total`
- `nornicdb_knowledge_policy_suppressions_total`
- `nornicdb_knowledge_policy_decay_score`
- `nornicdb_knowledge_policy_on_access_mutations_total`
- `nornicdb_knowledge_policy_deindex_enqueued_total`

Interpretation guidance lives in `docs/observability/knowledge-policy-metrics.md`.

Practical workflow:

1. watch `scored_total` and `suppressions_total` after enabling a new binding
2. confirm the score histogram is not flat or pinned at `1.0`
3. check `on_access_mutations_total` after shipping a new promotion policy
4. check `deindex_enqueued_total` if you intentionally tightened thresholds

## Common Failure Modes

### Everything Scores 1.0

Likely causes:

- decay is disabled
- no binding matches the target labels or edge type
- you authored a bundle but never created a binding
- you intentionally used `NO DECAY`

Check:

```cypher
CALL nornicdb.knowledgepolicy.info();
CALL nornicdb.knowledgepolicy.resolve('', 'YourLabel', '');
```

### Promotion Never Fires

Likely causes:

- the `WHEN` predicate depends on access metadata that is never being written
- the entity is suppressed before repeated access can accumulate
- same-session traffic is being gated and you expected cross-session growth

Check `policy(n)` output and the on-access-related tests in `pkg/knowledgepolicy/access_flusher_on_access_test.go` and `pkg/knowledgepolicy/kalman_multiagent_test.go`.

### Suppression Is Too Aggressive

Adjust in this order:

1. increase half-life
2. lower visibility threshold
3. add property-level `NO DECAY` or floor rules where appropriate
4. only then introduce promotion

### Policy Set Is Hard To Reason About

Usually this means you created too many narrow bindings before confirming the fallback behavior. Prefer:

- one reusable bundle
- one binding per durable target class
- one promotion policy per behavior family

The resolver precedence and conflict behavior are covered in:

- `pkg/knowledgepolicy/resolver_test.go`
- `pkg/knowledgepolicy/binding_builder_conflict_test.go`

## Related Guides

- [Knowledge-Layer Policies](knowledge-layer-policies.md)
- [Decay Profiles](decay-profiles.md)
- [Promotion Policies](promotion-policies.md)
- [Visibility Suppression and Deindex](visibility-suppression-deindex.md)
- [Knowledge Policy Metrics Reference](../observability/knowledge-policy-metrics.md)