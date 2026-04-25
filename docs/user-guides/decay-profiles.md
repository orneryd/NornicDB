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
| `halfLifeSeconds` | int | Time in seconds until score reaches 50% |
| `function` | string | `exponential`, `linear`, `step`, or `none` |
| `visibilityThreshold` | float | Score below which the entity is suppressed |
| `scoreFloor` | float | Minimum score |
| `scoreFrom` | string | `CREATED`, `VERSION`, or `CUSTOM` |
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
