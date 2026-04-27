# Appendix A: Ebbinghaus-Roynard Bootstrap

**A complete, ready-to-use knowledge-layer scoring configuration implementing the Ebbinghaus-Roynard four-layer decomposition.**

On database open, if `config.Memory.DecayEnabled` is true and the target namespace has no existing knowledge-policy schema entries at all, NornicDB seeds that namespace with this built-in bootstrap. It does not overwrite or merge with an existing knowledge-policy schema: if any decay bundles, decay bindings, promotion profiles, or promotion policies already exist, bootstrap is skipped. The built-in bootstrap covers the Ebbinghaus-Roynard defaults excluding the canonical-graph-ledger layer; that layer should be installed separately with a dedicated CGL bootstrap tailored to Roynard's model.

## Overview

This bootstrap implements the Ebbinghaus-Roynard model described in "The Missing Knowledge Layer in Cognitive Architectures for AI Agents" (Roynard, 2026; [arXiv:2604.11364](https://arxiv.org/pdf/2604.11364)). The paper identifies a category error in applying uniform cognitive decay to all content types and proposes a four-layer decomposition with distinct persistence semantics:

| Layer            | Content Type                    | Persistence Semantic                         | NornicDB Implementation                                                       |
| ---------------- | ------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------- |
| **Knowledge**    | Facts, claims, entities         | Supersession, not forgetting                 | `:KnowledgeFact` — `decayEnabled: false`; superseded via `:SUPERSEDES` edges  |
| **Memory**       | Experiences, episodes, sessions | Ebbinghaus forgetting + consolidation        | `:MemoryEpisode` — exponential decay, 7-day half-life, `scoreFrom: 'VERSION'` |
| **Wisdom**       | Behavioral directives, patterns | Evidence-gated revision with stability tiers | `:WisdomDirective` — `decayEnabled: false`; stability tiers via promotion     |
| **Intelligence** | The model itself                | Frozen in weights                            | The LLM — its effects persist only through writes to the other three layers   |

NornicDB implements the three persistence layers (Knowledge, Memory, Wisdom) as database-native primitives. The Intelligence layer is the model itself — it leaves no trace in storage and its effects persist only through writes to the other three layers. This is a fundamental architectural distinction, not a gap: there is nothing to configure because the model's weights are outside the persistence boundary.

This bootstrap provides the complete DDL to configure all three persistence layers, their edges, promotion tiers, and multi-agent session gating. Replace the label names with your domain's terminology.

## Quick Start

For a brand new decay-enabled database, these defaults are already installed automatically. Run the DDL below only when you want to inspect, customize, or reproduce the built-in bootstrap manually via the NornicDB shell or Bolt client.

```bash
nornicdb shell --data-dir ./data
```

---

## Step 1: Decay Profile Bundles

### Knowledge layer — no decay

Knowledge facts are permanent. They are superseded by newer evidence through graph operations, not by time-based decay. The Ebbinghaus curve does not apply here.

```cypher
CREATE DECAY PROFILE knowledge_fact_retention OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  function: 'none',
  scoreFrom: 'CREATED'
}
```

### Memory layer — Ebbinghaus exponential decay (7-day half-life)

Memory episodes decay according to the Ebbinghaus forgetting curve: `score(t) = e^(-ln(2) × t / 604800)`. Episodes that are not accessed or consolidated within this window lose score and eventually become invisible. Suppressed episodes are deindexed but remain accessible through `reveal()`.

```cypher
CREATE DECAY PROFILE memory_episode_retention OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFrom: 'VERSION'
}
```

### Memory summary — slower decay for summarized content

Episode summaries retain value even as the raw experience fades. A 14-day half-life with a score floor of 0.10 preserves summary accessibility longer.

```cypher
CREATE DECAY PROFILE session_summary OPTIONS {
  halfLifeSeconds: 1209600,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFloor: 0.10
}
```

### Wisdom layer — no time-based decay

Wisdom directives are revised by evidence-gated graph operations, not by time. Stability tiers (managed through promotion policies) gate revision.

```cypher
CREATE DECAY PROFILE wisdom_directive_retention OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  function: 'none',
  scoreFrom: 'CREATED'
}
```

### Evidence edges — 30-day decay

The relevance of supporting evidence fades, even though the Knowledge fact it supports does not. A 30-day half-life balances recency and retention.

```cypher
CREATE DECAY PROFILE evidence_decay OPTIONS {
  halfLifeSeconds: 2592000,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFrom: 'CREATED'
}
```

---

## Step 2: Decay Profile Bindings

### Memory Episodes — full Ebbinghaus decay with bi-temporal properties

```cypher
CREATE DECAY PROFILE memory_episode_retention_binding
FOR (n:MemoryEpisode)
APPLY {
  DECAY PROFILE 'memory_episode_retention'
  DECAY VISIBILITY THRESHOLD 0.10
  n.tenantId NO DECAY
  n.agentId NO DECAY
  n.sessionId NO DECAY
  n.system_created_at NO DECAY
  n.system_expired_at NO DECAY
  n.valid_from NO DECAY
  n.valid_to NO DECAY
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
  n.ephemeralContext DECAY HALF LIFE 86400
}
```

**Property-level `NO DECAY` on a decaying node** is meaningful — it marks structural properties that remain fully visible even as the node's content decays. Properties like `tenantId`, `sessionId`, and bi-temporal timestamps (`system_created_at`, `valid_from`, etc.) are query infrastructure, not decaying content. A `NO DECAY` property also acts as a suppression anchor: a node with at least one `NO DECAY` property cannot be fully suppressed because that property remains visible at score 1.0.

Note: property-level `NO DECAY` is only useful when the parent node *does* decay. On a `decayEnabled: false` node (like `:KnowledgeFact` or `:WisdomDirective`), property-level `NO DECAY` is redundant — the entire node is already permanent.

### Knowledge Facts — supersession model

```cypher
CREATE DECAY PROFILE knowledge_fact_retention_binding
FOR (n:KnowledgeFact)
APPLY {
  DECAY PROFILE 'knowledge_fact_retention'
}
```

The referenced profile has `decayEnabled: false` — the entire node is permanent. Property-level `NO DECAY` would be redundant here. Properties are protected by supersession, not by decay anchors.

### Wisdom Directives — evidence-gated revision

```cypher
CREATE DECAY PROFILE wisdom_directive_retention_binding
FOR (n:WisdomDirective)
APPLY {
  DECAY PROFILE 'wisdom_directive_retention'
}
```

Same as Knowledge: the referenced profile disables decay entirely. Wisdom directives are revised through evidence-gated graph operations (`:REVISES` edges), not by time.

### Evidence edges — 30-day decay with permanent provenance

The evidence edge decays with a 30-day half-life, but `r.sourceId` is declared `NO DECAY` — it is a structural property that must remain visible for provenance queries even as the edge's relevance fades.

```cypher
CREATE DECAY PROFILE evidence_edge_retention_binding
FOR ()-[r:EVIDENCES]-()
APPLY {
  DECAY PROFILE 'evidence_decay'
  DECAY VISIBILITY THRESHOLD 0.10
  r.sourceId NO DECAY
}
```

### Supersession edges — permanent provenance

Every `:SUPERSEDES` edge is a permanent record of the Knowledge-layer supersession chain. Entity-level `NO DECAY` makes the entire edge permanent — property-level `NO DECAY` would be redundant.

```cypher
CREATE DECAY PROFILE supersession_edge_retention
FOR ()-[r:SUPERSEDES]-()
APPLY {
  NO DECAY
}
```

### Consolidation edges — permanent link from episodes to facts

```cypher
CREATE DECAY PROFILE consolidation_edge_retention
FOR ()-[r:CONSOLIDATES_TO]-()
APPLY {
  NO DECAY
}
```

### Revision edges — permanent wisdom provenance

```cypher
CREATE DECAY PROFILE revision_edge_retention
FOR ()-[r:REVISES]-()
APPLY {
  NO DECAY
}
```

### Derived-from edges — permanent lineage

```cypher
CREATE DECAY PROFILE derivation_edge_retention
FOR ()-[r:DERIVED_FROM]-()
APPLY {
  NO DECAY
}
```

---

## Step 3: Promotion Profiles

### Memory Consolidation Tiers

```cypher
CREATE PROMOTION PROFILE memory_reinforced OPTIONS {
  multiplier: 1.25,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

```cypher
CREATE PROMOTION PROFILE consolidation_candidate OPTIONS {
  multiplier: 1.50,
  scoreFloor: 0.80,
  scoreCap: 1.0
}
```

### Wisdom Stability Tiers

```cypher
CREATE PROMOTION PROFILE wisdom_provisional OPTIONS {
  multiplier: 1.0,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

```cypher
CREATE PROMOTION PROFILE wisdom_established OPTIONS {
  multiplier: 1.0,
  scoreFloor: 0.50,
  scoreCap: 1.0
}
```

```cypher
CREATE PROMOTION PROFILE wisdom_canonical OPTIONS {
  multiplier: 1.0,
  scoreFloor: 0.90,
  scoreCap: 1.0
}
```

### Evidence Traversal Tier

```cypher
CREATE PROMOTION PROFILE reinforced_evidence OPTIONS {
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

---

## Step 4: Promotion Policies

### Memory Episode Consolidation

Tracks access patterns across sessions and agents. Uses Kalman filtering to smooth noisy behavioral signals and session gating to prevent within-session sycophancy loops.

**Architectural boundary:** the promotion policy determines _when an entity is a candidate for consolidation_ (via `WHEN` predicates and promotion tiers). It does **not** perform the consolidation itself — creating a `:KnowledgeFact` node and linking it with a `:CONSOLIDATES_TO` edge is an application-layer concern. The Heimdall plugin (`pkg/heimdall/plugin.go`) serves as the reference implementation.

```cypher
CREATE PROMOTION POLICY memory_episode_consolidation
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    SET n.accessIntervals = coalesce(n.accessIntervals, '') + ',' + toString(timestamp())
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'memory_reinforced'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.80
    APPLY PROFILE 'consolidation_candidate'
}
```

### Wisdom Directive Stability

Stability tiers gate revision: a provisional directive can be revised by any contradicting evidence; a canonical directive requires overwhelming counter-evidence. `evidenceCount`, `contradictionRate`, and `crossSessionSupport` in the `WHEN` predicates resolve from accessMeta first, falling back to stored node properties.

```cypher
CREATE PROMOTION POLICY wisdom_directive_stability
FOR (n:WisdomDirective)
APPLY {
  ON ACCESS {
    SET n.evaluationCount = coalesce(n.evaluationCount, 0) + 1
    SET n.lastEvaluatedAt = timestamp()
  }

  WHEN n.evidenceCount < 3
    APPLY PROFILE 'wisdom_provisional'

  WHEN n.evidenceCount >= 3 AND n.contradictionRate < 0.20
    APPLY PROFILE 'wisdom_established'

  WHEN n.evidenceCount >= 10 AND n.contradictionRate < 0.05 AND n.crossSessionSupport >= 3
    APPLY PROFILE 'wisdom_canonical'
}
```

### Evidence Edge Traversal

More frequently traversed evidence links carry higher weight in retrieval even as their base decay score drops.

```cypher
CREATE PROMOTION POLICY evidence_traversal_tiering
FOR ()-[r:EVIDENCES]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()
  }

  WHEN r.traversalCount >= 5
    APPLY PROFILE 'reinforced_evidence'
}
```

---

## How It Works

### Memory Layer — Ebbinghaus Forgetting with Spaced Repetition

A `:MemoryEpisode` created at time `t₀` has a base score of:

```
score(t) = e^(-ln(2) × (t - t₀) / 604800)
```

| Age     | Base Score | With `memory_reinforced` (×1.25) | With `consolidation_candidate` (floor 0.80) |
| ------- | ---------- | -------------------------------- | ------------------------------------------- |
| 0 days  | 1.000      | 1.000                            | 1.000                                       |
| 7 days  | 0.500      | 0.625                            | 0.800                                       |
| 14 days | 0.250      | 0.313                            | 0.800                                       |
| 21 days | 0.125      | 0.156                            | 0.800                                       |
| 30 days | 0.045      | 0.056                            | 0.800                                       |

The `consolidation_candidate` floor of 0.80 models the spaced-repetition finding: sufficiently rehearsed content resists forgetting indefinitely until consolidated into a `:KnowledgeFact`.

### Knowledge Layer — Canonical Graph Ledger

`:KnowledgeFact` nodes never decay. When a fact is updated, the canonical graph ledger pattern applies:

1. Create a new `:KnowledgeFact` node with the updated assertion
2. Create a `:SUPERSEDES` edge from the new node to the old, carrying `superseded_at`, `superseded_by_agent`, `evidence_source`
3. Move the `:CURRENT` pointer to the new version

The old fact remains in the graph as an immutable historical record. See [Canonical Graph Ledger](canonical-graph-ledger.md) for the full pattern.

### Wisdom Layer — Evidence-Gated Stability

`:WisdomDirective` nodes never decay. They progress through stability tiers based on accumulated evidence:

| Tier            | Trigger                                                                       | Score Floor | Revision Requirement                                    |
| --------------- | ----------------------------------------------------------------------------- | ----------- | ------------------------------------------------------- |
| **Provisional** | `evidenceCount < 3`                                                           | 0.0         | Any contradicting evidence                              |
| **Established** | `evidenceCount >= 3`, `contradictionRate < 0.20`                              | 0.50        | Multiple independent sources                            |
| **Canonical**   | `evidenceCount >= 10`, `contradictionRate < 0.05`, `crossSessionSupport >= 3` | 0.90        | Overwhelming counter-evidence; flagged for human review |

When evidence gates are met for revision, the operation creates a new `:WisdomDirective` (status: `active`), sets the old one to `status: 'retired'`, and links them via a `:REVISES` edge with provenance.

### Edge Decay

| Edge Type          | Behavior                                      | Purpose                                                  |
| ------------------ | --------------------------------------------- | -------------------------------------------------------- |
| `:EVIDENCES`       | 30-day half-life, reinforced at 5+ traversals | Supporting evidence fades; the fact it supports does not |
| `:SUPERSEDES`      | No decay                                      | Permanent provenance: old facts linked to successors     |
| `:CONSOLIDATES_TO` | No decay                                      | Permanent link from consolidated episode to fact         |
| `:REVISES`         | No decay                                      | Permanent wisdom revision history                        |
| `:DERIVED_FROM`    | No decay                                      | Permanent lineage                                        |

### Multi-Agent Session Gating

The `$_session` and `$_agent` variables are passed from HTTP headers and prevent within-session access inflation:

| Header            | Variable    | Purpose                    |
| ----------------- | ----------- | -------------------------- |
| `X-Query-Session` | `$_session` | Same-session deduplication |
| `X-Query-Agent`   | `$_agent`   | Per-agent tracking         |
| `X-Query-Tenant`  | `$_tenant`  | Multi-tenant isolation     |

The `WITH KALMAN SET n.crossSessionAccessRate = CASE WHEN ...` pattern gates the counter so that 50 accesses from one session count as one observation. The Kalman filter then smooths genuine cross-session signals.

---

## Verification

After running all DDL statements:

```cypher
SHOW DECAY PROFILES
```

Expected: 5 bundles and 8 bindings.

```cypher
SHOW PROMOTION POLICIES
```

Expected: 6 promotion profiles and 3 promotion policies.

### CLI Maintenance

```bash
nornicdb decay recalculate --data-dir ./data
nornicdb suppress --data-dir ./data --threshold 0.10
nornicdb decay stats --data-dir ./data
```

---

## Customization

1. **Replace node labels**: `:MemoryEpisode` → your ephemeral data label, `:KnowledgeFact` → your persistent fact label, `:WisdomDirective` → your policy/rule label
2. **Adjust half-lives**: `604800` (7 days) works for conversational memory; adjust for your domain's natural forgetting rate
3. **Tune promotion thresholds**: `accessCount >= 3` and `sourceAgreement >= 0.80` are starting points; tune based on your multi-agent setup
4. **Add property rules**: Extend `APPLY` blocks with domain-specific properties that need `NO DECAY` or custom half-lives

---

## Related Documentation

- **[Knowledge-Layer Policies](knowledge-layer-policies.md)** — System overview
- **[Decay Profiles](decay-profiles.md)** — DDL syntax reference
- **[Promotion Policies](promotion-policies.md)** — Promotion DDL reference
- **[Visibility Suppression](visibility-suppression-deindex.md)** — Suppression and deindex behavior
- **[Canonical Graph Ledger](canonical-graph-ledger.md)** — FactKey/FactVersion/SUPERSEDES pattern
