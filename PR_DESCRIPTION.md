# Knowledge-Layer Scoring and Visibility

Replaces the hardcoded three-tier memory decay model (`Episodic`/`Semantic`/`Procedural`) with a fully declarative, profile-and-policy-driven scoring system. Implements the Ebbinghaus-Roynard four-layer decomposition from [arXiv:2604.11364](https://arxiv.org/pdf/2604.11364).

## What changed

The old system had three hardcoded decay tiers with fixed half-lives baked into `pkg/decay/`. The new system lets operators define arbitrary decay profiles, bind them to any label or edge type, set property-level overrides, and configure promotion policies with Kalman-filtered behavioral signals — all via Cypher DDL.

Built across 8 phases with ~28 new test files. Gated behind the existing `config.Memory.DecayEnabled` flag (default `false`). Zero breaking changes when disabled.

---

## New features with example syntax

### 1. Decay profile bundles — reusable parameter sets

```cypher
CREATE DECAY PROFILE working_memory OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  visibilityThreshold: 0.10,
  scoreFrom: 'VERSION'
}
```

Supported functions: `exponential`, `linear`, `step`, `none`.
ScoreFrom modes: `CREATED` (default), `VERSION` (restarts on update), `CUSTOM` (from a named property).

### 2. Decay profile bindings — targeted to labels or edge types

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.tenantId NO DECAY
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
  n.ephemeralContext DECAY HALF LIFE 86400
}
```

Property-level `NO DECAY` acts as a suppression anchor — the node's content still decays, but the entity can never be suppressed because the anchored property remains at score 1.0.

Edge bindings:

```cypher
CREATE DECAY PROFILE evidence_edge_retention
FOR ()-[r:EVIDENCES]-()
APPLY {
  DECAY PROFILE 'evidence_decay'
  DECAY VISIBILITY THRESHOLD 0.10
  r.sourceId NO DECAY
}
```

No-decay bindings (permanent entities):

```cypher
CREATE DECAY PROFILE supersession_edge_retention
FOR ()-[r:SUPERSEDES]-()
APPLY {
  NO DECAY
}
```

### 3. Promotion profiles — score multipliers and floors

```cypher
CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
  multiplier: 1.25,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE consolidation_candidate OPTIONS {
  multiplier: 1.50,
  scoreFloor: 0.80,
  scoreCap: 1.0
}
```

### 4. Promotion policies — behavioral signals with Kalman filtering

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
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.80
    APPLY PROFILE 'consolidation_candidate'
}
```

`WITH KALMAN` smooths noisy behavioral signals. Session gating (`$_session`) prevents within-session access inflation — 50 accesses from the same session count as one observation.

### 5. Cypher query functions

```cypher
-- Current decay score (0.0-1.0)
MATCH (n:MemoryEpisode) RETURN n.id, decayScore(n) ORDER BY decayScore(n) DESC

-- Full scoring resolution with metadata
MATCH (n:MemoryEpisode {id: $id}) RETURN decay(n)

-- Which profile/policy applies
MATCH (n:MemoryEpisode) RETURN policy(n)

-- Bypass visibility suppression
MATCH (n:MemoryEpisode) CALL reveal(n) RETURN n, decayScore(n)
```

### 6. Visibility suppression and deindex

Entities whose score drops below the visibility threshold are suppressed — hidden from `MATCH` and search results, but data preserved. A background deindex job removes suppressed entities from secondary indexes (BM25, vector) without scanning.

```cypher
-- Suppressed nodes are invisible to normal queries
MATCH (n:MemoryEpisode) RETURN n  -- only returns visible nodes

-- reveal() bypasses suppression
MATCH (n:MemoryEpisode) CALL reveal(n) RETURN n  -- returns all nodes
```

### 7. Admin endpoints

```
GET /admin/knowledge-policies/profiles?database=X     -- list all profiles and bindings
GET /admin/knowledge-policies/policies?database=X     -- list all promotion policies
GET /admin/knowledge-policies/resolve?entityId=X      -- explain scoring for a specific entity
GET /admin/knowledge-policies/deindex/status?database=X -- deindex cleanup job status
```

### 8. Introspection DDL

```cypher
SHOW DECAY PROFILES;
SHOW PROMOTION POLICIES;
DROP DECAY PROFILE session_record_retention;
DROP PROMOTION POLICY memory_episode_consolidation;
```

---

## Scoring formula

```
finalScore = max(decayFloor, min(scoreCap, max(promoFloor, baseDecay(t) * multiplier)))
```

Where `baseDecay(t) = e^(-ln(2) * age / halfLife)` for exponential decay.

Suppression eligibility:

```
SuppressionEligible = finalScore < visibilityThreshold AND NOT hasNoDecayProperty
```

## Ebbinghaus-Roynard bootstrap

A complete ready-to-use configuration implementing the four-layer decomposition is provided as a bootstrap template in `docs/user-guides/ebbinghaus-roynard-bootstrap.md`:

| Layer | Label | Behavior |
|---|---|---|
| Knowledge | `:KnowledgeFact` | No decay. Superseded via `:SUPERSEDES` edges (canonical graph ledger) |
| Memory | `:MemoryEpisode` | 7-day exponential decay, consolidation promotion at 5+ cross-session accesses |
| Wisdom | `:WisdomDirective` | No decay. Stability tiers (provisional/established/canonical) gate revision |
| Intelligence | The LLM | Outside persistence boundary. Effects persist through writes to other layers |

## Key files

| Area | Files |
|---|---|
| Core types & scorer | `pkg/knowledgepolicy/` (types, resolver, scorer, binding_builder, access_meta, kalman) |
| Schema persistence | `pkg/storage/schema_knowledgepolicy.go` |
| DDL parser | `pkg/cypher/knowledgepolicy_ddl.go` |
| Runtime integration | `pkg/storage/badger_decay_filter.go`, `badger_deindex_cleanup.go` |
| Cypher functions | `pkg/cypher/decay_functions.go`, `reveal.go` |
| Admin endpoints | `pkg/server/server_knowledgepolicy.go` |
| Browser UI | `ui/src/pages/KnowledgePoliciesAdmin.tsx` |
| Documentation | `docs/user-guides/` (5 new guides + bootstrap) |

## Migration

Phase 7 extracts legacy `AccessCount`, `LastAccessed`, `DecayScore` fields from `Node` structs into `AccessMetaEntry` records on first startup. The migration is idempotent and runs automatically. Legacy `decay.Manager`, `Node.DecayScore`, and the `Episodic`/`Semantic`/`Procedural` tier model are removed.
