# Knowledge-Layer Scoring and Visibility — Implementation Plan

**Status:** Phase 6 COMPLETE — Ready for Phase 7
**Date:** April 21, 2026
**Last Audit:** April 24, 2026
**Parent Design:** [knowledge-layer-persistence-plan.md](./knowledge-layer-persistence-plan.md)
**Target Version:** 1.1.0 (incompatible break with experimental memory model)

---

## Overview

This document is the concrete implementation plan for the knowledge-layer scoring and visibility system described in the parent design document. It maps the design's six workstreams to specific Go packages, files, types, functions, Badger key prefixes, schema persistence changes, Cypher parser additions, and test suites — sequenced into phases with explicit dependencies, acceptance gates, and migration notes.

The plan is intentionally file-and-function-level. Each phase produces a shippable, testable increment. No phase depends on a later phase.

---

## Implementation Progress

_Updated: 2026-04-24_

### Phase 1: Schema Objects and Profile Model — COMPLETE

| Deliverable | File | Status | Notes |
|---|---|---|---|
| Core types | `pkg/knowledgepolicy/types.go` | DONE | All types with json+msgpack tags |
| AccessMeta types | `pkg/knowledgepolicy/access_meta.go` | DONE | |
| Compiled binding table | `pkg/knowledgepolicy/compiled_binding.go` | DONE | |
| Types msgpack tests | `pkg/knowledgepolicy/types_test.go` | DONE | 14 msgpack round-trip tests |
| Binding table tests | `pkg/knowledgepolicy/compiled_binding_test.go` | DONE | Includes concurrency tests |
| Schema persistence ext | `pkg/storage/schema_persistence.go` | DONE | export + replaceFrom extended |
| SchemaManager fields | `pkg/storage/schema.go` | DONE | 5 new fields |
| DDL parser | `pkg/cypher/knowledgepolicy_ddl.go` | DONE | All 14 statement types, 5 bugs fixed (see deviations) |
| SchemaManager CRUD | `pkg/storage/schema_knowledgepolicy.go` | DONE | Create/Drop/Alter/Show all types, 1 bug fixed (see deviations) |
| SchemaManager tests | `pkg/storage/schema_knowledgepolicy_test.go` | DONE | 23 tests |
| Badger key prefixes | `pkg/storage/badger.go` | DONE | 0x11–0x17 |
| Feature flag gating | `pkg/config/config.go` | DONE | Default=false, env var updated |
| DDL parser tests | `pkg/cypher/knowledgepolicy_ddl_test.go` | DONE | 93 tests covering all 14 DDL types + error cases |

**Acceptance gate:** 141 tests pass across `pkg/knowledgepolicy/...`, `pkg/storage/...`, `pkg/cypher/...`.

### Deviations and bugs found during Phase 1

1. **BUG (FIXED): `CreatePromotionPolicy` nil-map guard** — `schema_knowledgepolicy.go`: when `promotionProfiles` map was nil (no profiles created yet), the `wc.ProfileRef` validation was skipped entirely due to `sm.promotionProfiles != nil` guard. A policy with a dangling profile ref was silently accepted. Fixed by always validating profile refs and returning an error when the map is nil.

2. **BUG (FIXED): `kpScanBraceBlock` off-by-one** — `knowledgepolicy_ddl.go`: `return s[start:i-1], i` truncated the last character before the closing `}`. Input `{abc}` returned `"ab"` instead of `"abc"`. Fixed to `return s[start:i], i+1`.

3. **BUG (FIXED): `parseForTarget` edge pattern detection** — `knowledgepolicy_ddl.go`: when parsing `()-[r:SUPERSEDES]-()`, the parser saw `()` and immediately returned as a wildcard node pattern, never reaching the edge pattern parser. Fixed by checking for `-` after `)` before declaring wildcard, routing to `parseEdgeTarget` when found.

4. **BUG (FIXED): Unquoted name fallback in WHEN/APPLY blocks** — `knowledgepolicy_ddl.go`: `kpScanQuotedString` returns `("", i)` not `("", -1)` for non-quoted input, so the `if l < 0` fallback to `kpScanIdent` was unreachable for unquoted identifiers. Extracted `kpScanName` helper (quoted-then-ident) and applied it across 8+ call sites to DRY up the pattern.

5. **BUG (FIXED): `parsePropertyRule` missing whitespace skip** — `knowledgepolicy_ddl.go`: after parsing a value like `PROFILE 'slow_decay'`, `i` pointed right after the closing quote with no space skip between keyword match iterations, so subsequent keywords like `HALFLIFE` failed to match. Fixed by adding `i = kpSkipSpaces(s, i)` at the start of each loop iteration.

### Phase 2: Shared Resolver and Scorer — COMPLETE

| Deliverable | File | Status | Notes |
|---|---|---|---|
| ScoringResolution struct | `pkg/knowledgepolicy/resolution.go` | DONE | Pure data carrier + NeutralResolution var |
| CompiledPropertyOverride | `pkg/knowledgepolicy/compiled_binding.go` | DONE | Extended with override struct + field |
| Binding builder | `pkg/knowledgepolicy/binding_builder.go` | DONE | Pure function, ThresholdAgeNanos, conflict detection, property rule expansion |
| Resolver | `pkg/knowledgepolicy/resolver.go` | DONE | Multi-label subset resolution, Order tie-breaking |
| Scorer | `pkg/knowledgepolicy/scorer.go` | DONE | All 4 decay functions, scoring formula, ScoreFrom modes |
| SetBindingTable | `pkg/storage/schema_knowledgepolicy.go` | DONE | One-liner under write lock |
| Builder tests | `pkg/knowledgepolicy/binding_builder_test.go` | DONE | 19 tests |
| Conflict tests | `pkg/knowledgepolicy/binding_builder_conflict_test.go` | DONE | 5 tests |
| Resolver tests | `pkg/knowledgepolicy/resolver_test.go` | DONE | 14 tests |
| Scorer tests | `pkg/knowledgepolicy/scorer_test.go` | DONE | 24 tests |
| Scorer benchmarks | `pkg/knowledgepolicy/scorer_bench_test.go` | DONE | 4 benchmarks, zero-alloc disabled path |

**Acceptance gate:** All tests pass. Benchmarks: disabled=13ns/0alloc, exponential=98ns/2alloc, multi-label=154ns/4alloc.

**Design note:** WHEN clause predicate evaluation deferred to Phase 4 — scorer implements the full formula but always returns no-match for WHEN predicates in Phase 2.

### Phase 3: AccessMeta Index — COMPLETE

| Deliverable | File | Status | Notes |
|---|---|---|---|
| Badger AccessMeta CRUD | `pkg/storage/badger_access_meta.go` | DONE | Get/Put/Delete/Scan with 0x11 prefix, msgpack encoding |
| Badger AccessMeta tests | `pkg/storage/badger_access_meta_test.go` | DONE | 7 tests: CRUD, overwrite, scan, Kalman round-trip, variance tracker |
| P-local sharded accumulator | `pkg/knowledgepolicy/access_accumulator.go` | DONE | sync.Pool P-affinity, cache-line padded shards, zero-alloc hot path |
| Accumulator tests | `pkg/knowledgepolicy/access_accumulator_test.go` | DONE | 12 tests: disabled, single, concurrent (64×1000), custom, read-through, drain, clear, timestamps |
| Super-node tests | `pkg/knowledgepolicy/access_accumulator_supernode_test.go` | DONE | 3 tests: 128-goroutine contention, drain-under-contention, mixed entities |
| Accumulator benchmarks | `pkg/knowledgepolicy/access_accumulator_bench_test.go` | DONE | 5 benchmarks: 42ns/0alloc single, 87ns/0alloc parallel, 1ns disabled |
| Access flusher | `pkg/knowledgepolicy/access_flusher.go` | DONE | AccessMetaStore interface, Start/Stop/Flush, drain+merge+persist |
| Flusher tests | `pkg/knowledgepolicy/access_flusher_test.go` | DONE | 9 tests: basic, accumulate, merge, empty, disabled, start/stop, final flush, custom overflow, concurrent |
| Kalman accumulator | `pkg/knowledgepolicy/kalman_accumulator.go` | DONE | Inlined filter, velocity projection, auto-R variance tracker, zero-alloc |
| Kalman tests | `pkg/knowledgepolicy/kalman_accumulator_test.go` | DONE | 9 tests: init, convergence, spike dampening, auto-R, window wrap, observations |
| Kalman benchmarks | `pkg/knowledgepolicy/kalman_accumulator_bench_test.go` | DONE | 3 benchmarks: ManualR=16ns/0alloc, AutoR=16ns/0alloc, vs plain comparison |
| Anti-sycophancy tests | `pkg/knowledgepolicy/kalman_anti_sycophancy_test.go` | DONE | 4 tests: hallucinated spike, diagnostics, sustained sycophancy, genuine trend |

**Acceptance gate:** 97 tests pass across `pkg/knowledgepolicy/...`. All storage tests pass. Benchmarks: accumulator 42ns/0alloc, Kalman 16ns/0alloc, disabled 1ns.

**Deferred to Phase 4:** Query context variable projection (`X-Query-*` headers → `$_<key>`) and deletion cascade from node/edge deletion (requires runtime integration). AccessMeta retained on visibility suppression (requires suppression implementation in Phase 6).

### Phase 4: COMPLETE

| Deliverable | Status |
|---|---|
| `pkg/knowledgepolicy/scoring_filter.go` — ShouldSuppressNode/Edge, SynthesizeLegacyAccessMeta | DONE |
| `pkg/knowledgepolicy/scoring_filter_test.go` — 8 unit tests | DONE |
| `pkg/knowledgepolicy/legacy_fallback_test.go` — 5 legacy fallback tests | DONE |
| `pkg/knowledgepolicy/integration_test.go` — end-to-end DDL→score→visibility | DONE |
| `pkg/knowledgepolicy/access_flusher_property_suppression_test.go` — 5 flusher suppression tests | DONE |
| `pkg/storage/badger_decay_filter.go` — filterNodeByDecay, filterEdgeByDecay, FilterPropertyByDecay | DONE |
| `pkg/storage/badger_queries.go` — decay filter in GetNodesByLabel, AllNodes | DONE |
| `pkg/storage/badger_mvcc.go` — decay filter in MVCC read paths (6 methods) | DONE |
| `pkg/storage/badger_mvcc_decay_test.go` — 8 visibility/reveal/binding tests | DONE |
| `pkg/storage/schema_knowledgepolicy.go` — rebuildBindingTableLocked on all DDL | DONE |
| `pkg/storage/schema_persistence.go` — rebuildBindingTableLocked on schema load | DONE |
| `pkg/cypher/reveal.go` — hasRevealCall, setRevealOnEngine, unwrapBadgerEngine | DONE |
| `pkg/cypher/reveal_test.go` — 5 reveal unit tests | DONE |
| `pkg/cypher/fn/builtins_reveal.go` — reveal() identity function registration | DONE |
| `pkg/cypher/transaction.go` — reveal detection in executeQueryAgainstStorage | DONE |
| `pkg/cypher/node_helpers.go` — property-level decay filter in nodeToMap | DONE |
| `pkg/search/decay_filter.go` — NodeDecayFilterFunc, SetNodeDecayFilter, filterDecayedCandidates | DONE |
| `pkg/search/search.go` — decay filter in rrfHybridSearch before fusion | DONE |
| `pkg/search/decay_filter_test.go` — 4 search decay filter tests | DONE |
| `pkg/nornicdb/db.go` — accessAccumulator, accessFlusher, lifecycle wiring | DONE |

**Deferred to Phase 5:** `decayScore()`, `decay()`, `policy()` as native Cypher functions. Embed worker accessMeta projection (requires Phase 5 embed text builder changes). Indexed-property immunity check in property-level filter (trivial addition when schema awareness is wired to Cypher executor).

### Phase 5: COMPLETE

| Deliverable | File | Status | Notes |
|---|---|---|---|
| Export ScorerForNamespace | `pkg/storage/badger_decay_filter.go` | DONE | + IsDecayEnabled, ExtractNamespaceFromID |
| Dispatcher + functions | `pkg/cypher/knowledgepolicy_functions.go` | DONE | decayScore, decay, policy — inline dispatch |
| Wire into evaluator | `pkg/cypher/functions_eval_functions.go` | DONE | 1 line before math fallthrough |
| decayMismatchLogged field | `pkg/cypher/executor.go` | DONE | Reset in transaction.go |
| Options validation | `pkg/cypher/knowledgepolicy_functions.go` | DONE | validateDecayOptions, property key only |
| Unit tests | `pkg/cypher/knowledgepolicy_functions_test.go` | DONE | 10 tests |
| Disabled-mode tests | `pkg/cypher/knowledgepolicy_functions_disabled_test.go` | DONE | 4 tests |
| Reveal integration tests | `pkg/cypher/knowledgepolicy_reveal_integration_test.go` | DONE | 3 tests |

### Phase 6: COMPLETE

| Deliverable | File | Status | Notes |
|---|---|---|---|
| VisibilitySuppressed field | `pkg/storage/types.go` | DONE | Added to Node and Edge structs |
| Suppressed-bit fast path | `pkg/storage/badger_decay_filter.go` | DONE | Check before scorer in filterNodeByDecay/filterEdgeByDecay |
| IndexEntryCatalog CRUD | `pkg/storage/badger_index_catalog.go` | DONE | prefix 0x12, in-txn helpers |
| Catalog wiring on writes | `pkg/storage/badger_nodes.go`, `badger_edges.go` | DONE | CreateNode, UpdateNode, deleteNodeInTxn, CreateEdge, UpdateEdge, deleteEdgeInTxn |
| DeindexWorkItem CRUD | `pkg/storage/badger_deindex_work.go` | DONE | prefix 0x13, ScanPendingDeindexWorkItems |
| Tombstone helpers | `pkg/storage/badger_index_tombstone.go` | DONE | prefix 0x17, write/probe/delete |
| Tombstone read-path wiring | `pkg/storage/badger_queries.go` | DONE | GetNodesByLabel, GetFirstNodeByLabel, ForEachNodeIDByLabel, GetEdgesByType |
| DeindexCleanupJob | `pkg/storage/badger_deindex_cleanup.go` | DONE | AccessFlusher-pattern lifecycle |
| EnqueueDeindexIfSuppressed | `pkg/storage/badger_deindex_enqueue.go` | DONE | Mark suppressed, enqueue work, clear on recovery |
| Visibility suppressed tests | `pkg/storage/badger_visibility_suppressed_test.go` | DONE | 4 tests |
| Index catalog tests | `pkg/storage/badger_index_catalog_test.go` | DONE | 8 tests |
| Deindex work tests | `pkg/storage/badger_deindex_work_test.go` | DONE | 4 tests |
| Tombstone tests | `pkg/storage/badger_index_tombstone_test.go` | DONE | 6 tests |
| Cleanup tests | `pkg/storage/badger_deindex_cleanup_test.go` | DONE | 4 tests |

### Phase 7–8: Not started

---

## Subsystem Boundary: Compliance Retention vs Knowledge-Layer Visibility

The existing **compliance retention subsystem** (`pkg/retention`, `/admin/retention/`) and the new **knowledge-layer scoring subsystem** (`pkg/knowledgepolicy`, `/admin/knowledge-policies/`) are independent systems that operate at different layers.

### Compliance retention subsystem (existing — unchanged)

The checked-in `pkg/retention` package remains the authoritative system for:

- **Deletion** — permanent removal of data from storage
- **Legal holds** — preventing deletion during litigation
- **Erasure requests** — GDPR Art.17 "right to be forgotten"
- **Retention-policy-driven archival** — archive-before-delete behavior
- **Retention sweeps** — periodic enforcement of retention policies

This subsystem owns:
- `pkg/retention/retention.go` and all types therein
- `pkg/nornicdb/db_retention.go`
- `pkg/server/server_retention.go` and the `/admin/retention/` API namespace
- All compliance-related configuration in `config.Compliance` and `config.Retention`

### Knowledge-layer scoring subsystem (new — this plan)

The knowledge-layer subsystem is a **scoring and retrieval-visibility layer**. It does not replace the compliance retention subsystem. It sits on top of it.

This subsystem:
- Computes decay and promotion scores for nodes, edges, and properties
- **Suppresses** entities from retrieval and query results when their score falls below the visibility threshold
- Removes suppressed entities from retrieval indexes (deindexing) while leaving primary storage intact
- Does **not** delete data — suppressed entities remain persisted in Badger
- Does **not** replace compliance retention policies, legal holds, or erasure requests

If the underlying compliance retention system later deletes or archives an entity, that lower-layer lifecycle wins. The knowledge-layer visibility layer cannot override a hard deletion or compliance-driven archival.

`reveal()` bypasses only the knowledge-layer visibility gate. It does not resurrect deleted entities. It does not bypass compliance retention archival or deletion.

### Precedence ordering

When a query touches an entity, the layers apply in this order:

1. **MVCC** resolves which version of the entity exists at the transaction snapshot.
2. **Compliance retention** state still governs actual deletion / archival lifecycle. If the entity has been deleted or compliance-archived, it is gone — the knowledge-layer has no effect.
3. **Knowledge-layer scorer** applies retrieval-visibility rules on top. Entities whose score falls below the visibility threshold are suppressed from query results.
4. **`reveal()`** bypasses only step 3 — the knowledge-layer visibility gate.

### Terminology convention

Throughout this document:
- **"archive" / "archived" / "archival"** refers exclusively to the existing compliance retention subsystem's lifecycle behavior.
- **"suppress" / "suppressed" / "visibility-suppressed"** refers to the knowledge-layer scoring system hiding an entity from retrieval while it remains persisted.
- **"deindex"** refers to the knowledge-layer system removing a suppressed entity from secondary indexes while its primary storage record remains intact.

---

## Current State Audit

### What exists today (to be replaced)

| Component                | Location                                   | Description                                                                      |
| ------------------------ | ------------------------------------------ | -------------------------------------------------------------------------------- |
| `decay.Tier` enum        | `pkg/decay/decay.go:77-128`                | Hardcoded `TierEpisodic`, `TierSemantic`, `TierProcedural` with fixed half-lives |
| `decay.Manager`          | `pkg/decay/decay.go`                       | Tier-based scoring, reinforcement, suppression logic                             |
| `Node.DecayScore`        | `pkg/storage/types.go:197`                 | Stored float64 on the Node struct                                                |
| `Node.LastAccessed`      | `pkg/storage/types.go:198`                 | Stored time on the Node struct                                                   |
| `Node.AccessCount`       | `pkg/storage/types.go:199`                 | Stored int64 on the Node struct                                                  |
| `inference.EdgeDecay`    | `pkg/inference/edge_decay.go`              | Separate edge decay with confidence-based model                                  |
| Replication codec fields | `pkg/replication/codec.go`                 | Sends `DecayScore` in replication                                                |
| CLI decay commands       | `cmd/` (decay stats, recalculate, archive) | Tier-aware CLI                                                                   |

### What exists today (to be preserved or extended)

| Component                 | Location                                     | Description                                                                                         |
| ------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Cypher Kalman functions   | `pkg/cypher/kalman_functions.go`             | Kalman-filter scoring — internal Cypher functions, unrelated to decay policy system, retained as-is |
| `SchemaManager`           | `pkg/storage/schema.go:72-91`                | Constraint/index catalog — new subsystem maps go here                                               |
| `SchemaDefinition`        | `pkg/storage/schema_persistence.go:14-27`    | Persisted schema — new definition sections added here                                               |
| Badger prefix keys        | `pkg/storage/badger.go:21-36`                | `0x01`–`0x10` allocated — new prefixes start at `0x11`                                              |
| msgpack serialization     | `pkg/storage/badger_serialization.go`        | Already supports msgpack — accessMeta uses this                                                     |
| MVCC version system       | `pkg/storage/badger_mvcc.go`                 | Version resolution, snapshot reads — scorer receives snapshot timestamp                             |
| Feature flags             | `pkg/config/feature_flags.go`                | Global enable/disable — decay and promotion get flags here                                          |
| `constraint_contracts.go` | `pkg/storage/constraint_contracts.go`        | Pattern for keyword scanning DDL — decay/promotion DDL follows this                                 |
| Canonical graph ledger    | `docs/user-guides/canonical-graph-ledger.md` | Knowledge-layer persistence for facts — referenced by design                                        |

---

## New Badger Key Prefixes

```go
// pkg/storage/badger.go — append to existing prefix block
prefixAccessMeta        = byte(0x11) // accessmeta:entityID -> msgpack(AccessMetaEntry)
prefixIndexEntryCatalog = byte(0x12) // idxcat:entityID -> msgpack(IndexEntryCatalog)
prefixDeindexWorkItem   = byte(0x13) // deindexwork:workItemID -> msgpack(DeindexWorkItem)
prefixDecayProfile      = byte(0x14) // decayprofile:name -> msgpack(DecayProfileDef)
prefixPromotionProfile  = byte(0x15) // promoprofile:name -> msgpack(PromotionProfileDef)
prefixPromotionPolicy   = byte(0x16) // promopolicy:name -> msgpack(PromotionPolicyDef)
prefixIndexTombstone    = byte(0x17) // idxtomb:<original-index-key> -> []byte{} (presence marker)
```

---

## Phase 1: Schema Objects and Profile Model

**Goal:** Define all types, persistence, and DDL for decay profiles, promotion profiles, and promotion policies. No runtime scoring yet — just the authoring surface.

**Depends on:** Nothing. This is the foundation.

### 1.1 New package: `pkg/knowledgepolicy`

Create `pkg/knowledgepolicy/` as the home for all new decay/promotion types and the shared resolver. This package is separate from both `pkg/decay/` (legacy tier code) and `pkg/retention/` (compliance retention — GDPR, legal holds, erasure). The knowledge-layer scoring subsystem has no import dependency on the compliance retention subsystem.

#### Files and types

**`pkg/knowledgepolicy/types.go`** — Core schema objects:

```go
// DecayFunction identifies a scoring function.
type DecayFunction string

const (
    DecayFunctionExponential DecayFunction = "exponential"
    DecayFunctionLinear      DecayFunction = "linear"
    DecayFunctionStep        DecayFunction = "step"
    DecayFunctionNone        DecayFunction = "none"
)

// ScoreFromMode identifies the score start-time anchor.
type ScoreFromMode string

const (
    ScoreFromCreated ScoreFromMode = "CREATED"
    ScoreFromVersion ScoreFromMode = "VERSION"
    ScoreFromCustom  ScoreFromMode = "CUSTOM"
)

// ScopeType identifies whether a profile/policy targets nodes, edges, or properties.
type ScopeType string

const (
    ScopeNode     ScopeType = "NODE"
    ScopeEdge     ScopeType = "EDGE"
    ScopeProperty ScopeType = "PROPERTY"
)

// DecayProfileBundle is a reusable parameter bundle (no FOR clause).
type DecayProfileBundle struct {
    Name              string        `msgpack:"name"`
    HalfLifeSeconds   int64         `msgpack:"halfLifeSeconds"`
    VisibilityThreshold float64     `msgpack:"visibilityThreshold"`
    ScoreFloor        float64       `msgpack:"scoreFloor"`
    Function          DecayFunction `msgpack:"function"`
    Scope             ScopeType     `msgpack:"scope"`
    DecayEnabled      bool          `msgpack:"decayEnabled"`
    ScoreFrom         ScoreFromMode `msgpack:"scoreFrom"`
    ScoreFromProperty string        `msgpack:"scoreFromProperty,omitempty"` // for CUSTOM
    Enabled           bool          `msgpack:"enabled"`
}

// DecayProfilePropertyRule is an inline property-level rule inside a binding.
type DecayProfilePropertyRule struct {
    PropertyPath     string         `msgpack:"propertyPath"`
    NoDecay          bool           `msgpack:"noDecay,omitempty"`
    ProfileRef       string         `msgpack:"profileRef,omitempty"`    // reference to a bundle name
    HalfLifeSeconds  int64          `msgpack:"halfLifeSeconds,omitempty"`
    ScoreFloor       float64        `msgpack:"scoreFloor,omitempty"`
    Order            int            `msgpack:"order"` // deterministic precedence
}

// DecayProfileBinding is a targeted binding (has FOR clause).
type DecayProfileBinding struct {
    Name              string                     `msgpack:"name"`
    TargetLabels      []string                   `msgpack:"targetLabels,omitempty"` // sorted; empty = wildcard
    TargetEdgeType    string                     `msgpack:"targetEdgeType,omitempty"`
    IsWildcard        bool                       `msgpack:"isWildcard"`
    IsEdge            bool                       `msgpack:"isEdge"`
    ProfileRef        string                     `msgpack:"profileRef,omitempty"` // DECAY PROFILE 'name'
    NoDecay           bool                       `msgpack:"noDecay,omitempty"`
    VisibilityThreshold *float64                  `msgpack:"visibilityThreshold,omitempty"` // override
    PropertyRules     []DecayProfilePropertyRule `msgpack:"propertyRules,omitempty"`
}

// PromotionProfileDef is a reusable promotion parameter bundle.
type PromotionProfileDef struct {
    Name       string    `msgpack:"name"`
    Scope      ScopeType `msgpack:"scope"`
    Multiplier float64   `msgpack:"multiplier"`
    ScoreFloor float64   `msgpack:"scoreFloor"`
    ScoreCap   float64   `msgpack:"scoreCap"`
    Enabled    bool      `msgpack:"enabled"`
}

// PromotionPolicyWhenClause is a WHEN predicate inside a policy.
type PromotionPolicyWhenClause struct {
    PropertyPath   string `msgpack:"propertyPath,omitempty"` // empty = entity-level
    Predicate      string `msgpack:"predicate"`              // raw expression text
    ProfileRef     string `msgpack:"profileRef"`             // APPLY PROFILE 'name'
    Order          int    `msgpack:"order"`
}

// KalmanMode identifies how the Kalman filter is configured for a mutation.
type KalmanMode string

const (
    KalmanModeNone   KalmanMode = ""       // No Kalman filtering (plain write)
    KalmanModeAuto   KalmanMode = "auto"   // R auto-adjusted via VarianceTracker
    KalmanModeManual KalmanMode = "manual" // Fixed Q and R
)

// KalmanConfig holds the Kalman filter configuration for an ON ACCESS mutation.
// See design plan §7.4 "WITH KALMAN Behavioral Signal Smoother" for constraints.
// WITH KALMAN is appropriate for derived behavioral metrics (confidence scores,
// cross-session access rates, agreement ratios). Raw monotonic counters and
// timestamps should use plain SET without Kalman.
type KalmanConfig struct {
    Mode          KalmanMode `msgpack:"mode"`
    Q             float64    `msgpack:"q"`                       // Process noise (scaled by 0.001 like imu-f)
    R             float64    `msgpack:"r,omitempty"`              // Measurement noise (0 = auto)
    VarianceScale float64    `msgpack:"varianceScale,omitempty"` // For auto R (default: 10.0)
    WindowSize    int        `msgpack:"windowSize,omitempty"`    // Variance tracker window (default: 32)
}

// OnAccessMutation is a single SET expression inside an ON ACCESS block.
type OnAccessMutation struct {
    Expression string        `msgpack:"expression"` // Raw SET expression text
    Kalman     *KalmanConfig `msgpack:"kalman,omitempty"` // nil = plain write (no filtering)
}

// PromotionPolicyOnAccess is the ON ACCESS block definition.
type PromotionPolicyOnAccess struct {
    Mutations []OnAccessMutation `msgpack:"mutations"`
}

// PromotionPolicyDef is a targeted promotion policy.
type PromotionPolicyDef struct {
    Name           string                      `msgpack:"name"`
    TargetLabels   []string                    `msgpack:"targetLabels,omitempty"`
    TargetEdgeType string                      `msgpack:"targetEdgeType,omitempty"`
    IsWildcard     bool                        `msgpack:"isWildcard"`
    IsEdge         bool                        `msgpack:"isEdge"`
    OnAccess       *PromotionPolicyOnAccess    `msgpack:"onAccess,omitempty"`
    WhenClauses    []PromotionPolicyWhenClause `msgpack:"whenClauses,omitempty"`
    Enabled        bool                        `msgpack:"enabled"`
}
```

**`pkg/knowledgepolicy/access_meta.go`** — AccessMeta types:

```go
// AccessMetaFixedFields is the fast-path fixed-layout struct.
type AccessMetaFixedFields struct {
    AccessCount    int64 `msgpack:"accessCount"`
    LastAccessedAt int64 `msgpack:"lastAccessedAt"` // UnixNano
    TraversalCount int64 `msgpack:"traversalCount"`
    LastTraversedAt int64 `msgpack:"lastTraversedAt"` // UnixNano
}

// AccessMetaEntry is the full persisted entry per target.
type AccessMetaEntry struct {
    TargetID      string                            `msgpack:"targetId"`
    TargetScope   ScopeType                         `msgpack:"targetScope"` // NODE or EDGE
    Fixed         AccessMetaFixedFields             `msgpack:"fixed"`
    Overflow      map[string]interface{}            `msgpack:"overflow,omitempty"`
    KalmanFilters map[string]*KalmanPropertyState   `msgpack:"kalmanFilters,omitempty"` // key: property name
    LastMutatedAt int64                             `msgpack:"lastMutatedAt"` // UnixNano
    MutationCount int64                             `msgpack:"mutationCount"`
}

// KalmanPropertyState holds the Kalman filter state and variance tracker state
// for a single property that uses WITH KALMAN. Stored as a typed field on
// AccessMetaEntry, not in the overflow map.
type KalmanPropertyState struct {
    // FilteredValue is the Kalman filter's optimal estimate (x).
    // This is what WHEN predicates and policy() see for this property.
    FilteredValue float64          `msgpack:"filteredValue"`
    // Filter is the Kalman filter state (same fields as pkg/cypher/kalman_functions.go KalmanState).
    Filter        KalmanFilterState `msgpack:"filter"`
    // Variance is the variance tracker state (auto-R mode only). Nil for manual-R.
    Variance      *VarianceTrackerState `msgpack:"variance,omitempty"`
}

// KalmanFilterState is the per-property Kalman filter state.
// Reuses the same field layout as pkg/cypher/kalman_functions.go KalmanState.
type KalmanFilterState struct {
    X             float64 `msgpack:"x"`    // Current state estimate
    LastX         float64 `msgpack:"lx"`   // Previous state
    P             float64 `msgpack:"p"`    // Estimate covariance
    K             float64 `msgpack:"k"`    // Kalman gain
    E             float64 `msgpack:"e"`    // Setpoint error factor
    Q             float64 `msgpack:"q"`    // Process noise (scaled)
    R             float64 `msgpack:"r"`    // Measurement noise
    VarianceScale float64 `msgpack:"vs"`   // Adaptive noise scaling
    Observations  int     `msgpack:"n"`    // Observations processed
}

// VarianceTrackerState is the serializable state for auto-R calculation.
// Based on pkg/filter/kalman.go VarianceTracker (port of imu-f update_kalman_covariance).
type VarianceTrackerState struct {
    Window    []float64 `msgpack:"w"`
    WindowIdx int       `msgpack:"wi"`
    SumMean   float64   `msgpack:"sm"`
    SumVar    float64   `msgpack:"sv"`
    Mean      float64   `msgpack:"m"`
    Variance  float64   `msgpack:"v"`
    InverseN  float64   `msgpack:"in"`
}
```

**Kalman state on AccessMetaEntry — dedicated struct field:**

Per-property Kalman filter state is stored as a typed `map[string]*KalmanPropertyState` field on `AccessMetaEntry`, not in the `Overflow` map. The map key is the property name being filtered (e.g., `"confidenceScore"`, `"crossSessionAccessRate"`). This keeps Kalman state type-safe and co-located with the entity's access metadata without polluting the general-purpose overflow map with stringly-typed reserved keys.

No new Badger prefix is needed — `KalmanFilters` is serialized as part of the `AccessMetaEntry` under the existing `prefixAccessMeta (0x11)`.

**`pkg/knowledgepolicy/compiled_binding.go`** — Compiled binding table:

```go
// CompiledBinding is the pre-flattened lookup entry for a label/edge-type.
type CompiledBinding struct {
    DecayProfile       *DecayProfileBundle
    DecayBinding       *DecayProfileBinding
    PromotionPolicy    *PromotionPolicyDef
    VisibilityThreshold float64
    ScoreFrom           ScoreFromMode
    ScoreFromProperty   string
    Function            DecayFunction
    HalfLifeNanos       int64
    ThresholdAgeNanos   int64   // pre-computed: -halfLife * ln(visibilityThreshold) / ln(2)
    DecayFloor         float64
    NoDecay            bool
    HasNoDecayProperty bool    // true if any property rule has NoDecay — blocks entity suppression
    CompiledPropertyRules map[string]*CompiledPropertyOverride
}

// BindingTable is the compiled lookup for all labels and edge types.
type BindingTable struct {
    mu       sync.RWMutex
    nodes    map[string]*CompiledBinding // key: sorted label set (e.g., "Label1\x00Label2")
    edges    map[string]*CompiledBinding // key: edge type
    wildNode *CompiledBinding
    wildEdge *CompiledBinding
}
```

### 1.2 Schema persistence extensions

**`pkg/storage/schema_persistence.go`** — Add to `SchemaDefinition`:

```go
// Add these fields to the SchemaDefinition struct:
DecayProfileBundles  []knowledgepolicy.DecayProfileBundle  `json:"decay_profile_bundles,omitempty"`
DecayProfileBindings []knowledgepolicy.DecayProfileBinding  `json:"decay_profile_bindings,omitempty"`
PromotionProfiles    []knowledgepolicy.PromotionProfileDef  `json:"promotion_profiles,omitempty"`
PromotionPolicies    []knowledgepolicy.PromotionPolicyDef   `json:"promotion_policies,omitempty"`
```

**`pkg/storage/schema.go`** — Add to `SchemaManager`:

```go
// Add these fields to the SchemaManager struct:
decayProfileBundles  map[string]*knowledgepolicy.DecayProfileBundle  // key: profile name
decayProfileBindings map[string]*knowledgepolicy.DecayProfileBinding // key: binding name
promotionProfiles    map[string]*knowledgepolicy.PromotionProfileDef // key: profile name
promotionPolicies    map[string]*knowledgepolicy.PromotionPolicyDef  // key: policy name
bindingTable         *knowledgepolicy.BindingTable                   // compiled, rebuilt on DDL change
```

Add methods: `CreateDecayProfileBundle()`, `CreateDecayProfileBinding()`, `DropDecayProfile()`, `AlterDecayProfile()`, `ShowDecayProfiles()`, `CreatePromotionProfile()`, `CreatePromotionPolicy()`, `DropPromotionProfile()`, `DropPromotionPolicy()`, `AlterPromotionProfile()`, `AlterPromotionPolicy()`, `ShowPromotionProfiles()`, `ShowPromotionPolicies()`.

### 1.3 DDL parsing

**`pkg/cypher/knowledgepolicy_ddl.go`** — Keyword-scanning parser for decay/promotion DDL:

Parse the following statements using the `ccSkipSpaces`/`ccScanIdent`/`ccMatchKeywordAt` pattern from `pkg/storage/constraint_contracts.go`:

- `CREATE DECAY PROFILE <name> OPTIONS { ... }`
- `CREATE DECAY PROFILE <name> FOR (n:<Label>) APPLY { ... }`
- `CREATE DECAY PROFILE <name> FOR ()-[r:<Type>]-() APPLY { ... }`
- `ALTER DECAY PROFILE <name> SET OPTIONS { ... }`
- `DROP DECAY PROFILE <name>`
- `SHOW DECAY PROFILES`
- `CREATE PROMOTION PROFILE <name> OPTIONS { ... }`
- `ALTER PROMOTION PROFILE <name> SET OPTIONS { ... }`
- `DROP PROMOTION PROFILE <name>`
- `SHOW PROMOTION PROFILES`
- `CREATE PROMOTION POLICY <name> FOR ... APPLY { ... }`
- `ALTER PROMOTION POLICY <name> ...`
- `DROP PROMOTION POLICY <name>`
- `SHOW PROMOTION POLICIES`

Each parser function returns a typed command struct (e.g., `CreateDecayProfileCmd`). The executor maps these to `SchemaManager` calls.

#### ON ACCESS block parsing with WITH KALMAN

When parsing an `ON ACCESS { ... }` block inside a `CREATE PROMOTION POLICY` statement, the parser must recognize the `WITH KALMAN` modifier on individual `SET` expressions:

```
SET <expr>                           → OnAccessMutation{Expression: "<expr>", Kalman: nil}
WITH KALMAN SET <expr>               → OnAccessMutation{Expression: "<expr>", Kalman: &KalmanConfig{Mode: Auto, Q: 0.1, R: 88.0, VarianceScale: 10.0, WindowSize: 32}}
WITH KALMAN{q: 0.1, r: 88.0} SET <expr> → OnAccessMutation{Expression: "<expr>", Kalman: &KalmanConfig{Mode: Manual, Q: 0.1, R: 88.0}}
WITH KALMAN{q: 0.05} SET <expr>      → OnAccessMutation{Expression: "<expr>", Kalman: &KalmanConfig{Mode: Auto, Q: 0.05, R: 88.0, VarianceScale: 10.0, WindowSize: 32}}
```

The `{...}` config block is parsed as a comma-separated list of `key: value` pairs. Accepted keys: `q`, `r`, `varianceScale`, `windowSize`. Unknown keys are rejected at parse time. If `r` is present, mode is `Manual`. If `r` is absent, mode is `Auto` (R auto-adjusted by variance tracker).

#### Query context variable passthrough

The engine projects all `X-Query-*` HTTP headers (and Bolt metadata keys with prefix `query_`) into the query evaluation context as `$_<key>` variables. The header name is lowercased and the `X-Query-` prefix is stripped:

- `X-Query-Session` / `query_session` → `$_session`
- `X-Query-Agent` / `query_agent` → `$_agent`
- `X-Query-Tenant` / `query_tenant` → `$_tenant`
- Any `X-Query-<Key>` → `$_<key>`

These variables are available inside `ON ACCESS` blocks, `WHEN` predicates, and general Cypher queries. They are read-only and ephemeral — they exist for the duration of the query evaluation, not as stored graph state. If a header is not provided, the corresponding variable resolves to `""`.

The passthrough is generic — the engine does not decide which context matters. Operators define their own gating logic using whatever context they send. `$_session` and `$_agent` are conventions, not special cases. This enables expressions like `CASE WHEN n._lastSessionId <> $_session THEN ... END` for same-session deduplication before measurements reach the Kalman filter.

**Implementation:** Extract `X-Query-*` headers in the HTTP handler (`pkg/server/`) and Bolt metadata in the Bolt handler. Pass as `map[string]string` on the query context (`QueryContext.QueryVars`). The Cypher evaluator resolves `$_<key>` references from this map during expression evaluation.

### 1.4 Validation rules (creation-time)

Implement in `SchemaManager` methods:

1. Reject duplicate target bindings (at most one decay profile and one promotion policy per unique target).
2. Reject property-level rules targeting properties in structural indexes (lookup, range, composite). Query the existing index maps to check.
3. Reject `DROP PROMOTION PROFILE` when the profile is referenced by an active promotion policy.
4. Reject `DROP DECAY PROFILE` (bundle) when referenced by an active binding.
5. Reject multi-label conflicts (see design §6.1 multi-label node resolution).
6. Validate `OPTIONS` key names and value types.

### 1.5 Feature flag gating

The entire knowledge-layer scoring subsystem is gated behind the **existing** `config.Memory.DecayEnabled` flag (`NORNICDB_MEMORY_DECAY_ENABLED` env var, default `false`). No new feature flags are introduced for the subsystem itself.

**Rationale:** The knowledge-layer scoring system is the evolution of the existing memory-decay subsystem — it replaces the same code paths, serves the same purpose, and targets the same operator audience. Introducing separate flags would create a confusing matrix where the legacy decay and the new scoring system could be independently toggled into conflicting states. A single flag keeps the contract clean: when `DecayEnabled` is `true`, the knowledge-layer scoring subsystem is active; when `false`, no decay scoring, visibility suppression, or promotion logic executes. The compliance retention subsystem (`pkg/retention`) is unaffected by this flag.

**Gating behavior:**

| Flag state                            | Legacy decay (pre-1.1.0)      | Knowledge-layer scoring (1.1.0+)                                                                                                                         |
| ------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DecayEnabled = true`, pre-migration  | Legacy `decay.Manager` active | Inactive (migration not run)                                                                                                                             |
| `DecayEnabled = true`, post-migration | Removed                       | `knowledgepolicy.Scorer` active, profiles/policies evaluated                                                                                             |
| `DecayEnabled = false`                | Inactive                      | Inactive — DDL succeeds (profiles/policies are persisted schema), but runtime scoring, visibility suppression, `AccessAccumulator`, and flush goroutine are all no-ops. Compliance retention subsystem unaffected. |

**Gate points in code:**

1. **`pkg/storage/badger_mvcc.go`** (Phase 4) — The visibility check skips profile resolution and returns the entity unchanged when `!config.Memory.DecayEnabled`.
2. **`pkg/knowledgepolicy/access_flusher.go`** (Phase 3) — `AccessFlusher.Start()` exits immediately when `!config.Memory.DecayEnabled`. `AccessAccumulator.IncrementAccess()` is a no-op (check flag before atomic increment).
3. **`pkg/knowledgepolicy/scorer.go`** (Phase 2) — `Scorer.ScoreNode()` and `Scorer.ScoreEdge()` return a neutral `ScoringResolution{FinalScore: 1.0, NoDecay: true}` when `!config.Memory.DecayEnabled`.
4. **`pkg/storage/badger_deindex_cleanup.go`** (Phase 6) — `DeindexCleanupJob.Start()` exits immediately when `!config.Memory.DecayEnabled`.
5. **`pkg/cypher/knowledgepolicy_functions.go`** (Phase 5) — `decayScore()` returns `1.0`, `decay()` returns `{score: 1.0, applies: false, reason: "decay subsystem disabled — enable with NORNICDB_MEMORY_DECAY_ENABLED=true"}`, `policy()` returns empty metadata, `reveal()` returns entity unchanged. These do not error — they degrade gracefully. A config mismatch warning is logged once per query execution (§5.4).
6. **`pkg/cypher/knowledgepolicy_ddl.go`** (Phase 1) — DDL statements (`CREATE DECAY PROFILE`, etc.) always succeed regardless of flag state. Profiles and policies are schema objects and must persist across flag toggles. The flag only gates runtime evaluation.

**`pkg/config/config.go`** — Change the default and env var parsing:

The existing default is `config.Memory.DecayEnabled = true` at line 1515. Change to `false`:

```go
// DefaultConfig() — change default:
config.Memory.DecayEnabled = false
```

The existing env var parsing (line 1984) only handles the disable case. Update to handle explicit enable:

```go
// Before:
if getEnv("NORNICDB_MEMORY_DECAY_ENABLED", "") == "false" {
    config.Memory.DecayEnabled = false
}

// After:
if v := getEnv("NORNICDB_MEMORY_DECAY_ENABLED", ""); v == "true" || v == "1" {
    config.Memory.DecayEnabled = true
} else if v == "false" || v == "0" {
    config.Memory.DecayEnabled = false
}
```

The YAML key `memory.decay_enabled` continues to work unchanged — it already sets the bool directly.

**`pkg/config/feature_flags.go`** — No changes. The existing `NORNICDB_EDGE_DECAY_ENABLED` / `IsEdgeDecayEnabled()` flag continues to gate `inference.EdgeDecay` independently. During Phase 7 migration, `inference.EdgeDecay` is removed and `NORNICDB_EDGE_DECAY_ENABLED` becomes a no-op.

### Phase 1 acceptance gate

- All decay/promotion types compile and have msgpack round-trip tests.
- `KalmanConfig`, `OnAccessMutation`, and `VarianceTrackerState` compile and have msgpack/JSON round-trip tests.
- `OnAccessMutation` with `Kalman: nil` round-trips correctly (plain write, no filtering).
- `OnAccessMutation` with `Kalman: &KalmanConfig{Mode: Auto, ...}` round-trips correctly.
- DDL parsing produces correct command structs for all 14 statement types.
- DDL parsing of `WITH KALMAN SET ...` produces `OnAccessMutation` with auto-mode `KalmanConfig`.
- DDL parsing of `WITH KALMAN{q: 0.1, r: 88.0} SET ...` produces manual-mode `KalmanConfig`.
- DDL parsing of `WITH KALMAN{q: 0.05} SET ...` produces auto-mode `KalmanConfig` with user Q.
- DDL parsing of plain `SET ...` (no WITH KALMAN) produces `OnAccessMutation` with `Kalman: nil`.
- DDL parsing rejects unknown keys in `WITH KALMAN{...}` config block.
- Schema persistence round-trips all new definition sections including `OnAccessMutation` with Kalman configs.
- Validation rejects illegal targets, duplicates, and indexed-property rules.
- `config.Memory.DecayEnabled = false` (default) gates all runtime behavior; DDL persists regardless. Existing behavior unchanged when flag is off.
- `SHOW DECAY PROFILES`, `SHOW PROMOTION PROFILES`, `SHOW PROMOTION POLICIES` return stored definitions.

### Phase 1 test files

- `pkg/knowledgepolicy/types_test.go` — msgpack serialization round-trips for all types including `KalmanConfig`, `OnAccessMutation`, `VarianceTrackerState`; `OnAccessMutation` with and without `KalmanConfig`; `KalmanConfig` for all three modes (auto, manual, hybrid)
- `pkg/knowledgepolicy/compiled_binding_test.go` — binding table compilation
- `pkg/cypher/knowledgepolicy_ddl_test.go` — DDL parsing for all statement forms; `WITH KALMAN SET` → auto mode; `WITH KALMAN{q: 0.1, r: 88.0} SET` → manual mode; `WITH KALMAN{q: 0.05} SET` → hybrid; plain `SET` → Kalman nil; unknown keys in `{...}` rejected; query context variables (`$_session`, `$_agent`, `$_tenant`) recognized in ON ACCESS expressions
- `pkg/storage/schema_knowledgepolicy_test.go` — SchemaManager CRUD, validation, persistence

---

## Phase 2: Shared Resolver and Scorer

**Goal:** Implement the resolution cascade and scoring engine. After this phase, any code path can resolve the effective decay/promotion configuration for a node or edge and compute a score — but no read paths are wired yet.

**Depends on:** Phase 1.

### 2.1 Resolver

**`pkg/knowledgepolicy/resolver.go`**:

```go
// Resolver resolves effective decay and promotion configuration for a target.
type Resolver struct {
    bindingTable *BindingTable
}

// ResolveNode returns the ScoringResolution for a node identified by its sorted labels.
func (r *Resolver) ResolveNode(labels []string) *ScoringResolution

// ResolveEdge returns the ScoringResolution for an edge identified by its type.
func (r *Resolver) ResolveEdge(edgeType string) *ScoringResolution

// ResolveProperty returns the resolution for a specific property on a node or edge.
func (r *Resolver) ResolveProperty(entityLabelsOrType interface{}, propertyPath string) *ScoringResolution
```

**`pkg/knowledgepolicy/resolution.go`** — `ScoringResolution` struct (design §5.2):

Fields: `TargetID`, `TargetScope`, `ResolvedDecayProfileID`, `ResolvedScoreFrom`, `ResolutionSourceChain`, `AppliedDecayProfileNames`, `AppliedPromotionPolicyName`, `AppliedPromotionProfileName`, `EffectiveRate`, `EffectiveThreshold`, `EffectiveMultiplier`, `BaseScore`, `FinalScore`, `NoDecay`, `SuppressionEligible`, `Explanation`.

### 2.2 Scorer

**`pkg/knowledgepolicy/scorer.go`**:

```go
// Scorer computes decay and promotion scores.
type Scorer struct {
    resolver *Resolver
}

// ScoreNode computes the final score for a node at scoringTime.
// scoringTime is the MVCC snapshot timestamp (not time.Now()).
func (s *Scorer) ScoreNode(
    labels []string,
    createdAt int64,       // UnixNano
    versionAt int64,       // UnixNano — latest visible version timestamp
    scoringTime int64,     // UnixNano — transaction snapshot time
    accessMeta *AccessMetaEntry, // may be nil
) *ScoringResolution

// ScoreEdge computes the final score for an edge at scoringTime.
func (s *Scorer) ScoreEdge(
    edgeType string,
    createdAt int64,
    versionAt int64,
    scoringTime int64,
    accessMeta *AccessMetaEntry,
) *ScoringResolution

// ScoreProperty computes the score for a specific property.
func (s *Scorer) ScoreProperty(
    entityLabelsOrType interface{},
    propertyPath string,
    createdAt int64,
    versionAt int64,
    scoringTime int64,
    accessMeta *AccessMetaEntry,
) *ScoringResolution
```

Scoring formula (design §6.6):

```go
func computeFinalScore(baseDecayScore, multiplier, promoFloor, promoCap, decayFloor float64) float64 {
    promoted := baseDecayScore * multiplier
    floored := math.Max(promoted, promoFloor)
    capped := math.Min(floored, promoCap)
    return math.Max(capped, decayFloor)
}
```

Suppression eligibility:

```go
SuppressionEligible: finalScore < cb.VisibilityThreshold && !cb.HasNoDecayProperty
```

`HasNoDecayProperty` is pre-computed at binding compilation time by scanning `CompiledPropertyRules` for any entry with `NoDecay: true`. A `NO DECAY` property acts as a suppression anchor: the parent entity's content still decays, but the entity remains visible because the anchored property has a permanent score of 1.0. This prevents structural metadata (tenant IDs, session IDs, bi-temporal timestamps) from being hidden by entity-level decay. The `HasNoDecayProperty` flag is cleared when resolving property-level bindings (`ResolveProperty`) so that individual property suppression (vectorization exclusion) remains independent.

Property-level `NO DECAY` on an entity whose binding already has `decayEnabled: false` or entity-level `NO DECAY` is redundant and should be omitted.

Decay functions:

```go
func exponentialDecay(ageNanos, halfLifeNanos int64) float64 {
    return math.Exp(-float64(ageNanos) * math.Ln2 / float64(halfLifeNanos))
}

func linearDecay(ageNanos, halfLifeNanos int64) float64 {
    ratio := float64(ageNanos) / float64(halfLifeNanos * 2) // reaches 0 at 2×halfLife
    return math.Max(0, 1.0 - ratio)
}

func stepDecay(ageNanos, halfLifeNanos int64) float64 {
    if ageNanos < halfLifeNanos { return 1.0 }
    return 0.0
}
```

### 2.3 Compiled binding table builder

**`pkg/knowledgepolicy/binding_builder.go`**:

Called by `SchemaManager` on every DDL change. Rebuilds the `BindingTable` from all stored bundles, bindings, profiles, and policies. Pre-computes `thresholdAgeNanos` for the fast-path integer comparison (design §6.1 Tier 3).

#### 2.3.1 Multi-label tie-breaking with deterministic Order

**Problem:** If Node A has labels `:Recipe:Tested`, and separate decay profiles exist for `:Recipe` and `:Tested` but no combined profile for `:Recipe:Tested`, the most-specific-match rule cannot decide — both bindings have equal specificity (1 label each). The original plan falls back to "non-decaying + diagnostic warning," but this is operationally dangerous: a tie silently disables decay for the entity, and if the warning fires on every sub-millisecond traversal it will flood the log.

**Solution:** Add an `Order` field to `DecayProfileBinding` (already present on `DecayProfilePropertyRule`). When two bindings have equal label-set specificity, the binding with the **lower Order value** wins deterministically. If `Order` values are also equal (both defaulted to 0), the builder fails the rebuild with an explicit schema-level error at DDL time — not at query time.

**Binding builder changes:**

```go
// DecayProfileBinding — add Order field:
type DecayProfileBinding struct {
    // ... existing fields ...
    Order int `msgpack:"order"` // deterministic precedence for equal-specificity conflicts
}
```

**Resolution rules (updated):**

1. Most specific match wins (most labels in the target set).
2. On equal specificity: lower `Order` wins.
3. On equal specificity AND equal `Order`: `BindingBuilder.Rebuild()` returns an error. The `SchemaManager` rejects the DDL that introduced the conflict. The database state does not change.

**Compile-time conflict detection:** The builder checks for conflicts at rebuild time, not at query time. This means:
- `CREATE DECAY PROFILE foo FOR (n:Recipe)` succeeds.
- `CREATE DECAY PROFILE bar FOR (n:Tested)` succeeds only if no node in the database currently has both `:Recipe` and `:Tested`, **or** if `foo` and `bar` have different `Order` values.
- If a node gains a new label after profile creation (e.g., `SET n:Tested` on a `:Recipe` node), the conflict is detected on the next binding table rebuild (next DDL change or server restart) and logged as a diagnostic warning. The node is treated using the lower-`Order` binding until the operator resolves the conflict.

**Log behavior:** Runtime label conflicts (detected at query time when no DDL rebuild has happened) log a warning on every occurrence. No rate limiting — conflicts are errors in the schema configuration and must be visible to the operator immediately. If the warning volume is high, that is a signal to fix the conflicting profiles, not to suppress the warnings.

### Phase 2 acceptance gate

- Resolver returns correct `ScoringResolution` for all label/edge-type/property combinations.
- Scorer produces correct scores for all decay functions × score-from modes.
- Multi-label resolution picks the most specific match.
- Multi-label tie at equal specificity is broken by `Order` field (lower wins).
- Multi-label tie with equal specificity AND equal `Order` is rejected at DDL time with a clear error message.
- Runtime label conflict (label added after profile creation) logs a warning on every occurrence, resolves to lower-`Order` binding.
- Wildcard fallback works correctly.
- No-decay targets resolve to `1.0`.
- Targets with no matching profile resolve to `1.0` (neutral).
- `thresholdAgeNanos` fast-path matches `math.Exp` path for all test cases.
- Scorer returns neutral `ScoringResolution{FinalScore: 1.0, NoDecay: true}` when `!config.Memory.DecayEnabled`.

### Phase 2 test files

- `pkg/knowledgepolicy/resolver_test.go` — resolution cascade for all target types
- `pkg/knowledgepolicy/scorer_test.go` — scoring correctness, all functions, all score-from modes
- `pkg/knowledgepolicy/binding_builder_test.go` — compiled table correctness, multi-label conflict detection, Order-based tie-breaking
- `pkg/knowledgepolicy/binding_builder_conflict_test.go` — equal-specificity with equal Order rejected at DDL time; equal-specificity with different Order resolves deterministically; runtime label conflict logs warning on every occurrence
- `pkg/knowledgepolicy/scorer_bench_test.go` — benchmark: integer fast-path vs float path

---

## Phase 3: AccessMeta Index

**Goal:** Implement the accessMeta index, the sharded counter ring for hot-path accumulation, and the flush goroutine. After this phase, ON ACCESS mutations can be accumulated and persisted.

**Depends on:** Phase 1 (types). Independent of Phase 2.

### 3.1 Per-P sharded counter ring

**`pkg/knowledgepolicy/access_accumulator.go`**:

**Problem with entity-sharded design:** In graph datasets, power-law distributions are the norm. A super-node (e.g., a canonical root concept that everything connects to) hashes to a single shard. Hundreds of concurrent goroutines contending on the same `atomic.Int64` causes CPU cache-line bouncing and degrades the hot path from <30ns to >200ns under contention.

**Solution:** Shard by processor, not by entity. Each logical P (as in `runtime.GOMAXPROCS`) gets its own accumulator shard. Goroutines write to the shard of the P they are currently scheduled on, guaranteeing zero cross-core contention regardless of graph topology. The flush goroutine aggregates across all P-local shards before writing to Badger.

```go
type entityDelta struct {
    accessCount     int64
    traversalCount  int64
    lastAccessedAt  int64 // UnixNano — max-wins
    lastTraversedAt int64 // UnixNano — max-wins
    overflow        map[string]int64
}

type pLocalShard struct {
    mu     sync.Mutex
    deltas map[string]*entityDelta // key: entityID
    _pad   [128 - 64]byte          // cache-line padding to prevent false sharing
}

type AccessAccumulator struct {
    shards []pLocalShard // length = runtime.GOMAXPROCS(0), resized on GOMAXPROCS change
}

// currentShard returns the shard for the current P using sync.Pool's P-local affinity.
//
// Mechanism: A sync.Pool is initialized with a New func that returns the next shard index
// (atomic counter). Pool.Get() returns the shard index pinned to the current P. The goroutine
// does its mutation, then Pool.Put() returns the index. Because sync.Pool internally uses
// per-P local storage, repeated Get/Put from the same P returns the same shard index —
// giving us P-local affinity without go:linkname into runtime internals.
//
// Cost: ~15ns for the Get/Put pair, well within the <30ns budget. This is stable across
// Go versions — sync.Pool's P-local behavior is a documented performance property, not
// an implementation detail.
func (a *AccessAccumulator) currentShard() *pLocalShard

func (a *AccessAccumulator) IncrementAccess(entityID string)
func (a *AccessAccumulator) IncrementTraversal(entityID string)
func (a *AccessAccumulator) IncrementCustom(entityID string, key string, delta int64)
func (a *AccessAccumulator) SetTimestamp(entityID string, key string, ts int64)

// ReadThrough returns persisted + buffered delta for WHEN predicate evaluation.
// Scans ALL P-local shards to sum deltas for the requested entityID.
// Cost: O(GOMAXPROCS) mutex acquisitions — acceptable because WHEN predicate evaluation
// is not on the sub-millisecond visibility-check hot path; it runs only during
// full score computation (lazy scoring) after the entity has survived visibility.
func (a *AccessAccumulator) ReadThrough(entityID string, key string, persisted int64) int64
```

**Why `sync.Pool`, not `hash(goroutineID)`:** Goroutine IDs are not exposed by the Go runtime without unsafe hacks, and goroutine-local storage does not exist in Go. `sync.Pool` already solves P-local affinity internally — its `Get`/`Put` methods use per-P local storage under the hood. We exploit this: the pool's `New` func assigns shard indices via an atomic counter. A goroutine calls `pool.Get()` to retrieve the shard index for its current P, does the mutation, then `pool.Put()` returns it. Because `sync.Pool` reuses per-P local slots, repeated `Get`/`Put` from the same P returns the same shard index — giving us P-local affinity through a stable, public API. No `go:linkname`, no dependency on runtime internals, no breakage across Go versions.

**Flush aggregation:** The flush goroutine iterates all P-local shards, locks each briefly, swaps out the `deltas` map with a fresh empty map, and merges all per-entity deltas into a single aggregated map before writing to Badger. Counts are summed; timestamps take the maximum value.

#### 3.1.1 Snapshot isolation and ReadThrough — eventually-consistent by design

**Problem:** If `ReadThrough` returns the absolute latest buffered delta (which includes increments from transactions that committed after the current query's snapshot), then a `WHEN` predicate in a promotion policy evaluated at snapshot T_50 would observe access counts from T_60. This breaks MVCC snapshot isolation — the same query run twice at T_50 could see different access counts if a flush occurs between executions.

**Design decision:** Access counts in the accumulator are **explicitly eventually-consistent** and are **not bound by MVCC snapshot isolation**. This is the correct choice for the following reasons:

1. **Access counts are not graph state.** They are operational metadata — telemetry about how the graph has been used. They are analogous to query statistics or cache hit counters. Binding them to snapshot isolation would require versioning every atomic increment, which defeats the purpose of the lock-free accumulator.

2. **WHEN predicates are policy thresholds, not ACID reads.** A predicate like `WHEN n.accessCount > 10` is asking "has this node been accessed enough to warrant promotion?" The answer is operationally useful whether the count is 11 or 12. It is not a transactional invariant that must be repeatable.

3. **The alternative is prohibitively expensive.** Snapshot-consistent access counts would require either: (a) versioning every counter increment as an MVCC record (destroying the <30ns hot path), or (b) snapshotting the entire accumulator state at each transaction start (O(n) memory per concurrent reader).

**Documented contract:**

```go
// ReadThrough returns the best-effort current value: persisted + sum(P-local buffered deltas).
// This value is eventually consistent with a bounded staleness of one flush interval.
// It is NOT bound by MVCC snapshot isolation. WHEN predicates that evaluate access
// counts see a real-time-ish view, not a snapshot-consistent view.
//
// This is intentional: access counts are operational metadata, not graph state.
// Promotion policy evaluation is eventually-consistent by design.
```

**Test requirement:** Add a test that demonstrates and asserts this behavior — a WHEN predicate sees an access count that was incremented *after* the query's snapshot timestamp. This is the expected behavior, not a bug.

### 3.2 Badger persistence

**`pkg/storage/badger_access_meta.go`**:

```go
func (b *BadgerEngine) GetAccessMeta(entityID string) (*knowledgepolicy.AccessMetaEntry, error)
func (b *BadgerEngine) PutAccessMeta(entityID string, entry *knowledgepolicy.AccessMetaEntry) error
func (b *BadgerEngine) DeleteAccessMeta(entityID string) error
func (b *BadgerEngine) ScanAccessMetaPrefix(prefix string) ([]*knowledgepolicy.AccessMetaEntry, error)
```

Key format: `[prefixAccessMeta][entityID bytes]`

Serialization: fixed fields as known-size byte slice (no reflection), overflow map via msgpack. Pre-allocated byte buffers from `sync.Pool`.

### 3.3 Flush goroutine

**`pkg/knowledgepolicy/access_flusher.go`**:

```go
type AccessFlusher struct {
    accumulator *AccessAccumulator
    store       AccessMetaStore // interface satisfied by BadgerEngine
    interval    time.Duration   // configurable, default 2s
}

func (f *AccessFlusher) Start(ctx context.Context)
func (f *AccessFlusher) Stop()
```

Flush loop: iterate shards, atomically swap non-zero deltas, merge into persisted entries via single batched Badger write. Timestamps: write latest value, not accumulation.

### 3.3.1 Kalman-filtered mutation processing

**`pkg/knowledgepolicy/kalman_accumulator.go`** — Kalman filter processing for `WITH KALMAN` mutations:

```go
// ProcessKalmanMutation takes a raw measurement, loads/initializes Kalman state
// from the entity's AccessMetaEntry.KalmanFilters map, runs the filter, and
// writes the filtered value and updated state back to the same map.
//
// This is a behavioral signal smoother — it stabilizes derived metrics (confidence
// scores, cross-session access rates, agreement ratios) where the input stream is
// noisy. It must not be used on raw monotonic counters or timestamps.
//
// Reuses the exact algorithm from pkg/filter/kalman.go:375-409 (imu-f port).
// Reuses VarianceTracker logic from pkg/filter/kalman.go:648-668 for auto-R.
// Inlined for zero-allocation hot-path performance (~20 lines of pure scalar math).
func ProcessKalmanMutation(
    propertyKey string,
    rawMeasurement float64,
    kalmanCfg *KalmanConfig,
    entry *AccessMetaEntry,
) float64 // returns the filtered value
```

**Internal algorithm (inlined from `pkg/filter/kalman.go`, matching `docs/plans/kalman.md`):**

1. Load `KalmanPropertyState` from `entry.KalmanFilters[propertyKey]` or initialize a new one with seed values from `kalmanCfg` (P=30.0, e=1.0, Q=cfg.Q×0.001, R=cfg.R). If `entry.KalmanFilters` is nil, allocate the map.
2. If auto-R mode (`kalmanCfg.Mode == KalmanModeAuto`): initialize or update the `Variance` field on the `KalmanPropertyState`. Update the sliding window variance with `rawMeasurement`. Set `filter.R = sqrt(variance) * kalmanCfg.VarianceScale`. This is a direct port of `update_kalman_covariance` from the flight controller.
3. Run the Kalman process step on `state.Filter`:
   - Velocity projection: `x += (x - lastX)`
   - Save lastX
   - Error boosting: `e = abs(1 - target/lastX)` (target=0 → e=1.0)
   - Prediction: `p = p + q*e`
   - Measurement: `k = p / (p + r)`, `x += k * (measurement - x)`, `p = (1-k) * p`
4. Store filtered `x` as `state.FilteredValue`.
5. Write updated `KalmanPropertyState` back to `entry.KalmanFilters[propertyKey]`.
6. Return `state.FilteredValue`.

**Why inline instead of importing `pkg/filter`:** The accumulator mutation path targets <500ns per Kalman step. Importing `pkg/filter/kalman.go` would pull in `sync.RWMutex` overhead and allocation from the `innovations` slice. The inline version is ~20 lines of pure scalar math with zero allocations — exactly matching the flight controller's `kalman_process()`.

**Integration with `AccessAccumulator`:** When processing a mutation:

- If `mutation.Kalman == nil` → existing path (plain delta accumulation to P-local shard via atomic operations)
- If `mutation.Kalman != nil` → call `ProcessKalmanMutation()` with the raw RHS expression result and the entity's `AccessMetaEntry`

The Kalman path cannot use the pure-atomic P-local shard optimization because it needs read-modify-write on the `KalmanPropertyState`. Instead, Kalman mutations use the `pLocalShard.mu` mutex path (already exists) with the entity's `AccessMetaEntry`. This is still P-local (zero cross-core contention) and the lock is held for ~200ns (one Kalman step), well within the performance budget.

**ReadThrough** for Kalman-filtered properties returns `entry.KalmanFilters[propertyKey].FilteredValue` — the Kalman's optimal estimate `x` of the behavioral signal.

**`policy()` Cypher function** exposes the `KalmanFilters` map as part of the accessMeta result. For example, `policy(n).kalmanFilters.confidenceScore` returns the full `KalmanPropertyState` for that property, including `filteredValue`, `filter` (gain, covariance, observations), and `variance` (window, current variance) for diagnostics.

### 3.4 Entity lifecycle integration

- **Node/edge deletion:** Enqueue accessMeta key for deletion in the same transaction. Clear from accumulator immediately.
- **Node/edge suppression:** Retain accessMeta (accessible via `reveal()`). Delete only on physical reclamation by the compliance retention lifecycle.
- **MVCC version pruning:** Check for orphaned accessMeta entries when all versions are pruned.

### Phase 3 acceptance gate

- P-local sharded accumulator correctly accumulates increments under concurrent goroutines — including concurrent access to the **same** entityID from different goroutines (super-node scenario).
- Super-node benchmark: 128 goroutines incrementing the same entityID concurrently shows zero cache-line contention (throughput scales linearly with GOMAXPROCS, not inversely).
- Flush goroutine aggregates all P-local shards and persists accumulated values to Badger.
- Read-through scans all P-local shards and returns `persisted + sum(buffered deltas)`.
- accessMeta survives restart (msgpack round-trip).
- Deletion cascades from node/edge deletion.
- accessMeta retained on visibility suppression.
- All accumulator operations are no-ops when `!config.Memory.DecayEnabled`.
- `ProcessKalmanMutation` produces correct filtered values for a stable signal with Gaussian noise — filter converges.
- `ProcessKalmanMutation` dampens a hallucinated spike (e.g., value jumps from 5.0 to 500.0 then back to 5.0) — filtered value stays near 5.0.
- Auto-R mode: variance tracker adjusts R based on measurement variance; filter becomes more responsive as noise decreases.
- Manual-R mode: fixed R produces output matching `pkg/filter/kalman.go` for identical inputs.
- Kalman state and VarianceTrackerState round-trip through msgpack as part of AccessMetaEntry.KalmanFilters.
- Variance tracker window wraps correctly at `WindowSize` boundary.
- Kalman mutations use the mutex path on P-local shards — zero cross-core contention under concurrent access.
- Query context variables from `X-Query-*` headers are projected into ON ACCESS evaluation context as `$_<key>` (e.g., `$_session`, `$_agent`); default to `""` when header absent; generic passthrough — no hardcoded variable names.

### Phase 3 test files

- `pkg/knowledgepolicy/access_accumulator_test.go` — concurrent increment correctness, P-local shard isolation
- `pkg/knowledgepolicy/access_accumulator_supernode_test.go` — super-node contention: 128 goroutines × 1 entityID, assert no throughput degradation vs. 128 goroutines × 128 entityIDs
- `pkg/knowledgepolicy/access_flusher_test.go` — flush persistence, batching, cross-shard aggregation
- `pkg/storage/badger_access_meta_test.go` — CRUD, serialization round-trip, deletion cascade
- `pkg/knowledgepolicy/access_accumulator_bench_test.go` — throughput under contention, uniform vs. power-law distributions
- `pkg/knowledgepolicy/kalman_accumulator_test.go` — Kalman filter convergence on stable signal with Gaussian noise; hallucinated spike dampening (value jumps from 5.0→500.0→5.0, filtered value stays near 5.0); auto-R variance tracker adjusts R correctly; manual-R matches `pkg/filter/kalman.go` output; KalmanPropertyState, KalmanFilterState, VarianceTrackerState msgpack round-trips as part of AccessMetaEntry.KalmanFilters; window wrapping at WindowSize boundary; session-gated CASE expression evaluates correctly with `$_session` query context variable from `X-Query-Session` header
- `pkg/knowledgepolicy/kalman_accumulator_bench_test.go` — single Kalman mutation step: target <500ns (includes KalmanFilters map read/write); compare vs plain accumulation: should be <5x overhead
- `pkg/knowledgepolicy/kalman_anti_sycophancy_test.go` — end-to-end anti-sycophancy scenario: create a promotion policy with `WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence`; process 100 accesses with stable confidence ~0.6; inject hallucinated spike at access 50 (confidence jumps to 0.99); assert filtered `confidenceScore` stays near 0.6, not 0.99; assert by access 55 filter has recovered; assert `policy(n).confidenceScore` returns filtered value; assert `policy(n).kalmanFilters.confidenceScore` exposes full KalmanPropertyState for diagnostics (filteredValue, filter state, variance tracker state)

---

## Phase 4: Runtime Integration — Scoring-Before-Visibility

**Goal:** Wire the scorer into all read paths. Nodes and edges are scored before they become visible to queries. This is the core behavioral change.

**Depends on:** Phase 2 (scorer), Phase 3 (accessMeta).

### 4.1 MVCC read-path integration

**`pkg/storage/badger_mvcc.go`** — Modify node/edge read paths:

After MVCC version resolution, before returning the entity to the caller:

0. If `!config.Memory.DecayEnabled`: skip all steps below, return entity unchanged.
1. Check suppressed bit (Tier 2 fast path — one byte check, skip if suppressed and no `reveal()`).
2. Look up compiled binding for the entity's labels/edge-type (Tier 1 — single map lookup).
3. If binding exists and `NoDecay` is false:
   a. Check `now - scoreFrom > thresholdAgeNanos` (Tier 3 — integer comparison, no `math.Exp`).
   b. If below visibility threshold and no `reveal()` in query context: suppress entity (return nil/skip).
   c. If surviving visibility: attach `ScoringResolution` to entity context for lazy score computation.
4. If `reveal()` is active for this binding: always materialize, still compute score for `decayScore()`/`decay()`.

The `scoringTime` passed to the scorer is `txn.ReadTimestamp` (already available in the MVCC read path as the snapshot timestamp).

#### 4.1.1 Legacy field fallback (pre-migration safety)

**Problem:** Phase 4 wires the scorer into the read path. Phase 7 migrates legacy data. If Phase 4 is deployed but Phase 7 migration has not yet executed, the scorer will look up `AccessMetaEntry` for existing nodes, find `nil`, and treat them as having zero access history. This causes massive unintended decay of pre-existing nodes — nodes that had `AccessCount: 500` and `LastAccessed: yesterday` in the legacy fields would suddenly compute as if they were never accessed.

**Solution:** When `AccessMetaEntry` is `nil` for a node, the scorer falls back to the legacy fields on the `Node` struct (`DecayScore`, `LastAccessed`, `AccessCount`) to compute the score. This fallback is removed in Phase 7 after migration completes.

```go
// In Scorer.ScoreNode(), after accessMeta lookup:
func (s *Scorer) resolveAccessMeta(node *Node, accessMeta *AccessMetaEntry) *AccessMetaEntry {
    if accessMeta != nil {
        return accessMeta
    }
    // Pre-migration fallback: synthesize AccessMetaEntry from legacy Node fields.
    // These fields exist on the Node struct until Phase 7 removes them.
    if node.AccessCount > 0 || !node.LastAccessed.IsZero() {
        return &AccessMetaEntry{
            TargetID:    string(node.ID),
            TargetScope: ScopeNode,
            Fixed: AccessMetaFixedFields{
                AccessCount:    node.AccessCount,
                LastAccessedAt: node.LastAccessed.UnixNano(),
            },
        }
    }
    return nil // genuinely new node with no access history
}
```

**Lifecycle:**
- Phase 4 deploys with fallback active.
- Phase 7 migration runs: converts all legacy fields to `AccessMetaEntry` records, then removes the legacy fields from the `Node` struct.
- After Phase 7, the fallback code path is dead (the legacy fields no longer exist on the struct). Remove the fallback function in the same commit that removes the legacy fields.

**Test requirement:** Add `pkg/knowledgepolicy/legacy_fallback_test.go` — test that a node with legacy fields but no `AccessMetaEntry` scores identically to the same node after its legacy fields have been migrated to an `AccessMetaEntry`.

### 4.2 Query context for reveal()

**`pkg/cypher/query_context.go`** — Add:

```go
type QueryContext struct {
    // ... existing fields ...
    RevealedBindings map[string]bool // variable names marked as visibility-bypassed
}
```

The query planner detects `reveal(variable)` during plan compilation and sets the flag. The storage read path checks this flag.

### 4.3 Property-level visibility

When the scorer indicates property-level decay hiding:

- During node/edge projection (Cypher RETURN), omit hidden properties from the result map.
- If `reveal()` is active: include all properties.
- Properties in structural indexes: never hidden (immune).

### 4.3.1 Property-level vectorization exclusion

When a property's decay score falls below its visibility threshold, the property must be excluded from future embeddings so that stale content does not pollute the vector index. This uses the existing accessMeta-first resolution pattern and the existing embed worker infrastructure — no background scorer, no separate state store, and no new embedding pipeline.

**Mechanism: nil-projection in accessMeta overflow map.**

When the `AccessFlusher` (§3.3) drains P-local shards to Badger, it evaluates property-level decay rules for any entity whose accessMeta changed during this flush cycle. If a property's score is below its visibility threshold, the flusher writes an explicit `nil` value for that property key in the entity's `AccessMetaEntry.Overflow` map:

```go
// In AccessFlusher, post-flush property suppression check:
if scorer.PropertyScoreBelowThreshold(labels, propertyKey, scoringTime, accessMeta) {
    accessMeta.Overflow[propertyKey] = nil // explicit nil = suppressed
}
```

If a property was previously `nil` in the overflow map but now scores above the visibility threshold (e.g., via promotion), the flusher deletes the key from the overflow map, restoring the stored property's visibility.

**Re-embedding trigger:** If the flusher wrote or removed any `nil` entries (suppression state changed for any property), it calls `embeddingutil.InvalidateManagedEmbeddings(node)` followed by `storage.AddToPendingEmbeddings(node.ID)` to re-queue the node for embedding.

**Embed worker integration — `embed_queue.go:668`:**

When the embed worker picks up a node, it applies accessMeta-first resolution over the node's properties before building embed text. This is the same projection used by `ON ACCESS` and `WHEN` predicates — accessMeta values override stored properties. After projection, any property that resolved to an explicit `nil` is excluded from the embed text.

```go
// In EmbedWorker.processNode(), before building embed text:
// 1. Load accessMeta for this node
// 2. Project: for each key in accessMeta.Overflow, override node.Properties[key]
// 3. Any property that resolves to explicit nil is added to the exclude list

projectedProps := mergeAccessMetaProjection(node.Properties, accessMeta)
// projectedProps contains stored values overridden by accessMeta;
// nil values signal decay-suppressed properties

opts := embeddingutil.EmbedTextOptionsFromFields(
    ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels,
)
for key, val := range projectedProps {
    if val == nil {
        opts.Exclude = append(opts.Exclude, key)
    }
}
text := embeddingutil.BuildText(projectedProps, node.Labels, opts)
```

**Why this works:**

- The accessMeta overflow map is already the canonical store for per-entity runtime state that overlays stored properties.
- Explicit `nil` in the overflow map is an unambiguous signal — it cannot be confused with "key not present" (which means "fall back to stored property") or "key present with a value" (which means "ON ACCESS wrote this value").
- The flusher is the only writer to Badger accessMeta keys (§3.3), so suppression state updates are serialized with access count updates — no write contention.
- The embed worker already loads nodes from storage. Adding an accessMeta lookup per node is one additional Badger point read on the cold path — negligible.
- No separate `SuppressedProperties()` method or background suppression pass needed. The flusher does the property scoring check inline during its existing flush cycle.
- `embeddingutil.BuildText()` already filters via the `Exclude` set — no changes needed there.

**Gating:** This entire mechanism is gated on `config.Memory.DecayEnabled`. When decay is disabled, the flusher does not evaluate property-level rules, no `nil` entries are written, and the embed worker builds text from raw stored properties as it does today.

### 4.4 Search path integration

**`pkg/search/`** — Unified search:

- After vector/BM25/RRF candidate retrieval, apply scorer to each candidate.
- Suppress invisible candidates.
- If LIMIT was requested and visible results < LIMIT, continue pulling chunks until LIMIT is satisfied or index is exhausted (design §6.9 chunked retrieval).
- Add `scores` metadata to response (design §6.9 preferred shape).

### 4.5 Background maintenance paths

- **Recalc:** Use scorer with `scoringTime = time.Now()` at cycle start, frozen for batch.
- **Suppression pass:** Entities whose final score falls below visibility threshold → mark suppressed in primary storage, enqueue deindex work item.
- **Stats:** Report decay distribution based on scorer resolution, not hardcoded tiers.

### Phase 4 acceptance gate

- `MATCH (n:MemoryEpisode)` does not return nodes below visibility threshold.
- `MATCH (n:MemoryEpisode) RETURN reveal(n)` returns all nodes including suppressed ones.
- `MATCH (n:KnowledgeFact)` always returns facts (no-decay).
- Property-level hiding works: hidden properties absent from RETURN unless `reveal()`.
- Indexed properties always visible regardless of decay.
- Vector search returns expected LIMIT count despite decay filtering.
- Same scorer invoked by Cypher and unified search.
- Property-level suppression triggers re-embedding: flusher writes `nil` in accessMeta overflow map, node re-queued via `InvalidateManagedEmbeddings` + `AddToPendingEmbeddings`.
- Property restoration (score rises above threshold) triggers re-embedding: flusher removes `nil` key from overflow map, node re-queued.
- Embed worker applies accessMeta projection before building embed text: explicit `nil` values excluded from embed text.

### Phase 4 test files

- `pkg/storage/badger_mvcc_decay_test.go` — visibility suppression, reveal bypass, property hiding, feature-flag-off pass-through
- `pkg/cypher/reveal_test.go` — reveal() plan compilation, per-variable bypass
- `pkg/search/decay_filter_test.go` — chunked retrieval, LIMIT satisfaction
- `pkg/knowledgepolicy/integration_test.go` — end-to-end: DDL → score → visibility
- `pkg/knowledgepolicy/legacy_fallback_test.go` — node with legacy fields but no AccessMetaEntry scores identically to migrated node; fallback is no-op when AccessMetaEntry exists
- `pkg/knowledgepolicy/access_flusher_property_suppression_test.go` — flusher writes `nil` in overflow map when property score crosses below threshold; flusher removes `nil` key when property score rises above threshold; flusher calls `InvalidateManagedEmbeddings` + `AddToPendingEmbeddings` on suppression state change; embed worker excludes `nil`-projected properties from embed text; no suppression logic runs when `!config.Memory.DecayEnabled`

---

## Phase 5: Cypher Functions

**Goal:** Implement `decayScore()`, `decay()`, `policy()`, and `reveal()` as native Cypher functions.

**Depends on:** Phase 4 (scoring wired into read paths).

### 5.1 Function registration

**`pkg/cypher/knowledgepolicy_functions.go`**:

Register in the existing function registry (same pattern as `kalman_functions.go`):

- `decayScore(entity)` → `float64` — calls `Scorer.ScoreNode/ScoreEdge`, returns `FinalScore`.
- `decayScore(entity, options)` → `float64` — `options.property`, `options.scoringMode`.
- `decay(entity)` → `map[string]any` — structured result with `.score`, `.policy`, `.scope`, `.function`, `.visibilityThreshold`, `.floor`, `.applies`, `.reason`, `.scoreFrom`.
- `decay(entity, options)` → `map[string]any` — property/scoringMode variants.
- `policy(entity)` → `map[string]any` — reads from accessMeta index via accumulator read-through.
- `reveal(entity)` → plan-level marker, not a runtime function. Detected by planner, not function registry. Returns entity unchanged at runtime.

### 5.2 Options validation

Validate `options` map keys against accepted set: `property`, `scoringMode`. Reject unknown keys at parse time. Validate `scoringMode` values against `exponential`, `linear`, `step`, `none`.

### 5.3 Default behavior (decay enabled, no matching profile)

- `decayScore()` on a target with no matching profile: return `1.0`.
- `decay()` on a target with no matching profile: return `{score: 1.0, applies: false, reason: "no decay profile", ...}`.
- `policy()` on a target with no accessMeta: return `{targetId: id, targetScope: "node"}`.

### 5.4 Behavior when `DecayEnabled = false` — graceful degradation

When `config.Memory.DecayEnabled` is `false`, all four knowledge-layer functions remain callable and return semantically correct values. They do **not** error. This is the PostgreSQL/Elasticsearch pattern — queries are portable across configurations without requiring conditional function calls.

**Return values when disabled:**

| Function | Return value | Rationale |
|----------|-------------|-----------|
| `decayScore(n)` | `1.0` | Nothing decays → everything is full score. Semantically correct. |
| `decayScore(n, options)` | `1.0` | Same — options are accepted but have no effect. |
| `decay(n)` | `{score: 1.0, applies: false, reason: "decay subsystem disabled — enable with NORNICDB_MEMORY_DECAY_ENABLED=true", ...}` | Self-documenting: `applies: false` is queryable, `reason` tells the operator exactly what to do. |
| `policy(n)` | `{targetId: id, targetScope: "node"}` | Empty metadata — correct, since no access tracking is running. |
| `reveal(n)` | Identity (returns entity unchanged) | There is no visibility filtering to bypass — `reveal()` is inherently a no-op. |

**Config mismatch log:** Every time one of these functions is evaluated with `DecayEnabled = false`, the runtime logs a warning:

```
WARN  knowledgepolicy: decayScore() called but decay subsystem is disabled (NORNICDB_MEMORY_DECAY_ENABLED=false). Returning 1.0. Enable decay to get actual scores.
```

This warning is logged **once per query execution**, not once per entity per query. The log is scoped to the query context — if a single `MATCH (n) RETURN decayScore(n)` touches 10,000 nodes, it produces one log line, not 10,000. This gives operators visibility into misconfiguration without flooding logs during bulk scans.

Implementation: set a `decayMismatchLogged` boolean on the `QueryContext`. On first function invocation with `!DecayEnabled`, log the warning and set the flag. Subsequent invocations in the same query skip the log.

**Documented caveat — inverted predicates return empty results silently:**

When decay is disabled, `decayScore(n)` always returns `1.0`. This means:

```cypher
// This returns ALL nodes (1.0 > 0.5 is always true) — correct, expected.
MATCH (n) WHERE decayScore(n) > 0.5 RETURN n

// This returns NOTHING (1.0 < 0.5 is always false) — correct but potentially surprising.
MATCH (n) WHERE decayScore(n) < 0.5 RETURN n
```

The second query asks "give me nodes that have decayed below 0.5." When decay is disabled, no node has ever decayed, so returning nothing is semantically correct. However, an operator who expects decay to be active may not realize the subsystem is off.

This edge case is mitigated by:
1. The config mismatch log (above) — the operator sees the warning in the query's log output.
2. The `decay(n).applies` field — operators can inspect subsystem status: `MATCH (n) RETURN decay(n).applies LIMIT 1` returns `false` when disabled.
3. Documentation in `docs/user-guides/decay-profiles.md` explicitly calls out this behavior with an example.

This is an acceptable tradeoff: returning empty results for "find me decayed nodes" when decay is off is the correct answer to the question being asked. The alternative — erroring on any knowledge-layer function call — would force operators to maintain two query variants and would break `reveal()` semantics (since `reveal()` is inherently a no-op when decay is off).

### Phase 5 acceptance gate

- All Cypher examples from the design document (§6.8, Appendix A) execute correctly.
- `decayScore(n)` matches the score used for visibility determination.
- `decay(n).score` is accessible and stable for ORDER BY.
- `policy(n).accessCount` reads from accessMeta.
- `reveal(n)` works in RETURN, WITH, WHERE, ORDER BY.
- Options validation rejects unknown keys.
- With `DecayEnabled = false`: `decayScore(n)` returns `1.0`, `decay(n).applies` returns `false`, `policy(n)` returns empty metadata, `reveal(n)` returns entity unchanged.
- With `DecayEnabled = false`: config mismatch warning logged once per query execution (not per entity).
- With `DecayEnabled = false`: `WHERE decayScore(n) < 0.5` returns empty results (correct behavior, not an error).

### Phase 5 test files

- `pkg/cypher/knowledgepolicy_functions_test.go` — all function variants, default behavior
- `pkg/cypher/knowledgepolicy_functions_options_test.go` — options validation
- `pkg/cypher/knowledgepolicy_reveal_integration_test.go` — reveal in all clause positions
- `pkg/cypher/knowledgepolicy_functions_disabled_test.go` — all functions with `DecayEnabled = false`: return values correct, config mismatch warning logged once per query (assert log output), `WHERE decayScore(n) < 0.5` returns empty results, `WHERE decayScore(n) > 0.5` returns all nodes

---

## Phase 6: Visibility Suppression and Deindex Infrastructure

**Goal:** Implement per-entity index-entry catalogs, deindex work items, and the background deindex cleanup job. This phase does not redefine the existing compliance retention archival lifecycle.

**Depends on:** Phase 4 (suppression marking).

### 6.1 Index-entry catalog

**`pkg/storage/badger_index_catalog.go`**:

When a node or edge is indexed (written to any secondary index), record the exact index keys in an `IndexEntryCatalog` entry:

```go
type IndexEntryCatalog struct {
    TargetID           string   `msgpack:"targetId"`
    TargetScope        string   `msgpack:"targetScope"`           // "NODE" or "EDGE"
    IndexKeys          [][]byte `msgpack:"indexKeys"`             // exact Badger keys written
    Deindexed          bool     `msgpack:"deindexed,omitempty"`   // set by cleanup job (§6.3.1)
}
```

Key format: `[prefixIndexEntryCatalog][entityID bytes]`

This is maintained on every index write. When a node is re-indexed (properties changed), the catalog is updated with the new key set. The `Deindexed` flag is set by the deindex cleanup job after writing index tombstones (§6.3.1) and prevents re-processing on subsequent cleanup runs.

### 6.2 Deindex work items

**`pkg/storage/badger_deindex_work.go`**:

When a node or edge is marked visibility-suppressed:

1. Persist `DeindexWorkItem` with the entity's index catalog reference.
2. The background cleanup job drains work items.

```go
type DeindexWorkItem struct {
    WorkItemID   string    `msgpack:"workItemId"`
    TargetID     string    `msgpack:"targetId"`
    TargetScope  string    `msgpack:"targetScope"`
    EnqueuedAt   int64     `msgpack:"enqueuedAt"` // UnixNano
    NextAttemptAt int64    `msgpack:"nextAttemptAt"`
    RetryCount   int       `msgpack:"retryCount"`
    Status       string    `msgpack:"status"` // "pending", "completed", "failed"
}
```

### 6.3 Background deindex job

**`pkg/storage/badger_deindex_cleanup.go`**:

```go
type DeindexCleanupJob struct {
    engine    *BadgerEngine
    interval  time.Duration // default: 24h (nightly), configurable in seconds
}

func (j *DeindexCleanupJob) Start(ctx context.Context)
func (j *DeindexCleanupJob) RunOnce(ctx context.Context) (deindexed int, err error)
```

Process: scan `prefixDeindexWorkItem` for pending items → for each item, load its `IndexEntryCatalog` → perform batched deindex writes against the exact index keys → mark work item completed → delete completed work items.

No full index scans. Idempotent. Retry-safe (exponential backoff on failures).

#### 6.3.1 Deindexing — presence-marker tombstones

**Why version-aware tombstones are unnecessary:** NornicDB's time-travel queries (`GetNodesByLabelVisibleAt`, `GetEdgesByTypeVisibleAt`, `GetEdgesBetweenVisibleAt`) do **not** traverse secondary indexes. They iterate MVCC version records directly (`prefixMVCCNode` / `prefixMVCCEdge`) with iterator pinning on the snapshot version, then filter by label/type in-memory. Secondary indexes (`0x03`–`0x06`) are only used by current-time query paths (`GetNodesByLabel`, `GetFirstNodeByLabel`, `ForEachNodeIDByLabel`).

This means the deindex tombstone does **not** need to carry an `MVCCVersion` or support version-aware comparisons. Time-travel correctness is already guaranteed by the MVCC version iteration — a suppressed entity's MVCC records are untouched and remain resolvable at any historical snapshot. The tombstone is a simple **presence marker** that tells current-time index scans to skip the entry.

**Dedicated tombstone prefix:**

```go
// pkg/storage/badger.go — append to existing prefix block:
prefixIndexTombstone = byte(0x17) // idxtomb:<original-index-key> -> []byte{}
```

**Index tombstone format:**

The tombstone value is `[]byte{}` — a zero-length marker. No struct, no version, no msgpack. Existence of the key is the only signal. This matches the format of the secondary index entries themselves (`labelName:nodeID → []byte{}`).

```go
// No IndexTombstoneEntry struct needed — the tombstone is a presence marker.
// Key:   [prefixIndexTombstone][original-index-key]
// Value: []byte{}
```

**Tombstone key construction:** For an original index key `K` (e.g., `[0x03][labelName][nodeID]`), the tombstone key is `[prefixIndexTombstone][K]`. The original prefix byte is preserved inside the tombstone key so that the read path can reconstruct the original key if needed.

**Deindex write:** For each key `K` in the `IndexEntryCatalog`, the cleanup job writes `[prefixIndexTombstone][K] → []byte{}` in a single batched Badger transaction. The original key `K` is untouched.

**Read-path behavior:**

- **Current-time queries:** When scanning a secondary index and finding a candidate key `K`, probe for `[prefixIndexTombstone][K]`. If the tombstone key exists, skip the entry. Cost: one additional Badger point lookup per candidate. This is acceptable because the tombstone prefix is small and Badger's bloom filters will reject the lookup in <50ns for the common case (no tombstone exists).
- **Time-travel queries:** Not affected. Time-travel never reads secondary indexes — it iterates MVCC version records directly with iterator pinning on the snapshot version, then filters by label/type in-memory. Tombstones are invisible to this path.
- **Optimization: batch tombstone prefetch.** During label-index scans that iterate many entries, the read path can do a single prefix scan on `[prefixIndexTombstone][original-prefix][labelName]` and build a local `map[NodeID]bool` of tombstoned IDs for that label. Subsequent checks are map lookups instead of point reads. This is O(tombstones) not O(candidates), and is efficient because the suppressed population is typically small relative to the live population.

**Why a dedicated prefix, not a value overwrite:** The current secondary indexes (`0x03`–`0x06`) write `[]byte{}` as the value. Overwriting the value with a sentinel would couple the tombstone format to every index value format. If any future index writes non-empty values (e.g., composite indexes with score metadata), a sentinel-byte scheme creates a fragile format dependency. A dedicated prefix key is fully decoupled — the original key and value are never modified, and tombstone lifecycle is independent.

**`reveal()` and re-indexing:** When `reveal()` restores an entity to visibility (e.g., via promotion or manual score boost), the tombstone keys for that entity are deleted and the entity's index entries become live again. The original index keys were never removed, so no re-indexing write is needed — only the tombstone deletion.

**Physical reclamation:** Both the tombstone key and the original index key are eligible for physical deletion when the underlying entity is hard-deleted by the compliance retention lifecycle or MVCC version pruning. This is handled by the existing node/edge deletion paths, not by the deindex cleanup job. The cleanup job's sole responsibility is writing tombstone keys for suppressed entities. The reverse (deleting tombstones for revealed entities) is handled by the suppression-pass when an entity's score rises above the visibility threshold.

**IndexEntryCatalog update:** After writing tombstones, the catalog entry is marked as deindexed to prevent re-processing:

```go
type IndexEntryCatalog struct {
    TargetID           string   `msgpack:"targetId"`
    TargetScope        string   `msgpack:"targetScope"`
    IndexKeys          [][]byte `msgpack:"indexKeys"`
    Deindexed          bool     `msgpack:"deindexed,omitempty"` // set by cleanup job
}
```

### 6.4 Suppressed-bit fast path

Add a `VisibilitySuppressed` bool field to the node and edge serialization format. On read, check this bit before any profile resolution. Cost: one byte check. This is the Tier 2 fast path from the design.

### Phase 6 acceptance gate

- Index-entry catalog tracks correct keys for all index types (property, composite, vector, fulltext, range).
- Deindex work items are enqueued when an entity is marked visibility-suppressed.
- Background cleanup writes presence-marker tombstone keys under `prefixIndexTombstone` for exact index keys without scanning.
- Cleanup is idempotent — running twice produces the same tombstones, no error.
- Suppressed entities are skipped in current-time read paths even before deindex completes (suppressed-bit fast path).
- Time-travel queries are unaffected — they iterate MVCC version records directly and never touch secondary indexes or tombstones.
- Tombstones are zero-length presence markers (`[]byte{}`), not versioned structs.
- `reveal()` restoring an entity to visibility deletes its tombstone keys — no re-indexing write needed since original index keys were never removed.
- Tombstone keys are physically reclaimed when the underlying entity is hard-deleted by compliance retention or MVCC version pruning.
- Cleanup interval is configurable.

### Phase 6 test files

- `pkg/storage/badger_index_catalog_test.go` — catalog CRUD, correctness, Deindexed flag prevents re-processing
- `pkg/storage/badger_deindex_work_test.go` — work item lifecycle
- `pkg/storage/badger_deindex_cleanup_test.go` — end-to-end cleanup, idempotency, retry
- `pkg/storage/badger_index_tombstone_test.go` — presence-marker tombstone key written under `prefixIndexTombstone`; current-time query skips tombstoned entries; time-travel query unaffected (does not read secondary indexes); batch tombstone prefetch optimization during label-index scan; tombstone deletion on reveal restores index visibility
- `pkg/storage/badger_deindex_cleanup_bench_test.go` — throughput

---

## Phase 7: Seamless On-Start Migration and Legacy Removal

**Goal:** Seamless rolling upgrade — existing databases are migrated in-place on first startup with 1.1.0. No manual steps, no downtime, no data loss. After migration, remove all hardcoded tier assumptions.

**Depends on:** All previous phases.

### 7.0 Migration analysis — what actually needs migrating

The migration surface is smaller than it appears because of how NornicDB serializes data:

**Does NOT need re-serialization:**
- **Node bytes in Badger** — `vmihailenco/msgpack/v5` encodes `Node` as field-name-keyed maps (no `msgpack:` struct tags, no `StructAsArray`). When the `Node` struct drops `DecayScore`, `LastAccessed`, and `AccessCount` fields, existing stored bytes are still decodable — msgpack silently ignores unknown keys during decode. No re-serialization pass is needed.
- **Edge bytes in Badger** — Same format, no decay fields to remove.
- **MVCC version records** — `mvccNodeRecord` wraps `*Node` via `msgpack.Marshal()`. Same field-name map behavior. Old version records with legacy fields decode cleanly into the new struct — the legacy keys are ignored.
- **Embeddings** — Unchanged.
- **Schema definitions** — New knowledge-layer fields are `omitempty` and default to nil/empty on old data.

**Needs migration (access state extraction):**
- **Nodes with non-zero access state** — `DecayScore`, `LastAccessed`, `AccessCount` must be extracted from each node's stored bytes and written to a new `AccessMetaEntry` under `prefixAccessMeta`. This is the only data transformation.

**Needs new on-disk state:**
- **Schema version marker** — A version marker under `prefixMVCCMeta` to track which migrations have run.
- **`VisibilitySuppressed` bit on nodes/edges** — New field, defaults to `false` on existing data. No migration needed — the zero value is correct (nothing is suppressed yet).

### 7.1 Schema version marker

**`pkg/storage/badger_helpers.go`** — Add a schema version key alongside the existing MVCC sequence key:

```go
// mvccSchemaVersionKey stores the on-disk schema version for migration gating.
// Uses prefixMVCCMeta (0x10) with sub-key 0x02 (0x01 is the MVCC sequence).
func mvccSchemaVersionKey() []byte {
    return []byte{prefixMVCCMeta, 0x02}
}
```

Schema versions:

| Version | Meaning |
|---------|---------|
| absent  | Pre-1.1.0 database (no schema version key exists) |
| 1       | 1.1.0 — accessMeta extraction complete |

### 7.2 On-start migration runner

**`pkg/storage/migration_runner.go`**:

The migration runner hooks into `BadgerEngine` initialization. It runs inline during `NewBadgerEngineWithOptions()`, after the database is opened but before any reads or writes from higher layers. This follows the same pattern as the existing serializer migration (`serializer_migration.go`) but is automatic — no CLI invocation needed.

```go
// RunOnStartMigrations performs all necessary schema migrations.
// Called once during engine initialization. Idempotent — checks the schema
// version marker before each migration and skips if already applied.
func (b *BadgerEngine) RunOnStartMigrations() error {
    currentVersion := b.readSchemaVersion() // returns 0 if key absent

    if currentVersion < 1 {
        if err := b.migrateV0ToV1(); err != nil {
            return fmt.Errorf("migration v0→v1 failed: %w", err)
        }
    }

    // Future migrations: if currentVersion < 2 { ... }
    return nil
}
```

**Hook point in `nornicdb.Open()`** — Insert after `NewBadgerEngineWithOptions()` returns at `db.go:836`, before WAL, lifecycle, or retention manager initialization:

```go
badgerEngine, err := storage.NewBadgerEngineWithOptions(badgerOpts)
if err != nil { ... }

// Seamless on-start migration — runs before any higher-layer initialization.
if err := badgerEngine.RunOnStartMigrations(); err != nil {
    badgerEngine.Close()
    return nil, fmt.Errorf("on-start migration failed: %w", err)
}
```

### 7.3 Migration v0 → v1: Access state extraction

**`pkg/storage/migration_v0_to_v1.go`**:

```go
func (b *BadgerEngine) migrateV0ToV1() error
```

**Process:**

1. **Scan primary node keys** (`prefixNode`). For each node:
   a. Deserialize the node using the existing `decodeValue()` path (which handles both gob and msgpack with header auto-detection).
   b. Check if the node has non-zero access state: `DecayScore != 0 || AccessCount != 0 || !LastAccessed.IsZero()`.
   c. If yes, construct an `AccessMetaEntry`:
      ```go
      entry := &knowledgepolicy.AccessMetaEntry{
          TargetID:    string(node.ID),
          TargetScope: knowledgepolicy.ScopeNode,
          Fixed: knowledgepolicy.AccessMetaFixedFields{
              AccessCount:    node.AccessCount,
              LastAccessedAt: node.LastAccessed.UnixNano(),
          },
          LastMutatedAt: time.Now().UnixNano(),
          MutationCount: 1, // migration counts as one mutation
      }
      ```
   d. Write the `AccessMetaEntry` to `[prefixAccessMeta][entityID]` via `PutAccessMeta()`.
   e. **Do NOT re-serialize the node.** The old `DecayScore`/`LastAccessed`/`AccessCount` keys remain in the stored bytes. They are harmlessly ignored on future decodes because the `Node` struct no longer has those fields. This eliminates the most expensive part of the migration (re-encoding every node) and avoids any risk of data corruption from a failed re-write.

2. **Batch writes.** Accumulate `AccessMetaEntry` writes in batched Badger transactions (batch size: 1000, matching the existing serializer migration pattern). Flush after each batch.

3. **Scan MVCC node versions** (`prefixMVCCNode`). MVCC version records embed the full `Node` struct. The same legacy fields exist in historical versions. **No migration needed** — the `mvccNodeRecord` deserializer will silently ignore the legacy keys. Historical access state is not extracted because it represents point-in-time snapshots, not cumulative counters.

4. **Write schema version marker.** After all batches are flushed, write `schemaVersion = 1` to the schema version key. This is the commit point — if the process crashes before this write, the migration re-runs on next startup (idempotent because `PutAccessMeta` is an upsert).

**Why no node re-serialization is needed:**

| Concern | Resolution |
|---------|-----------|
| Old bytes have `DecayScore` key in msgpack map | Ignored by `vmihailenco/msgpack/v5` — unknown map keys are silently skipped during decode into a struct without those fields |
| Old bytes have `LastAccessed` / `AccessCount` | Same — silently skipped |
| Old gob-encoded nodes | `decodeValue()` already handles gob → struct fallback. Gob also ignores extra fields when decoding into a struct that has dropped them. The `encodeValue()` path will re-encode as msgpack (with header) on next write, completing the gob→msgpack migration lazily |
| MVCC historical versions | Never re-written. Decoded correctly with legacy fields ignored. |
| Node size unchanged after migration | The legacy keys remain as dead bytes in the stored value. They are reclaimed naturally when the node is next written (any `SET` or `UPDATE` re-serializes without the dropped fields) or when Badger compacts the LSM tree |

**Migration cost:**
- **Read cost:** One full scan of `prefixNode` keys. No scan of `prefixMVCCNode`, `prefixEdge`, or any index.
- **Write cost:** One `AccessMetaEntry` write per node with non-zero access state + one schema version marker write.
- **Memory cost:** O(batch_size) — holds at most 1000 nodes in memory at a time.
- **Time estimate:** ~10ms per 1000 nodes (dominated by Badger read I/O). A database with 1M nodes migrates in ~10 seconds.

### 7.4 Legacy field handling during transition

The `Node` struct change happens in two steps to ensure zero-downtime:

**Step 1 (Phase 4 — deployed with migration):** Keep legacy fields on `Node` struct but stop using them in the scorer. The scorer reads `AccessMetaEntry` with fallback to legacy fields (§4.1.1). `mergeInternalProperties()` and `ExtractInternalProperties()` continue to read/write `_decayScore`, `_lastAccessed`, `_accessCount` for backward compatibility with any external tooling that reads Neo4j JSON exports.

**Step 2 (Phase 7 — after migration is deployed and confirmed stable):** Remove the legacy fields from the `Node` struct:

```go
// Remove from pkg/storage/types.go:
// DecayScore      float64              `json:"-"`    // REMOVED in 1.1.0
// LastAccessed    time.Time            `json:"-"`    // REMOVED in 1.1.0
// AccessCount     int64                `json:"-"`    // REMOVED in 1.1.0
```

Remove `_decayScore`, `_lastAccessed`, `_accessCount` from `mergeInternalProperties()` and `ExtractInternalProperties()`.

Remove the legacy fallback from `Scorer.resolveAccessMeta()` (§4.1.1) — it becomes dead code since all access state is now in `AccessMetaEntry`.

### 7.5 Remove legacy types

- Delete `decay.Tier` enum (`TierEpisodic`, `TierSemantic`, `TierProcedural`) from `pkg/decay/decay.go`.
- Delete `decay.Manager` — replaced by `knowledgepolicy.Scorer`.
- Remove `Node.DecayScore`, `Node.LastAccessed`, `Node.AccessCount` from `pkg/storage/types.go` (Step 2 above).
- Remove `inference.EdgeDecay` from `pkg/inference/edge_decay.go`.
- Remove tier-specific fields from `pkg/replication/codec.go`.
- Update all call sites that reference removed fields.

### 7.6 CLI updates

- `nornicdb decay stats` → uses `Scorer` and `Resolver` instead of tier-based stats.
- `nornicdb decay recalculate` → uses `Scorer` to recompute, respects profiles.
- `nornicdb decay suppress` → uses visibility threshold from resolved profile, not CLI flag.
- Add `nornicdb knowledge-policy show` as the new canonical introspection command.
- Add `nornicdb migration status` → shows current schema version and migration history.

### 7.7 Replication codec

- Stop sending `DecayScore` in replication.
- Send accessMeta entries as part of the replication stream.
- Send suppressed-bit changes.

### Phase 7 acceptance gate

- `RunOnStartMigrations()` completes without error on a fresh database (no-op, writes version marker).
- `RunOnStartMigrations()` completes without error on a pre-1.1.0 database with existing tier-based data.
- Migration is idempotent — running twice produces the same result, no duplicate `AccessMetaEntry` records.
- Migration is crash-safe — if the process crashes mid-migration, the next startup re-runs from the beginning (schema version marker is written last).
- The migration only covers legacy decay/access state. It does **not** migrate or redefine compliance retention policies, legal holds, erasure requests, or checked-in retention config/endpoints.
- Nodes with `DecayScore=0, AccessCount=0, LastAccessed=zero` produce no `AccessMetaEntry` (no empty entries).
- Nodes with non-zero access state produce correct `AccessMetaEntry` with matching values.
- After migration, the scorer reads `AccessMetaEntry` for all nodes — the legacy fallback path (§4.1.1) is never exercised.
- Old node bytes with legacy keys decode correctly into the new `Node` struct (keys silently ignored).
- MVCC historical versions with legacy keys decode correctly (no re-serialization needed).
- Gob-encoded nodes from pre-msgpack databases migrate access state correctly. The gob→msgpack serialization conversion happens lazily on next node write (existing behavior from `serializer_migration.go` / `decodeValue()` fallback).
- No reference to `TierEpisodic`, `TierSemantic`, or `TierProcedural` in any Go source file (after Step 2).
- No `DecayScore`, `LastAccessed`, or `AccessCount` on `Node` struct (after Step 2).
- CLI commands work with new resolver.
- Replication codec sends/receives accessMeta.
- All existing tests updated to use new types. Zero test failures.

### Phase 7 test files

- `pkg/storage/migration_runner_test.go` — schema version read/write, runner skips already-applied migrations, runner applies pending migrations in order
- `pkg/storage/migration_v0_to_v1_test.go` — access state extraction correctness, idempotency, crash-safety (write version marker last), batch boundaries, nodes with zero access state skipped, gob-encoded nodes handled
- `pkg/storage/migration_v0_to_v1_bench_test.go` — throughput: 10K nodes, 100K nodes, 1M nodes
- `pkg/storage/migration_legacy_decode_test.go` — old msgpack bytes with `DecayScore`/`LastAccessed`/`AccessCount` keys decode into new `Node` struct with those fields absent (keys silently ignored); old gob bytes same; MVCC version records same
- `pkg/decay/` — package can be archived or deleted after Step 2
- Full regression suite re-run with `go test ./...`

---

## Phase 8: UI, Diagnostics, and Documentation

**Goal:** Surface the new system in the browser UI, admin endpoints, and user documentation.

**Depends on:** All previous phases.

### 8.1 UI additions

- Show effective decay profile and promotion policy on node/edge detail views.
- Show `decayScore`, `scoreFrom`, suppression status, deindex status.
- Show accessMeta (access count, last accessed, traversal count).
- Show resolution trace (explain view).

### 8.2 Admin endpoints

The `/admin/retention/` namespace already belongs to the compliance retention subsystem. The knowledge-layer scoring subsystem uses a separate namespace.

- `GET /admin/knowledge-policies/profiles` — list all decay profiles and bindings.
- `GET /admin/knowledge-policies/policies` — list all promotion policies.
- `GET /admin/knowledge-policies/resolve?entityId=X` — explain resolution for a specific entity.
- `GET /admin/knowledge-policies/deindex/status` — deindex cleanup job status.

### 8.3 Documentation

- `docs/user-guides/knowledge-layer-policies.md` — update existing doc (currently describes old tier system).
- `docs/user-guides/decay-profiles.md` — new: authoring decay profiles with Cypher DDL.
- `docs/user-guides/promotion-policies.md` — new: authoring promotion profiles and policies.
- `docs/user-guides/visibility-suppression-deindex.md` — new: visibility suppression behavior, deindex cleanup.
- `docs/features/memory-decay.md` — rewrite to reference new system.
- `docs/operations/cli-commands.md` — update decay CLI section.

### Phase 8 acceptance gate

- UI shows decay/promotion metadata for any node or edge.
- Admin endpoints return correct data.
- All documentation reflects the new system.
- No documentation references old tier names.

---

## Cross-Cutting Concerns

### Concurrency

- `BindingTable` is read-locked during queries, write-locked only during DDL (rare).
- `AccessAccumulator` uses P-local sharding — each goroutine writes to the shard of the P it is scheduled on, eliminating cross-core contention entirely. No `atomic.Int64` contention even under super-node access patterns.
- Each P-local shard is mutex-protected (not lock-free) but the mutex is P-local, so contention only occurs if multiple goroutines on the same P increment in the same scheduling quantum — effectively zero contention.
- Flush goroutine is the sole Badger writer for accessMeta keys. It briefly locks each P-local shard to swap out the delta map (lock held for ~50ns per shard — map pointer swap, not iteration).
- Kalman mutations use the same P-local shard mutex as entity delta writes. Lock held for ~200ns (one Kalman step — ~20 lines of scalar math, KalmanFilters map lookup). Still P-local, zero cross-core contention. No additional synchronization primitives.
- Deindex cleanup job runs single-threaded with batched writes.

### Performance budget

| Operation                                     | Target         | Mechanism                           |
| --------------------------------------------- | -------------- | ----------------------------------- |
| Visibility check (suppressed)                  | <10ns          | One byte check                      |
| Visibility check (non-suppressed, non-decaying) | <50ns          | Map lookup + NoDecay flag           |
| Visibility check (decaying, threshold)        | <100ns         | Integer subtraction on UnixNano     |
| Full score computation (lazy)                 | <500ns         | One `math.Exp` + multiply/floor/cap |
| AccessMeta increment (hot path)               | <30ns          | P-local shard mutex + map write (zero cross-core contention) |
| AccessMeta Kalman mutation (hot path)         | <500ns         | P-local shard mutex + KalmanFilters map read/write + ~20 lines scalar math (zero cross-core contention) |
| AccessMeta flush (cold path)                  | <5ms per batch | Batched Badger write, aggregate across P-local shards        |
| Binding table rebuild (DDL)                   | <10ms          | Map construction, rare              |

### Error handling

- Missing profile → neutral score `1.0`, not error.
- Missing accessMeta → empty map from `policy()`, not error. If legacy `Node` fields exist (pre-migration), synthesize `AccessMetaEntry` from them (§4.1.1).
- Unparsable CUSTOM scoreFromProperty → warn, fall back to CREATED.
- DDL validation errors → return Cypher error to client.
- Multi-label conflict at equal specificity and equal Order → reject DDL at creation time with clear error message (§2.3.1).
- Runtime label conflict (label added post-DDL) → warning logged on every occurrence, resolve to lower-Order binding.
- `DecayEnabled = false` → all scoring functions degrade gracefully, no errors. `decayScore()` returns `1.0`, `decay()` returns `{applies: false, reason: "..."}`, `policy()` returns empty metadata, `reveal()` returns identity. Config mismatch warning logged once per query execution (§5.4). `WHERE decayScore(n) < threshold` returning empty results is correct, documented behavior. Compliance retention subsystem remains unaffected.

### Testing strategy

Total new test files: ~28. All tests are deterministic (no `time.Now()` in assertions — inject frozen timestamps). Benchmarks cover all hot-path operations including Kalman mutation throughput.

Run order: `go test ./pkg/knowledgepolicy/... ./pkg/storage/... ./pkg/cypher/... ./pkg/search/...`

---

## Implementation Sequence Summary

```
Phase 1: Schema Objects and Profile Model        (foundation — no runtime changes)
  ↓
Phase 2: Shared Resolver and Scorer               (pure computation — no I/O)
  ↓                                               
Phase 3: AccessMeta Index                         (I/O layer — parallel with Phase 2)
  ↓
Phase 4: Runtime Integration                      (wires scoring into read paths, legacy fallback active)
  ↓
Phase 5: Cypher Functions                         (user-facing query surface)
  ↓
Phase 6: Visibility Suppression and Deindex        (background cleanup)
  ↓
Phase 7: Seamless On-Start Migration              (access state extraction on first 1.1.0 startup,
         and Legacy Removal                        then legacy field removal after migration confirmed)
  ↓
Phase 8: UI, Diagnostics, and Documentation       (polish)
```

Phases 2 and 3 can be developed in parallel. All other phases are sequential.

Phase 7 is internally two steps: Step 1 (migration runner + access state extraction) ships with the 1.1.0 binary and runs automatically on startup. Step 2 (legacy field removal from `Node` struct) ships in a follow-up commit after migration is confirmed stable across deployments. The legacy fallback in §4.1.1 bridges the gap between Step 1 and Step 2.

---

## Risk Register

| Risk | Mitigation |
|------|------------|
| Serialization format change breaks existing databases | No re-serialization needed — msgpack field-name maps silently ignore removed keys; access state extracted to `AccessMetaEntry` on first startup; schema version marker gates idempotent migration (§7.0–7.3) |
| Scoring overhead on hot read path | Three-tier fast path: suppressed-bit → no-decay flag → integer threshold comparison; entire path skipped when `!config.Memory.DecayEnabled` |
| AccessMeta flush lag causes stale WHEN predicate evaluation | Read-through path: `persisted + sum(P-local buffered deltas)`. Explicitly eventually-consistent by design — access counts are operational metadata, not MVCC-snapshotted graph state (§3.1.1) |
| Super-node accumulator contention | P-local sharding eliminates cross-core contention regardless of graph topology; benchmarked with 128 goroutines × 1 entityID (§3.1) |
| Multi-label node resolution complexity | Compile-time expansion + most-specific-match rule; equal-specificity ties broken by `Order` field; equal-specificity + equal-Order rejected at DDL time; runtime conflicts warn on every occurrence (§2.3.1) |
| Suppression deindex corrupts time-travel queries | Not possible — time-travel queries iterate MVCC version records directly with iterator pinning and never read secondary indexes. Tombstones are zero-length presence markers under `prefixIndexTombstone` (`0x17`) that only affect current-time index scans; original index keys are untouched (§6.3.1) |
| Phase 4 deployed before Phase 7 migration | Scorer falls back to legacy `Node.DecayScore`/`LastAccessed`/`AccessCount` fields when `AccessMetaEntry` is nil; fallback removed when Phase 7 deletes legacy fields (§4.1.1) |
| Default flag change surprises existing deployments | `DecayEnabled` default changes `true` → `false` in 1.1.0; safe direction — no unexpected scoring on upgrade; operators opt in with `NORNICDB_MEMORY_DECAY_ENABLED=true` (§1.5) |
| DDL parsing complexity | Keyword-scanning pattern (proven in constraint_contracts.go), not regex |
| Replication codec breaking change | Version bump to 1.1.0 signals incompatible break |
| Kalman filter converges to wrong value under systematic bias (sycophancy loop) | Kalman smooths variance, not bias. Bias correction is handled by session-aware gating via query context variables (`$_session`, `$_agent` from `X-Query-*` headers) which deduplicates same-session accesses before the measurement reaches the filter. The two layers compose: gating removes bias, Kalman removes variance. Design constraint documented in §7.4 of the design plan. |
| Operator misuses WITH KALMAN on raw monotonic counters | Design constraint 3 in §7.4 explicitly states Kalman is for derived behavioral metrics, not counters. Documentation and examples show plain `SET` for counters. No engine-level enforcement — this is a usage guidance constraint, not a hard gate. |
| Kalman state grows AccessMetaEntry size | Each Kalman-filtered property adds one `KalmanPropertyState` entry to `AccessMetaEntry.KalmanFilters`: `KalmanFilterState` (~72 bytes msgpack) + `VarianceTrackerState` (~300 bytes msgpack for 32-window, auto-R only). Bounded per-entity by the number of `WITH KALMAN` mutations in the policy. Typical: 1–3 filtered properties per entity = <1.2KB overhead. Type-safe struct — no stringly-typed overflow keys. |
| Truth-immune labels inadvertently receive promotion multipliers that outweigh behavioral entities | Design constraint 1 in §7.4: truth-immune labels should have no promotion policy, or `multiplier: 1.0` with no score floor override. Their relevance comes from semantic matching, not access-pattern promotion. Enforced by operator convention, documented in design plan. |
