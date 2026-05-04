# Knowledge-Layer Policy System — Implementation Instructions

## What to Build

Implement the knowledge-layer scoring and visibility system as specified in two existing plan documents:

1. **Design doc:** `docs/plans/knowledge-layer-persistence-plan.md` — the full architecture
2. **Implementation plan:** `docs/plans/knowledge-layer-persistence-implementation.md` — file-by-file, phase-by-phase plan
3. **Kalman ON ACCESS plan:** `docs/plans/kalman-behavioral-signals.md` — WITH KALMAN modifier for ON ACCESS mutations

Read all three thoroughly before writing any code. They are the source of truth.

## Development Approach — TDD

Follow the project's mandatory test-driven workflow (see AGENTS.md "Golden Rules"):

1. **Write failing tests first** for each component
2. **Verify the tests fail** (compile but fail assertions)
3. **Implement the minimum code** to make tests pass
4. **Verify tests pass**
5. **Move to the next component**

Run tests frequently with: `go test ./pkg/knowledgepolicy/... -v -count=1`

## What's Already Working (verified against live server)

- **Decay system** — `nornicdb.decay.info()` returns enabled=true, half-lives set
- **Kalman Cypher functions** — 11 functions registered: `kalman.init`, `kalman.process`, `kalman.predict`, `kalman.state`, `kalman.reset`, plus velocity and adaptive variants
- **22 indexes** all ONLINE — property indexes on knowledge layer labels + vector indexes on OriginalText
- **3 uniqueness constraints** — CodeChange.change_id, Commit.hash, MutationEvent.event_id
- **35,437 nodes / 57,243 relationships** in the graph

## What's Missing (what you're building)

- `SHOW PROMOTION POLICIES` / `CREATE PROMOTION POLICY` / `DROP PROMOTION POLICY` DDL
- `CREATE DECAY PROFILE` / `SHOW DECAY PROFILES` / `DROP DECAY PROFILE` DDL
- `CREATE PROMOTION PROFILE` / `SHOW PROMOTION PROFILES` / `DROP PROMOTION PROFILE` DDL
- `policy()` and `decay()` Cypher functions
- `pkg/knowledgepolicy/` package — types, persistence, accumulator
- Admin API endpoints for knowledge policies
- `WITH KALMAN` modifier on ON ACCESS SET expressions
- Promotion policy admin procedures (`nornicdb.decay.info` equivalent for policies)
- Feature flags for the knowledge-layer subsystem

## Phase 1: Schema Objects and Profile Model (Start Here)

Per the implementation plan §Phase 1, build these in order:

### 1a. Types (`pkg/knowledgepolicy/types.go`)

Define all types from the implementation plan:
- `DecayFunction`, `ScoreFrom`, `DecayScope` enums
- `DecayProfileDef` struct
- `PromotionProfileDef` struct  
- `PromotionPolicyDef` struct with `OnAccessMutation` and `KalmanConfig`
- `AccessMetaEntry` with `KalmanFilters map[string]*KalmanPropertyState`
- Validation methods on each type

**Test file:** `pkg/knowledgepolicy/types_test.go`
- Validation: required fields, enum ranges, invalid configs
- Msgpack round-trip for all structs
- `KalmanConfig` modes: auto, manual, hybrid
- `OnAccessMutation` with and without Kalman

### 1b. Schema Persistence (`pkg/knowledgepolicy/persistence.go`)

Per the implementation plan, use Badger prefixes:
- `0x14` — `prefixDecayProfile`
- `0x15` — `prefixPromotionProfile`  
- `0x16` — `prefixPromotionPolicy`
- `0x11` — `prefixAccessMeta` (already reserved)

CRUD operations: `PutDecayProfile`, `GetDecayProfile`, `ListDecayProfiles`, `DeleteDecayProfile`, same for promotion profiles and policies.

**Test file:** `pkg/knowledgepolicy/persistence_test.go`
- Store/retrieve/list/delete for each object type
- Msgpack serialization round-trips
- Not-found behavior

### 1c. DDL Parsing — Decay Profiles

Add `CREATE DECAY PROFILE`, `SHOW DECAY PROFILES`, `DROP DECAY PROFILE` to the Cypher executor.

Follow the pattern in `pkg/cypher/executor_show.go` for SHOW commands and `pkg/storage/constraint_contracts.go` for keyword scanning (`ccSkipSpaces`, `ccScanIdent`, `ccMatchKeywordAt`).

**Test file:** `pkg/cypher/knowledge_ddl_test.go`
- Parse valid CREATE DECAY PROFILE statements
- Parse SHOW DECAY PROFILES
- Parse DROP DECAY PROFILE
- Reject malformed statements

### 1d. DDL Parsing — Promotion Profiles and Policies

Add `CREATE PROMOTION PROFILE`, `SHOW PROMOTION PROFILES`, `DROP PROMOTION PROFILE`.
Add `CREATE PROMOTION POLICY`, `SHOW PROMOTION POLICIES`, `DROP PROMOTION POLICY`.

Parse `ON ACCESS { ... }` blocks including `WITH KALMAN` modifier.
Parse `WHEN` predicates. Parse `FOR (n:Label)` targets. Parse `APPLY PROFILE 'name'`.

**Test file:** `pkg/cypher/knowledge_ddl_test.go` (extend)
- `WITH KALMAN SET n.x = ...` → auto mode
- `WITH KALMAN{q: 0.1, r: 88.0} SET n.x = ...` → manual mode
- `WITH KALMAN{q: 0.05} SET n.x = ...` → hybrid
- `SET n.x = ...` (no WITH KALMAN) → Kalman: nil
- Full policy with FOR, WHEN, ON ACCESS, APPLY

## Key Existing Code to Reference

| Pattern | File | What to learn |
|---------|------|---------------|
| Keyword scanning DDL | `pkg/storage/constraint_contracts.go` | `ccSkipSpaces`, `ccScanIdent`, `ccMatchKeywordAt` |
| SHOW commands | `pkg/cypher/executor_show.go` | How SHOW INDEXES/CONSTRAINTS work |
| Kalman filter algorithm | `pkg/filter/kalman.go:375-409` | The process step to inline for ON ACCESS |
| Kalman Cypher functions | `pkg/cypher/kalman_functions.go` | Field names/semantics for KalmanFilterState |
| Feature flags | `pkg/config/feature_flags.go` | How to add new flags |
| Badger prefixes | `pkg/storage/badger.go:21-36` | Prefix key allocation |
| Msgpack serialization | `pkg/storage/badger_serialization.go` | How to serialize/deserialize |
| Decay system | `pkg/decay/decay.go` | Existing decay manager structure |
| Existing tests | `pkg/decay/decay_test.go` | Test patterns for this codebase |

## Go Module

```
module: github.com/orneryd/nornicdb
go version: check go.mod
```

New package: `github.com/orneryd/nornicdb/pkg/knowledgepolicy`

## File Size Rule

Hard limit: 2500 lines per file. Split into focused files when approaching this.

## Test Execution

```bash
# Run knowledge policy tests
go test ./pkg/knowledgepolicy/... -v -count=1

# Run DDL tests
go test ./pkg/cypher/... -run TestKnowledge -v -count=1

# Run all tests (takes a while)
go test ./... -count=1
```

## Do NOT

- Modify the existing compliance retention system (`pkg/retention/`)
- Modify the existing decay system (`pkg/decay/`) — it will be replaced later, not now
- Delete or rename existing Kalman Cypher functions
- Break any existing tests
- Write code without tests first
