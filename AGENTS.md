# NornicDB Development Guide for AI Agents

**Version:** 1.0.0  
**Last Updated:** 2024-12-01  
**Target Audience:** AI coding agents (Claude, GPT-4, etc.)

---

## Mandatory task entry protocol

Read and apply all 22 [standing design principles](.agents/standing-design-principles.md) on entry and
throughout the task. They are mandatory alongside Rule 73 and sections 1–9. Their references to sections 5,
6, and 9 resolve to this protocol's corresponding sections below. These current explicit instructions clarify
older examples and general guidance without deleting them; they authorize no unrelated work.

Read and apply sections 1–9 before implementation, and reapply them when resuming work or receiving new
findings. These are binding instructions for scope, causal ownership, convergence, and readiness. Existing
standards below remain in force. This protocol clarifies that a “minimal fix” must complete the applicable
correction unit; active Rule 73 takes precedence over per-finding or per-consumer patch guidance. Explicit
user instructions take precedence over repository guidance.

## 1. Choose and record the execution route

Inspect the working tree, applicable instructions, and [CONTRIBUTING.md](CONTRIBUTING.md#development-workflow).
Preserve unrelated work. Record the requested outcome, current evidence, execution route, and correction unit
before writing code. Execution routes describe how work is corrected; they do not replace Git branch/PR routing
or authorize pushes, merges, releases, or repository settings changes.

- **Direct:** a bounded target with a clear contract and completion boundary. Its correction unit is the complete
  bounded target, including affected consumers of an existing shared owner, not just the reported example.
- **Stage 2:** implementation organized around a family when the contract or causal mechanism crosses consumers,
  implementations, or execution paths. Its correction unit is the affected implementation family and its current
  accepted correction set.
- **Stage 3:** a narrow fix-forward discovered in work owned by an existing Stage-2 implementation family. Record
  it against that owning family; it does not become a separate family or restart convergence history.

If discovery changes the applicable route or boundary, update the record before continuing. Do not choose a
narrower label to avoid a family-wide correction or proof obligation.

## 2. Establish family identity, boundary, and history

A family is the set of implementations and consumers that must satisfy the same affected contract or causal
invariant. Identify its shared causal owner, affected public operations, representations, execution paths,
accepted corrections, and required proof. Bound it by evidence and the user’s requested outcome; do not import
unrelated features or turn an affected family into an unrestricted repository rewrite.

Maintain one current family record across follow-ups: identity, boundary, accepted correction set, owner,
implementation state, prior readiness claims, review/live-proof outcomes, and unresolved obligations. New
findings in that unit update this record. Work on another family is tracked separately. Preserve the distinction
between ordinary implementation feedback and a correction presented as ready that fails required review or
live proof, as defined in section 4.

## 3. Find the existing owner before writing

Search existing helpers, interfaces, tests, and call sites before adding or changing an implementation. Record
which existing owner can implement the contract, its current consumers, and why reuse or extension is sufficient
or insufficient. Search the relevant symbols and behavior, not only the file named in the report.

Trace the actual public path to that owner and inventory sibling consumers: argument/arity variants, registry
and evaluator paths, planned/text execution, parsers, transports, transaction modes, and representations where
applicable. Reuse the existing helper and migrate affected consumers to it. Do not add a parallel helper, local
formula, route workaround, or test-only substitute while an existing shared owner can carry the correction.
When evidence requires a different causal owner, document that decision and remove superseded duplication.

Creating new behavioral divergence is prohibited. Do not add parser-, route-, transport-, or evaluator-specific
leaf workarounds, duplicate formulas, or parallel implementations of the same contract. Converge affected
existing divergence encountered in the current correction unit through one shared semantic owner. If the
existing owner cannot carry the contract, change the causal architecture and migrate the complete unit; do
not retain parallel implementations or defer that convergence. Preserve genuine protocol boundaries, but do not use those boundaries to excuse duplicated behavior.

Graphify is mandatory for finding overlapping implementations and verifying that a correction converges the
whole affected family. Follow [the local Graphify workflow](docs/user-guides/graphify-local.md): extract fresh
base and candidate code graphs, ingest with this repository's custom `scripts/graphify_local.py`, and use the
configured local NornicDB MCP server to perform semantic searches over the ingested `nornicdbcode` graph (or
its explicitly configured repository/database target). Inspect actual callers, callees, sibling consumers, and
source contracts after semantic discovery. Standalone Graphify MCP, an AST export, `rg`, or source inspection
alone does not replace custom ingestion and NornicDB MCP semantic search.

Record the repository, database, base/candidate source SHAs, graph artifacts, extraction/ingestion commands,
`CodeRepository` commit metadata, embedding readiness, semantic search requests/results, and call-graph/source
review. Verify that ingested metadata and searchable affected symbols match the candidate source; stale graphs
or unfinished embeddings are not current proof. Use configured credentials without printing or hardcoding
secrets. Preserve unrelated graph data and repository snapshots; use explicit snapshot/database targets when
extracting or ingesting different worktrees, and never relabel stale graph contents as a current revision. Missing Graphify, ingestion, authenticated MCP access, or current searchable graph evidence blocks
readiness; report the missing prerequisite rather than waiving it. Keep generated graph artifacts ignored.

## 4. Convergence and Rule 73

A failed correction attempt is a family correction presented as ready that still fails its required review or live proof. Ordinary implementation work before the family is presented as ready is not a failed correction attempt.

- Two failed correction attempts in the same family without an intervening PASS enter Rule 73.
- PASS ends the active failed-attempt sequence. It preserves the family history.
- If materially the same failed treatment later recurs, use that preserved history and enter Rule 73 instead of treating it as new work.
- Work in another family neither consumes nor resets this family's sequence.
- A failed Stage-3 narrow fix-forward belongs to the existing Stage-2 implementation family that owns its correction and uses this same convergence rule.

Enter Rule 73 sooner when the agent is already repeating materially the same failed treatment without new evidence, changed conditions, corrected implementation, or another concrete reason it should now succeed.

When this trigger is reached, apply `rule73.md` to the complete affected implementation family. Family identity, boundary, history, and current accepted correction set remain governed here; the shared module owns the course change, readiness boundary, repeated-failure behavior, and completion rule.


The shared module named `rule73.md` above is [.agents/rule73.md](.agents/rule73.md). Its verification reference
resolves to [section 9, Verification and completion](#9-verification-and-completion), below.

## 5. Reproduce the contract before correction

Distinguish an established defect in required behavior from an unapproved precaution. Establish the approved
product result, affected population, ordinary-operation likelihood, and meaningful product consequence before
adding behavior. A merely reproducible exceptional scenario does not by itself authorize engineering for it.
Tests of approved contracts do not authorize new safeguards or adjacent changes. Return ambiguous operator-owned
meaning or necessary out-of-scope changes to the user; ordinary technical choices remain agent-owned.

Use the [bug-fix workflow](.agents/bug-fix-workflow.md) to reproduce the actual reported operation, establish its
expected contract, and observe the failure before fixing it. Preserve the original failing evidence. For
compatibility work, use the applicable pinned reference and record its actual results; do not infer a reference
result from the current implementation or rewrite an assertion to accept the failure.

Add discriminating controls for neighboring valid and invalid inputs and for affected consumers identified in
section 3. A helper-only test does not establish that the public path reaches that helper. Static source
reasoning, statement coverage, a successful build, and an old green suite do not replace a failing reproduction
or proof of the relevant input boundary.

## 6. Correct the complete causal unit

Apply the standing principles' single semantic owner, single writer per state field, exact scope, and prohibition
on permanent migration behavior. Preserve an older contract only when a named current specification or explicit
user decision requires it. The approved Neo4j compatibility contract and explicitly accepted ID-string contract
remain current requirements; they are not blanket authorization for backward-compatibility adapters or models.
Externally maintained code requires the explicit THIRD-PARTY COMPONENT PATCH approval described in principle 15.

For Jev efficiency, load the TypeSafe skill from the session's actual available-skill catalog and use its
capabilities as specified there. Main agents and authorized reviewer agents apply principle 22 when Jev improves
useful coverage or saves time or tokens; set confidence thresholds by demonstrated reliability and consequences,
accept sufficient judgments, and independently verify insufficient judgments. Do not invent a skill, tool,
capability, invocation, or result. If TypeSafe or Jev is unavailable, report the exact missing capability and
continue independent authorized work; do not silently claim that the required tool-assisted work was performed.

Implement the smallest complete correction at the causal owner, carrying it through every affected consumer.
Preserve unrelated contracts, public APIs, precision, error semantics, cancellation, concurrency, and resource
ownership. Keep the original reproduction and meaningful neighboring regression controls permanent.

Under Stage 2 or an owned Stage-3 fix-forward, “minimal” does not permit stopping at one leaf while the same
cause survives in the owning family. When Rule 73 is active, follow its changed causal approach across the
complete current unit; do not resume serial leaf corrections or present an incomplete intermediate state as a
new ready candidate. Rule 73 prescribes no particular architecture or technical recipe.

## 7. Coordinate owners and integration

Before parallel work, identify existing issue/PR ownership and assign disjoint causal owners and files. Share
the accepted family boundary, dependencies, immutable candidate SHAs, and proof receipts. Avoid duplicate
implementations or competing fixes for the same owner. Coordinate merges, rebases, and shared baseline changes
with the maintainer under CONTRIBUTING.md; an execution route is not authorization to publish work.

Integrate the current accepted correction set before presenting the family as ready. Related fixes in separate
branches or separate test runs are not proof that their combined implementation works. Keep unrelated families
separate without dropping obligations in the owning family.

## 8. Protect sources, measurements, and evidence

Keep an actively verified source tree immutable, including untracked source files. Use an isolated worktree for
follow-up investigation. Pin source and baseline SHAs, retain failed-run logs, and distinguish a changed source
candidate from an unchanged candidate whose environment was repaired. Respect coordinated resource limits;
isolate timing measurements from competing workloads.

A canceled or superseded run is incomplete, not PASS. A cached result is cached evidence, not a fresh run.
Reconcile concurrent work before readiness and rerun the affected proof against the integrated current source.
Do not carry a pass claim across a source change without establishing and explaining its precise applicability.

## 9. Verification and completion

Lead proof with the requested product outcome and its supported conclusion, followed by evidence and practical
limits. Complete every requested part; route remaining work truthfully or return missing operator meaning.
Establish completeness with a repeatable population extractor, query, parser, capture, or comparator, assess
every member, and add relevant semantic members the extraction misses. Tools may assist semantic assessment
under Jev's confidence rule; the agent retains responsibility for complete coverage and supported conclusions.
Do not claim broad correctness from statement coverage, static candidates, or a narrower input population.

Prove the complete current correction unit, not just its last failing example. Before declaring readiness:

- Complete the required Graphify extraction, custom ingestion, NornicDB MCP semantic searches, and actual
  caller/callee/consumer review for the fresh base and candidate. Retain commands, artifact locations, full SHAs,
  repository/database and `CodeRepository` provenance, embedding readiness, search results, reviewed graph
  paths, and limitations. The local extraction command is
  `"$(uv tool dir --bin)/graphify" extract . --code-only --no-cluster`; use the documented custom importer and
  configured authenticated MCP target rather than assuming a default server or credentials. Where available,
  `scripts/graphify_divergence.py --graph <candidate> --compare-graph <base> --component pkg/cypher` supplements
  that review. A static candidate report is not semantic proof; zero reported candidates does not establish
  completeness. Missing required graph evidence blocks readiness and cannot be replaced by `rg` alone.
- Recheck the existing-helper and consumer inventory against the integrated source. Prove that every affected
  consumer uses the accepted shared correction and that no active duplicate or bypass retains the cause.
- Exercise the relevant family matrix: public and helper paths, argument/arity and default/explicit modes,
  literals, parameters, stored properties, projection stages, parsers, transports, transaction modes, and native
  or remote representations as applicable. Include error, null/empty, concurrency, recovery, and cache/publication
  boundaries where the contract depends on them. Scope checks by evidence; do not silently exclude an affected
  path or require unrelated feature implementation.
- For numeric work, test both sides of decision boundaries and representational limits, not only simple decimal
  examples: adjacent IEEE values, half ties, large integral floats near 2^52 and 2^53, signs and signed zero,
  precision/arity/mode changes, and NaN/infinity or integer limits when supported. Assert the actual reference
  contract and retain exactness where required; coverage percentages do not prove these input domains.
- Run the applicable checks required by CONTRIBUTING.md and current user instructions against the actual PR
  base and current head. Include the necessary regressions, race checks, conformance/differential checks, and
  workload-scoped performance/allocation comparisons. Existing baselines and thresholds remain binding: do not
  waive them, weaken assertions, accept a wrong result, or substitute an unrelated benchmark to obtain PASS.
- Record full head/base SHAs, exact commands and relevant flags/configuration, tested scope, pass/fail results,
  cached results, failures, cancellations, and skips with reasons. Distinguish proposed commands from commands
  actually executed. Explain any explicitly authorized reduction in checks; do not claim unperformed proof.

Recurring failures require reassessing the causal owner and approach, using the preserved family history and
section 4. Do not blindly rerun materially the same failed treatment or call a later retry a clean original run.
An infrastructure retry needs evidence of the repaired condition and retains the original failure record; it is
not a waiver of a product regression or a performance baseline. An active Rule-73 candidate that fails required
review or live proof remains in Rule 73 and must materially change its failed causal approach before a successor
is presented as ready.

Completion requires the integrated current unit to pass its required review and proof. State unresolved work
honestly and keep the owning family open until its accepted obligations are satisfied. This is the verification
and completion section referenced by [.agents/rule73.md](.agents/rule73.md).

---

## Core Philosophy

NornicDB is an **enterprise-grade, high-performance graph database** that prioritizes:

1. **Neo4j Compatibility** - Drop-in replacement with zero code changes
2. **Performance** - Reproducible, workload-scoped gains validated against current benchmark reports
3. **Correctness** - 90%+ test coverage, regression prevention mandatory
4. **Maintainability** - Clean architecture, separation of concerns, DRY principles
5. **Documentation** - Every public API fully documented with real-world examples

---

## 🎯 Golden Rules

### 1. **Prove Value Before Merging**

Every change must demonstrate **measurable improvements** without significant regressions:

- **Performance**: Benchmark before/after. Show ops/sec improvements.
- **Memory**: Profile memory usage. No >10% increases without justification.
- **Code Quality**: Maintain or improve test coverage (90% minimum).
- **Compatibility**: All Neo4j compatibility tests must pass.

**Example:**

```go
// ❌ BAD: No proof of improvement
func optimizeQuery() { /* new algorithm */ }

// ✅ GOOD: Benchmarked and documented
// BenchmarkQueryOptimization shows 2.3x speedup:
//   Before: 4,252 ops/sec
//   After:  9,780 ops/sec
//   Memory: -15% (reduced allocations)
func optimizeQuery() { /* new algorithm with proof */ }
```

### 2. **Test-Driven Bug Fixes**

Apply the [mandatory task entry protocol](#mandatory-task-entry-protocol) first. When Stage 2, an owned
Stage-3 fix-forward, or Rule 73 applies, the minimal fix must complete that correction unit; the reproduction
and regression requirements below remain mandatory.

**MANDATORY WORKFLOW** for all bugs:

1. **Write failing test** - Reproduce the exact bug condition
2. **Verify test fails** - Confirm it catches the bug
3. **Fix the bug** - Implement minimal fix
4. **Verify test passes** - Confirm fix works
5. **Add regression tests** - Prevent future occurrences

**Example from codebase:**

```go
// File: pkg/cypher/aggregation_bugs_test.go
// BUG #1: WHERE ... IS NOT NULL combined with WITH aggregation returns empty results

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    // 1. Setup test data that triggers bug
    setupAggregationTestData(t, store)

    // 2. Execute query that fails in production
    result, err := exec.Execute(ctx, `
        MATCH (f:File)
        WHERE f.extension IS NOT NULL
        WITH f.extension as ext, COUNT(f) as count
        RETURN ext, count
    `, nil)

    // 3. Assert expected behavior (this WILL fail before fix)
    require.NoError(t, err)
    assert.Equal(t, int64(2), extCounts[".ts"])
    assert.Equal(t, int64(3), extCounts[".md"])
}
```

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for detailed testing guidelines.

### 3. **File Size Limits**

**Hard limit: 2500 lines per file**

When a file approaches 2500 lines:

1. **Identify separation of concerns** - What are the distinct responsibilities?
2. **Define clear contracts** - What interfaces connect the pieces?
3. **Extract cohesive modules** - Group related functionality
4. **Update imports** - Maintain clean dependency graph

**Example refactoring:**

```go
// Before: executor.go (3752 lines) ❌
// - Query parsing
// - Execution logic
// - Result formatting
// - Caching
// - Optimization

// After: Split into focused modules ✅
// executor.go (800 lines)      - Core execution orchestration
// parser.go (600 lines)         - Query parsing & AST
// optimizer.go (500 lines)      - Query optimization
// cache.go (400 lines)          - Result caching
// formatter.go (300 lines)      - Result formatting
```

See [.agents/refactoring-guidelines.md](.agents/refactoring-guidelines.md) for detailed strategies.

### 4. **Functional Go Patterns**

**Leverage function types for dependency injection:**

```go
// ✅ GOOD: Function interface enables runtime swapping
type EmbeddingFunc func(text string) ([]float32, error)

type SearchEngine struct {
    embedder EmbeddingFunc  // Inject different implementations
}

// Production: Use GPU-accelerated embeddings
engine := &SearchEngine{
    embedder: gpuEmbeddings.Generate,
}

// Testing: Use mock embeddings
testEngine := &SearchEngine{
    embedder: func(text string) ([]float32, error) {
        return []float32{0.1, 0.2, 0.3}, nil
    },
}

// Development: Use cached embeddings
devEngine := &SearchEngine{
    embedder: cachedEmbeddings.GetOrGenerate,
}
```

**Real example from codebase:**

```go
// pkg/storage/types.go - Storage interface for DI
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    // ... more methods
}

// Multiple implementations:
// - MemoryEngine (testing)
// - BadgerEngine (production)
// - MockEngine (unit tests)
```

See [.agents/functional-patterns.md](.agents/functional-patterns.md) for advanced patterns.

### 5. **Documentation Standards**

**Every public API must have:**

1. **Package-level documentation** - What does this package do?
2. **Type documentation** - What does this type represent?
3. **Function documentation** - What does this do? When to use it?
4. **1-3 Real-world examples** - Show actual usage

**Example from codebase:**

```go
// Package cypher provides Neo4j-compatible Cypher query execution for NornicDB.
//
// This package implements a Cypher query parser and executor that supports
// the core Neo4j Cypher query language features. It enables NornicDB to be
// compatible with existing Neo4j applications and tools.
//
// Supported Cypher Features:
//   - MATCH: Pattern matching with node and relationship patterns
//   - CREATE: Creating nodes and relationships
//   - MERGE: Upsert operations with ON CREATE/ON MATCH clauses
//   [... more features ...]
//
// Example Usage:
//
//	// Create executor with storage backend
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Execute Cypher queries
//	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
package cypher
```

See [.agents/documentation-standards.md](.agents/documentation-standards.md) for complete guide.

### 6. **Test Coverage Requirements**

**Minimum 90% coverage for all new code:**

```bash
# Run coverage report
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# View HTML coverage report
go tool cover -html=coverage.out
```

**Coverage targets by package type:**

- **Core logic** (cypher, storage): 95%+ coverage
- **Utilities** (cache, pool): 90%+ coverage
- **Integration** (bolt, server): 85%+ coverage
- **UI/CLI**: 70%+ coverage (focus on business logic)

**What to test:**

✅ **Always test:**

- Happy path (normal usage)
- Error conditions (invalid input, not found, etc.)
- Edge cases (empty, nil, boundary values)
- Concurrency (race conditions, deadlocks)
- Regression cases (all bugs get tests)

❌ **Don't waste time testing:**

- Third-party library internals
- Generated code (unless custom logic added)
- Trivial getters/setters (unless they have logic)

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for comprehensive testing guide.

### 7. **DRY Principle (Don't Repeat Yourself)**

**Identify and extract common patterns:**

```go
// ❌ BAD: Repeated error handling
func GetUser(id string) (*User, error) {
    if id == "" {
        return nil, fmt.Errorf("invalid id: empty")
    }
    // ... logic
}

func GetPost(id string) (*Post, error) {
    if id == "" {
        return nil, fmt.Errorf("invalid id: empty")
    }
    // ... logic
}

// ✅ GOOD: Extract common validation
func validateID(id string) error {
    if id == "" {
        return fmt.Errorf("invalid id: empty")
    }
    return nil
}

func GetUser(id string) (*User, error) {
    if err := validateID(id); err != nil {
        return nil, err
    }
    // ... logic
}
```

**Real example from codebase:**

```go
// pkg/cypher/helpers.go - Shared helper functions
func toInt64(v interface{}) (int64, error) { /* ... */ }
func toString(v interface{}) (string, error) { /* ... */ }
func toBool(v interface{}) (bool, error) { /* ... */ }

// Used throughout cypher package instead of repeating type conversions
```

### 8. **Separation of Concerns**

**Clear boundaries between layers:**

```
┌─────────────────────────────────────┐
│   API Layer (bolt, http, mcp)       │  ← Protocol handling
├─────────────────────────────────────┤
│   Query Layer (cypher)               │  ← Query parsing & execution
├─────────────────────────────────────┤
│   Storage Layer (storage)            │  ← Data persistence
├─────────────────────────────────────┤
│   Infrastructure (cache, pool)       │  ← Cross-cutting concerns
└─────────────────────────────────────┘
```

**Each layer has clear responsibilities:**

- **API Layer**: Protocol translation, authentication, request/response formatting
- **Query Layer**: Cypher parsing, query planning, execution orchestration
- **Storage Layer**: CRUD operations, indexing, transactions
- **Infrastructure**: Caching, connection pooling, metrics

**Example contract:**

```go
// Storage layer exposes clean interface
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    // ... more methods
}

// Query layer depends on interface, not implementation
type Executor struct {
    storage Engine  // Can swap implementations
}

// API layer depends on query layer, not storage
type BoltServer struct {
    executor *Executor  // Clean dependency chain
}
```

See [.agents/architecture-patterns.md](.agents/architecture-patterns.md) for detailed architecture.

### 9. **Follow Industry Standards**

**Use established patterns and conventions:**

- **Go conventions**: Follow [Effective Go](https://golang.org/doc/effective_go.html)
- **Neo4j compatibility**: Match Neo4j Cypher semantics exactly
- **Bolt protocol**: Implement Neo4j Bolt protocol specification
- **Graph algorithms**: Use standard graph theory algorithms
- **Testing**: Follow Go testing best practices (testify, table-driven tests)

**Example - Table-driven tests:**

```go
func TestNodeValidation(t *testing.T) {
    tests := []struct {
        name    string
        node    *Node
        wantErr bool
    }{
        {
            name: "valid node",
            node: &Node{ID: "123", Labels: []string{"Person"}},
            wantErr: false,
        },
        {
            name: "empty ID",
            node: &Node{ID: "", Labels: []string{"Person"}},
            wantErr: true,
        },
        {
            name: "nil labels",
            node: &Node{ID: "123", Labels: nil},
            wantErr: false,  // Labels are optional
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := ValidateNode(tt.node)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

### 10. **Open Standards & Broad Support**

**Prioritize compatibility without sacrificing performance:**

✅ **Support:**

- Neo4j Bolt protocol (industry standard)
- Cypher query language (open standard)
- JSON import/export (universal format)
- Standard graph algorithms (shortest path, PageRank, etc.)
- Multiple embedding providers (Ollama, OpenAI, local GGUF)

❌ **Avoid:**

- Vendor lock-in (proprietary formats)
- Single-provider dependencies (must support alternatives)
- Non-standard query syntax (unless significant benefit)

**Example - Multi-provider embeddings:**

```go
// Support multiple embedding providers
type EmbeddingProvider interface {
    Generate(text string) ([]float32, error)
}

// Implementations:
// - OllamaProvider (local)
// - OpenAIProvider (cloud)
// - LocalGGUFProvider (offline)
// - MockProvider (testing)
```

---

## 📁 Repository Structure

```
nornicdb/
├── cmd/
│   └── nornicdb/          # Main application entry point
├── pkg/
│   ├── bolt/              # Neo4j Bolt protocol server
│   ├── cypher/            # Cypher query parser & executor
│   ├── storage/           # Storage engine (memory, badger)
│   ├── embed/             # Embedding generation (GPU-accelerated)
│   ├── search/            # Vector search (HNSW index)
│   ├── inference/         # Auto-relationship inference
│   ├── cache/             # Query result caching
│   ├── pool/              # Connection pooling
│   ├── config/            # Configuration management
│   ├── audit/             # Audit logging
│   └── auth/              # Authentication
├── docs/                  # Documentation
│   ├── getting-started/   # Installation & quick start
│   ├── api-reference/     # API documentation
│   ├── user-guides/       # Usage examples
│   ├── architecture/      # System design
│   └── performance/       # Benchmarks
├── docker/                # Docker build files
├── scripts/               # Build & deployment scripts
├── ui/                    # Web UI (optional)
└── .agents/               # AI agent development guides
    ├── testing-patterns.md
    ├── refactoring-guidelines.md
    ├── functional-patterns.md
    ├── documentation-standards.md
    ├── architecture-patterns.md
    ├── performance-optimization.md
    ├── bug-fix-workflow.md
    └── cypher-compatibility.md
```

---

## Development Workflow

### Branches, PRs, and releases

[CONTRIBUTING.md](CONTRIBUTING.md#development-workflow) is the canonical source for branch routing,
release ownership, milestones, and verification. Read it before choosing a branch or PR base.

- **Product work:** branch from `develop` into `feature/<topic>` or `fix/<topic>` and integrate into `develop`.
  Maintainers can push or merge into `develop` for rapid iteration; do not force-push or delete it.
- **Releases:** integrate into `release/vX.Y.Z` through PRs on milestone `vX.Y.Z`. Maintainers own release
  branch creation, merges, tags, and the final fast-forward of `main`. Use the documented `make release-*` helpers.
- **Maintainer maintenance:** scoped documentation, agent/contributor instructions, repository administration,
  and CI/CD or operational changes may use a short-lived branch from `main` and a PR targeting `main`.
  Explain the scope in the PR; a release milestone is optional. Product behavior changes use the release path.
  Do not include unrelated unreleased feature commits in a maintenance PR.
- After a maintenance PR merges, sync `main` into `develop`. An active release branch must also include the
  updated `main` before final verification and tagging. Coordinate history changes with the maintainer.
- PRs and labels never start CI. Manually dispatch checks and obtain approval through the
  `admin-approved-actions` environment. Automatic develop nightly/canary runs are paused.
  Existing main push and tag publishing triggers remain active.
- Choosing a PR route does not authorize pushing, merging, tagging, or changing repository settings.

### Temporary local PR verification policy (through October 31, 2026)

Through October 31, 2026 inclusive (America/Phoenix), run `make verify BASE=origin/<pr-base>` locally
for every PR created. Post a result comment on the PR, not just a description: include the checked head SHA,
base SHA, exact command, results, failures, and any skipped or unexecuted checks with reasons. Re-run after
changes or rebases and post the updated result. Path-filtered no-op runs must be reported as such, not as a
full-suite pass. GitHub Actions remains manual and administrator-approved.

For a maintainer-approved release batch, focused-validated fixes that merge cleanly may be integrated before
full verification. Run one combined `make verify ALL=1 BASE=<immutable-base>` on the frozen release head,
and post its checked head, base, command, result, and skips on every participating PR. Clearly identify this
as shared release verification, not an individual PR-head pass. Cancel redundant individual full runs, preserve
their logs, and open follow-up PRs against the release branch for any integration regressions. Re-run the
combined checks after fixes; do not declare the release verified while failures remain.

Fix every regression found before declaring the PR or combined release verified. If several PRs share a root cause, open a dedicated
fix PR against their common integration base, link it from affected PRs, and propagate it into each affected
branch before re-running verification. Do not waive regressions or claim a blocked check passed. Report local
toolchain blockers separately, resolve them where possible, and rerun the affected checks.

### 1. **Before Starting Work**

Inspect the working tree and applicable instructions. Choose the PR base using the rules above and preserve
unrelated work. Do not update dependencies unless the task requires it.

```bash
# Preview checks for the intended PR base (substitute origin/main or origin/release/vX.Y.Z as needed)
make verify-list BASE=origin/develop

# For performance work, record a workload-specific baseline before editing
go test ./pkg/cypher -bench='BenchmarkRelevantName$' -benchmem
```

### 2. **During Development**

```bash
# Run tests for package you're working on
go test ./pkg/cypher -v

# Run specific test
go test ./pkg/cypher -run TestBug_WhereIsNotNull -v

# Watch mode (requires entr or similar)
find . -name "*.go" | entr -c go test ./pkg/cypher -v

# Check for race conditions
go test ./... -race
```

### 3. **Before Committing**

Run `make verify BASE=origin/<pr-base>` for the actual target branch; `make verify` defaults to `origin/main`.
Use `make verify-help` for options and report any skips. Documentation-only changes need the applicable docs
checks; runtime tests, coverage, and benchmarks below apply when the change affects code or performance.

```bash
# Format code
go fmt ./...

# Run linter
golangci-lint run

# Run all tests
go test ./... -v

# Check coverage (must be ≥90%)
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total

# Run benchmarks (compare with baseline)
go test ./pkg/cypher -bench=. -benchmem > after.txt
benchcmp before.txt after.txt

# Build binary
go build -o nornicdb ./cmd/nornicdb
```

### 4. **Commit Message Format**

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**

- `feat`: New feature
- `fix`: Bug fix
- `perf`: Performance improvement
- `refactor`: Code refactoring
- `test`: Adding tests
- `docs`: Documentation changes
- `chore`: Maintenance tasks

**Example:**

```
fix(cypher): resolve WHERE IS NOT NULL with aggregation bug

The WHERE clause with IS NOT NULL was incorrectly filtering out all
results when combined with WITH aggregation. This was caused by the
filter being applied after the aggregation instead of before.

Changes:
- Apply WHERE filters before WITH aggregation
- Add regression test TestBug_WhereIsNotNullWithAggregation
- Update executor to preserve filter order

Fixes #123
Benchmark: No performance impact (same ops/sec)
Coverage: +2.3% (added 15 test cases)
```

---

## 🧪 Testing Philosophy

### Test Pyramid

```
        ┌─────────────┐
        │   E2E (5%)  │  ← Full system tests
        ├─────────────┤
        │ Integration │  ← Multi-component tests
        │    (15%)    │
        ├─────────────┤
        │    Unit     │  ← Single-function tests
        │    (80%)    │
        └─────────────┘
```

### Testing Checklist

For every new feature or bug fix:

- [ ] Unit tests for core logic (80% of tests)
- [ ] Integration tests for component interaction (15% of tests)
- [ ] End-to-end tests for critical paths (5% of tests)
- [ ] Benchmark tests for performance-critical code
- [ ] Race condition tests (`go test -race`)
- [ ] Error path tests (all error returns tested)
- [ ] Edge case tests (nil, empty, boundary values)
- [ ] Regression tests (all bugs get permanent tests)

### Example Test Structure

```go
func TestFeatureName(t *testing.T) {
    // Setup
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()

    t.Run("happy path", func(t *testing.T) {
        // Test normal usage
    })

    t.Run("error: invalid input", func(t *testing.T) {
        // Test error handling
    })

    t.Run("edge case: empty result", func(t *testing.T) {
        // Test edge cases
    })

    t.Run("concurrent access", func(t *testing.T) {
        // Test concurrency
    })
}
```

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for comprehensive testing guide.

---

## 📊 Performance Optimization

### Benchmarking Requirements

**Every performance optimization must include:**

1. **Baseline benchmark** - Before optimization
2. **Optimized benchmark** - After optimization
3. **Comparison** - Show improvement percentage
4. **Memory profile** - Show memory impact
5. **Justification** - Why this optimization matters

**Example:**

```go
// BenchmarkQueryExecution measures query execution performance
func BenchmarkQueryExecution(b *testing.B) {
    store := setupBenchmarkData()
    exec := NewStorageExecutor(store)
    ctx := context.Background()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n LIMIT 100", nil)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// Results:
// Before: 4,252 ops/sec, 2.3 MB/op
// After:  9,780 ops/sec, 1.8 MB/op
// Improvement: 2.3x faster, 22% less memory
```

### Optimization Priorities

1. **Algorithmic improvements** - O(n²) → O(n log n)
2. **Memory allocations** - Reduce allocations in hot paths
3. **Concurrency** - Parallelize independent operations
4. **Caching** - Cache expensive computations
5. **I/O optimization** - Batch operations, reduce syscalls

See [.agents/performance-optimization.md](.agents/performance-optimization.md) for detailed strategies.

---

## 🐛 Bug Fix Workflow

Start with the [mandatory task entry protocol](#mandatory-task-entry-protocol) and
[Rule 73 convergence requirements](#4-convergence-and-rule-73). An active family correction supersedes a
leaf-only interpretation of the five-step process below; it does not waive fail-first reproduction or proof.

**MANDATORY PROCESS** - No exceptions:

### Step 1: Reproduce the Bug

```go
// Create failing test that reproduces the exact bug
func TestBug_DescriptiveName(t *testing.T) {
    // Setup conditions that trigger bug
    store := storage.NewMemoryEngine()
    setupBugConditions(t, store)

    // Execute operation that fails
    result, err := performBuggyOperation()

    // Assert expected behavior (this WILL fail before fix)
    assert.NoError(t, err)
    assert.Equal(t, expectedValue, result)
}
```

### Step 2: Verify Test Fails

```bash
# Run test - it MUST fail
go test ./pkg/cypher -run TestBug_DescriptiveName -v

# Output should show failure:
# --- FAIL: TestBug_DescriptiveName (0.00s)
#     Expected: 2 rows
#     Got: 0 rows
```

### Step 3: Fix the Bug

```go
// Implement minimal fix
// Add comments explaining WHY the bug occurred
func fixedFunction() {
    // BUG FIX: Previously applied filter after aggregation,
    // causing all results to be filtered out. Now apply
    // filter before aggregation as per Cypher semantics.
    applyFilters()  // ← Moved before aggregation
    performAggregation()
}
```

### Step 4: Verify Test Passes

```bash
# Run test - it MUST pass now
go test ./pkg/cypher -run TestBug_DescriptiveName -v

# Output should show success:
# --- PASS: TestBug_DescriptiveName (0.00s)
```

### Step 5: Add Regression Tests

```go
// Add variations to prevent similar bugs
func TestBug_DescriptiveName_Variations(t *testing.T) {
    t.Run("with ORDER BY", func(t *testing.T) { /* ... */ })
    t.Run("with LIMIT", func(t *testing.T) { /* ... */ })
    t.Run("with multiple filters", func(t *testing.T) { /* ... */ })
}
```

See [.agents/bug-fix-workflow.md](.agents/bug-fix-workflow.md) for detailed workflow.

---

## 📚 Additional Resources

### Detailed Guides

- [Testing Patterns](.agents/testing-patterns.md) - Comprehensive testing guide
- [Refactoring Guidelines](.agents/refactoring-guidelines.md) - How to refactor large files
- [Functional Patterns](.agents/functional-patterns.md) - Advanced Go patterns
- [Documentation Standards](.agents/documentation-standards.md) - Documentation requirements
- [Architecture Patterns](.agents/architecture-patterns.md) - System design principles
- [Performance Optimization](.agents/performance-optimization.md) - Optimization strategies
- [Bug Fix Workflow](.agents/bug-fix-workflow.md) - Bug fixing process
- [Cypher Compatibility](.agents/cypher-compatibility.md) - Neo4j compatibility guide

### External Resources

- [Effective Go](https://golang.org/doc/effective_go.html) - Go best practices
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/) - Cypher reference
- [Neo4j Bolt Protocol](https://neo4j.com/docs/bolt/current/) - Protocol specification
- [Go Testing](https://golang.org/pkg/testing/) - Testing package documentation

---

## 🎓 Learning Path for New Contributors

### Week 1: Understanding the Codebase

1. Read this AGENTS.md thoroughly
2. Explore `pkg/storage/types.go` - Core data structures
3. Read `pkg/cypher/executor.go` - Query execution
4. Run test suite: `go test ./... -v`
5. Read existing tests to understand patterns

### Week 2: Making Your First Contribution

1. Find a "good first issue" or documentation improvement
2. Follow the development workflow
3. Write tests first (TDD)
4. Submit PR with benchmarks and coverage report

### Week 3: Advanced Topics

1. Study `pkg/cypher/` - Complex query execution
2. Understand `pkg/storage/badger_engine.go` - Persistence
3. Learn `pkg/embed/` - GPU-accelerated embeddings
4. Contribute a performance optimization

---

## ✅ Pre-Commit Checklist

Before submitting any code:

- [ ] All tests pass: `go test ./... -v`
- [ ] Coverage ≥90%: `go tool cover -func=coverage.out`
- [ ] No race conditions: `go test ./... -race`
- [ ] Code formatted: `go fmt ./...`
- [ ] Linter passes: `golangci-lint run`
- [ ] Benchmarks run: `go test -bench=. -benchmem`
- [ ] Documentation updated (if public API changed)
- [ ] Examples added (if new feature)
- [ ] CHANGELOG.md updated
- [ ] Commit message follows format

---

## 🤝 Code Review Standards

### What Reviewers Look For

1. **Correctness**: Does it work? Are there edge cases?
2. **Tests**: 90%+ coverage? All error paths tested?
3. **Performance**: Benchmarks provided? No regressions?
4. **Documentation**: Public APIs documented? Examples included?
5. **Style**: Follows Go conventions? DRY principle?
6. **Architecture**: Separation of concerns? Clean interfaces?

### Common Review Comments

- "Add test for error case"
- "Extract this repeated logic"
- "Document this public function"
- "Show benchmark results"
- "This file is >2500 lines, please refactor"

---

## 🎯 Success Metrics

### Code Quality Metrics

- **Test Coverage**: ≥90% for all packages
- **Benchmark Performance**: No regressions >5%
- **Memory Usage**: No increases >10% without justification
- **File Size**: No files >2500 lines
- **Documentation**: 100% of public APIs documented

### Compatibility Metrics

- **Neo4j Compatibility**: 100% of supported Cypher features work identically
- **Bolt Protocol**: Full protocol compliance
- **Import/Export**: Neo4j JSON format compatibility

### Performance Metrics

- **Query Execution**: Maintain or improve the current [Northwind benchmark](docs/performance/1.1.0-northwind-results/comparison.md) baseline; report workload, hardware, latency, throughput, and correctness together
- **Retrieval Quality**: Preserve the recorded [BEIR SciFact metrics](docs/performance/retrieval-recall-benchmark.md#recorded-scifact-results) when changing search or ranking behavior
- **Memory and startup**: Measure under a named workload and configuration; do not rely on unverified cross-system headline ranges

---

## 📞 Getting Help

- **Documentation**: Check `docs/` directory first
- **Examples**: See `docs/user-guides/complete-examples.md`
- **Tests**: Read existing tests for patterns
- **Issues**: Search GitHub issues for similar problems
- **Architecture**: Read `.agents/architecture-patterns.md`

---

**Remember**: Quality over speed. Take time to write tests, document code, and prove improvements. NornicDB's reputation depends on reliability and performance.

**Happy coding!**
