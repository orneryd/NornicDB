# Evaluation Plan

This document expands on §7 of the paper. The evaluation is designed for a workshop-style submission: small, reproducible, and focused on demonstrating that NornicDB's primitives correctly implement the claimed persistence semantics. A full comparative benchmark (NornicDB vs. MemGPT, Mem0, Graphiti, etc.) is future work.

---

## Evaluation Strategy

We evaluate **correctness and overhead** of six database-native primitives. We do not evaluate downstream task accuracy (e.g., QA performance) because the claim is architectural, not accuracy-based. The claim is: "these persistence semantics can be expressed as database primitives." The evaluation verifies that the primitives behave correctly and that the overhead is acceptable.

---

## E1: Supersession Query Correctness

**What:** Given a sequence of fact versions for the same `FactKey`, verify that:
- `CURRENT` always points to the latest version.
- Superseded versions are linked via `SUPERSEDES` edges (new version → old version).
- Point-in-time queries (`AS OF <timestamp>`) return the unique version valid at that time.
- No version is deleted or modified after supersession.

**Method:** Generate N fact keys, each with K sequential versions (varying validity windows). For each key, issue:
- A `CURRENT` query (should return version K).
- An `AS OF` query for each validity window (should return the corresponding version).
- A full-history query (should return all K versions in order).

**Parameters:** N ∈ {100, 1000, 10000}, K ∈ {2, 5, 10, 50}.

**Pass criteria:** 100% correctness on all queries. This is a database correctness test, not a statistical evaluation.

---

## E2: Temporal No-Overlap Constraint

**What:** Verify that the `TEMPORAL NO OVERLAP` constraint correctly rejects writes that would create overlapping validity windows for the same `FactKey`.

**Method:**
- Attempt to create two `FactVersion` nodes for the same key with overlapping `validFrom`/`validUntil` ranges.
- Verify that the write is rejected with an appropriate error.
- Verify that non-overlapping versions for the same key are accepted.
- Verify that overlapping versions for *different* keys are accepted (overlap is per-key, not global).

**Pass criteria:** All invalid writes rejected, all valid writes accepted.

---

## E3: Mutation Log Replay

**What:** Verify that the mutation log is a faithful record of all writes, and that replaying it from empty state produces an identical graph.

**Method:**
1. Execute a sequence of M operations (creates, supersessions, suppressions, restores) on a fresh graph.
2. Checkpoint the graph state (node/edge counts, property values, all `CURRENT` pointers).
3. Create a new empty graph.
4. Replay the mutation log entry by entry.
5. Verify that the replayed graph matches the checkpointed state at each intermediate point.

**Parameters:** M ∈ {100, 1000, 10000}. Operations drawn from a weighted distribution: 50% create, 30% supersede, 10% suppress, 10% restore.

**Pass criteria:** Byte-level match at each checkpoint. Any divergence is a correctness failure.

---

## E4: Scoring-Before-Visibility (Suppression and Reveal)

**What:** Verify that:
- Entities whose score falls below the retrieval threshold are absent from default queries.
- `reveal()` returns all entities regardless of score.
- Compliance-deleted entities are absent from both default and `reveal()` queries.
- `ON ACCESS` mutations do not execute for suppressed entities.

**Method:**
1. Create entities with varying decay profiles and ages (above threshold, below threshold, at threshold).
2. Issue default `MATCH` queries and verify only above-threshold entities appear.
3. Issue `MATCH ... RETURN reveal(n)` queries and verify all non-deleted entities appear.
4. Compliance-delete some entities and verify they are absent from both query types.
5. Verify that suppressed entities have not accumulated `accessCount` increments via `policy()`.

**Additional tests:**
- Apply an Ebbinghaus decay profile to memory entities and verify that their scores drop below threshold after the expected elapsed time (within floating-point tolerance).
- Verify that `decayScore(n)` returns `1.0` and `decay(n).applies` returns `false` when `DecayEnabled = false`.
- Verify that `WHERE decayScore(n) < 0.5` returns empty results when decay is disabled (correct behavior, not an error).

**Pass criteria:** 100% correctness on visibility semantics.

---

## E5: Latency Overhead of Scoring Pipeline

**What:** Measure the added latency of the scoring-before-visibility pipeline relative to unscored queries.

**Method:**
- Create graphs of size S ∈ {1K, 10K, 100K, 1M} entities.
- For each graph, run 1000 retrieval queries with scoring enabled and 1000 without.
- Report median, p95, and p99 latency for each condition.
- Report the overhead ratio (scored / unscored) at each graph size.

**Acceptable overhead:** The three-tier optimization minimizes the hot path:
- Tier 2 (suppressed bit): 1 byte check per entity.
- Tier 1 (compiled binding table): 1 map lookup per entity.
- Tier 3 (integer age comparison): 1 integer subtraction per entity.
- Precise `math.Exp` score: computed only for entities that survive visibility and are projected into results (lazy scoring).

Target: < 2× overhead at p95 for result sets ≤ 100 entities.

**AccessMeta hot-path benchmark:** Verify that per-P sharded counter ring achieves <30ns per increment. Super-node scenario: 128 goroutines incrementing the same entityID concurrently should show zero throughput degradation vs. 128 goroutines × 128 distinct entityIDs (no cache-line contention).

**Kalman mutation overhead:** Single Kalman filter step (including KalmanFilters map read/write) target: <500ns. Compared to plain accumulation: should be <5× overhead.

---

## E6: Kalman Smoothing Robustness

**What:** Verify that the Kalman filter dampens synthetic confidence spikes below promotion thresholds while allowing genuine sustained signals to pass.

### E6.1: Spike Dampening

**Method:**
1. Initialize a Kalman filter with default parameters (auto mode, Q=0.0001, R seeded at 88.0, P seeded at 30.0).
2. Feed N baseline observations at confidence ~0.5–0.6 (with small Gaussian noise ε ~ N(0, 0.02)).
3. At observation N+1, inject a single spike at 0.99.
4. Verify that the post-spike estimate is below the promotion threshold (expected: ≈ 0.63 for the trace in §4.3).
5. Feed M more baseline observations and verify the estimate returns to baseline within 3–5 observations.
6. Repeat with varying spike magnitudes (0.8, 0.9, 0.95, 0.99, 1.0) and varying N (5, 10, 20, 50).

**Pass criteria:** Single spikes dampened below threshold in all cases.

### E6.2: Sustained Signal Pass-Through

**Method:**
1. Feed a sequence of sustained high-confidence observations (10 consecutive 0.95 readings from independent sources, i.e., each from a different `$_session`).
2. Verify that the estimate eventually rises above the promotion threshold.

**Pass criteria:** Sustained independent signals cross threshold. The filter must not suppress genuine behavioral trends.

### E6.3: Session Gating Verification

**Method:**
1. Create a promotion policy with `WITH KALMAN SET n.crossSessionRate = CASE WHEN n._lastSessionId <> $_session THEN ... END`.
2. Process 50 accesses from `session-A`, 1 from `session-B`, 1 from `session-C`.
3. Verify `crossSessionRate` reflects 3 sessions (not 52 accesses).
4. Verify that the Kalman filter smooths the genuine cross-session increment correctly.

**Pass criteria:** Same-session accesses gated out. Only new sessions contribute measurements to the filter.

### E6.4: End-to-End Anti-Sycophancy Scenario

**Method:**
1. Create a promotion policy with `WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence`.
2. Process 100 accesses with stable confidence ~0.6.
3. Inject hallucinated spike at access 50 (confidence jumps to 0.99).
4. Assert filtered `confidenceScore` stays near 0.6, not 0.99.
5. Assert by access 55 the filter has recovered.
6. Assert `policy(n).confidenceScore` returns the filtered value.
7. Assert `policy(n).kalmanFilters.confidenceScore` exposes full `KalmanPropertyState` for diagnostics (filteredValue, filter state with gain/covariance/observations, and variance tracker state with window/current variance).

**Pass criteria:** No promotion triggered by hallucinated spike. Full diagnostic state accessible.

---

## What This Evaluation Does Not Cover

- **Downstream task accuracy.** We do not evaluate whether NornicDB improves QA accuracy, decision quality, or agent performance. The claim is about persistence semantics, not task performance.
- **Comparative benchmarks.** We do not compare NornicDB against MemGPT, Mem0, Graphiti, or TG-RAG on shared benchmarks. This is future work. The BEAM benchmark's near-zero contradiction-resolution scores [3] suggest a natural evaluation target.
- **Scale beyond 1M entities.** Production graphs may be larger; scaling evaluation is deferred.
- **Evidence extraction quality.** The anti-sycophancy guarantee depends on upstream evidence extraction. We test the database's behavior given correct evidence inputs, not the quality of the upstream pipeline.
- **Multi-label conflict resolution.** The binding builder's deterministic Order-based tie-breaking for multi-label nodes is tested in the implementation's unit test suite but not in this evaluation.
