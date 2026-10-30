# Related Work — Expanded Notes

These notes expand on §2 of the main paper. They are organized by research thread and include detailed summaries of each source paper, key claims, and specific connections to NornicDB's design.

---

## Thread 1: Temporal GraphRAG and Time-Sensitive Retrieval

### TG-RAG (Han et al., arXiv:2510.13590, October 2025)

**Core problem:** Current RAG systems treat knowledge as static. The same entity at different times (e.g., a company's revenue in 2021 vs. 2022) is indistinguishable in vector embeddings or conventional knowledge graphs.

**Proposed solution:** Temporal GraphRAG (TG-RAG) models corpora as a bi-level temporal graph:
1. A *temporal knowledge graph* with timestamped relations (entity-relation-entity-time tuples).
2. A *hierarchical time graph* with multi-granularity temporal summaries (day → month → quarter → year).

**Key design properties:**
- Identical facts at different times are represented as distinct edges, avoiding temporal ambiguity.
- Incremental updates extract new temporal facts from incoming corpora and merge them into the existing graph. Only new leaf time nodes and their ancestors need re-summarization.
- Inference-time retrieval dynamically selects a subgraph within the temporal and semantic scope of the query.

**Evaluation:** TG-RAG introduces ECT-QA, a time-sensitive QA dataset with both specific and abstract queries, plus a protocol for evaluating incremental update capabilities. TG-RAG significantly outperforms baselines on temporal questions.

**Connection to NornicDB:**
- NornicDB's `FactVersion` with validity windows (`validFrom`, `validUntil`) addresses the same temporal ambiguity problem, but at the storage layer rather than the application layer.
- The `TEMPORAL NO OVERLAP` constraint on versions sharing a `FactKey` is the database-native equivalent of TG-RAG's distinct temporal edges. The constraint is enforced at write time.
- NornicDB's mutation log supports incremental updates without full reindex — new facts are appended and indexed, with exact `IndexEntryCatalog` entries (Badger prefix `0x12`) tracking which secondary-index keys correspond to which graph entities. Deindexing uses blind batched deletes against exact keys, not full-index scans.
- Time-travel queries (`GetNodesByLabelVisibleAt`) iterate MVCC version records directly with iterator pinning, enabling point-in-time retrieval without touching secondary indexes.
- TG-RAG's hierarchical time summaries are an application-layer concern that could be built *on top of* NornicDB's temporal graph, but are not part of the storage substrate.

---

## Thread 2: Enterprise Replayable Memory (DPM)

### Deterministic Projection Memory (Srinivasan, arXiv:2604.20158, April 2026)

**Core problem:** Enterprise deployment of decision agents in regulated domains (underwriting, claims adjudication, clinical review) is dominated by retrieval-augmented pipelines despite a decade of sophisticated stateful memory research. The gap is explained by four systems properties the research evaluation has underweighted:
1. *Deterministic replay* — a denied applicant can be re-scored and the same decision justified.
2. *Auditable rationale* — a regulator or court can inspect the decision trail.
3. *Multi-tenant isolation* — one applicant's data cannot leak into another's decision.
4. *Statelessness for horizontal scale* — no shared mutable memory bottleneck.

**Proposed solution:** DPM treats memory as:
1. An *append-only event log* that accumulates raw events in arrival order. The log is immutable.
2. A *task-conditioned projection* π(E, T, B) → M applied at decision time. One LLM call at temperature zero produces a structured memory view (facts / reasoning / compliance notes) within a budget.

**Key results:**
- At generous memory budgets, DPM matches summarization-based memory (no significant difference on four decision-alignment axes, n=10).
- At tight budgets (20× compression), DPM improves factual precision by +0.52 (Cohen's h=1.17, p=0.0014) and reasoning coherence by +0.53 (h=1.13, p=0.0034).
- DPM is 7–15× faster (one LLM call vs. N calls across the trajectory).
- A determinism study confirms both conditions inherit residual API nondeterminism, but DPM exposes *one* nondeterministic call vs. N compounding calls.

**TAMS heuristic:** A practitioner decision rule for architecture selection based on task properties.

**Connection to NornicDB:**
- NornicDB's WAL + mutation log implement the same append-only event log pattern, but as a durable database structure rather than an in-memory trajectory log.
- WAL receipts provide the replay surface DPM requires: any past state can be reconstructed by replaying the log.
- Multi-tenant isolation is structural in NornicDB via tenant/session/agent context on all entities.
- DPM's projection-at-read-time model is analogous to NornicDB's scoring-before-visibility: both defer policy application to query time rather than write time.
- The key difference: DPM is an architecture for a single decision trajectory. NornicDB is a persistent substrate across trajectories, agents, and tenants. DPM's event log could be *stored in* NornicDB.

---

## Thread 3: The Missing Knowledge Layer

### Roynard (arXiv:2604.11364, April 2026)

**Core problem:** CoALA and JEPA both lack an explicit Knowledge layer with its own persistence semantics. This produces a category error: systems apply cognitive decay to factual claims, or treat facts and experiences with identical update mechanics.

**The critique of NornicDB (§1):** Roynard uses NornicDB's original three-tier decay model as a motivating example: "episodic memories receive a 7-day half-life, semantic memories a 69-day half-life, and procedural memories a 693-day half-life." The critique: "a paper's findings do not become less true after 69 days." The conflation of "I have not accessed this recently" with "this is less valuable" is a category error.

**Four-layer decomposition (§3, Table 1):**

| Layer | Definition | Persistence | Update mechanism | Scope |
|-------|-----------|------------|-----------------|-------|
| Knowledge | What is true | Indefinite; supersession | Append-only + provenance | Shared |
| Memory | What happened | Ebbinghaus decay | Bi-temporal event sourcing | Per-agent |
| Wisdom | What works | Durable; revision-gated | Evidence-threshold review | Multi-source |
| Intelligence | Capacity to reason | Ephemeral | N/A | Per-invocation |

**Key arguments:**
- Knowledge does not decay. Facts get superseded, which is qualitatively different from forgetting.
- Memory decays via Ebbinghaus forgetting curve unless consolidated through reinforcement.
- Wisdom does not decay but updates via evidence-gated revision. Access count ≠ evidence (sycophancy concern, citing Cheng et al., Science 2026).
- Intelligence is out of persistence scope — it is the model's inference-time capacity.
- The same observation can produce entries in both Knowledge and Wisdom with different persistence semantics (e.g., "gradient clipping above 1.0 destabilizes training" is Knowledge; "set gradient clipping to 1.0 or below" is a Wisdom directive).

**Eight convergence points (§4):** Roynard surveys 9 independent sources (including Karpathy's LLM Knowledge Base, BEAM benchmark near-zero contradiction-resolution scores) that point to the same architectural gap.

**Connection to NornicDB:**
- NornicDB's April redesign directly responds to Roynard's critique. The redesigned scoring subsystem assigns per-content-type decay profiles: `NO DECAY` for Knowledge, Ebbinghaus for Memory, evidence-gated stability tiers for Wisdom.
- The `FactKey`/`FactVersion`/`CURRENT` structure implements supersession as graph structure rather than fact-property annotation.
- `scoreFrom: 'VERSION'` on `:MemoryEpisode` combined with MVCC-snapshot visibility provides the bi-temporal model Roynard calls for.
- The Wisdom stability tiers (`evidenceCount`, `contradictionRate`, `crossSessionSupport`) implement evidence-gated revision with explicit anti-sycophancy: access does not increment evidence.

### Roynard's Response to NornicDB (April 2026)

Roynard reviewed the full NornicDB implementation plan and provided detailed feedback. Key points:

**Strong alignment noted:**
- Appendix A's four-layer profile set is "a four-different-update-mechanisms mapping, which is precisely what the paper's persistence-semantics table argues for."
- The canonical graph ledger is "a legitimate supersession mechanism" — different engineering shape from four-timestamp-per-fact but addressing the same semantic requirement.
- The compliance/scoring separation (`reveal()` bypasses only the scoring gate) is "a genuinely useful engineering distinction."
- `scoreFrom: 'VERSION'` is "closer to the paper's bi-temporal requirement than the original hardcoded-tier model ever was."

**Architectural concern raised:**
- Evidence provenance for Wisdom stability tiers: if external processes treat agent repetition (correlated reasoning paths) as evidence, the sycophancy concern returns. The architecture is sound; the operational instantiation is where the failure mode would appear.

**Clarifying questions:**
1. How do Appendix A's `supersededBy` property and the canonical graph ledger's `FactKey`/`FactVersion`/`CURRENT` pattern relate? (Two patterns for the same semantic — are they alternatives or does one resolve to the other?)
2. Is the `evidenceCount`/`contradictionRate`/`crossSessionSupport` contract in scope or deferred?

These questions should be addressed in the paper.

---

## Thread 4: Existing Graph Memory Systems

### MemGPT (Packer et al., 2023)
OS-style memory management: managed working set + persistent archival store. Pioneered the idea of LLM-managed memory tiers. Stateful by construction.

### Mem0
Unified memory layer with CRUD operations applied identically to facts and experiences. No persistence-semantic distinction between content types.

### Graphiti (Zep, 2024)
Bi-temporal event sourcing for graph-based agent memory. Introduces four timestamps (system-created, system-expired, real-world-valid, real-world-invalid). Closest to NornicDB's temporal model among existing systems, but operates as an application layer on top of Neo4j rather than as database-native primitives.

### GAM (Graph Agent Memory)
Heterogeneous graph with traversal-based retrieval. Facts stored as typed nodes and edges. No temporal validity, no decay profiles, no supersession.

### HyMem
Retrieval-addressable and summarization-addressable memory tiers. No persistence-semantic separation.

### Signet
Entity-aspect-attribute graph with partial supersession but uniform 0.95^days decay to all content types. The decay uniformity is precisely the category error Roynard identifies.

### Ori Mnemos
Three-zone decay rates but all data types in one graph without formal layer separation.

### A-Mem
Agentic memory operators for self-organizing memory. Novel interface but no persistence-semantic guarantees.

### MEM1
Co-trains memory and reasoning. Interesting research direction but stateful and not designed for enterprise properties.

---

## Cross-Cutting Observations

1. **No existing system provides configurable persistence semantics at the database layer.** All implement fixed update mechanics (either uniform decay, no decay, or hard-coded tiers).

2. **Temporal modeling is consistently application-layer.** TG-RAG, Graphiti, and GAM all implement temporal semantics above the database, not in it.

3. **Append-only immutability is validated by DPM but not adopted by memory systems.** DPM shows that append-only logs with projection-at-read are sufficient for enterprise requirements, but existing memory systems continue to use mutable state.

4. **The sycophancy concern is raised independently by Roynard and implied by DPM.** Roynard frames it as "access ≠ evidence." DPM frames it as "the projection is pure — it does not carry state from previous invocations." Both argue against path-dependent memory evolution.
