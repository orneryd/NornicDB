# NornicDB Post-TLP Integration Roadmap

**Date**: November 26, 2025  
**Status**: Planning Phase  
**Prerequisite**: Complete Topological Link Prediction (TLP) Integration

---

## Executive Summary

This roadmap outlines features to implement **after** the current TLP integration is complete. These enhancements make NornicDB's auto-edge/decay system production-grade for LLM/agent workflows.

The TLP integration (in `pkg/linkpredict/`) is adding:

- ✅ `topology.go` - 5 canonical algorithms (Common Neighbors, Jaccard, Adamic-Adar, Preferential Attachment, Resource Allocation)
- ✅ `hybrid.go` - Blends topological + semantic signals
- 🔄 Tests and benchmarks (in progress)

**Do not modify `pkg/linkpredict/` until TLP integration is merged.**

---

## Priority Tiers

### 🔴 Tier 1: Critical (Implement First) — ~1 week

These are foundational for safe production use.

| Feature                         | Effort   | Files to Create/Modify       |
| ------------------------------- | -------- | ---------------------------- |
| Edge Provenance Logging         | 1 day    | `pkg/storage/edge_meta.go`   |
| Per-Node Config (pin/deny/caps) | 1 day    | `pkg/storage/node_config.go` |
| Cooldown Logic                  | 0.5 day  | `pkg/inference/cooldown.go`  |
| Evidence Buffering              | 1 day    | `pkg/inference/evidence.go`  |
| WAL + Snapshots                 | 2-3 days | `pkg/storage/wal.go`         |

### 🟡 Tier 2: High Value (Implement Second) — ~1-2 weeks

These significantly improve quality and observability.

| Feature                    | Effort  | Files to Create/Modify |
| -------------------------- | ------- | ---------------------- |
| MMR Diversification        | 1 day   | `pkg/search/mmr.go`    |
| RRF (BM25 + Vector Fusion) | 2 days  | `pkg/search/rrf.go`    |
| Cross-Encoder Rerank       | 3 days  | `pkg/search/rerank.go` |
| Index Stats Exposure       | 0.5 day | `pkg/index/stats.go`   |
| Eval Harness               | 2 days  | `eval/` directory      |

### 🟢 Tier 3: Nice to Have (Future) — ~2+ weeks

| Feature                      | Effort    | Notes                                      |
| ---------------------------- | --------- | ------------------------------------------ |
| Extended OpenCypher/Bolt     | 1-2 weeks | Already have `pkg/bolt/` and `pkg/cypher/` |
| GPU→CPU Fallback             | 3-7 days  | Extend `pkg/gpu/`                          |
| Cross-Encoder GPU Service    | 1 week    | Dedicated microservice                     |
| Link Prediction AUC Pipeline | 1 week    | Academic-grade evaluation                  |

---

## Tier 1: Detailed Implementation Specs

### 1.1 Edge Provenance Logging

**Why**: Every auto-generated edge needs an audit trail (why it exists, when, what evidence).

**Create**: `pkg/storage/edge_meta.go`

```go
// EdgeMeta stores provenance for auto-generated edges.
// This is append-only for auditability.
type EdgeMeta struct {
    EdgeID       string    `json:"edge_id"`
    Src          string    `json:"src"`
    Dst          string    `json:"dst"`
    Label        string    `json:"label"`
    Score        float64   `json:"score"`
    SignalType   string    `json:"signal_type"` // "coaccess", "similarity", "topology", "llm-infer"
    Timestamp    time.Time `json:"timestamp"`
    SessionID    string    `json:"session_id,omitempty"`
    EvidenceCount int      `json:"evidence_count"`
    DecayState   float64   `json:"decay_state"`
    Materialized bool      `json:"materialized"`
    Origin       string    `json:"origin"` // agent/commit ID

    // TLP-specific (post-integration)
    TopologyAlgorithm string  `json:"topology_algorithm,omitempty"`
    TopologyScore     float64 `json:"topology_score,omitempty"`
    SemanticScore     float64 `json:"semantic_score,omitempty"`
}

// EdgeMetaStore manages edge provenance.
type EdgeMetaStore interface {
    // Append adds a new evidence record (immutable)
    Append(ctx context.Context, meta EdgeMeta) error

    // GetHistory returns all evidence for an edge
    GetHistory(ctx context.Context, src, dst, label string) ([]EdgeMeta, error)

    // GetBySignalType filters by how the edge was created
    GetBySignalType(ctx context.Context, signalType string, limit int) ([]EdgeMeta, error)
}
```

**Integration Point**: Call `Append()` from:

- `pkg/inference/` when creating edges
- `pkg/linkpredict/hybrid.go` when materializing predictions (after TLP merge)

---

### 1.2 Per-Node Configuration

**Why**: Prevent spam, honor user preferences, enforce limits.

**Create**: `pkg/storage/node_config.go`

```go
// NodeConfig holds per-node materialization rules.
type NodeConfig struct {
    NodeID      string

    // Pin list: edges to these targets never decay
    Pins        map[string]bool

    // Deny list: never create edges to these targets
    Deny        map[string]bool

    // Edge caps (0 = unlimited)
    MaxOutEdges int
    MaxInEdges  int

    // Per-label caps
    LabelCaps   map[string]int

    // Cooldown between same-pair materializations
    CooldownMS  int64

    // Trust level affects thresholds
    TrustLevel  TrustLevel // "admin", "verified", "inferred"
}

type TrustLevel string
const (
    TrustAdmin    TrustLevel = "admin"    // Immediate materialization
    TrustVerified TrustLevel = "verified" // Standard thresholds
    TrustInferred TrustLevel = "inferred" // Higher thresholds required
)

// NodeConfigStore manages per-node configuration.
type NodeConfigStore interface {
    Get(ctx context.Context, nodeID string) (*NodeConfig, error)
    Set(ctx context.Context, config NodeConfig) error
    Delete(ctx context.Context, nodeID string) error
}

// AllowMaterialize checks if edge creation is allowed.
func AllowMaterialize(ctx context.Context, store NodeConfigStore, src, dst, label string) (bool, string) {
    cfg, _ := store.Get(ctx, src)
    if cfg == nil {
        return true, "" // No restrictions
    }

    // Check deny list
    if cfg.Deny[dst] {
        return false, "target in deny list"
    }

    // Check caps (would need edge count lookup)
    // ...

    return true, ""
}
```

---

### 1.3 Cooldown Logic

**Why**: Prevent echo chambers from rapid co-access bursts.

**Create**: `pkg/inference/cooldown.go`

```go
// CooldownTable tracks when edges were last materialized.
type CooldownTable struct {
    mu       sync.RWMutex
    entries  map[string]time.Time // key: "src:dst:label"
    defaults map[string]time.Duration // per-label defaults
}

func NewCooldownTable() *CooldownTable {
    return &CooldownTable{
        entries: make(map[string]time.Time),
        defaults: map[string]time.Duration{
            "relates_to":  5 * time.Minute,
            "similar_to":  10 * time.Minute,
            "coaccess":    1 * time.Minute,
            "topology":    15 * time.Minute, // For TLP edges
        },
    }
}

func (ct *CooldownTable) CanMaterialize(src, dst, label string) bool {
    ct.mu.RLock()
    defer ct.mu.RUnlock()

    key := fmt.Sprintf("%s:%s:%s", src, dst, label)
    last, exists := ct.entries[key]
    if !exists {
        return true
    }

    cooldown := ct.defaults[label]
    if cooldown == 0 {
        cooldown = 5 * time.Minute // default
    }

    return time.Since(last) >= cooldown
}

func (ct *CooldownTable) RecordMaterialization(src, dst, label string) {
    ct.mu.Lock()
    defer ct.mu.Unlock()

    key := fmt.Sprintf("%s:%s:%s", src, dst, label)
    ct.entries[key] = time.Now()
}
```

---

### 1.4 Evidence Buffering

**Why**: Only materialize edges after repeated evidence (reduces false positives).

**Create**: `pkg/inference/evidence.go`

```go
// EvidenceBuffer accumulates signals before materialization.
type EvidenceBuffer struct {
    mu      sync.RWMutex
    entries map[EvidenceKey]*Evidence

    // Thresholds per label
    thresholds map[string]EvidenceThreshold
}

type EvidenceKey struct {
    Src, Dst, Label string
}

type Evidence struct {
    Count     int
    ScoreSum  float64
    FirstTs   time.Time
    LastTs    time.Time
    Sessions  map[string]bool // Unique sessions
    Signals   []string        // Signal types seen
}

type EvidenceThreshold struct {
    MinCount    int     // Minimum evidence count
    MinScore    float64 // Minimum cumulative score
    MinSessions int     // Minimum unique sessions
    MaxAge      time.Duration // Evidence expires after this
}

func (eb *EvidenceBuffer) AddEvidence(src, dst, label string, score float64, signalType, sessionID string) bool {
    eb.mu.Lock()
    defer eb.mu.Unlock()

    key := EvidenceKey{src, dst, label}
    ev, exists := eb.entries[key]
    if !exists {
        ev = &Evidence{
            FirstTs:  time.Now(),
            Sessions: make(map[string]bool),
        }
        eb.entries[key] = ev
    }

    ev.Count++
    ev.ScoreSum += score
    ev.LastTs = time.Now()
    ev.Sessions[sessionID] = true
    ev.Signals = append(ev.Signals, signalType)

    return eb.shouldMaterialize(key, ev)
}

func (eb *EvidenceBuffer) shouldMaterialize(key EvidenceKey, ev *Evidence) bool {
    thresh := eb.thresholds[key.Label]
    if thresh.MinCount == 0 {
        thresh = EvidenceThreshold{MinCount: 3, MinScore: 0.5, MinSessions: 2}
    }

    if ev.Count < thresh.MinCount {
        return false
    }
    if ev.ScoreSum < thresh.MinScore {
        return false
    }
    if len(ev.Sessions) < thresh.MinSessions {
        return false
    }

    return true
}
```

---

### 1.5 WAL + Snapshots

**Why**: Durability and crash recovery.

**Create**: `pkg/storage/wal.go`

```go
// WAL provides write-ahead logging for durability.
type WAL struct {
    mu       sync.Mutex
    file     *os.File
    encoder  *json.Encoder
    sequence uint64
}

type WALEntry struct {
    Sequence  uint64    `json:"seq"`
    Timestamp time.Time `json:"ts"`
    Operation string    `json:"op"` // "create_node", "create_edge", "update", "delete"
    Data      []byte    `json:"data"`
}

func (w *WAL) Append(op string, data interface{}) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    w.sequence++
    entry := WALEntry{
        Sequence:  w.sequence,
        Timestamp: time.Now(),
        Operation: op,
        Data:      mustMarshal(data),
    }

    return w.encoder.Encode(entry)
}

// Snapshot creates a point-in-time snapshot.
type Snapshot struct {
    Sequence  uint64    `json:"sequence"`
    Timestamp time.Time `json:"timestamp"`
    Nodes     []Node    `json:"nodes"`
    Edges     []Edge    `json:"edges"`
}

func (w *WAL) CreateSnapshot(engine Engine) (*Snapshot, error) {
    // ... serialize current state
}

func (w *WAL) Recover(snapshotPath string) ([]WALEntry, error) {
    // Load snapshot, then replay WAL entries after snapshot sequence
}
```

**Test**: `pkg/storage/wal_test.go`

```go
func TestWAL_CrashRecovery(t *testing.T) {
    // 1. Create WAL and write entries
    // 2. Simulate crash (close without flush)
    // 3. Recover and verify state matches expected
}
```

---

## Tier 2: Implementation Specs

### 2.1 MMR Diversification

**Location**: `pkg/search/mmr.go`

Maximal Marginal Relevance prevents redundant results:

```go
// MMR selects diverse results from candidates.
// λ=1.0 = pure relevance, λ=0.0 = pure diversity
func MMR(candidates []SearchResult, lambda float64, k int) []SearchResult {
    selected := make([]SearchResult, 0, k)
    remaining := make([]SearchResult, len(candidates))
    copy(remaining, candidates)

    for len(selected) < k && len(remaining) > 0 {
        bestIdx := -1
        bestScore := math.Inf(-1)

        for i, cand := range remaining {
            relevance := cand.Score
            maxSim := maxSimilarityToSelected(cand, selected)
            mmrScore := lambda*relevance - (1-lambda)*maxSim

            if mmrScore > bestScore {
                bestScore = mmrScore
                bestIdx = i
            }
        }

        selected = append(selected, remaining[bestIdx])
        remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
    }

    return selected
}
```

### 2.2 RRF Fusion

**Location**: `pkg/search/rrf.go`

Reciprocal Rank Fusion combines BM25 + vector rankings:

```go
// RRF fuses multiple ranked lists.
// k is a smoothing constant (typically 60).
func RRF(lists [][]SearchResult, k float64) []SearchResult {
    scores := make(map[string]float64)
    items := make(map[string]SearchResult)

    for _, list := range lists {
        for rank, item := range list {
            score := 1.0 / (k + float64(rank+1))
            scores[item.ID] += score
            items[item.ID] = item
        }
    }

    // Sort by fused score
    // ...
}
```

### 2.3 Eval Harness

**Create**: `eval/` directory

```
eval/
├── recall_at_k.go      # Recall@1/5/10 for retrieval
├── link_auc.go         # Link prediction AUC (uses TLP!)
├── rag_faithfulness.go # RAG answer quality
├── latency_bench.go    # p50/p95/p99 for mixed workloads
└── run_eval.go         # CLI runner
```

---

## Integration with TLP

Once TLP is merged, these features integrate as follows:

### Edge Provenance + TLP

```go
// In hybrid.go Predict(), after materialization:
meta := EdgeMeta{
    Src:               source,
    Dst:               pred.TargetID,
    Label:             "topology_suggested",
    Score:             pred.Score,
    SignalType:        "topology",
    TopologyAlgorithm: pred.TopologyMethod,
    TopologyScore:     pred.TopologyScore,
    SemanticScore:     pred.SemanticScore,
    // ...
}
metaStore.Append(ctx, meta)
```

### Evidence Buffer + TLP

```go
// TLP predictions feed into evidence buffer
for _, pred := range hybridScorer.Predict(ctx, graph, source, 20) {
    shouldMaterialize := evidenceBuffer.AddEvidence(
        source, pred.TargetID, "topology",
        pred.Score, pred.TopologyMethod, sessionID,
    )

    if shouldMaterialize && cooldownTable.CanMaterialize(...) {
        // Actually create the edge
    }
}
```

### Eval Harness + TLP

```go
// eval/link_auc.go
func EvaluateTLP(graph Graph, testEdges []Edge) float64 {
    scorer := linkpredict.NewHybridScorer(linkpredict.DefaultHybridConfig())
    // ... compute AUC on held-out edges
}
```

---

## File Locations Summary

```
pkg/
├── storage/
│   ├── edge_meta.go      # NEW: Edge provenance
│   ├── node_config.go    # NEW: Per-node config
│   └── wal.go            # NEW: Write-ahead log
├── inference/
│   ├── cooldown.go       # NEW: Cooldown logic
│   └── evidence.go       # NEW: Evidence buffering
├── search/
│   ├── mmr.go            # NEW: MMR diversification
│   ├── rrf.go            # NEW: RRF fusion
│   └── rerank.go         # NEW: Cross-encoder rerank
├── index/
│   └── stats.go          # NEW: Index statistics
└── linkpredict/
    ├── topology.go       # EXISTING (TLP)
    └── hybrid.go         # EXISTING (TLP)

eval/                     # NEW: Evaluation harness
├── recall_at_k.go
├── link_auc.go
├── rag_faithfulness.go
├── latency_bench.go
└── run_eval.go
```

---

## Timeline

| Week | Focus                                 |
| ---- | ------------------------------------- |
| 1    | Complete TLP integration (current)    |
| 2    | Tier 1: Provenance, Config, Cooldown  |
| 3    | Tier 1: Evidence, WAL                 |
| 4    | Tier 2: MMR, RRF                      |
| 5    | Tier 2: Rerank, Stats, Eval           |
| 6+   | Tier 3: Extended Cypher, GPU fallback |

---

## Notes for the TLP Agent

**DO NOT** implement these features until TLP is merged. Once merged:

1. Add `TopologyAlgorithm`, `TopologyScore`, `SemanticScore` fields to EdgeMeta
2. Wire `hybrid.go` materialization to use EvidenceBuffer + CooldownTable
3. Add eval tests that exercise TLP algorithms

The architecture is designed so TLP and these features compose cleanly.

---

## Questions for Review

1. Should EdgeMeta be stored in the same engine or a separate audit log?
2. Should per-node config be in YAML files or database?
3. What's the default evidence threshold for TLP-generated edges?
4. Should we expose cooldown settings via API?

---

_Last updated: November 26, 2025_
