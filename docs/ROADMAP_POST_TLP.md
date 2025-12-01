# NornicDB Post-TLP Integration Roadmap

**Date**: December 1, 2025  
**Status**: Tier 1 Complete, Tier 2 In Progress  
**Last Updated**: December 1, 2025

---

## Executive Summary

This roadmap tracks features implemented **after** the TLP (Topological Link Prediction) integration. These enhancements make NornicDB's auto-edge/decay system production-grade for LLM/agent workflows.

### Current Status Overview

```
Tier 1 (Critical):     █████████████████████ 100% (5/5) ✅
Tier 2 (High Value):   ████████░░░░░░░░░░░░░  40% (2/5)
Tier 3 (Nice to Have): █████████████████░░░░  80% (4/5)
TLP Integration:       █████████████████████ 100% ✅
```

---

## ✅ PREREQUISITE: TLP Integration - COMPLETE

The TLP integration in `pkg/linkpredict/` is **fully implemented**:

| File | Lines | Status | Description |
|------|-------|--------|-------------|
| `topology.go` | 722 | ✅ Done | 5 canonical algorithms |
| `hybrid.go` | 477 | ✅ Done | Topology + Semantic fusion |
| `graph_builder.go` | 820 | ✅ Done | Graph construction helpers |
| `*_test.go` | 3,557 | ✅ Done | Comprehensive test coverage |

**Algorithms Implemented:**
- ✅ Common Neighbors
- ✅ Jaccard Coefficient
- ✅ Adamic-Adar
- ✅ Resource Allocation
- ✅ Preferential Attachment
- ✅ Hybrid (Topology + Semantic)

---

## 🔴 Tier 1: Critical - COMPLETE ✅

All foundational features for safe production use are implemented.

| Feature | File | Lines | Tests | Status |
|---------|------|-------|-------|--------|
| Edge Provenance Logging | `pkg/storage/edge_meta.go` | 482 | 580 | ✅ Done |
| Per-Node Config (pin/deny/caps) | `pkg/storage/node_config.go` | 876 | 885 | ✅ Done |
| Cooldown Logic | `pkg/inference/cooldown.go` | 408 | 324 | ✅ Done |
| Evidence Buffering | `pkg/inference/evidence.go` | 645 | 390 | ✅ Done |
| WAL + Snapshots | `pkg/storage/wal.go` | 1,108 | 1,044 | ✅ Done |

**Total: 3,519 lines of implementation + 3,223 lines of tests**

---

## 🟡 Tier 2: High Value - IN PROGRESS

Features that significantly improve quality and observability.

| Feature | File | Status | Notes |
|---------|------|--------|-------|
| RRF (BM25 + Vector Fusion) | `pkg/search/search.go` | ✅ Done | `rrfHybridSearch` implemented (921 lines) |
| MMR Diversification | `pkg/search/mmr.go` | ❌ Not Started | ~1 day effort |
| Cross-Encoder Rerank | `pkg/search/rerank.go` | ❌ Not Started | ~3 days effort |
| Index Stats Exposure | `pkg/index/stats.go` | ⚠️ Partial | Basic stats in `index.go` |
| Eval Harness | `eval/` | ❌ Not Started | ~2 days effort |

### Remaining Tier 2 Work

#### MMR Diversification (~1 day)
Maximal Marginal Relevance prevents redundant results:

```go
// pkg/search/mmr.go
func MMR(candidates []SearchResult, lambda float64, k int) []SearchResult {
    // λ=1.0 = pure relevance, λ=0.0 = pure diversity
    // Returns k diverse results from candidates
}
```

#### Cross-Encoder Rerank (~3 days)
Re-rank candidates using a cross-encoder model for quality improvement.

#### Eval Harness (~2 days)
Create `eval/` directory with:
- `recall_at_k.go` - Recall@1/5/10 for retrieval
- `link_auc.go` - Link prediction AUC
- `latency_bench.go` - p50/p95/p99 benchmarks

---

## 🟢 Tier 3: Nice to Have - MOSTLY COMPLETE

| Feature | Status | Notes |
|---------|--------|-------|
| Extended OpenCypher/Bolt | ✅ Done | `pkg/cypher/` (12+ files, 25k+ lines), `pkg/bolt/` |
| GPU Acceleration | ✅ Done | `pkg/gpu/` with Metal, CUDA, Vulkan, OpenCL (6,500+ lines) |
| GPU→CPU Fallback | ✅ Done | `pkg/gpu/accelerator.go` abstraction |
| Cross-Encoder GPU Service | ❌ Not Started | Dedicated microservice needed |
| Link Prediction AUC Pipeline | ⚠️ Partial | Tests exist, formal pipeline missing |

---

## 🆕 Capabilities Beyond Original Roadmap

NornicDB has gained significant capabilities not in the original roadmap:

| Feature | Package | Description |
|---------|---------|-------------|
| **MCP Server** | `pkg/mcp/` | Native Model Context Protocol for LLM tool use |
| **Cypher Query Embedding** | `pkg/cypher/` | `db.index.vector.queryNodes` accepts string queries |
| **Encryption** | `pkg/encryption/` | At-rest encryption |
| **Auth/RBAC** | `pkg/auth/` | Full authentication and authorization |
| **Retention Policies** | `pkg/retention/` | Automatic data retention |
| **Temporal Features** | `pkg/temporal/` | Time-based queries |
| **Decay Engine** | `pkg/decay/` | Kalman-filter based edge decay |
| **Audit Logging** | `pkg/audit/` | Comprehensive audit trail |
| **Caching** | `pkg/cache/` | Query and result caching |
| **Connection Pooling** | `pkg/pool/` | Database connection management |

---

## Implementation Details

### Edge Provenance (Completed)

```go
// pkg/storage/edge_meta.go
type EdgeMeta struct {
    EdgeID            string    `json:"edge_id"`
    Src               string    `json:"src"`
    Dst               string    `json:"dst"`
    Label             string    `json:"label"`
    Score             float64   `json:"score"`
    SignalType        string    `json:"signal_type"` // "coaccess", "similarity", "topology", "llm-infer"
    Timestamp         time.Time `json:"timestamp"`
    SessionID         string    `json:"session_id,omitempty"`
    EvidenceCount     int       `json:"evidence_count"`
    DecayState        float64   `json:"decay_state"`
    Materialized      bool      `json:"materialized"`
    Origin            string    `json:"origin"`
    TopologyAlgorithm string    `json:"topology_algorithm,omitempty"`
    TopologyScore     float64   `json:"topology_score,omitempty"`
    SemanticScore     float64   `json:"semantic_score,omitempty"`
}
```

### Per-Node Configuration (Completed)

```go
// pkg/storage/node_config.go
type NodeConfig struct {
    NodeID      string
    Pins        map[string]bool  // Never decay edges to these
    Deny        map[string]bool  // Never create edges to these
    MaxOutEdges int
    MaxInEdges  int
    LabelCaps   map[string]int
    CooldownMS  int64
    TrustLevel  TrustLevel
}
```

### Evidence Buffering (Completed)

```go
// pkg/inference/evidence.go
type EvidenceBuffer struct {
    entries    map[EvidenceKey]*Evidence
    thresholds map[string]EvidenceThreshold
}

type EvidenceThreshold struct {
    MinCount    int
    MinScore    float64
    MinSessions int
    MaxAge      time.Duration
}
```

### RRF Hybrid Search (Completed)

```go
// pkg/search/search.go
func (s *Service) rrfHybridSearch(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
    // Reciprocal Rank Fusion combining vector + BM25 results
    // k = 60 smoothing constant
}
```

---

## File Structure

```
pkg/
├── storage/
│   ├── edge_meta.go      ✅ Edge provenance
│   ├── edge_meta_test.go ✅ Tests
│   ├── node_config.go    ✅ Per-node config
│   ├── node_config_test.go ✅ Tests
│   ├── wal.go            ✅ Write-ahead log
│   └── wal_test.go       ✅ Tests
├── inference/
│   ├── cooldown.go       ✅ Cooldown logic
│   ├── cooldown_test.go  ✅ Tests
│   ├── evidence.go       ✅ Evidence buffering
│   └── evidence_test.go  ✅ Tests
├── search/
│   ├── search.go         ✅ RRF hybrid search
│   ├── mmr.go            ❌ TODO: MMR diversification
│   └── rerank.go         ❌ TODO: Cross-encoder rerank
├── linkpredict/
│   ├── topology.go       ✅ TLP algorithms
│   ├── hybrid.go         ✅ Hybrid scoring
│   └── graph_builder.go  ✅ Graph construction
├── gpu/
│   ├── gpu.go            ✅ GPU abstraction
│   ├── accelerator.go    ✅ CPU/GPU fallback
│   ├── kmeans.go         ✅ GPU k-means
│   ├── metal/            ✅ Apple Metal
│   ├── cuda/             ✅ NVIDIA CUDA
│   ├── vulkan/           ✅ Vulkan
│   └── opencl/           ✅ OpenCL
├── mcp/                  ✅ MCP server (bonus)
├── decay/                ✅ Edge decay (bonus)
├── auth/                 ✅ Authentication (bonus)
├── encryption/           ✅ At-rest encryption (bonus)
├── retention/            ✅ Data retention (bonus)
└── temporal/             ✅ Time queries (bonus)

eval/                     ❌ TODO: Evaluation harness
├── recall_at_k.go
├── link_auc.go
└── latency_bench.go
```

---

## Next Steps

### Immediate (This Week)
1. [ ] Implement MMR diversification in `pkg/search/mmr.go`
2. [ ] Add index stats API to `pkg/index/`

### Short Term (Next 2 Weeks)
3. [ ] Implement cross-encoder rerank in `pkg/search/rerank.go`
4. [ ] Create eval harness in `eval/`

### Future
5. [ ] Cross-encoder GPU microservice
6. [ ] Formal link prediction AUC pipeline

---

## References

- Liben-Nowell & Kleinberg (2007). "The link-prediction problem for social networks"
- Adamic & Adar (2003). "Friends and neighbors on the Web"
- Zhou et al. (2009). "Predicting missing links via local information"
- Carbonell & Goldstein (1998). "The use of MMR, diversity-based reranking"
- Neo4j GDS Documentation: https://neo4j.com/docs/graph-data-science/

---

_Last updated: December 1, 2025_
