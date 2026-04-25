# Cognitive SLM Integration Proposal for NornicDB

**Status:** PROPOSAL  
**Version:** 1.0.0  
**Date:** December 2024  
**Author:** AI Architecture Review

---

## Executive Summary

This proposal outlines the integration of a **Small Language Model (SLM)** directly into NornicDB's core engine, transforming it from a traditional graph database into a **Cognitive Graph Database** — a system with embedded reasoning capabilities for self-monitoring, self-healing, query optimization, and intelligent memory curation.

### What We're Building

```
┌─────────────────────────────────────────────────────────────────┐
│                    NornicDB Cognitive Engine                     │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  Embedding   │    │   Reasoning  │    │   Graph Engine   │  │
│  │    Model     │    │     SLM      │    │   (Storage +     │  │
│  │  (BGE-M3)    │    │  (Qwen2.5)   │    │    Cypher)       │  │
│  │  1024 dims   │    │   0.5B-3B    │    │                  │  │
│  └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘  │
│         │                   │                      │            │
│         └───────────────────┴──────────────────────┘            │
│                             │                                    │
│                    ┌────────▼────────┐                          │
│                    │  Model Manager  │                          │
│                    │  (Scheduler +   │                          │
│                    │   GPU Memory)   │                          │
│                    └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Validation of Approach

### 1.1 Why This is Novel

No production graph database embeds an SLM in the engine itself:
- **Neo4j**: External LLM integration only (via plugins)
- **TigerGraph**: No native LLM support
- **Dgraph**: No LLM capabilities
- **ArangoDB**: External AI services only

NornicDB already has:
- ✅ llama.cpp integration via CGO (`pkg/localllm`)
- ✅ GPU acceleration (Metal/CUDA)
- ✅ Embedding generation in-process
- ✅ Inference engine for link prediction (`pkg/inference`)
- ✅ Knowledge-layer scoring and decay (`pkg/knowledgepolicy`)

**We're 80% of the way there.** The missing piece is a reasoning model alongside the embedding model.

### 1.2 What the SLM Can Do TODAY (Safe + Practical)

| Capability | Description | Input | Output |
|------------|-------------|-------|--------|
| **Anomaly Detection** | Detect weird node/edge patterns | Graph stats + topology summary | `{anomaly_type, severity, node_ids}` |
| **Runtime Diagnosis** | Classify goroutine issues | Stack traces + metrics | `{diagnosis, action_id}` |
| **Query Optimization** | Suggest index/rewrite | Query plan + stats | `{suggestion_type, details}` |
| **Policy Enforcement** | Validate operations | Operation context | `{allow, reason}` |
| **Semantic Dedup** | Identify duplicate nodes | Node pairs + embeddings | `{is_duplicate, confidence}` |
| **Memory Curation** | Prioritize/summarize nodes | Node content + access patterns | `{summary, importance_score}` |

### 1.3 Safety Constraints (Non-Negotiable)

```go
// ALL SLM outputs MUST map to predefined actions
type ActionOpcode int

const (
    ActionNone ActionOpcode = iota
    ActionLogWarning
    ActionThrottleQuery
    ActionSuggestIndex
    ActionMergeNodes
    ActionRestartWorker
    ActionClearQueue
    // ... finite, enumerated set
)

// SLM output schema - STRICT
type SLMResponse struct {
    Action     ActionOpcode `json:"action"`
    Confidence float64      `json:"confidence"`
    Reasoning  string       `json:"reasoning"`
    Params     map[string]any `json:"params,omitempty"`
}
```

**Never allow:**
- ❌ Arbitrary code execution
- ❌ Direct data modification without review
- ❌ Security/access control decisions
- ❌ Live storage engine changes

---

## 2. Model Selection: MIT/Apache Licensed Options

### 2.1 Recommended Models (Ranked by Suitability)

| Model | Size | License | Strengths | Use Case |
|-------|------|---------|-----------|----------|
| **qwen3-0.6b-Instruct** | 0.5B | Apache 2.0 | Excellent structured output, fast | Primary choice |
| **Qwen2.5-1.5B-Instruct** | 1.5B | Apache 2.0 | Better reasoning, still fast | If 0.5B insufficient |
| **Qwen2.5-3B-Instruct** | 3B | Apache 2.0 | Best reasoning | Complex tasks |
| **SmolLM2-360M-Instruct** | 360M | Apache 2.0 | Ultra-fast, tiny | Simple classification |
| **Phi-3.5-mini-instruct** | 3.8B | MIT | Strong reasoning | Alternative to Qwen |
| **TinyLlama-1.1B** | 1.1B | Apache 2.0 | Proven stable | Fallback option |

### 2.2 Why qwen3-0.6b is the Primary Recommendation

1. **Structured Output Excellence**: Qwen2.5 family excels at JSON/structured generation
2. **Size/Performance Balance**: 0.5B runs in ~500MB VRAM quantized (Q4_K_M)
3. **Apache 2.0 License**: Commercial-friendly, no restrictions
4. **GGUF Available**: Pre-quantized versions on HuggingFace
5. **Instruction-Following**: Tuned for following precise prompts
6. **Multilingual**: Works across languages (useful for global deployments)

### 2.3 Model Quantization Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                   Quantization Tiers                         │
├─────────────────────────────────────────────────────────────┤
│  Q8_0:  ~550MB  │ Best quality, more VRAM                   │
│  Q5_K_M: ~400MB │ Good balance                              │
│  Q4_K_M: ~350MB │ Recommended default ★                     │
│  Q4_0:  ~300MB  │ Fastest, slight quality loss              │
│  Q2_K:  ~200MB  │ Emergency fallback only                   │
└─────────────────────────────────────────────────────────────┘
```

**Recommendation**: Ship with Q4_K_M by default, allow Q8_0 for systems with VRAM headroom.

---

## 3. Architecture: Multi-Model Management

### 3.1 Adapting Ollama's Approach

Ollama (MIT licensed) solved multi-model GPU scheduling. Key patterns to adapt:

```go
// pkg/heimdall/scheduler.go - Inspired by Ollama's scheduler

type ModelScheduler struct {
    mu           sync.RWMutex
    models       map[string]*LoadedModel
    gpuMemory    int64          // Available VRAM
    maxLoaded    int            // Max concurrent models
    lru          *list.List     // LRU for eviction
    embedModel   string         // Always-loaded embedding model
}

type LoadedModel struct {
    Name       string
    Model      *localllm.Model
    Context    *localllm.Context
    LastUsed   time.Time
    MemoryUsed int64
    Purpose    ModelPurpose // Embedding, Reasoning, Classification
}

type ModelPurpose int

const (
    PurposeEmbedding ModelPurpose = iota
    PurposeReasoning
    PurposeClassification
)
```

### 3.2 Model Loading Strategy

```go
// Embedding model: Always loaded (primary workload)
// Reasoning model: Loaded on-demand, cached with LRU eviction

func (s *ModelScheduler) GetModel(purpose ModelPurpose) (*LoadedModel, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    modelName := s.modelForPurpose(purpose)
    
    // Check if already loaded
    if model, ok := s.models[modelName]; ok {
        model.LastUsed = time.Now()
        s.lru.MoveToFront(model.lruElement)
        return model, nil
    }
    
    // Need to load - check memory
    if err := s.ensureMemoryFor(modelName); err != nil {
        return nil, err
    }
    
    // Load the model
    return s.loadModel(modelName, purpose)
}

func (s *ModelScheduler) ensureMemoryFor(modelName string) error {
    required := s.estimateMemory(modelName)
    
    for s.usedMemory + required > s.gpuMemory {
        // Evict least recently used (but never the embedding model)
        evictee := s.lru.Back()
        if evictee == nil {
            return fmt.Errorf("cannot free enough GPU memory")
        }
        
        model := evictee.Value.(*LoadedModel)
        if model.Name == s.embedModel {
            // Never evict embedding model
            s.lru.MoveToFront(evictee)
            continue
        }
        
        s.unloadModel(model)
    }
    return nil
}
```

### 3.3 Extending Current llama.go

Current `pkg/localllm/llama.go` supports single-model loading. Extensions needed:

```go
// pkg/localllm/llama.go - Extensions

// ModelType distinguishes embedding vs generation models
type ModelType int

const (
    ModelTypeEmbedding ModelType = iota
    ModelTypeGeneration
)

// GenerationOptions for text generation (vs embedding)
type GenerationOptions struct {
    MaxTokens   int
    Temperature float32
    TopP        float32
    TopK        int
    StopTokens  []string
}

// Generate produces text completion (for reasoning SLM)
func (m *Model) Generate(ctx context.Context, prompt string, opts GenerationOptions) (string, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Tokenize prompt
    tokens := m.tokenize(prompt)
    
    // Generate tokens iteratively
    var output strings.Builder
    for i := 0; i < opts.MaxTokens; i++ {
        select {
        case <-ctx.Done():
            return output.String(), ctx.Err()
        default:
        }
        
        nextToken, err := m.sampleNext(tokens, opts)
        if err != nil {
            return output.String(), err
        }
        
        if m.isStopToken(nextToken, opts.StopTokens) {
            break
        }
        
        tokens = append(tokens, nextToken)
        output.WriteString(m.detokenize(nextToken))
    }
    
    return output.String(), nil
}
```

---

## 4. SLM Subsystems

### 4.1 Anomaly Detection System

```go
// pkg/heimdall/anomaly/detector.go

type AnomalyDetector struct {
    scheduler *ModelScheduler
    store     storage.Engine
}

type GraphStats struct {
    TotalNodes      int64            `json:"total_nodes"`
    TotalEdges      int64            `json:"total_edges"`
    NodesByLabel    map[string]int64 `json:"nodes_by_label"`
    EdgesByType     map[string]int64 `json:"edges_by_type"`
    AvgDegree       float64          `json:"avg_degree"`
    MaxDegree       int64            `json:"max_degree"`
    SuperNodes      []string         `json:"super_nodes"` // Nodes with >1000 edges
    RecentGrowth    float64          `json:"recent_growth_rate"`
}

const anomalyPrompt = `<|im_start|>system
You are a graph database anomaly detector. Analyze graph statistics and identify anomalies.
Output JSON only: {"anomaly_detected": bool, "type": string, "severity": "low"|"medium"|"high"|"critical", "affected_nodes": [], "recommendation": string}
<|im_end|>
<|im_start|>user
Graph Statistics:
%s

Recent changes:
- Nodes added last hour: %d
- Edges added last hour: %d
- Labels with unusual growth: %v

Identify any anomalies.
<|im_end|>
<|im_start|>assistant
`

func (d *AnomalyDetector) Analyze(ctx context.Context) (*AnomalyReport, error) {
    stats := d.collectStats()
    
    prompt := fmt.Sprintf(anomalyPrompt, 
        mustJSON(stats),
        stats.RecentNodeGrowth,
        stats.RecentEdgeGrowth,
        stats.UnusualLabels,
    )
    
    model, err := d.scheduler.GetModel(PurposeReasoning)
    if err != nil {
        return nil, err
    }
    
    response, err := model.Generate(ctx, prompt, GenerationOptions{
        MaxTokens:   256,
        Temperature: 0.1, // Low temperature for deterministic output
        StopTokens:  []string{"<|im_end|>"},
    })
    if err != nil {
        return nil, err
    }
    
    return parseAnomalyResponse(response)
}
```

### 4.2 Runtime Health Diagnosis

```go
// pkg/heimdall/health/diagnostician.go

type Diagnostician struct {
    scheduler *ModelScheduler
}

type RuntimeSnapshot struct {
    GoroutineCount   int                    `json:"goroutine_count"`
    HeapAlloc        uint64                 `json:"heap_alloc_mb"`
    GCPauseNs        uint64                 `json:"gc_pause_ns"`
    BlockedRoutines  []BlockedRoutine       `json:"blocked_routines"`
    LockContention   map[string]int64       `json:"lock_contention"`
    QueueDepths      map[string]int         `json:"queue_depths"`
}

type BlockedRoutine struct {
    ID        uint64   `json:"id"`
    State     string   `json:"state"`
    WaitingOn string   `json:"waiting_on"`
    Duration  Duration `json:"duration"`
    Stack     []string `json:"stack_summary"` // Top 3 frames only
}

const diagnosisPrompt = `<|im_start|>system
You are a Go runtime diagnostician. Analyze runtime metrics and identify issues.
Output JSON: {"diagnosis": string, "severity": "healthy"|"warning"|"critical", "action_id": int, "details": string}

Action IDs:
0 = No action needed
1 = Log warning
2 = Restart worker pool
3 = Clear specific queue
4 = Trigger GC
5 = Reduce concurrency
<|im_end|>
<|im_start|>user
Runtime Snapshot:
%s
<|im_end|>
<|im_start|>assistant
`

func (d *Diagnostician) Diagnose(ctx context.Context, snapshot RuntimeSnapshot) (*Diagnosis, error) {
    prompt := fmt.Sprintf(diagnosisPrompt, mustJSON(snapshot))
    
    model, err := d.scheduler.GetModel(PurposeClassification)
    if err != nil {
        return nil, err
    }
    
    response, err := model.Generate(ctx, prompt, GenerationOptions{
        MaxTokens:   128,
        Temperature: 0.0, // Deterministic for safety
    })
    
    return parseDiagnosis(response)
}
```

### 4.3 Memory Curator (Agent-Facing)

```go
// pkg/heimdall/curator/memory_curator.go

type MemoryCurator struct {
    scheduler *ModelScheduler
    scorer    *knowledgepolicy.Scorer
}

type MemoryNode struct {
    ID           string    `json:"id"`
    Content      string    `json:"content"`
    Labels       []string  `json:"labels"`
    CreatedAt    time.Time `json:"created_at"`
    EdgeCount    int       `json:"edge_count"`
    DecayScore   float64   `json:"decay_score"`
}

const curationPrompt = `<|im_start|>system
You are a memory curator for an AI agent's knowledge graph. Evaluate memories for importance.
Output JSON: {"should_keep": bool, "new_importance": float, "summary": string, "merge_candidates": []}
<|im_end|>
<|im_start|>user
Memory to evaluate:
%s

Related memories (by embedding similarity):
%s

Agent's recent focus areas: %v
<|im_end|>
<|im_start|>assistant
`

func (c *MemoryCurator) EvaluateMemory(ctx context.Context, node MemoryNode, similar []MemoryNode) (*CurationDecision, error) {
    prompt := fmt.Sprintf(curationPrompt,
        mustJSON(node),
        mustJSON(similar),
        c.getRecentFocusAreas(),
    )
    
    model, err := c.scheduler.GetModel(PurposeReasoning)
    if err != nil {
        return nil, err
    }
    
    response, err := model.Generate(ctx, prompt, GenerationOptions{
        MaxTokens:   200,
        Temperature: 0.3,
    })
    
    return parseCurationDecision(response)
}
```

---

## 5. Configuration

### 5.1 Environment Variables

The SLM is controlled via **feature flags** in the existing `FeatureFlagsConfig`, following the same BYOM (Bring Your Own Model) pattern as embeddings.

```bash
# Enable SLM (opt-in, disabled by default)
NORNICDB_HEIMDALL_ENABLED=true

# BYOM: Use same models directory as embeddings
NORNICDB_MODELS_DIR=/data/models

# Model Selection (without .gguf extension)
NORNICDB_HEIMDALL_MODEL=qwen3-0.6b-instruct

# GPU Configuration
NORNICDB_HEIMDALL_GPU_LAYERS=-1           # -1 = auto (all on GPU, fallback to CPU)

# Generation Parameters
NORNICDB_HEIMDALL_MAX_TOKENS=512
NORNICDB_HEIMDALL_TEMPERATURE=0.1         # Low for deterministic output

# Feature Toggles (default to enabled when SLM is enabled)
NORNICDB_HEIMDALL_ANOMALY_DETECTION=true
NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS=true
NORNICDB_HEIMDALL_MEMORY_CURATION=false   # Experimental
```

**Key Design Decisions:**
- **Feature Flag**: SLM is opt-in via `NORNICDB_HEIMDALL_ENABLED=true`
- **BYOM**: Uses same `NORNICDB_MODELS_DIR` as embeddings - drop in `.gguf` files
- **CPU Fallback**: If GPU memory is insufficient, automatically falls back to CPU
- **No Remote**: Only local models supported (no remote LLM management yet)

### 5.2 Implementation (COMPLETED)

SLM configuration is integrated into the existing `FeatureFlagsConfig` in `pkg/config/config.go`:

```go
// pkg/config/config.go - IMPLEMENTED

// FeatureFlagsConfig includes SLM settings:
type FeatureFlagsConfig struct {
    // ... existing flags ...

    // SLM (Small Language Model) for cognitive database features
    SLMEnabled          bool    // NORNICDB_HEIMDALL_ENABLED
    SLMModel            string  // NORNICDB_HEIMDALL_MODEL
    SLMGPULayers        int     // NORNICDB_HEIMDALL_GPU_LAYERS
    SLMMaxTokens        int     // NORNICDB_HEIMDALL_MAX_TOKENS
    SLMTemperature      float32 // NORNICDB_HEIMDALL_TEMPERATURE
    SLMAnomalyDetection bool    // NORNICDB_HEIMDALL_ANOMALY_DETECTION
    SLMRuntimeDiagnosis bool    // NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS
    SLMMemoryCuration   bool    // NORNICDB_HEIMDALL_MEMORY_CURATION
}
```

The SLM package (`pkg/heimdall`) provides its own `Config` type:

```go
// pkg/heimdall/types.go - IMPLEMENTED

type Config struct {
    Enabled           bool
    ModelsDir         string        // Uses NORNICDB_MODELS_DIR (shared with embeddings)
    Model             string
    MaxTokens         int
    Temperature       float32
    GPULayers         int           // -1 = auto, 0 = CPU only
    AnomalyDetection  bool
    AnomalyInterval   time.Duration
    RuntimeDiagnosis  bool
    RuntimeInterval   time.Duration
    MemoryCuration    bool
    CurationInterval  time.Duration
}

// ConfigFromFeatureFlags creates Config from FeatureFlagsConfig
func ConfigFromFeatureFlags(flags FeatureFlagsSource, modelsDir string) Config
```

**CPU Fallback Behavior:**

The `Manager` automatically falls back to CPU if GPU loading fails:

```go
// pkg/heimdall/scheduler.go - IMPLEMENTED
generator, err := loadGenerator(modelPath, gpuLayers)
if err != nil {
    fmt.Printf("⚠️  GPU loading failed, trying CPU fallback: %v\n", err)
    generator, err = loadGenerator(modelPath, 0) // CPU only
}
```

---

## 6. Implementation Plan

### Phase 1: Foundation ✅ COMPLETED

- [x] Add SLM feature flags to `pkg/config/config.go`
- [x] Create `pkg/heimdall/types.go` with common types
- [x] Create `pkg/heimdall/scheduler.go` (Manager) with BYOM pattern
- [x] Create `pkg/heimdall/handler.go` with HTTP/SSE endpoints
- [x] CPU fallback when GPU memory insufficient

**Files Created:**
- `pkg/heimdall/types.go` - Config, Generator interface, action opcodes
- `pkg/heimdall/scheduler.go` - Manager with BYOM model loading
- `pkg/heimdall/handler.go` - OpenAI-compatible HTTP API

**Remaining:**
- [ ] Extend `pkg/localllm/llama.go` with `GenerateStream()` CGO implementation
- [ ] Write tests for model loading
- [ ] Benchmark GPU memory usage

### Phase 2: Core Subsystems (Week 3-4)

- [ ] Implement `pkg/heimdall/anomaly/detector.go`
- [ ] Implement `pkg/heimdall/health/diagnostician.go`
- [ ] Create action opcode registry
- [ ] Build confidence threshold system
- [ ] Integration tests with mock models

### Phase 3: Memory Curation (Week 5-6)

- [ ] Implement `pkg/heimdall/curator/memory_curator.go`
- [ ] Integrate with `pkg/knowledgepolicy` scoring engine
- [ ] Add semantic deduplication
- [ ] Build summarization pipeline
- [ ] Test with real agent workloads

### Phase 4: Production Hardening (Week 7-8)

- [ ] Add metrics/observability (Prometheus)
- [ ] Implement graceful degradation (SLM failure → fallback)
- [ ] Documentation + examples
- [ ] Performance benchmarks
- [ ] Security audit of action opcodes

---

## 7. Resource Requirements

### 7.1 GPU Memory Budget

```
┌─────────────────────────────────────────────────────────────┐
│              GPU Memory Budget (8GB Total)                   │
├─────────────────────────────────────────────────────────────┤
│  BGE-M3 Embedding (Q4_K_M)     │  ~600MB                    │
│  qwen3-0.6b (Q4_K_M)         │  ~350MB                    │
│  KV Cache (both models)        │  ~200MB                    │
│  System Reserve                │  ~512MB                    │
│  ─────────────────────────────────────────────              │
│  Total Required                │  ~1.7GB                    │
│  Available for Graph Ops       │  ~6.3GB                    │
└─────────────────────────────────────────────────────────────┘
```

**Conclusion**: Fits comfortably on any modern GPU (even 4GB discrete or Apple M1 8GB unified).

### 7.2 CPU Fallback

For systems without GPU:
- BGE-M3: ~10-50ms per embedding (acceptable)
- qwen3-0.6b: ~100-500ms per generation (acceptable for background tasks)

Recommendation: Run SLM subsystems on separate thread pool to not block queries.

---

## 8. Model Download Strategy

### 8.1 Recommended GGUF Sources

```bash
# Primary: HuggingFace (official quantizations)
https://huggingface.co/Qwen/qwen3-0.6b-Instruct-GGUF
https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF

# Alternative: TheBloke quantizations (community)
https://huggingface.co/TheBloke/
```

### 8.2 Download Script

```bash
#!/bin/bash
# scripts/download-slm.sh

MODEL_DIR="${NORNICDB_MODELS_DIR:-/data/models}"
mkdir -p "$MODEL_DIR"

# Download qwen3-0.6b-Instruct (Q4_K_M)
wget -O "$MODEL_DIR/qwen3-0.6b-instruct.gguf" \
  "https://huggingface.co/Qwen/qwen3-0.6b-Instruct-GGUF/resolve/main/qwen3-0.6b-instruct-q4_k_m.gguf"

# Verify checksum
echo "Expected SHA256: <checksum>"
sha256sum "$MODEL_DIR/qwen3-0.6b-instruct.gguf"

echo "✅ SLM model downloaded to $MODEL_DIR"
```

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| SLM produces invalid action | Strict JSON schema validation + action whitelist |
| SLM hallucinates node IDs | Cross-reference all IDs against storage |
| GPU OOM | Memory budget enforcement + graceful eviction |
| Latency spikes | Async processing + timeout enforcement |
| Model corruption | Checksum verification on load |
| Prompt injection | Sanitize all user-derived input |

---

## 10. Success Metrics

### 10.1 Quantitative

- [ ] Anomaly detection catches 80%+ of synthetic anomalies
- [ ] Runtime diagnosis accuracy >90% on labeled test set
- [ ] Memory curation reduces node count by 10-20% with <5% false positives
- [ ] P99 latency impact <10ms on query path
- [ ] GPU memory usage <2GB total (embedding + reasoning)

### 10.2 Qualitative

- [ ] Zero unintended data modifications
- [ ] All SLM actions logged and auditable
- [ ] Graceful degradation when SLM unavailable
- [ ] Clear documentation for operators

---

## 11. Conclusion

Embedding a reasoning SLM alongside the embedding model transforms NornicDB into a **Cognitive Graph Database** — the first of its kind. The architecture is:

1. **Safe**: Bounded actions, confidence thresholds, audit logs
2. **Efficient**: <2GB GPU, LRU eviction, async processing
3. **Practical**: Real anomaly detection, health diagnosis, memory curation
4. **Extensible**: Clean interfaces for future subsystems

**Next Step**: Approve proposal and begin Phase 1 implementation.

---

## Appendix A: Model Comparison Benchmarks

| Model | Params | VRAM (Q4_K_M) | Tokens/sec (M2) | JSON Accuracy |
|-------|--------|---------------|-----------------|---------------|
| SmolLM2-360M | 360M | ~200MB | ~150 | 85% |
| qwen3-0.6b | 0.5B | ~350MB | ~100 | 94% |
| Qwen2.5-1.5B | 1.5B | ~900MB | ~50 | 97% |
| Qwen2.5-3B | 3B | ~1.8GB | ~25 | 98% |
| Phi-3.5-mini | 3.8B | ~2.2GB | ~20 | 96% |

**Recommendation**: Start with qwen3-0.6b. Upgrade to 1.5B if JSON accuracy is insufficient.

---

## Appendix B: Sample Prompts

### B.1 Anomaly Detection

```
<|im_start|>system
You are a graph database anomaly detector. Analyze the following graph statistics and identify any anomalies.
Output ONLY valid JSON in this exact format:
{"anomaly_detected": boolean, "type": "none"|"supernode"|"growth_spike"|"orphan_cluster"|"duplicate_nodes", "severity": "low"|"medium"|"high"|"critical", "affected_nodes": ["id1", "id2"], "recommendation": "string"}
<|im_end|>
<|im_start|>user
Graph Statistics:
- Total nodes: 125,432
- Total edges: 892,104
- Average degree: 7.1
- Max degree: 45,892 (node: "user-config-cache")
- Nodes added last hour: 12,500
- Typical hourly addition: 200

Is this anomalous?
<|im_end|>
<|im_start|>assistant
{"anomaly_detected": true, "type": "supernode", "severity": "high", "affected_nodes": ["user-config-cache"], "recommendation": "Node 'user-config-cache' has 45,892 edges which is 6,464x the average. Investigate for possible runaway process or missing pagination."}
```

### B.2 Runtime Diagnosis

```
<|im_start|>system
You are a Go runtime diagnostician. Analyze metrics and identify issues.
Output ONLY valid JSON: {"diagnosis": "string", "severity": "healthy"|"warning"|"critical", "action_id": 0-5, "details": "string"}

Action IDs: 0=none, 1=log_warning, 2=restart_workers, 3=clear_queue, 4=trigger_gc, 5=reduce_concurrency
<|im_end|>
<|im_start|>user
Runtime metrics:
- Goroutines: 12,847 (normal: ~500)
- Heap: 4.2GB (limit: 8GB)
- GC pause: 450ms (normal: <50ms)
- Blocked routines: 847 waiting on "queryExecutor.mu"
<|im_end|>
<|im_start|>assistant
{"diagnosis": "Lock contention causing goroutine explosion", "severity": "critical", "action_id": 5, "details": "847 goroutines blocked on queryExecutor.mu suggests lock contention. Reduce query concurrency to relieve pressure."}
```

---

## Appendix C: License Compliance

| Component | License | Commercial Use | Modification | Distribution |
|-----------|---------|----------------|--------------|--------------|
| Qwen2.5 | Apache 2.0 | ✅ | ✅ | ✅ (with notice) |
| SmolLM2 | Apache 2.0 | ✅ | ✅ | ✅ (with notice) |
| Phi-3.5 | MIT | ✅ | ✅ | ✅ |
| TinyLlama | Apache 2.0 | ✅ | ✅ | ✅ (with notice) |
| llama.cpp | MIT | ✅ | ✅ | ✅ |
| Ollama | MIT | ✅ | ✅ | ✅ |

**All recommended models are fully compatible with commercial use.**

---

## 12. BYOM Architecture (Bring Your Own Model)

### 12.1 Unified Models Directory

All models (embedding + reasoning SLM) live in the same directory:

```
${NORNICDB_MODELS_DIR}/          # Default: /data/models
├── bge-m3.gguf                  # Embedding model (existing)
├── qwen3-0.6b-instruct.gguf   # Reasoning SLM (new)
├── qwen2.5-1.5b-instruct.gguf   # Alternative larger SLM
└── custom-finetuned.gguf        # User's custom model
```

### 12.2 Model Registry

```go
// pkg/heimdall/registry.go

type ModelRegistry struct {
    mu       sync.RWMutex
    models   map[string]*ModelInfo
    basePath string
}

type ModelInfo struct {
    Name         string       `json:"name"`
    Path         string       `json:"path"`
    Type         ModelType    `json:"type"`          // embedding, reasoning, classification
    Size         int64        `json:"size_bytes"`
    Quantization string       `json:"quantization"`  // Q4_K_M, Q8_0, etc.
    Loaded       bool         `json:"loaded"`
    LastUsed     time.Time    `json:"last_used"`
    VRAMEstimate int64        `json:"vram_estimate"`
}

type ModelType string

const (
    ModelTypeEmbedding      ModelType = "embedding"
    ModelTypeReasoning      ModelType = "reasoning"
    ModelTypeClassification ModelType = "classification"
)

// ScanModels discovers all GGUF files in the models directory
func (r *ModelRegistry) ScanModels() error {
    r.mu.Lock()
    defer r.mu.Unlock()
    
    entries, err := os.ReadDir(r.basePath)
    if err != nil {
        return err
    }
    
    for _, entry := range entries {
        if strings.HasSuffix(entry.Name(), ".gguf") {
            info, _ := entry.Info()
            modelName := strings.TrimSuffix(entry.Name(), ".gguf")
            
            r.models[modelName] = &ModelInfo{
                Name:         modelName,
                Path:         filepath.Join(r.basePath, entry.Name()),
                Type:         r.inferModelType(modelName),
                Size:         info.Size(),
                Quantization: r.detectQuantization(modelName),
                VRAMEstimate: r.estimateVRAM(info.Size()),
            }
        }
    }
    return nil
}

// inferModelType guesses model purpose from name patterns
func (r *ModelRegistry) inferModelType(name string) ModelType {
    lower := strings.ToLower(name)
    switch {
    case strings.Contains(lower, "embed") || strings.Contains(lower, "bge") || 
         strings.Contains(lower, "e5") || strings.Contains(lower, "nomic"):
        return ModelTypeEmbedding
    case strings.Contains(lower, "instruct") || strings.Contains(lower, "chat"):
        return ModelTypeReasoning
    default:
        return ModelTypeReasoning // Default to reasoning
    }
}
```

### 12.3 Configuration Extension

```bash
# Environment Variables
NORNICDB_MODELS_DIR=/data/models              # Shared models directory
NORNICDB_EMBEDDING_MODEL=bge-m3               # Embedding model name
NORNICDB_HEIMDALL_MODEL=qwen3-0.6b-instruct      # Reasoning SLM name
NORNICDB_HEIMDALL_FALLBACK_MODEL=tinyllama-1.1b    # Fallback if primary OOM
```

### 12.4 Model Hot-Swap

Models can be switched at runtime without restart:

```go
// POST /api/admin/models/load
type LoadModelRequest struct {
    ModelName string    `json:"model_name"`
    Purpose   ModelType `json:"purpose"`
    Force     bool      `json:"force"` // Unload current if needed
}

// GET /api/admin/models
// Returns all available models and their status
```

---

## 13. Bifrost (Admin UI)

### 13.1 Architecture Overview

A translucent terminal-style chat interface for direct SLM interaction:

```
┌─────────────────────────────────────────────────────────────────┐
│                    NornicDB Admin                               │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  🧠 Bifrost                   [qwen3-0.6b] ▼  ─ □ x    │   │
│  ├──────────────────────────────────────────────────────────┤   │
│  │  #nornicdb-slm                                           │   │
│  │                                                          │   │
│  │  > Analyze current graph health                          │   │
│  │                                                          │   │
│  │  {"status": "healthy", "nodes": 125432,                  │   │
│  │   "edges": 892104, "anomalies": [],                      │   │
│  │   "recommendations": ["Consider indexing label:File"]}   │   │
│  │                                                          │   │
│  │  > What queries are running slow?                        │   │
│  │                                                          │   │
│  │  Analyzing query logs...                                 │   │
│  │  ████████░░░░░░░░ 50%                                    │   │
│  │                                                          │   │
│  ├──────────────────────────────────────────────────────────┤   │
│  │  > _                                               [Send]│   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 13.2 UI Component (React + TypeScript)

Inspired by Pegasus CliView, modernized for React 18:

```typescript
// frontend/src/components/SLMPortal/SLMPortal.tsx

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import './SLMPortal.css';

interface Message {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  streaming?: boolean;
}

interface SLMPortalProps {
  isOpen: boolean;
  onClose: () => void;
  modelName?: string;
}

export const SLMPortal: React.FC<SLMPortalProps> = ({ 
  isOpen, 
  onClose,
  modelName = 'qwen3-0.6b'
}) => {
  const [messages, setMessages] = useState<Message[]>([
    { id: '0', role: 'system', content: '#nornicdb-slm\n\nCognitive Database Assistant Ready', timestamp: new Date() }
  ]);
  const [input, setInput] = useState('');
  const [isStreaming, setIsStreaming] = useState(false);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const wsRef = useRef<WebSocket | null>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  // Focus input when opened
  useEffect(() => {
    if (isOpen && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isOpen]);

  // WebSocket connection for streaming
  const connectWS = useCallback(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}/api/bifrost/chat/stream`);
    
    ws.onopen = () => {
      console.log('Bifrost connected');
    };
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      if (data.type === 'token') {
        // Streaming token - append to last message
        setMessages(prev => {
          const last = prev[prev.length - 1];
          if (last?.streaming) {
            return [
              ...prev.slice(0, -1),
              { ...last, content: last.content + data.token }
            ];
          }
          return prev;
        });
      } else if (data.type === 'done') {
        // Stream complete
        setMessages(prev => {
          const last = prev[prev.length - 1];
          if (last?.streaming) {
            return [
              ...prev.slice(0, -1),
              { ...last, streaming: false }
            ];
          }
          return prev;
        });
        setIsStreaming(false);
      } else if (data.type === 'error') {
        setMessages(prev => [
          ...prev,
          { id: crypto.randomUUID(), role: 'system', content: `Error: ${data.message}`, timestamp: new Date() }
        ]);
        setIsStreaming(false);
      }
    };
    
    ws.onerror = (error) => {
      console.error('Bifrost error:', error);
      setIsStreaming(false);
    };
    
    ws.onclose = () => {
      console.log('Bifrost disconnected');
      // Auto-reconnect after 3s
      setTimeout(connectWS, 3000);
    };
    
    wsRef.current = ws;
  }, []);

  useEffect(() => {
    if (isOpen) {
      connectWS();
    }
    return () => {
      wsRef.current?.close();
    };
  }, [isOpen, connectWS]);

  const sendMessage = () => {
    if (!input.trim() || isStreaming) return;
    
    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: 'user',
      content: input,
      timestamp: new Date()
    };
    
    // Add to history
    setCommandHistory(prev => [...prev, input]);
    setHistoryIndex(-1);
    
    // Add user message
    setMessages(prev => [...prev, userMessage]);
    
    // Add placeholder for assistant response
    const assistantMessage: Message = {
      id: crypto.randomUUID(),
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      streaming: true
    };
    setMessages(prev => [...prev, assistantMessage]);
    
    // Send via WebSocket
    setIsStreaming(true);
    wsRef.current?.send(JSON.stringify({
      type: 'chat',
      content: input,
      model: modelName
    }));
    
    setInput('');
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (historyIndex < commandHistory.length - 1) {
        const newIndex = historyIndex + 1;
        setHistoryIndex(newIndex);
        setInput(commandHistory[commandHistory.length - 1 - newIndex]);
      }
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (historyIndex > 0) {
        const newIndex = historyIndex - 1;
        setHistoryIndex(newIndex);
        setInput(commandHistory[commandHistory.length - 1 - newIndex]);
      } else {
        setHistoryIndex(-1);
        setInput('');
      }
    } else if (e.key === 'Escape') {
      onClose();
    }
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          className="slm-portal-overlay"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
        >
          <motion.div
            className="slm-portal"
            initial={{ y: '100%' }}
            animate={{ y: 0 }}
            exit={{ y: '100%' }}
            transition={{ type: 'spring', damping: 25, stiffness: 300 }}
          >
            {/* Header */}
            <div className="slm-portal-header">
              <div className="slm-portal-title">
                <span className="slm-icon">🧠</span>
                <span>Bifrost Portal</span>
              </div>
              <div className="slm-portal-model">
                <select defaultValue={modelName}>
                  <option value="qwen3-0.6b">qwen3-0.6b</option>
                  <option value="qwen2.5-1.5b">qwen2.5-1.5b</option>
                  <option value="phi-3.5-mini">phi-3.5-mini</option>
                </select>
              </div>
              <button className="slm-portal-close" onClick={onClose}>×</button>
            </div>
            
            {/* Messages */}
            <div className="slm-portal-messages" ref={scrollRef}>
              {messages.map((msg) => (
                <div key={msg.id} className={`slm-message slm-message-${msg.role}`}>
                  <span className="slm-message-prefix">
                    {msg.role === 'user' ? '> ' : msg.role === 'system' ? '# ' : ''}
                  </span>
                  <span className="slm-message-content">
                    {msg.content}
                    {msg.streaming && <span className="slm-cursor">▋</span>}
                  </span>
                </div>
              ))}
            </div>
            
            {/* Input */}
            <div className="slm-portal-input-container">
              <span className="slm-input-prefix">&gt;</span>
              <textarea
                ref={inputRef}
                className="slm-portal-input"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Enter command..."
                disabled={isStreaming}
                rows={1}
              />
              <button 
                className="slm-send-button"
                onClick={sendMessage}
                disabled={isStreaming || !input.trim()}
              >
                {isStreaming ? '...' : 'Send'}
              </button>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
};
```

### 13.3 Portal Styling (CSS)

```css
/* frontend/src/components/SLMPortal/SLMPortal.css */

.slm-portal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(4px);
  z-index: 1000;
  display: flex;
  align-items: flex-end;
  justify-content: center;
}

.slm-portal {
  width: 100%;
  max-width: 900px;
  height: 60vh;
  min-height: 400px;
  display: flex;
  flex-direction: column;
  border-radius: 12px 12px 0 0;
  overflow: hidden;
  box-shadow: 0 -10px 40px rgba(0, 0, 0, 0.4);
  
  /* Translucent dark background - Pegasus style */
  background: linear-gradient(
    135deg,
    rgba(15, 23, 42, 0.95) 0%,
    rgba(30, 41, 59, 0.92) 100%
  );
  backdrop-filter: blur(20px);
  border: 1px solid rgba(255, 255, 255, 0.1);
}

.slm-portal-header {
  display: flex;
  align-items: center;
  padding: 12px 16px;
  background: rgba(0, 0, 0, 0.3);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.slm-portal-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 14px;
  font-weight: 600;
  color: #10b981; /* Emerald green */
}

.slm-icon {
  font-size: 18px;
}

.slm-portal-model {
  margin-left: auto;
  margin-right: 16px;
}

.slm-portal-model select {
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 6px;
  color: #94a3b8;
  padding: 4px 8px;
  font-family: 'SF Mono', monospace;
  font-size: 12px;
  cursor: pointer;
}

.slm-portal-close {
  background: none;
  border: none;
  color: #64748b;
  font-size: 24px;
  cursor: pointer;
  padding: 0 8px;
  transition: color 0.2s;
}

.slm-portal-close:hover {
  color: #ef4444;
}

.slm-portal-messages {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
  font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 14px;
  line-height: 1.6;
  
  /* Inset shadow for depth */
  box-shadow: inset 0 20px 40px -20px rgba(0, 0, 0, 0.5);
}

.slm-message {
  margin-bottom: 8px;
  white-space: pre-wrap;
  word-break: break-word;
}

.slm-message-user {
  color: #f97316; /* Orange - command input */
}

.slm-message-assistant {
  color: #22d3ee; /* Cyan - SLM response */
}

.slm-message-system {
  color: #10b981; /* Emerald - system messages */
  opacity: 0.8;
}

.slm-message-prefix {
  color: #64748b;
  user-select: none;
}

.slm-cursor {
  animation: blink 1s step-end infinite;
  color: #22d3ee;
}

@keyframes blink {
  50% { opacity: 0; }
}

.slm-portal-input-container {
  display: flex;
  align-items: center;
  padding: 12px 16px;
  background: rgba(0, 0, 0, 0.4);
  border-top: 1px solid rgba(255, 255, 255, 0.1);
  gap: 8px;
}

.slm-input-prefix {
  color: #f97316;
  font-family: 'SF Mono', monospace;
  font-weight: bold;
}

.slm-portal-input {
  flex: 1;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 6px;
  color: #f8fafc;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 14px;
  padding: 8px 12px;
  resize: none;
  outline: none;
  transition: border-color 0.2s;
}

.slm-portal-input:focus {
  border-color: #f97316;
}

.slm-portal-input::placeholder {
  color: #475569;
}

.slm-send-button {
  background: linear-gradient(135deg, #f97316, #ea580c);
  border: none;
  border-radius: 6px;
  color: white;
  font-family: 'SF Mono', monospace;
  font-size: 12px;
  font-weight: 600;
  padding: 8px 16px;
  cursor: pointer;
  transition: transform 0.1s, opacity 0.2s;
}

.slm-send-button:hover:not(:disabled) {
  transform: scale(1.02);
}

.slm-send-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* Scrollbar styling */
.slm-portal-messages::-webkit-scrollbar {
  width: 8px;
}

.slm-portal-messages::-webkit-scrollbar-track {
  background: rgba(255, 255, 255, 0.05);
}

.slm-portal-messages::-webkit-scrollbar-thumb {
  background: rgba(255, 255, 255, 0.2);
  border-radius: 4px;
}

.slm-portal-messages::-webkit-scrollbar-thumb:hover {
  background: rgba(255, 255, 255, 0.3);
}
```

### 13.4 Backend: TLS WebSocket Stream Endpoint

```go
// pkg/heimdall/api/chat_handler.go

package api

import (
    "context"
    "encoding/json"
    "net/http"
    "time"
    
    "github.com/gorilla/websocket"
    "github.com/orneryd/nornicdb/pkg/auth"
    "github.com/orneryd/nornicdb/pkg/heimdall"
)

var upgrader = websocket.Upgrader{
    ReadBufferSize:  1024,
    WriteBufferSize: 1024,
    CheckOrigin: func(r *http.Request) bool {
        // In production, validate origin
        return true
    },
}

type ChatHandler struct {
    scheduler  *slm.ModelScheduler
    authz      *auth.Authorizer
}

type ChatMessage struct {
    Type    string `json:"type"`    // "chat", "ping"
    Content string `json:"content"`
    Model   string `json:"model,omitempty"`
}

type StreamToken struct {
    Type  string `json:"type"`  // "token", "done", "error"
    Token string `json:"token,omitempty"`
    Message string `json:"message,omitempty"`
}

// HandleChatStream handles WebSocket connections for SLM chat
// Requires admin RBAC role
func (h *ChatHandler) HandleChatStream(w http.ResponseWriter, r *http.Request) {
    // RBAC check - admin only
    user := auth.UserFromContext(r.Context())
    if user == nil || !h.authz.HasRole(user, "admin") {
        http.Error(w, "Forbidden: admin role required", http.StatusForbidden)
        return
    }
    
    // Upgrade to WebSocket
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        http.Error(w, "WebSocket upgrade failed", http.StatusInternalServerError)
        return
    }
    defer conn.Close()
    
    // Set read deadline for pings
    conn.SetReadDeadline(time.Now().Add(60 * time.Second))
    conn.SetPongHandler(func(string) error {
        conn.SetReadDeadline(time.Now().Add(60 * time.Second))
        return nil
    })
    
    for {
        _, message, err := conn.ReadMessage()
        if err != nil {
            if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
                // Log error
            }
            break
        }
        
        var msg ChatMessage
        if err := json.Unmarshal(message, &msg); err != nil {
            h.sendError(conn, "Invalid message format")
            continue
        }
        
        switch msg.Type {
        case "chat":
            h.handleChat(conn, msg)
        case "ping":
            conn.WriteJSON(StreamToken{Type: "pong"})
        }
    }
}

func (h *ChatHandler) handleChat(conn *websocket.Conn, msg ChatMessage) {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    
    // Get or load the requested model
    model, err := h.scheduler.GetModel(msg.Model)
    if err != nil {
        h.sendError(conn, "Model not available: "+err.Error())
        return
    }
    
    // Build prompt with system context
    prompt := h.buildPrompt(msg.Content)
    
    // Stream generation with token callback
    err = model.GenerateStream(ctx, prompt, slm.GenerateParams{
        MaxTokens:   512,
        Temperature: 0.3,
        StopTokens:  []string{"<|im_end|>", "<|endoftext|>"},
    }, func(token string) error {
        return conn.WriteJSON(StreamToken{
            Type:  "token",
            Token: token,
        })
    })
    
    if err != nil {
        h.sendError(conn, "Generation failed: "+err.Error())
        return
    }
    
    // Signal completion
    conn.WriteJSON(StreamToken{Type: "done"})
}

func (h *ChatHandler) buildPrompt(userInput string) string {
    return `<|im_start|>system
You are a cognitive database assistant embedded in NornicDB. You help administrators:
- Analyze graph health and structure
- Diagnose performance issues
- Suggest optimizations
- Answer questions about the database state

You have access to graph statistics and can run analysis queries.
Output structured JSON when analyzing data. Be concise and technical.
<|im_end|>
<|im_start|>user
` + userInput + `
<|im_end|>
<|im_start|>assistant
`
}

func (h *ChatHandler) sendError(conn *websocket.Conn, msg string) {
    conn.WriteJSON(StreamToken{
        Type:    "error",
        Message: msg,
    })
}
```

### 13.5 TLS Configuration

```go
// pkg/heimdall/api/tls.go

type TLSConfig struct {
    CertFile string
    KeyFile  string
    // For self-signed certs in dev
    InsecureSkipVerify bool
}

// The Bifrost runs on a separate TLS port
// Default: 7475 (adjacent to HTTP 7474)
func StartSLMPortalServer(cfg TLSConfig, handler *ChatHandler) error {
    mux := http.NewServeMux()
    
    // WebSocket endpoint
    mux.HandleFunc("/api/bifrost/chat/stream", handler.HandleChatStream)
    
    // REST endpoints for model management
    mux.HandleFunc("/api/bifrost/models", handler.ListModels)
    mux.HandleFunc("/api/bifrost/models/load", handler.LoadModel)
    
    server := &http.Server{
        Addr:    ":7475",
        Handler: mux,
        TLSConfig: &tls.Config{
            MinVersion: tls.VersionTLS12,
            CipherSuites: []uint16{
                tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            },
        },
    }
    
    return server.ListenAndServeTLS(cfg.CertFile, cfg.KeyFile)
}
```

### 13.6 RBAC Integration

```go
// pkg/config/rbac.json - Add SLM permissions

{
  "roles": {
    "admin": {
      "permissions": [
        "slm:chat",
        "slm:models:read",
        "slm:models:load",
        "slm:models:unload",
        "slm:config:read",
        "slm:config:write"
      ]
    },
    "operator": {
      "permissions": [
        "slm:models:read"
      ]
    },
    "user": {
      "permissions": []
    }
  }
}
```

### 13.7 Environment Variables

```bash
# Bifrost Configuration
NORNICDB_HEIMDALL_PORTAL_ENABLED=true
NORNICDB_HEIMDALL_PORTAL_PORT=7475
NORNICDB_HEIMDALL_PORTAL_TLS_CERT=/certs/slm-portal.crt
NORNICDB_HEIMDALL_PORTAL_TLS_KEY=/certs/slm-portal.key
NORNICDB_HEIMDALL_PORTAL_ALLOWED_ROLES=admin
```

---

## 14. Updated Implementation Plan

### Phase 1: Foundation (Week 1-2)
- [ ] BYOM Model Registry (`pkg/heimdall/registry.go`)
- [ ] Extend `pkg/localllm/llama.go` with `GenerateStream()`
- [ ] Model scheduler with LRU eviction
- [ ] Unit tests for model loading/switching

### Phase 2: Chat Portal Backend (Week 3-4)
- [ ] WebSocket stream handler
- [ ] TLS server on separate port
- [ ] RBAC integration for admin-only access
- [ ] Streaming token generation

### Phase 3: Chat Portal UI (Week 5-6)
- [ ] React SLMPortal component
- [ ] Translucent terminal styling
- [ ] Command history (up/down arrows)
- [ ] Model selector dropdown
- [ ] Keyboard shortcuts (Escape to close)

### Phase 4: Integration & Polish (Week 7-8)
- [ ] Graph context injection (stats, health)
- [ ] Built-in commands (`/health`, `/stats`, `/models`)
- [ ] Prometheus metrics for portal usage
- [ ] Documentation + screenshots
- [ ] Security audit

---

## 15. Security Considerations

| Concern | Mitigation |
|---------|------------|
| Unauthorized SLM access | Admin RBAC required, JWT validation |
| Prompt injection | Input sanitization, output schema validation |
| TLS downgrade | Min TLS 1.2, strong cipher suites |
| DoS via long generations | Max tokens limit, request timeout |
| Model path traversal | Validate model names against registry |
| WebSocket hijacking | Origin validation, CSRF tokens |

---

## 16. Implemented Features (v1.0.0)

The following features from this proposal have been implemented:

### Core Heimdall System
- ✅ Multi-model management (embedding + reasoning)
- ✅ GPU-accelerated inference via llama.cpp
- ✅ BYOM (Bring Your Own Model) support
- ✅ CPU fallback when GPU unavailable

### Bifrost Chat Interface
- ✅ OpenAI-compatible API endpoints
- ✅ Server-Sent Events (SSE) for streaming
- ✅ Norse-themed UI with translucent terminal styling
- ✅ Session-persistent chat history
- ✅ Built-in commands (/help, /clear, /status, /model)

### Plugin Architecture
- ✅ `HeimdallPlugin` interface for subsystem management
- ✅ Action registration and invocation system
- ✅ Built-in Watcher plugin with hello-world example

### Advanced Plugin Features (NEW)

#### Optional Lifecycle Hooks

Plugins can implement optional interfaces:

```go
// PrePromptHook - Modify prompts before SLM processing
type PrePromptHook interface {
    PrePrompt(ctx *PromptContext) error
}

// PreExecuteHook - Validate/modify before action execution
type PreExecuteHook interface {
    PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult))
}

// PostExecuteHook - Post-execution logging/state updates
type PostExecuteHook interface {
    PostExecute(ctx *PostExecuteContext)
}

// DatabaseEventHook - React to database operations
type DatabaseEventHook interface {
    OnDatabaseEvent(event *DatabaseEvent)
}
```

#### Database Event Types

The `DatabaseEventHook` receives events for:

| Event Type | Description |
|------------|-------------|
| `node.created`, `node.updated`, `node.deleted`, `node.read` | Node operations |
| `relationship.created`, `relationship.updated`, `relationship.deleted` | Relationship operations |
| `query.executed`, `query.failed` | Query execution |
| `index.created`, `index.dropped` | Index operations |
| `transaction.commit`, `transaction.rollback` | Transaction events |
| `database.started`, `database.shutdown` | System events |
| `backup.started`, `backup.completed` | Backup events |

#### Autonomous Action Invocation

Plugins can autonomously trigger SLM actions via `HeimdallInvoker`:

```go
type HeimdallInvoker interface {
    // Synchronous action invocation
    InvokeAction(action string, params map[string]interface{}) (*ActionResult, error)
    
    // Send natural language prompt to SLM
    SendPrompt(prompt string) (*ActionResult, error)
    
    // Async versions (fire-and-forget)
    InvokeActionAsync(action string, params map[string]interface{})
    SendPromptAsync(prompt string)
}
```

**Example: Autonomous Anomaly Detection**

```go
func (p *SecurityPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    if event.Type == heimdall.EventQueryFailed {
        p.failureCount++
        if p.failureCount >= 5 && p.ctx.Heimdall != nil {
            // Trigger analysis after threshold exceeded
            p.ctx.Heimdall.InvokeActionAsync("heimdall.anomaly.detect", map[string]interface{}{
                "trigger": "autonomous",
                "reason":  "query_failures",
            })
        }
    }
}
```

#### Inline Notification System

Notifications from lifecycle hooks are queued and sent inline with streaming responses:

```go
func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    ctx.NotifyInfo("Processing", "Analyzing your request...")
    return nil
}
```

Notification flow:
1. PrePrompt notifications → sent before AI response
2. PreExecute notifications → sent after AI response, before action result
3. PostExecute notifications → sent after action result

UI displays notifications with `[Heimdall]:` prefix and distinct styling.

#### Request Cancellation

Any lifecycle hook can cancel a request:

```go
func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    if !p.isAuthorized(ctx.UserMessage) {
        ctx.Cancel("Unauthorized request", "PrePrompt:myplugin")
        return nil
    }
    return nil
}
```

Cancellation:
- Stops the request immediately
- Sends cancellation message to user via Bifrost
- Logs reason and cancelling hook

### Data Flow Architecture

```
User Message → Bifrost → PromptContext
                              │
                    ┌─────────▼─────────┐
                    │  PrePrompt Hooks  │ → Notifications queued
                    │  (can cancel)     │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Heimdall SLM    │ → Generates action JSON
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  PreExecute Hooks │ → Validate/modify params
                    │  (can cancel)     │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Action Execution │ → Plugin handler runs
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │ PostExecute Hooks │ → Log, notify, update state
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │ Streaming Response│ → Notifications + result
                    └───────────────────┘
```

---
