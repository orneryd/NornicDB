# RFC: Heimdall SLM Quality Control for Auto-TLP

**Status:** Proposal  
**Author:** NornicDB Team  
**Created:** December 2024  

## Summary

Add an optional LLM-based quality control layer to Auto-TLP (Automatic Topological Link prediction) that validates relationship suggestions before they're created. This "Heimdall" layer uses a small, local instruction-tuned model to review TLP's algorithmic suggestions and can optionally suggest additional relationships.

## Motivation

Auto-TLP automatically creates edges based on:
- Embedding similarity
- Co-access patterns
- Temporal proximity
- Transitive inference

While these algorithms are fast and effective, they can produce false positives:
- **Similarity noise**: Similar embeddings don't always mean meaningful relationships
- **Spurious co-access**: Users might access unrelated nodes in the same session
- **Transitive errors**: A→B and B→C doesn't always mean A should connect to C

An LLM can provide **semantic validation** that algorithms can't:
- "These two notes are about the same project" ✅
- "These nodes share keywords but aren't actually related" ❌
- "This relationship would be more accurately typed as INSPIRED_BY" 🔄

## Design Goals

1. **Opt-in via feature flags** - Disabled by default, zero impact if not enabled
2. **Small model friendly** - Works with 1-3B parameter instruction models
3. **Fail-open** - LLM failures don't block edge creation
4. **Batch efficient** - Multiple suggestions per LLM call
5. **Size aware** - Gracefully handles large nodes that exceed context limits
6. **Augmentation capable** - LLM can suggest edges TLP missed (optional)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Auto-TLP Pipeline                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Node Created/Accessed                                          │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────┐                                          │
│  │ TLP Algorithms   │  Fast, algorithmic candidate generation  │
│  │ • Similarity     │                                          │
│  │ • Co-access      │                                          │
│  │ • Temporal       │                                          │
│  │ • Transitive     │                                          │
│  └────────┬─────────┘                                          │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐     ┌─────────────────────────────────┐  │
│  │ LLM_QC Enabled?  │────▶│ Skip QC, return all candidates  │  │
│  └────────┬─────────┘ No  └─────────────────────────────────┘  │
│           │ Yes                                                 │
│           ▼                                                     │
│  ┌──────────────────┐                                          │
│  │ Batch & Check    │  Group candidates, check size limits     │
│  │ Size Limits      │                                          │
│  └────────┬─────────┘                                          │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐     ┌─────────────────────────────────┐  │
│  │ Prompt too big?  │────▶│ Log warning, pass batch through │  │
│  └────────┬─────────┘ Yes └─────────────────────────────────┘  │
│           │ No                                                  │
│           ▼                                                     │
│  ┌──────────────────┐                                          │
│  │ Heimdall SLM     │  Local instruct model reviews batch      │
│  │ Batch Review     │                                          │
│  └────────┬─────────┘                                          │
│           │                                                     │
│           ├──────── LLM Error ──────▶ Log, pass through        │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐                                          │
│  │ Parse Response   │  Extract approved/rejected indices       │
│  └────────┬─────────┘                                          │
│           │                                                     │
│           ├──────── Parse Error ────▶ Fuzzy parse or approve   │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐     ┌─────────────────────────────────┐  │
│  │ Augment Enabled? │────▶│ Include LLM's new suggestions   │  │
│  └────────┬─────────┘ Yes └─────────────────────────────────┘  │
│           │ No                                                  │
│           ▼                                                     │
│  Return approved edges                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `NORNICDB_AUTO_TLP_ENABLED` | ❌ Off | Enable TLP candidate generation |
| `NORNICDB_AUTO_TLP_LLM_QC_ENABLED` | ❌ Off | Enable Heimdall batch review |
| `NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED` | ❌ Off | Allow Heimdall to suggest new edges |

**Progressive enablement:**
```bash
# Stage 1: TLP only (fast, no LLM)
export NORNICDB_AUTO_TLP_ENABLED=true

# Stage 2: TLP + Heimdall review (higher quality)
export NORNICDB_AUTO_TLP_ENABLED=true
export NORNICDB_AUTO_TLP_LLM_QC_ENABLED=true

# Stage 3: Full hybrid (TLP + review + augmentation)
export NORNICDB_AUTO_TLP_ENABLED=true
export NORNICDB_AUTO_TLP_LLM_QC_ENABLED=true
export NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED=true
```

## Unified SLM Architecture

Heimdall QC uses the **same SLM instance** as Bifrost commands:
- **Stateless**: No context accumulates between calls
- **One-shot**: Each call is independent, complete in single pass
- **KV Cache**: Static system prompt cached, only data varies

```
┌─────────────────────────────────────────────────────────┐
│                    SINGLE SLM INSTANCE                  │
├─────────────────────────────────────────────────────────┤
│  KV Cache (static, loaded once):                        │
│  ┌─────────────────────────────────────────────────────┐│
│  │ [Bifrost Commands] [Heimdall QC Instructions]       ││
│  └─────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────┤
│  Per-call (dynamic):                                    │
│  ┌───────────────────┐  ┌──────────────────────────────┐│
│  │ Bifrost: "CREATE  │  │ Heimdall: "SRC:node-1[Note]  ││
│  │ (n:Person)"       │  │ EDGES:0.node-2→REL(80%)"    ││
│  └───────────────────┘  └──────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## Prompt Format

**System Prompt (static, KV cached):**
```
Review graph edges. Output JSON only.
Format: {"approved":[indices],"rejected":[indices],"reasoning":"why"}
Approve if nodes are meaningfully related. Reject spam/duplicates.
```

**User Content (dynamic, per-call):**
```
SRC:node-123[Memory,Note]
 title:Machine Learning Basics
 content:Introduction to neural networks...
EDGES:
0.node-456→RELATES_TO(85%)
1.node-789→RELATES_TO(72%)
```

**Response (JSON only):**
```json
{"approved":[0],"rejected":[1],"reasoning":"First related, second unrelated task"}
```

**With augmentation:**
```json
{"approved":[0],"additional":[{"target_id":"node-999","type":"INSPIRED_BY","conf":0.8,"reason":"both discuss backprop"}]}
```

## Configuration

```go
type HeimdallQCConfig struct {
    Enabled               bool          // Master switch
    Timeout               time.Duration // Default: 10s
    MaxContextBytes       int           // Default: 4096 (~1000 tokens)
    MaxBatchSize          int           // Default: 5 suggestions per call
    MaxNodeSummaryLen     int           // Default: 200 chars per property
    MinConfidenceToReview float64       // Default: 0.5 (skip weak candidates)
    CacheDecisions        bool          // Default: true
    CacheTTL              time.Duration // Default: 1 hour
}
```

## Error Handling

**Principle: Fail-open, log, continue**

| Error | Action |
|-------|--------|
| LLM timeout | Log warning, approve batch, continue |
| LLM crash | Log error, approve batch, continue |
| Invalid JSON | Fuzzy parse or approve all |
| Prompt too large | Log warning, skip review, pass through |
| Context cancelled | Return immediately with current results |

**No retries** - If the LLM fails, we don't retry. We log the decision made without LLM input and move on.

## Usage Example

```go
import (
    "github.com/orneryd/nornicdb/pkg/inference"
    "github.com/orneryd/nornicdb/pkg/config"
    "github.com/orneryd/nornicdb/pkg/heimdall"
)

// Heimdall QC uses the SAME Generator as Bifrost commands
// Direct llama.cpp via localllm - no HTTP calls
func setupHeimdallQC(generator heimdall.Generator) {
    systemPrompt := inference.GetSystemPrompt(config.IsAutoTLPLLMAugmentEnabled())
    
    heimdallFunc := func(ctx context.Context, userContent string) (string, error) {
        // Combine static system prompt + dynamic user content
        prompt := systemPrompt + "\n\n" + userContent
        return generator.Generate(ctx, prompt, heimdall.GenerateParams{
            MaxTokens:   256,
            Temperature: 0.1, // Low temp for deterministic QC
        })
    }
    
    qc := inference.NewHeimdallQC(heimdallFunc, nil)
    engine.SetHeimdallQC(qc)
}

// Both Bifrost commands and Heimdall QC share:
// - Same heimdall.Generator (in-memory llama.cpp)
// - Same KV cache (system prompts cached)
// - Stateless one-shot calls
```

## Performance Expectations

| Metric | Without QC | With QC |
|--------|-----------|---------|
| Latency per node | ~5-20ms | ~100-500ms |
| Edge quality | Good | Better |
| False positives | Some | Fewer |
| LLM calls | 0 | ~1 per 5 suggestions |

**Mitigations:**
- Batch processing reduces calls
- Caching prevents redundant reviews
- Size limits prevent slow large-context calls
- Async processing possible for background indexing

## Alternatives Considered

### 1. Pre-trained classifier
- **Pro**: Faster than LLM
- **Con**: Requires training data, less flexible
- **Decision**: LLM is more adaptable to diverse data

### 2. Rule-based filtering
- **Pro**: Zero latency
- **Con**: Can't understand semantics
- **Decision**: TLP already does this; LLM adds semantic layer

### 3. Post-hoc cleanup
- **Pro**: Doesn't slow down creation
- **Con**: Edges exist until cleaned; user sees noise
- **Decision**: Better to validate before creation

## Open Questions

1. **Batch size tuning**: Is 5 the right default? Should it auto-tune based on model?

2. **Augmentation scope**: Should augmented edges have lower initial confidence?

3. **Model recommendations**: Which small models work best? (Qwen 1.5B? Phi-3? Gemma 2B?)

4. **Async mode**: Should there be an option to review edges asynchronously?

## Feedback Requested

- Does this solve a real problem for your use case?
- Are the feature flags granular enough?
- What small models have you had success with?
- Should there be a "strict mode" that blocks on LLM errors?
- Other edge types Heimdall should suggest?

---

**Implementation PR:** [Link to PR when ready]

**Related Issues:**
- #XXX Auto-TLP implementation
- #XXX Edge decay system
