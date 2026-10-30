# Heimdall Context & Token Budget Guide

This guide explains how Heimdall manages context and token allocation with the default qwen3-0.6b-instruct model.

## Overview

Heimdall uses a **single-shot command architecture** - each request is independent with no conversation history accumulation. This maximizes the available context for rich system prompts while keeping responses fast.

## Token Budget Allocation

```
┌─────────────────────────────────────────────────────────────────┐
│                    8K CONTEXT WINDOW (default)                    │
│              (model may support larger; configurable)             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │            SYSTEM PROMPT (6K budget)                      │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │ Identity & Role (~50 tokens)                       │  │   │
│  │  │ "You are Heimdall, the AI assistant..."            │  │   │
│  │  ├────────────────────────────────────────────────────┤  │   │
│  │  │ Available Actions (~200-500 tokens)                │  │   │
│  │  │ - heimdall_watcher_status                          │  │   │
│  │  │ - heimdall_watcher_query                           │  │   │
│  │  │ - [plugin-registered actions]                      │  │   │
│  │  ├────────────────────────────────────────────────────┤  │   │
│  │  │ Cypher Query Primer (~400 tokens)                  │  │   │
│  │  │ - Basic patterns, filtering, aggregations          │  │   │
│  │  │ - Path queries, modifications, subqueries          │  │   │
│  │  ├────────────────────────────────────────────────────┤  │   │
│  │  │ Response Modes (~100 tokens)                       │  │   │
│  │  │ - ACTION MODE: JSON for operations                 │  │   │
│  │  │ - HELP MODE: Conversational for questions          │  │   │
│  │  ├────────────────────────────────────────────────────┤  │   │
│  │  │ Plugin Instructions (~variable)                    │  │   │
│  │  │ - AdditionalInstructions from plugins              │  │   │
│  │  ├────────────────────────────────────────────────────┤  │   │
│  │  │ Examples (~500 tokens)                             │  │   │
│  │  │ - 20 built-in examples for common commands         │  │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  │                                                           │   │
│  │  Total base system: ~1,200 tokens                         │   │
│  │  Available for plugins: ~4,800 tokens                     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │            USER MESSAGE (2K budget)                       │   │
│  │  Single-shot command from Bifrost UI                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │            RESPONSE (1K max tokens)                       │   │
│  │  JSON action OR conversational help                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Default Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `NORNICDB_HEIMDALL_CONTEXT_SIZE` | 8192 | Model context window (tokens) |
| `NORNICDB_HEIMDALL_BATCH_SIZE` | 2048 | Batch size for prefill |
| `NORNICDB_HEIMDALL_MAX_TOKENS` | 1024 | 1K response limit |

Heimdall also enforces a **prompt construction budget** (so plugins can’t blow up the system prompt):

| Setting | Default | Description |
|---------|---------|-------------|
| `NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS` | 8192 | Total prompt budget (system + user) |
| `NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS` | 6000 | System prompt budget (base + plugins) |
| `NORNICDB_HEIMDALL_MAX_USER_TOKENS` | 2000 | User message budget |

The same settings apply to **all providers** (local GGUF, Ollama, OpenAI). Override via the env vars above or via YAML (`heimdall.max_context_tokens`, `max_system_tokens`, `max_user_tokens`).

### Higher token budgets for remote providers (Ollama/OpenAI)

For **remote providers** (Ollama, OpenAI), inference runs on the provider’s infrastructure, so there is no local GPU memory limit. You can safely **increase** the token budgets to match the model’s context window (e.g. 32K or 128K).

Example — 32K context (e.g. GPT-4, many Ollama models):

```bash
export NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS=32768
export NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS=24000
export NORNICDB_HEIMDALL_MAX_USER_TOKENS=8000
```

Example — 128K context (e.g. **GPT-4o-mini**, GPT-4 Turbo, Claude, or large Ollama models):

```bash
export NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS=131072
export NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS=100000
export NORNICDB_HEIMDALL_MAX_USER_TOKENS=30000
```

Keep `max_system_tokens` + `max_user_tokens` within your model’s context size and leave headroom for the response. Startup logs show the active budget, e.g. `Token budget: 32K context = 24K system + 8K user`.

## How Multi-Batch Prefill Works

When the system prompt exceeds the batch size, Heimdall automatically splits it into multiple batches:

```
System Prompt (2K tokens) + User Message (500 tokens) = 2.5K total

Batch 1: [System prompt tokens 0-2047]      → KV cache stores
Batch 2: [Remaining tokens + user message]  → KV cache accumulates
                                            → Generation starts
```

The KV cache accumulates across batches, so the model "sees" the entire context when generating.

## Token Budget Constants

These defaults are defined in `pkg/heimdall/types.go` and can be overridden via environment variables:

```go
const (
    DefaultMaxContextTokens      = 8192  // 8K total context budget
    DefaultMaxSystemPromptTokens = 6000  // 6K for system + plugins
    DefaultMaxUserMessageTokens  = 2000  // 2K for user commands
    TokensPerChar         = 0.25   // ~4 chars per token estimate
)
```

## What Fits in the System Prompt

| Component | Estimated Tokens | Notes |
|-----------|-----------------|-------|
| Base identity | ~50 | Fixed header |
| Available actions | 200-500 | Depends on plugin count |
| Cypher primer | ~400 | Reference guide |
| Response modes | ~100 | Action + Help modes |
| Built-in examples | ~500 | 20 comprehensive examples |
| **Base total** | **~1,200** | Before plugins |
| Plugin instructions | ~10,800 available | Plugins can add context |

## Fallback Behavior

If plugins add too many instructions and the system prompt exceeds the system budget, Heimdall automatically falls back to a minimal prompt:

```go
// Minimal fallback prompt (~200 tokens)
"You are Heimdall, AI assistant for NornicDB graph database.

ACTIONS:
[plugin actions only]

For queries: {"action": "heimdall_watcher_query", "params": {"cypher": "..."}}
Respond with JSON only."
```

## Performance Characteristics

### What Affects Speed

| Factor | Impact | Notes |
|--------|--------|-------|
| **MaxTokens** | High | Each output token takes ~same time |
| **GPU vs CPU** | Very High | GPU is 10-50x faster |
| **Prompt size** | Low | Only affects prefill, not generation |
| **Context/Batch size** | Minimal | Memory allocation only |

### Why Context Size Doesn't Slow Down Inference

1. **KV Cache is lazy** - Only allocates for actual tokens used
2. **Prefill is fast** - Parallel processing of input tokens
3. **Generation dominates** - 90% of time is in token generation
4. **Your prompts are small** - typically ~1–3K tokens vs 8K default capacity (and you can increase the context window if needed)

## Model Specifications

### qwen3-0.6b-instruct

| Spec | Value |
|------|-------|
| Parameters | 500M |
| Context Length | 32,768 tokens |
| Quantization | Q4_K_M recommended |
| VRAM (GPU) | ~500MB |
| RAM (CPU) | ~1GB |
| License | Apache 2.0 |

## Configuration Examples

### Default (Balanced)
```bash
NORNICDB_HEIMDALL_ENABLED=true
# Uses defaults - 8K context, 2K batch, 1K output
```

### Memory Constrained
```bash
NORNICDB_HEIMDALL_ENABLED=true
NORNICDB_HEIMDALL_CONTEXT_SIZE=4096   # Reduce if low RAM
NORNICDB_HEIMDALL_BATCH_SIZE=1024
NORNICDB_HEIMDALL_MAX_TOKENS=512      # Shorter responses
```

### Verbose Responses
```bash
NORNICDB_HEIMDALL_ENABLED=true
NORNICDB_HEIMDALL_MAX_TOKENS=2048     # Allow longer explanations
```

## Monitoring Token Usage

The handler logs token budget information:

```
[Bifrost] Token budget: system=1247, user=156, total=1403/8192
```

If you see truncation errors, check:
1. Is MaxTokens high enough for the response?
2. Are plugins adding too many instructions?
3. Is the user message within budget?

## See Also

- [Heimdall AI Assistant](./heimdall-ai-assistant.md) - Overview and configuration
- [Heimdall Plugins](./heimdall-plugins.md) - Writing custom plugins
- [Operations - Monitoring](../operations/monitoring.md) - Prometheus metrics
