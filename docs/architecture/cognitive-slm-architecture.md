# Cognitive SLM Architecture (Heimdall)

This document is the architectural overview for **Heimdall**: NornicDB's embedded Small Language Model subsystem. Code lives in `pkg/heimdall/` and `pkg/inference/`. For operator setup and YAML/env configuration, see [Heimdall AI Assistant](../user-guides/heimdall-ai-assistant.md).

## What's in scope

- An embedded reasoning model that runs alongside the BGE-M3 embedding model.
- An OpenAI-compatible chat API (Bifrost) for in-app and IDE-tool use.
- A plugin system that lets the model invoke registered actions, observe database events, and (optionally) trigger autonomous remediation.
- Inference-engine integration for Auto-TLP edge quality control (`pkg/inference/HeimdallQC`).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    NornicDB Cognitive Engine                     │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  Embedding   │    │   Reasoning  │    │   Graph Engine   │  │
│  │    Model     │    │     SLM      │    │   (Storage +     │  │
│  │  (BGE-M3)    │    │  (Qwen3 0.6B)│    │    Cypher)       │  │
│  │  1024 dims   │    │   default    │    │                  │  │
│  └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘  │
│         │                   │                      │            │
│         └───────────────────┴──────────────────────┘            │
│                             │                                    │
│                    ┌────────▼────────┐                          │
│                    │ heimdall.Manager│                          │
│                    │ (single-model   │                          │
│                    │  per process)   │                          │
│                    └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

The reasoning SLM is a separate model from the embedding model. Both run inside the NornicDB process; the embedding model serves all vector-search paths, and the reasoning SLM serves the Bifrost chat endpoint and any registered Heimdall plugins.

## Provider abstraction

`heimdall.Manager` is the entry point. It hides three provider backends behind a `Generator` interface:

| Provider                       | Source file                                          | Selected by                                   |
| ------------------------------ | ---------------------------------------------------- | --------------------------------------------- |
| **Local GGUF** (CGO llama.cpp) | `pkg/heimdall/generator_cgo.go`, `generator_yzma.go` | `NORNICDB_HEIMDALL_PROVIDER=local` (or unset) |
| **Ollama**                     | `pkg/heimdall/generator_ollama.go`                   | `NORNICDB_HEIMDALL_PROVIDER=ollama`           |
| **OpenAI / OpenAI-compatible** | `pkg/heimdall/generator_openai.go`                   | `NORNICDB_HEIMDALL_PROVIDER=openai`           |

The Manager is constructed once at startup with `NewManager(cfg)`. Provider selection cannot change at runtime; restart the server to switch. Each provider implements the same `Generate`, `GenerateStream`, `Chat`, and `GenerateWithTools` methods.

CPU fallback for the local provider is implemented in `loadLocalGenerator`: if GPU loading fails, the manager retries with `gpu_layers=0`.

## Model selection

Default model: `qwen3-0.6b-instruct` (Apache 2.0). Override via `NORNICDB_HEIMDALL_MODEL`. The model name is the GGUF filename without the `.gguf` extension for the local provider; for Ollama and OpenAI it's the provider-specific model identifier.

| Model                  | License             | Quantization | VRAM (Q4_K_M) | Notes                        |
| ---------------------- | ------------------- | ------------ | ------------- | ---------------------------- |
| qwen3-0.6b-instruct    | Apache 2.0          | Q4_K_M       | ~350 MB       | Default; good JSON adherence |
| qwen2.5-1.5b-instruct  | Apache 2.0          | Q4_K_M       | ~900 MB       | When 0.6B is insufficient    |
| qwen2.5-3b-instruct    | Apache 2.0          | Q4_K_M       | ~1.8 GB       | Strongest reasoning          |
| phi-3-mini-4k-instruct | MIT                 | Q4_K_M       | ~2.3 GB       | Microsoft alternative        |
| llama-3.2-1b-instruct  | Llama 3.2 community | Q4_K_M       | ~1.3 GB       | Llama alternative            |

All recommended models are commercial-use compatible.

## Bifrost (chat surface)

The Bifrost endpoint is an OpenAI-compatible chat API. It shares the regular HTTP listener and uses standard NornicDB authentication.

| Path                            | Method | Description                                                 |
| ------------------------------- | ------ | ----------------------------------------------------------- |
| `/api/bifrost/status`           | GET    | Heimdall + Bifrost status (auth: read)                      |
| `/api/bifrost/chat/completions` | POST   | Native chat endpoint (auth: write)                          |
| `/api/bifrost/autocomplete`     | POST   | Cypher autocomplete (auth: read)                            |
| `/api/bifrost/events`           | GET    | SSE stream for real-time events (auth: read)                |
| `/v1/chat/completions`          | POST   | OpenAI-compatible alias for `/api/bifrost/chat/completions` |
| `/v1/models`                    | GET    | OpenAI-compatible model list                                |

Streaming uses Server-Sent Events when the request sets `stream: true`, matching OpenAI's wire format. The `model` field in chat requests is accepted but ignored — Heimdall always uses the configured backend model.

## Plugin system

Heimdall plugins implement `heimdall.HeimdallPlugin` (`pkg/heimdall/plugin.go`). Plugins register **actions** that the SLM can invoke, plus optional **lifecycle hooks**:

| Hook                | Purpose                                                                     |
| ------------------- | --------------------------------------------------------------------------- |
| `PrePromptHook`     | Modify the system prompt before the SLM call (or cancel the request).       |
| `PreExecuteHook`    | Validate or rewrite action params before execution (or cancel the request). |
| `PostExecuteHook`   | Log, audit, or send follow-up notifications after action execution.         |
| `DatabaseEventHook` | React to graph mutations, query events, transaction commits, etc.           |

Plugins receive a `SubsystemContext` with handles to:

- The SLM (`HeimdallInvoker`) — for autonomous action invocation or natural-language prompts.
- The graph (`DatabaseRouter`) — for read/write Cypher.
- The Bifrost bridge (`BifrostBridge`) — for sending notifications to active chat sessions.
- Per-database RBAC context (`PrincipalRoles`, `DatabaseAccessMode`, `ResolvedAccess`) when the request is authenticated.

For the full plugin contract see [Heimdall Plugins](../user-guides/heimdall-plugins.md). For event-driven remediation patterns see [Event Triggers and Automatic Remediation](../user-guides/heimdall-event-triggers-remediation.md).

## Inference-engine integration

Heimdall's reasoning capability is also wired into Auto-TLP through `pkg/inference.HeimdallQC`. When `NORNICDB_AUTO_TLP_LLM_QC_ENABLED=true`, candidate edges from the topological link-prediction algorithms are reviewed by the SLM before materialisation. The relevant feature flags:

- `NORNICDB_AUTO_TLP_ENABLED` — turn on automatic edge inference.
- `NORNICDB_AUTO_TLP_LLM_QC_ENABLED` — gate inference on SLM approval.
- `NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED` — let the SLM suggest additional edges beyond what TLP found.

See [Auto-TLP Heimdall](../features/auto-tlp-heimdall.md) and [Feature Flags](../features/feature-flags.md) for the full surface.

## Safety constraints

The Heimdall surface is intentionally narrow:

- Plugin actions execute in the host process with the same OS permissions as `nornicdb serve`. Plugins are not sandboxed; only load plugins from trusted sources.
- The SLM itself does not directly mutate the storage engine. All mutations go through the Cypher executor, the standard transaction layer, and the existing per-database RBAC enforcement.
- `ActionOpcode` (in `pkg/heimdall/types.go`) defines a finite enumerated set of operations the SLM is allowed to emit; any output outside that schema is rejected before execution.
- Chat content from users is treated as untrusted input: prompt-injection mitigations rely on system-prompt design and the `ActionOpcode` whitelist, not on input sanitization alone.

## Token budget

Heimdall enforces a structured token budget at prompt-construction time so plugin authors can't blow up the system prompt. The defaults (`pkg/heimdall/types.go`):

```go
const (
    DefaultMaxContextTokens      = 8192  // Total prompt budget
    DefaultMaxSystemPromptTokens = 6000  // System prompt + plugin contributions
    DefaultMaxUserMessageTokens  = 2000  // User message
)
```

These apply to all providers. Override via `NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS`, `NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS`, `NORNICDB_HEIMDALL_MAX_USER_TOKENS`, or YAML (`heimdall.max_context_tokens` etc.). For remote providers (Ollama, OpenAI) you can safely raise these to 32K or 128K to use the provider's full context window — see [Heimdall Context & Tokens](../user-guides/heimdall-context.md).

## Configuration surface

The minimal env var set:

| Variable                         | Purpose                                                                                                             |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_HEIMDALL_ENABLED`      | Master switch. Default `false`.                                                                                     |
| `NORNICDB_HEIMDALL_PROVIDER`     | `local` \| `ollama` \| `openai`. Default `local`.                                                                   |
| `NORNICDB_HEIMDALL_MODEL`        | Model identifier.                                                                                                   |
| `NORNICDB_HEIMDALL_API_URL`      | Provider URL (Ollama/OpenAI).                                                                                       |
| `NORNICDB_HEIMDALL_API_KEY`      | Provider API key (OpenAI).                                                                                          |
| `NORNICDB_MODELS_DIR`            | Local GGUF directory.                                                                                               |
| `NORNICDB_HEIMDALL_GPU_LAYERS`   | `-1` = auto, `0` = CPU only. Local provider only.                                                                   |
| `NORNICDB_HEIMDALL_CONTEXT_SIZE` | Local context window size in tokens.                                                                                |
| `NORNICDB_HEIMDALL_BATCH_SIZE`   | Local batch size.                                                                                                   |
| `NORNICDB_HEIMDALL_MAX_TOKENS`   | Max tokens per response.                                                                                            |
| `NORNICDB_HEIMDALL_TEMPERATURE`  | Sampling temperature.                                                                                               |
| `NORNICDB_HEIMDALL_MCP_ENABLE`   | Add MCP memory tools (`store`, `recall`, `discover`, `link`, `tasks`) to the agentic loop. Default `false`. |
| `NORNICDB_HEIMDALL_MCP_TOOLS`    | Allowlist for MCP tools. Unset = all (when MCP enabled); empty = none; comma-separated list = subset.               |

For the complete reference (including the token-budget overrides above) see [Environment Variables Reference](../operations/environment-variables.md).

## Resource expectations

For the default configuration (BGE-M3 embedding + qwen3-0.6b reasoning, both Q4_K_M):

| Component            | VRAM        |
| -------------------- | ----------- |
| BGE-M3 embedding     | ~600 MB     |
| qwen3-0.6b reasoning | ~350 MB     |
| KV cache             | ~200 MB     |
| **Total**            | **~1.2 GB** |

Both fit comfortably on Apple Silicon's unified memory and on NVIDIA GPUs with 4 GB+ VRAM. CPU fallback is supported for both models when GPU memory is insufficient or unavailable.

## Related Documentation

- [Heimdall AI Assistant](../user-guides/heimdall-ai-assistant.md) — operator setup and configuration.
- [Heimdall Context & Tokens](../user-guides/heimdall-context.md) — token budgeting and provider-specific context windows.
- [Heimdall Agentic Loop](../user-guides/heimdall-agentic-loop.md) — how plugin actions and MCP tools chain together.
- [Enabling MCP Tools](../user-guides/heimdall-mcp-tools.md) — how to add `store/recall/discover/link/task/tasks` to the loop.
- [Heimdall Plugins](../user-guides/heimdall-plugins.md) — writing custom plugins, lifecycle hooks, autonomous invocation.
- [Event Triggers and Automatic Remediation](../user-guides/heimdall-event-triggers-remediation.md) — wiring database events through the SLM.
- [Auto-TLP Heimdall](../features/auto-tlp-heimdall.md) — SLM-gated edge inference.
- [Feature Flags](../features/feature-flags.md) — full feature-flag surface including Heimdall.
