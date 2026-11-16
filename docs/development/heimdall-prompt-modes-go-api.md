# Heimdall prompt modes

Use one prompt API with an explicit response contract:

```go
system := promptCtx.BuildFinalPrompt(heimdall.PromptModeJSONAction)
// Or, for a provider using native tool calls:
system = promptCtx.BuildFinalPrompt(heimdall.PromptModeNativeTools)
```

JSON-action mode answers from supplied context or emits one JSON action envelope.
Native-tool mode answers in natural language and uses the provider's tool-call protocol
when an action is needed. Both retain the same actions, examples and additional context.

## Go migration

| Previous call | Replacement |
| --- | --- |
| `ctx.BuildFinalPrompt()` | `ctx.BuildFinalPrompt(heimdall.PromptModeJSONAction)` |
| `ctx.BuildFinalPromptForTools()` | `ctx.BuildFinalPrompt(heimdall.PromptModeNativeTools)` |

Rebuild Go callers and plugins. The old signatures and private forwarding layer are
removed. `PromptMode` is a named boolean: exactly two values are representable, and its
zero value selects JSON actions. There is no unknown-mode fallback.

## Preserved behavior

Stateless prompts use the existing minimal prompt when the full prompt exceeds the
system-token budget. Conversation prompts retain their database context; existing
budget validation reports an oversized prompt instead of discarding that context.
Budget estimates continue to measure JSON-action mode, including conversation compaction.

Handlers select the generation contract before rendering and pass the rendered prompt
to the response path. Budget estimation can still render a prompt independently; this
change does not claim that every request performs exactly one prompt construction.
Database storage and wire formats are unchanged.

## Tests

`prompt_modes_test.go` checks both response contracts, zero-value mode, conversation
context, minimal-budget fallback and the prompt received by each provider path.
Conversation tool-event, context-budget and streaming controls cover the associated
workspace behavior. Full release checks remain separate CI evidence.
