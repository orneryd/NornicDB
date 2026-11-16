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

## Automatic backend selection

The HTTP assistant selects the execution path automatically. `Manager.SupportsTools()`
checks whether the configured generator implements `GeneratorWithTools`. When it does
and tools are available, the agent loop uses native tool calls. Otherwise, it asks the
text generator for a JSON action envelope. Go embedding callers select the prompt
contract explicitly with the API above.

This is an adapter capability check, not a model-name lookup or a live test of the
model. An adapter can advertise native tool support while its selected model cannot
fulfil that contract. Choose a model that supports its backend's tool API; a rejected
native-tool request returns an error rather than automatically retrying in JSON mode.

MCP-shaped tool definitions are shared by both paths. Native provider responses and
text-generated action envelopes still enter separate execution loops; this API change
does not claim to consolidate those loops.

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
