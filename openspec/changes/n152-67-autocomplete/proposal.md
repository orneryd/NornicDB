# Preserve autocomplete Cypher at its response boundary

## Problem and contract

N152-67's fresh public witnesses reproduce quote corruption on release base
`f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210`: the generated-response cleanup
mistakes the last three backticks of an escaped alias for a closing Markdown
fence and treats spaces within an alias as repeated output. Other cleanup
stages likewise inspect quoted text as instructions or split/join its lines.
The React consumer independently strips the same legitimate backticks.
Separately, the handler requests an action name that the bundled Watcher does
not register, preventing the normal public route from reaching suggestions.

The backend owns model-response framing and extraction; the API carries Cypher
text, and the UI offers that text unchanged. Preserve the existing first-query
and outside-quote instruction/prose controls. Preserve quoted token and comment
bytes through every extraction stage. Whitespace alone is not response framing.
Do not change model prompts, identifier decoding, or the existing first-query-line
behavior into multiline query support. Parent review additionally approved removal
of autocomplete text stop strings: the native generator otherwise truncates quoted
content before extraction. Preserve its 128-token bound and model EOS handling.

## Owners and implementation

- `pkg/heimdall/handler.go`: call the actual bundled
  `heimdall_watcher_autocomplete_suggest` action. Reuse the canonical
  `internal/cyphertext.SkipToken` for opaque quote/comment boundaries when
  extracting generated Cypher; no second lexical grammar.
- `QueryAutocomplete.tsx`: remove duplicate Markdown parsing of API Cypher.
- Public endpoint tests retain the ticket's fresh witness bytes; actual bundled
  registration and actual React editor selection are independently exercised.

Parent approved this scope before production edits. The prior staged proposal
at `codex/n152-67-autocomplete-quoted-cypher-v152` was inspected read-only and its
patch archived as `/tmp/n152-67-prior-proposal.patch`. Its paired-fence and action
corrections inform this change, but its remaining quote-blind cleanup is not
accepted as a complete fix.

## Duplicate investigation and limits

The backend and UI duplicate fence removal, and the backend duplicates quote
decisions already owned by cyphertext. Converge those in-scope behaviors through
backend extraction and direct SkipToken reuse. The shared primitive recognizes
backslash-escaped strings, doubled backtick identifiers and comments. It does
not own model-specific prose or Markdown framing, which remain in Heimdall.

DeepSeek read-only investigation `499c7b2c29f94b49ba9e6f34a126a4d6` corroborated
the destructive stages. Graph retrieval failed; the worker could not read the
coverage test because its snapshot scanner rejected that file. Parent inspected
the real existing assertion independently. No clean indexed-duplicate claim.

Historical receipt fc79 is missing. The exact current ticket witnesses are
fresh reconstructed cases, not an exact replay of that receipt. Keep that
acceptance criterion pending and do not imply the whole ticket is resolved.

## Native generation boundary

The local CGO adapter forwards GenerateParams.StopTokens to GenerationModel.
Generate delegates to GenerateStream, which calls splitVisibleTextForStopTokens
using strings.Index before returning text to Heimdall. All old autocomplete
text stops (including triple newline) can appear inside quoted Cypher. Remove
them at the request owner; retain MaxTokens=128 and native EOS termination.
A public-handler parameter assertion failed before this correction. Model-free
native helper tests exercise the real stop implementation with each old marker,
then demonstrate exact chunk preservation with nil text stops. No model loading
or actual inference is claimed. OpenAI/Ollama request adapters currently do not
forward these custom stops; no provider serialization changes are required.
