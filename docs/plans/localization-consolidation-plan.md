# Production Message Localization Plan

## Goal

Move human-readable production text out of individual call sites and into a
versioned message catalog that can render other languages without changing
error identity, protocol codes, retry behavior, structured log fields, or the
default English API contract.

This plan covers the core database production packages. APOC and other
separately deployable plugins will use separate inventories and migration plans.
This plan does not propose translating identifiers that are part of a machine
contract.

## Inventory Baseline

The baseline was generated on 2026-08-31 from the working tree rooted at
revision `16596bc6a6eb671cca3bb56a773090d332baf12b`.

Reproduce the local working reports with:

```bash
go run ./scripts/localization_inventory \
  -out docs/plans/localization-message-inventory.csv \
  -duplicates-out docs/plans/localization-duplicate-candidates.csv \
  -normalized-out docs/plans/localization-normalized-candidates.csv \
  -near-out docs/plans/localization-near-candidates.csv
```

These CSV files are ignored local review artifacts, not checked-in source or
production inputs. The occurrence report records audience, delivery channel,
package, file, line, callee or field, text/template, whether interpolation is
dynamic, and the required review action.

### Scope

Included:

- 64 maintained core Go package directories in total: packages under `pkg`,
  `resolvers`, and `ui`, plus only `cmd/nornicdb` and `cmd/nornicdb-admin` from
  `cmd`.
- 606 non-test, non-generated Go files, including build-tagged files.
- Four maintained C, C++, or Objective-C files inside those packages.
- Seven cgo preambles containing native error or return text.
- Errors, logs, panics, CLI output, Bolt failures, HTTP errors, JSON-RPC errors,
  gRPC statuses, response/message fields, procedure metadata, embedded JSON
  descriptions, direct string returns, and native error buffers.

Excluded deliberately:

- Tests, benchmarks, fixtures, documentation, migration scripts, and generated
  protocol/parser code.
- `apoc/**`, `plugins/**`, and their separately deployable plugin binaries.
  They require independent inventories when plugin localization is scheduled.
- Non-core command binaries, including evaluation, benchmark, OAuth-provider,
  documentation-generator, Swagger UI, and other utility commands under `cmd`.
- Third-party source and external-provider messages that NornicDB does not own.
- Browser TypeScript/JavaScript assets; those require a separate UI catalog
  inventory even though the maintained Go `ui` package is included here.
- User data, Cypher text, model output, file contents, and other runtime payloads.

Seven generated Go files were skipped. Their user-visible defaults must be
changed at the schema or generator boundary, never by editing generated files.

### Results

| Measure                                                       | Count |
| ------------------------------------------------------------- | ----: |
| Occurrences                                                   | 9,031 |
| Unique text/templates                                         | 4,632 |
| Static occurrences                                            | 3,063 |
| Interpolated or dynamic occurrences                           | 5,968 |
| Files containing inventory entries                            |   394 |
| Catalog candidates (`localize`)                               | 8,158 |
| Operator-log policy decisions (`policy`)                      |   584 |
| Dynamic expressions requiring source tracing (`trace-source`) |   289 |

| Channel                       | Occurrences |
| ----------------------------- | ----------: |
| Direct returned values        |       3,347 |
| Go errors                     |       3,173 |
| Response/message fields       |         666 |
| Logs                          |         562 |
| Procedure/CLI/schema metadata |         357 |
| CLI output                    |         246 |
| HTTP errors                   |         290 |
| Native errors and returns     |         172 |
| gRPC statuses                 |         137 |
| Bolt failures                 |          41 |
| Panics                        |          22 |
| Error methods                 |           9 |
| Embedded JSON descriptions    |           3 |
| JSON-RPC errors               |           3 |
| Direct wire output            |           3 |

Highest-concentration packages:

| Package           | Occurrences |
| ----------------- | ----------: |
| `pkg/cypher`      |       2,751 |
| `pkg/storage`     |       1,539 |
| `pkg/server`      |         503 |
| `pkg/search`      |         385 |
| `pkg/nornicdb`    |         361 |
| `pkg/replication` |         315 |
| `pkg/bolt`        |         298 |
| `pkg/heimdall`    |         284 |
| `pkg/mcp`         |         207 |

These are conservative candidate counts, not 9,031 translations. Procedure
signatures, protocol enums, technical names, and repeated text are intentionally
present so review can explicitly retain them as machine text rather than lose
them through an optimistic heuristic.

### Implementation Snapshot

The working-tree snapshot after the current boundary and initial core migration
contains 8,519 occurrences and 4,433 unique text/templates. The source additions
for the localization infrastructure are included in those numbers. The ignored
working report currently contains 22 HTTP, four gRPC, 43 Bolt, and one JSON-RPC
candidate occurrences; these conservative counts still include machine tokens,
localized fallbacks, route names, and other rows requiring classification.

Local review reports are generated alongside the inventory:

- `localization-duplicate-candidates.csv` contains exact duplicate templates
  grouped by audience, channel, and placeholder schema.
- `localization-normalized-candidates.csv` contains case, punctuation,
  whitespace, and compatible format-verb variants for human review.
- `localization-near-candidates.csv` contains token-similarity suggestions.
  Reports never merge messages automatically.

Implemented:

- Immutable embedded `en-US`, `es-ES`, and `en-XA` catalogs with cross-domain
  duplicate-ID and placeholder validation.
- Environment, YAML, OS-preference, context, HTTP, gRPC, and Bolt locale
  resolution with deterministic English fallback.
- Localized error boundaries for the initial HTTP, MCP JSON-RPC, Bolt,
  Nornic gRPC, and Qdrant gRPC message families while preserving machine codes,
  including database lookup and access/write authorization failures, common
  HTTP authentication responses, and Qdrant validation, authentication, and
  direct not-found responses. Shared HTTP/Neo4j invalid-body, method-not-allowed,
  GET-required, transaction lookup, GET-or-PUT, required-field, and generic
  not-found responses also render through catalog IDs. GPU manager, temporal
  graph capability, multi-method, OAuth configuration, and invalid-JSON
  responses are catalog-backed as well. Central HTTP authentication,
  authorization, and panic-recovery responses now localize at middleware
  boundaries. Bolt HELLO authentication, RUN database access/write failures,
  database lookup diagnostics, and commit-without-transaction failures use the
  session/process locale while preserving Neo4j error codes. Repeated Qdrant
  vector-dimension, mutation-policy, result-limit, point-retrieval, and query
  embedding failures preserve gRPC codes and diagnostic arguments while
  rendering through catalog IDs.
- Repeated Qdrant collection lookup and snapshot directory/save/list/delete
  diagnostics preserve their gRPC codes and untranslated cause details.
- Heimdall now resolves request language at its HTTP handler boundary and
  carries locale context into streaming and generation helpers. Its method,
  request-body, streaming, availability, validation, autocomplete, and
  generation failures preserve plain-text `http.Error` semantics.
- The exact-duplicate review report currently contains no repeated HTTP, Bolt,
  gRPC, or JSON-RPC families; remaining public messages require semantic rather
  than exact-duplicate migration.
- Server boundary helpers preserve source-English fallback text even when a
  manager is not injected, rather than exposing internal message IDs.
- Retention availability, policy/hold/erasure validation, DELETE method, GDPR
  ownership, confirmation, and legal-hold responses now localize at the HTTP
  boundary while retaining their existing status and error classifications.
- Token generation, OAuth callback, account/profile validation, and API-token
  authorization responses use a split auth-domain catalog. Provider error
  values and protocol field names remain untranslated.
- Per-database configuration and MVCC lifecycle responses use a split
  DB-config catalog while preserving Neo4j codes, HTTP statuses, and machine
  configuration keys.
- Search/embedding service availability, query chunking, and missing-embedding
  responses use a split search-domain catalog while preserving the auto-embed
  Neo4j error contract.
- Historical graph route guidance, compound node-ID validation, and
  non-disclosing database authorization use a split graph-domain catalog. The
  centralized graph resolver now receives request context for locale-aware
  boundary rendering.
- Typed localizable errors that preserve wrapping and `errors.Is`/`errors.As`.
- Admin import/export errors carry semantic descriptors and render at the CLI
  boundary while preserving exit codes, wrapped causes, and source English.
- Initial Phase 3 search vector/lifecycle and multi-database constituent,
  quota, and remote-credential errors are catalog-backed. Search index and
  multi-database quota sentinels remain discoverable through `errors.Is`.
- Deterministic exact, normalized, and near-duplicate local review reports.
- Generated constructor manifest, strict catalog validation, and catalog-only
  CI checks. CI and production code do not read or require inventory CSV files.

Still pending:

- Owner classification and semantic keys for the remaining inventory rows.
- Remaining public error families, core returned errors, procedure metadata,
  CLI output, stable log event IDs, and native error descriptors.

## Current State

- The localization catalog and locale negotiation layer are active at the
  migrated boundaries; source English remains the process fallback.
- `pkg/errors` already centralizes several sentinels and maps transient failures
  by error identity rather than text. Localizable descriptors extend this
  without changing wrapped error identity.
- HTTP, Neo4j HTTP, MCP JSON-RPC, gRPC, Bolt, and admin-import responses already
  converge through a small number of boundary helpers.
- Many internal errors wrap causes with `%w`; localization must preserve
  `errors.Is`, `errors.As`, and retry classification.
- Logs mix prose, subsystem markers, emoji, and machine-parsed key/value text.
  Translating prose in place would break dashboards unless stable event IDs and
  structured attributes are introduced first.
- Some response messages are assembled dynamically from lower-level errors.
  The 279 `trace-source` rows need data-flow review before catalog extraction.

## Translation Boundary

Store message identity and arguments internally. Render text only at an external
boundary.

First resolve one immutable process default at startup. Request/session language
preferences may override it only for that response or session.

| Boundary               | Locale source                                       | Fallback                     |
| ---------------------- | --------------------------------------------------- | ---------------------------- |
| Process default        | `NORNICDB_LANGUAGE`, config, OS preferences         | `en-US`                      |
| HTTP and MCP over HTTP | `Accept-Language`, then process default             | matched parent, then `en-US` |
| gRPC                   | `accept-language` metadata, then process default    | matched parent, then `en-US` |
| Bolt                   | optional HELLO/RUN `locale`, then process default   | matched parent, then `en-US` |
| CLI                    | process default                                     | `en-US`                      |
| Operator logs          | process default                                     | `en-US`                      |
| Embedded Go API        | locale from `context.Context`, then process default | matched parent, then `en-US` |

Malformed or unsupported locale tags must fall back deterministically and must
not fail the request.

For HTTP responses selected by `Accept-Language`, return `Content-Language` and
add `Accept-Language` to `Vary` when the response is cacheable. Prefer caching
semantic message descriptors and rendering after cache lookup; otherwise locale
must be part of the cache key.

## 2026 Research Decision

Use these layers rather than one localization dependency directly throughout
the database:

1. `golang.org/x/text/language` for BCP 47 parsing and matching. Its matcher
   handles scripts, regional variants, deprecated tags, and non-trivial parent
   relationships. The first supported tag is the final fallback.
2. `github.com/nicksnyder/go-i18n/v2/i18n` behind a NornicDB-owned interface for
   catalog loading and rendering. Version 2.6.1 provides stable IDs, named
   template data, CLDR plural forms, embedded `fs.FS` loading, and extraction and
   merge tooling. Build the bundle once, then treat it as immutable because
   mutation while localizers are reading is not goroutine-safe.
3. A small in-repository OS detector behind an interface. Go has no standard
   cross-platform API for ordered OS language preferences. `go-locale` was
   evaluated, but its current macOS implementation invokes `defaults` and its
   Windows implementation reads one registry locale instead of the ordered
   preferred UI-language API. Those behaviors are not ideal for a long-running
   database service.

Do not expose `go-i18n` types to storage, query, protocol, or command packages.
The NornicDB abstraction must remain replaceable. Unicode MessageFormat 2 is a
stable 2026 standard and should guide named variables, plural selection,
fallback, and bidirectional-text tests, but Go ecosystem support is not yet a
reason to bind core APIs directly to an MF2 implementation.

Use canonical BCP 47 tags in configuration and catalog filenames: `en-US`,
`pt-BR`, `zh-Hans`. Accept `_` for operator convenience and normalize
`en_US.UTF-8` to `en-US`, but never emit or persist the underscore form.
In this plan, `en-US` is the application source language and final fallback;
Unicode's technical root locale is `und`, and the two must not be conflated.

## Language Configuration

Add this configuration to `pkg/config.Config`:

```yaml
localization:
  language: auto
```

`NORNICDB_LANGUAGE` overrides `localization.language`. Unset, empty, or `auto`
means detect the OS preference list. The source-locale default is always
`en-US`; it is not configurable because every release must contain a complete
English catalog.

`NORNICDB_LANGUAGE` selects the process default, not a hard protocol lock. An
HTTP, gRPC, Bolt, or embedded client that explicitly supplies a supported
language preference may receive that language for its own response. Requests
without a preference use the process default.

Process-default precedence, highest first:

1. `NORNICDB_LANGUAGE` when non-empty and not `auto`.
2. `localization.language` from YAML when non-empty and not `auto`.
3. The OS ordered language preference list.
4. Embedded `en-US`.

If a future `--language` option is added to `cmd/nornicdb` or
`cmd/nornicdb-admin`, it follows the repository's normal precedence and sits
above the environment variable. Do not introduce a separate log-language knob
until compliance or operations demonstrates a need; one process default is
easier to reason about and test.

### OS Detection

Implement `Preferences() ([]language.Tag, error)` behind platform files and
inject it into tests.

- Linux and other POSIX systems: read process message-locale settings in the
  order `LC_ALL`, `LC_MESSAGES`, GNU `LANGUAGE`, then `LANG`. `LANGUAGE` may be a
  colon-separated preference list. Strip charset suffixes such as `.UTF-8`,
  normalize modifiers where possible, and treat `C`, `POSIX`, and `C.UTF-8` as
  language-neutral rather than English.
- macOS: use Foundation/CoreFoundation's ordered preferred languages
  (`NSLocale.preferredLanguages` or the corresponding CoreFoundation API), then
  fall back to process locale variables for no-cgo builds. Do not use only
  `Locale.current`; it primarily represents regional formatting and can already
  reflect application resource availability.
- Windows: call `GetUserPreferredUILanguages` with `MUI_LANGUAGE_NAME` to obtain
  the ordered, null-delimited BCP 47 display-language list. Do not use only
  `GetUserDefaultLocaleName`, which is a single formatting locale rather than
  the user's ordered UI-language preferences.
- Unsupported platforms: use the POSIX-style environment detector and return a
  typed detection error if no preference exists.

Resolve this once during startup after the early `slog` logger is available and
before database services are created. Never mutate a process-global locale in a
running server.

### Validation And Fallback

- A malformed explicit `NORNICDB_LANGUAGE` or YAML value is a startup
  configuration error. Failing fast prevents a typo from silently selecting the
  wrong language.
- Failure to detect an OS language is non-fatal. Select `en-US` and emit one
  English bootstrap warning with event ID
  `localization.os_language_undetected`.
- A valid requested language with no installed pack uses the language matcher
  to try a supported parent/related language, then `en-US`. Emit one structured
  warning with event ID `localization.language_pack_missing`, including
  `requested_language`, `resolved_language`, and `source` (`env`, `config`,
  `os`, `http`, `grpc`, or `bolt`).
- A missing message key in an otherwise installed pack falls back to the same
  key in `en-US`, increments a metric, and logs a rate-limited/deduplicated
  warning keyed by `(requested_language, message_id)`. Never log once per
  request.
- Catalog parse errors and missing `en-US` keys fail startup or CI. A binary must
  never start without a complete source catalog.

The bootstrap warnings are fixed English structured log messages because they
must work before localization is available. Their event IDs and fields remain
stable in every language.

## Target Design

### Stable Message Identity

Add `pkg/localization` with these concepts:

- `MessageID`: stable dotted key such as `auth.invalid_credentials`.
- `Message`: ID plus typed formatting arguments, with no rendered prose.
- `Localizer`: resolves a `language.Tag` and renders a message.
- Context helpers for attaching and retrieving a negotiated locale.
- An embedded, immutable catalog with `en-US` as the required source locale.

Wrap `go-i18n` rather than using its global state. Construct one `Manager` at
startup, load every embedded catalog before serving traffic, and create or cache
lightweight localizers for matched preference lists. Generate typed Go message
IDs and constructors so misspelled keys and placeholder drift fail before
runtime.

Catalog keys describe semantics, not English wording. Changing English text
must not change a key unless the meaning changes.

### Package Layout

```text
pkg/localization/
  doc.go
  config.go                 # Config and validation; no OS access
  manager.go                # Immutable bundle, matcher, render API
  message.go                # MessageID, Message, named argument values
  context.go                # Context locale helpers
  negotiate.go              # HTTP/gRPC/Bolt preference parsing
  missing.go                # Once-only warnings and metrics
  catalog.go                # go:embed and startup catalog loading
  messages_gen.go           # Generated IDs and typed constructors
  catalog/
    active.auth.en-US.yaml
    active.cypher.en-US.yaml
    active.search.en-US.yaml
    active.server.en-US.yaml
  internal/oslocale/
    detector.go
    detector_posix.go
    detector_darwin_cgo.go
    detector_darwin_nocgo.go
    detector_windows.go
    detector_fallback.go
scripts/localization_catalog/
  main.go                   # Validate, generate, compare, pseudo-localize
```

Keep OS detection internal and injectable. Keep catalogs embedded in the binary
for deterministic deployments; external pack loading introduces versioning,
integrity, and partial-update risks and should be a later feature if needed.

Recommended public surface:

```go
type MessageID string

type Message struct {
    ID          MessageID
    Data        any
    PluralCount any
}

type Manager interface {
    DefaultTag() language.Tag
    Match(preferred ...language.Tag) Match
    Render(ctx context.Context, message Message) (string, language.Tag, error)
}
```

Core packages construct semantic `Message` values. Only boundary adapters call
`Render`. Avoid package-level mutable defaults and `MustLocalize` on request
paths; rendering errors must return the English fallback and observable
diagnostics rather than panic.

### Localizable Errors

Extend `pkg/errors` with a typed error descriptor containing:

- Stable application or Neo4j-compatible error code.
- `localization.MessageID` and arguments.
- Wrapped cause.
- Retryability/classification metadata where applicable.

`Error()` must continue rendering the exact `en-US` fallback during the first
migration phase. `Unwrap`, `Is`, and `As` must preserve current behavior.
HTTP status, gRPC code, Bolt/Neo4j code, admin-import exit code, and JSON-RPC code
remain machine-stable and are never translated.

### Catalog Files

Use one source catalog per locale and domain:

```text
pkg/localization/catalog/
  active.auth.en-US.yaml
  active.bolt.en-US.yaml
  active.cypher.en-US.yaml
  active.search.en-US.yaml
  active.server.en-US.yaml
  active.auth.es-ES.yaml
  ...
```

Each entry contains a stable key, text, translator context, argument names and
types, and optional CLDR plural forms. YAML reuses the repository's existing
parser; filenames retain the language tag expected by catalog tooling. A
generator validates files and emits typed IDs and constructors. Generated code
is committed and verified for drift in CI.

## Deduplication And English Consistency

The core source already contains repeated wording families: `invalid request
body` appears at 28 call sites, `failed to create` at 112, `not found` at 202,
and `is required` at 120. These substring counts are triage signals, not proof
that every occurrence should share one translation.

Extend the inventory tool to produce three review reports:

1. Exact duplicate templates grouped by audience, semantic code, argument names
   and types, and delivery channel.
2. Normalized candidates that ignore case, punctuation, whitespace, and format
   verb spelling while preserving named placeholder schemas.
3. Near-duplicate candidates using token similarity, presented only as review
   suggestions. Never merge automatically from English similarity.

Apply these consolidation rules:

- Share a message ID only when meaning, audience, severity, remediation, and
  argument schema are the same. `Open` as a verb and noun must remain separate.
- Catalog complete messages, not reusable translated words or sentence
  fragments. Word order, agreement, case, and plural forms differ by language.
- Replace string concatenation with complete templates containing named,
  translator-visible variables.
- Prefer generated semantic constructors such as `InvalidRequestBody()`,
  `DatabaseNotFound(name)`, and `PermissionRequired(operation, permission)` over
  repeated maps and positional formatting.
- Standardize source English: sentence case; no trailing period for short API
  errors; `required` for missing input; `invalid` for malformed input;
  `unsupported` for understood but unavailable features; and `not found` only
  when absence may safely be disclosed.
- Move emoji and severity labels out of prose into structured fields or CLI
  presentation helpers. Translators should not manage log syntax.
- Keep translator descriptions specific: where the text appears, who sees it,
  what each variable means, and whether disclosure is security-sensitive.

Shared helpers should consolidate mechanics, not erase context:

- locale preference parsing and normalization;
- context attachment and boundary lookup;
- semantic error construction and protocol-code adapters;
- HTTP `Content-Language`/`Vary` handling;
- missing-pack/key deduplication and metrics;
- CLI table/progress rendering; and
- structured log event rendering with stable event IDs.

### Logs

Every log event must gain a stable `event_id` before its prose is translated.
Subsystem, reason, status, metrics, paths, IDs, and error codes remain structured
attributes. Only the human message is catalog-backed. JSON logs therefore remain
queryable across languages, and English remains the default for compatibility.

Native code should return stable native error enums plus arguments where
possible. Until that refactor is complete, native strings remain source-locale
catalog entries translated at the Go boundary.

### Text That Must Not Be Translated

- Neo4j, Bolt, gRPC, HTTP, JSON-RPC, OpenCL, CUDA, Vulkan, and admin exit codes.
- Procedure names/signatures, Cypher keywords, property names, enum values, and
  JSON field names.
- Environment variables, command flags, metric names, trace attributes, log
  `event_id` values, database identifiers, paths, URLs, and model names.
- User content, query text, provider output, and wrapped third-party details.

Descriptions and help text for those machine elements are localizable even when
the identifiers themselves are not.

## Migration Plan

### Phase 0: Inventory And Classification

Status: baseline complete.

1. Review the CSV by package owner.
2. Replace the provisional `review` value with one of `localize`,
   `keep-machine`, `keep-operator-English`, `trace-source`, or `external`.
3. Assign a semantic message key to every `localize` row and collapse duplicate
   occurrences onto shared keys only when their meaning and arguments match.
4. Resolve all 289 dynamic-source rows to their originating descriptor or mark
   them as external/user content.

Exit criteria: every row has an owner, disposition, and message key where
applicable; no unreviewed dynamic source remains.

### Phase 1: Localization Foundation

1. Add `LocalizationConfig` to `pkg/config.Config`, YAML parsing, strict
   `NORNICDB_LANGUAGE` parsing, and environment-variable documentation.
2. Add the injectable OS detector and platform tests for ordered preferences,
   POSIX normalization, neutral locales, and unavailable native APIs.
3. Add `pkg/localization`, the immutable `go-i18n` bundle, context helpers,
   source-catalog parsing, and catalog generation.
4. Initialize the manager after the early structured logger and before database
   construction. Add once-only bootstrap warnings for OS and pack fallback.
5. Add exact-English golden tests, placeholder/type validation, plural-form
   validation, and an intentionally expanded pseudo-locale.
6. Add locale negotiation helpers for HTTP, gRPC, Bolt, CLI, and logs without
   changing any current English output.

Exit criteria: `en-US` rendering is byte-for-byte compatible at representative
boundaries; ENV-over-OS precedence is tested on every supported platform; and
missing OS preferences, language packs, and individual keys fall back safely
with bounded observability.

### Phase 2: Public Error Boundaries

Status: complete for the scoped core protocol and admin-import boundaries.

Migrate the smallest, highest-risk boundary helpers first:

1. `pkg/server` HTTP and Neo4j error writers.
2. `pkg/mcp` JSON-RPC and HTTP error writers.
3. `pkg/bolt` failure helpers while preserving Neo4j codes.
4. `pkg/nornicgrpc` and `pkg/qdrantgrpc` status construction.
5. `pkg/adminimport` errors while preserving exit codes.
6. Authentication, authorization, encryption, and security messages.

Exit criteria: public errors render through message IDs; status/code/retry
behavior and default English remain unchanged in protocol compatibility tests.

### Phase 3: Core Errors And Returned Messages

Status: complete. Client-reaching errors in `pkg/storage`, `pkg/cypher`,
`pkg/search`, `pkg/nornicdb`, `pkg/replication`, and `pkg/multidb` use typed
message descriptors with sentinel and wrapped-cause compatibility tests.

Migrate package by package, starting with `pkg/storage`, `pkg/cypher`,
`pkg/search`, `pkg/nornicdb`, `pkg/replication`, and `pkg/multidb`.

Do not pass a locale into storage or query algorithms. Return typed message
descriptors and localize at the caller boundary. Keep wrapped third-party causes
as structured diagnostic details; do not concatenate them into translated
sentences.

Exit criteria: no reviewed client-facing inline English remains in these
packages and all sentinel/classification tests still pass.

Reviewed deferrals are stable machine sentinels, private parser/registration
invariants, storage codec and WAL corruption diagnostics, plugin/APOC scope,
and operator-only startup or recovery diagnostics. These remain machine text
or belong to the later procedure-metadata and native-message phases.

### Phase 4: Procedure Metadata And CLI

Status: complete. All 71 core built-in procedure descriptions use generated
typed descriptors and render per request without changing names, signatures,
modes, or user-defined literal metadata. The two CLIs localize command help,
flags, validation failures, progress output, and startup summaries while
preserving exact English defaults and machine-facing contracts.

1. Move core Cypher procedure descriptions/examples to catalog keys while
   keeping names and signatures unchanged.
2. Migrate command help, validation failures, progress output, and startup
   summaries.
3. Add width/format tests for translated CLI tables and multiline help.

Generate repetitive core procedure registrations from catalog metadata instead
of hand-editing repeated calls. APOC and external plugin metadata are outside
this plan.

The live procedure contract has no separate example field, so there were no
core procedure examples to migrate. Reviewed deferrals are `SHOW FUNCTIONS`
metadata, APOC and external plugin metadata, pre-localizer bootstrap failures,
operator-only readiness diagnostics, and fixed machine/version/prompt tokens.
Pseudo-locale help tests cover multiline output, stable flag alignment, and
bounded line widths.

### Phase 5: Logs And Native Messages

Status: in progress. `pkg/localization` now defines typed log events with
stable event IDs, catalog-backed prose, and structured `slog.Attr` fields. The
server composition boundary uses this contract for MCP-disabled startup,
remote-credential key fallback, headless UI, and authentication-disabled
events. English remains exact; Spanish changes only prose; event IDs, levels,
component attribution, fallback reason, and remediation fields remain stable.

1. Assign stable event IDs and structured fields to all 584 operator-policy
   occurrences.
2. Decide which prose is translated; keep security audit logs in a configured
   canonical language when compliance requires it.
3. Replace native string-only errors with enums/descriptors where practical.
4. Localize remaining native fallback text at Go boundaries.

Exit criteria: changing log language does not change event IDs, fields, alert
queries, or metrics.

### Phase 6: Additional Locales

1. Ship a pseudo-locale first to expose concatenation, truncation, and missing
   boundary propagation.
2. Pilot one real locale with native speakers and domain review.
3. Require source-locale completeness; allow explicit fallback for incomplete
   non-source catalogs.
4. Version catalog changes and document compatibility expectations.

## CI And Regression Controls

Extend `localization_inventory` with a check mode and add CI gates for:

- New inline text at a reviewed sink without an allowlist disposition.
- Missing source-locale keys or orphaned catalog entries.
- Placeholder count/type mismatches across locales.
- Missing plural forms and invalid locale tags.
- Generated catalog drift.
- Changes to protocol codes, retry classification, or default English golden
  responses.
- Pseudo-locale boundary tests for HTTP, Bolt, gRPC, MCP, CLI, and logs.

The inventory must remain occurrence-based. A unique-string-only report would
hide cases where identical English has different semantics or argument types.

Inventory reports are intentionally generated and reviewed locally; CI does not
depend on review CSVs. Catalog validation runs with `go test ./pkg/localization`,
and CI regenerates the typed catalog manifest and fails on manifest drift.
Workflow-level gates for reviewed dispositions remain pending.

## Completion Criteria

- Every inventory row is classified and owned.
- Every localizable occurrence uses a typed message ID.
- Every external boundary negotiates locale and falls back to `en-US`.
- Default English remains backward compatible unless a separately documented
  correction is approved.
- Machine codes, structured fields, retryability, and `errors.Is/As` behavior
  are language-independent.
- CI prevents new unmanaged production prose.
- At least one pseudo-locale and one reviewed real locale pass protocol, CLI,
  formatting, and fallback tests.

## Research References

Reviewed on 2026-08-31:

- [Go `x/text/language`](https://pkg.go.dev/golang.org/x/text/language): BCP 47
  parsing, application-supported language matching, confidence, and fallback.
- [Go language and locale matching](https://go.dev/blog/matchlang): why matching
  scripts, dialects, and parents must not be implemented with string trimming.
- [`go-i18n` v2](https://pkg.go.dev/github.com/nicksnyder/go-i18n/v2/i18n):
  immutable-at-runtime bundles, named template data, CLDR plurals, embedded FS
  loading, and translation workflow.
- [Unicode MessageFormat 2](https://messageformat.unicode.org/) and
  [UTS #35 Part 9](https://unicode.org/reports/tr35/tr35-messageFormat.html):
  named inputs, selection, pluralization, fallback, and bidirectional isolation.
- [Linux `locale(7)`](https://man7.org/linux/man-pages/man7/locale.7.html):
  `LC_ALL`, category-specific locale, and `LANG` precedence.
- [Apple preferred languages](https://developer.apple.com/documentation/foundation/nslocale/preferredlanguages)
  and [current locale](https://developer.apple.com/documentation/foundation/nslocale/current):
  ordered language preferences versus regional/application locale behavior.
- [Windows `GetUserPreferredUILanguages`](https://learn.microsoft.com/en-us/windows/win32/api/winnls/nf-winnls-getuserpreferreduilanguages):
  ordered installed UI languages in language-name form.
- [RFC 9110 Accept-Language](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language)
  and [RFC 4647](https://www.rfc-editor.org/rfc/rfc4647): language priority
  lists, lookup, defaults, content negotiation, `Content-Language`, and `Vary`.
- [`go-locale`](https://github.com/Xuanwo/go-locale): evaluated as a detector;
  useful coverage, but its current macOS subprocess and Windows registry
  behavior do not meet this service's preferred native API contract.
