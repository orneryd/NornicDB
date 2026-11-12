# URL sink inventory review — v1.5.0 RC rebase (PR #1789)

Reviewed base: `b798a4af5` ("Reorganize browser tasks with URL state, 3D graphs, guided
policies and seeded pivot demos") as rebased into PR #1789 over `acf7426b8`
("fix(cypher): account for adjacency cost when choosing traversal seeds (#1788)").
The reviewed source is unchanged between `b798a4af5` and the current head `57c31c273`
(`git diff b798a4af5..HEAD -- pkg/server/server_middleware.go pkg/server/server_nornicdb.go`
is empty).

CI fail-first drift from `TestCredentialURLSinkStructure` (`pkg/observability/url_sink_structure_test.go`):
one stale `*Server.withAuth` key and seven new `*Server.handleEmbedTrigger` keys.

## Method

* Both changed functions were read in full from `git diff acf7426b8..b798a4af5 -- pkg/server/server_middleware.go pkg/server/server_nornicdb.go` and from the current source, and the sink population was derived with the scanner's own rules (`endpointName`, `endpointTainted`, `endpointLocals`, `returns`, the sink-word call list and the non-identifier assignment rule) rather than by inspection of individual lines.
* `sourceText` prints with `token.NewFileSet()`, so the hashed text is canonical printed source: column 0 for the node, comments and blank lines dropped, call argument lists collapsed onto one line. Every value written below therefore has to equal that canonical text and `sha256(value)` has to equal the key hash; both directions were checked before writing.
* Cross-check: each of the seven CI fail-first `handleEmbedTrigger` hashes matched exactly one reviewed expression, and the previous `da86da40…` `withAuth` value with the new guard applied hashes to exactly `cad7222154ed…`, so the correction reproduces CI's own keys instead of guessing them.

## `*Server.withAuth` — `pkg/server/server_middleware.go:29-130`

* The only change in the function is `pkg/server/server_middleware.go:119`
  `if requiredPerm != "" && !hasPermission(s, claims.Roles, requiredPerm) {` (was
  `if !hasPermission(s, claims.Roles, requiredPerm) {`); the comment at `:118` is not printed by the scanner.
* The inventoried unit is the whole returned handler literal `:30-129`. It is endpoint-tainted because the token extraction at `:59-65` reads `r.URL.Query()`, so the `return func(w http.ResponseWriter, r *http.Request) {…}` statement is itself one reviewed expression. That is why a one-line guard change moves the key even though no egress changed; nothing else in the closure changed, and the inner reviewed expressions keep their own keys unchanged:
  * `:71`, `:86`, `:104` `s.logEvent(r.Context(), slog.LevelDebug, localization.ServerAuthTraceEvent(r.Method, r.URL.Path, …))` (existing keys `b9dffd67…`, `31366fbf…`, `692394c7…`),
  * `:114` `s.writeBoundaryNeo4jError(w, r, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", err)` (`41e77bf9…`),
  * `:120-121` `s.logAudit(r, claims.Sub, "access_denied", false, fmt.Sprintf(…))` (`47decf6a…`).
* Recorded correction: `da86da40…` → `cad7222154ed44702abcab7ca9885dfde540a90af6f940e481f4eb39ceb3690b`, value identical apart from the new guard.

## `*Server.handleEmbedTrigger` — `pkg/server/server_nornicdb.go:56-191`

Taint origin: `:57` `database := r.URL.Query().Get("database")`. `:61-63` clears it unless
`regenerate=true`, so only the regeneration path carries a database name; `:74`
`scopedStorage, err = s.dbManager.GetStorage(database)` and `:139`
`cleared, err = s.db.ClearDatabaseEmbeddings(database, scopedStorage)` propagate the taint to
`err`/`cleared` under the scanner's assignment rule, which is why the log statements that report
those two names are in the population.

Seven reviewed egress expressions (all added to `testdata/url_sink_inventory.json`):

| hash | line | expression | emitted value |
|---|---|---|---|
| `f84dd30f…` | `:76` | `s.writeDatabaseNotFound(w, r, http.StatusNotFound, ErrNotFound, database)` | caller's own `database` query value in the 404 body |
| `29deab1c…` | `:112` | `response["database"] = database` | same value as 202 response metadata (`:114` writes it) |
| `ef2a47fa…` | `:131` | `s.logEvent(context.Background(), slog.LevelWarn, localization.ServerEmbedWorkerResetFailedEvent(err))` | reset error text |
| `987f9163…` | `:148` | `s.logEvent(context.Background(), slog.LevelError, localization.ServerEmbedClearFailedEvent(err))` | clear error text |
| `3a00a7a1…` | `:151` | `s.logEvent(context.Background(), slog.LevelInfo, localization.ServerEmbedClearedEvent(cleared))` | cleared count |
| `ab143b61…` | `:155` | `s.logEvent(ctx, slog.LevelError, localization.ServerEmbedWorkerTriggerFailedEvent(err))` | trigger error text |
| `7098ffbf…` | `:169` | `s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)` | trigger error text in the 500 body |

`:70` `s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported", "Embedding regeneration requires a concrete database; select a constituent database.")` is *not* in the population: the database name appears only inside a string literal and no argument is tainted, which is consistent with CI reporting seven new keys, not eight.

## Owners traced (unchanged by this correction)

* Authorization: `:66` `authorizeDatabaseOperation` (`pkg/server/server_helpers.go:285`, `principalAccess(...).Allows(database, permission)`) and `:79` `authorizeAllDatabaseOperation` (`:296`, population-wide check used when no database is named); the regeneration-only admin check stays at `:98-104`.
* Client error owners: `writeDatabaseNotFound` (`pkg/server/localization.go:144`) → `writeLocalizedError` with `localization.HTTPDatabaseNotFound(name)` (`pkg/localization/message.go:516`); `writeBoundaryNeo4jError` (`pkg/server/localization.go:51`) → `localization.ExternalDiagnostic(cause)` (`pkg/localization/message.go:702`) → `writeNeo4jError` (`pkg/server/server_helpers.go:595`).
* Log owner: `logEvent` (`pkg/server/log_event.go:10`) → `localization.Manager.Log` (`pkg/localization/log_event.go:19`) → `logger.LogAttrs` with the event's identity and the fields the `ServerEmbed…Event` constructors build (`error` attribute or `cleared` int, `pkg/localization/log_events_server_runtime.go:308-338`).
* Classification: all seven are client- or log-egress and stay in `url_sink_inventory.json`; `url_sink_non_egress.json` is unchanged. The shapes already have reviewed peers in the same file, so the verdict matches existing ownership: `handleSearch` `s.writeDatabaseNotFound(w, r, http.StatusNotFound, ErrNotFound, dbName)` (`7c117b88…`), `handleSearch` `s.logEvent(r.Context(), slog.LevelWarn, localization.ServerSearchStorageLookupFailedEvent(dbName, err))` (`ec19026e…`), `handleEmbeddingFailures` `s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)` (`7098ffbf…`, same expression hash, different owning function), `handleMe` `response["oauth_provider"] = issuer` (`1257f07b…`).
* These expressions do not read configured endpoint credentials: they carry the caller-provided database selector, an `int` count, or errors from local storage/worker operations. The selector may contain arbitrary caller text; this review does not claim that its bytes can never resemble a URL. `EmbedExisting` only wakes the worker and returns no provider response (`pkg/nornicdb/db.go:2309`). No runtime, authorization or redaction code was changed, and none is required for this correction.

## Manifest changes

* `pkg/observability/testdata/url_sink_inventory.json`: replaced the stale `*Server.withAuth` key, added the seven `*Server.handleEmbedTrigger` keys in sorted position (8 insertions, 1 deletion).
* `pkg/observability/testdata/url_sink_non_egress.json`: unchanged.
* `pkg/observability/url_sink_structure_test.go`: unchanged — scanner logic preserved.

## Limitations

* The scanner was not executed here (this task is limited to the two reviewed functions and forbids Go/gofmt/build/test commands). The keys were reconstructed from canonical printed source and validated against CI's fail-first hashes in both directions, but `go test ./pkg/observability -run TestCredentialURLSinkStructure` — including the stale-key direction that must drop `da86da40…` — remains the parent's acceptance check.
* Only the two drifted functions were corrected. The other non-test Go changes in this rebase (`pkg/nornicdb/db.go`, `pkg/nornicdb/embed_queue.go`, `pkg/server/server.go`, `pkg/server/server_auth.go`, `pkg/server/server_router.go`) were read: comment text, route permission literals and new non-egress helpers only, none altering an inventoried expression; they were not rescanned.

## Parent acceptance

The parent independently read the changed functions and their authorization,
local storage/worker and diagnostic owners. Full observability package race
checks pass: `go test -race ./pkg/observability -count=1` (193.546 s), including
`TestCredentialURLSinkStructure`. The scanner, non-egress classifications and
assertions remain unchanged. Evidence: `/tmp/v150-url-inventory-observability-race.log`.
