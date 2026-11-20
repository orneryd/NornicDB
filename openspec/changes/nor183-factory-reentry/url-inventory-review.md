# URL sink inventory review — NOR-183 storage-view split (PR #1867)

Reviewed source: `027814f2ae9f3ca4ee637212089b496fae69fdbf` (`fix/nor183-factory-reentry-v152`,
draft PR #1867 → `release/v1.5.2`). Pre-change owner: `e8cd436e540ed421327bb72478d4d98479f7a2ac`
(`*DatabaseManager.GetStorageWithAuth` in `pkg/multidb/manager.go`). The production change under
review is the parent-published fix that moved resolution into `pkg/multidb/storage_view.go`
(`*DatabaseManager.resolveStorageSelection`, `*DatabaseManager.buildCompositeStorageSelection`).
This document covers the maintenance of
`pkg/observability/testdata/url_sink_inventory.json` and the follow-up reconciliation of
`internal/cyphertext/testdata/lexical_population.json` with the same split (see
[Correction: lexical population drift](#correction-lexical-population-drift-follow-up)); no runtime,
scanner, assertion or non-egress change is part of it.

Fail-first drift, reproduced locally at `027814f2a`
(`go test ./pkg/observability -run TestCredentialURLSinkStructure -count=1 -v`, FAIL 1.60 s) and in
CI job 135883845402 (three attempts in run 44615611187): ten stale
`pkg/multidb/manager.go:*DatabaseManager.GetStorageWithAuth <hash>` entries and ten unreviewed
`pkg/multidb/storage_view.go` entries (nine `resolveStorageSelection`, one
`buildCompositeStorageSelection`). No other key was reported stale or unreviewed, and no non-egress
classification was reported stale.

## Method

* The sink population was re-derived with the test's own rules (`endpointName`, `endpointTainted`,
  `endpointLocals`, the `returns` fixpoint, the sink-word call list, the non-identifier assignment
  rule and the duplicate-occurrence key collapsing), executed by a temporary analysis program that
  copied those helpers verbatim and printed `key → canonical printed expression` for `pkg/multidb`.
  The program was deleted after use; it is not part of the change. Every key it printed matches the
  CI/local drift list exactly (ten stale, ten new).
* `sourceText` prints with `token.NewFileSet()`, so each recorded value is canonical printed source.
  It was re-checked in the written manifest: for all ten new keys, `sha256(value) == key hash`
  (including the duplicated-hash case below).
* Old owner behaviour was read from `git show e8cd436e5:pkg/multidb/manager.go:653-754` and the new
  owners from the current `pkg/multidb/storage_view.go:80-200`, so each moved expression is mapped to
  the statement it replaced rather than to a similar-looking line.

## Mapping (old key → new key)

| old `manager.go:*DatabaseManager.GetStorageWithAuth` | new `storage_view.go` key | expression / treatment |
|---|---|---|
| `17ec04f8…` | `resolveStorageSelection 8fa12856…` | `composite.accessModes[ref.Alias] = ref.AccessMode` — same catalog metadata copy, receiver renamed |
| `bacd2941…` | `resolveStorageSelection 47f84a44…` | `composite.constituents[ref.Alias] = constituentStorage` — same engine handle, receiver renamed |
| `acb624e1…` | `resolveStorageSelection 24930d25…` | `composite.constituentNames[ref.Alias] = actualName` — same catalog name copy |
| `a3bf87df…` | `resolveStorageSelection 691e38d3…` | `localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)`; only the zero-value return changed from `nil` to `storageSelection{}` |
| `955cfbf5…` | `resolveStorageSelection 2a85d177…` | `localizedError(localization.MultidbManagerGetConstituentStorageFailed(ref.DatabaseName, err), err)`; same change of zero-value return only |
| `56bee82d…` | `buildCompositeStorageSelection 56bee82d…` | `return compositeEngine, nil` — identical expression text and hash, moved with its owning function |
| `723dfe27…` `return engine, err` | `resolveStorageSelection 415b6840…` | `return storageSelection{}, err` — the two pre-move `return engine, err` exits (dotted-constituent and standard-namespace path) became explicit `if err != nil` branches returning the same error |
| `7bed1a5f…` `return engine, nil` | `resolveStorageSelection e951dcff…` | `return storageSelection{engine: engine}, nil` — cached/local engine wrapped in the selection type |
| `80646ca5…` (prose: precomputed runtime entry, wrapped only after the remote owner redacts) | `resolveStorageSelection 94ed48b5…` | `return storageSelection{remote: constituent}, nil` — the resolved runtime constituent is captured for post-lock construction instead of invoking the factory inline |
| `f1ffa301…` `return nil, localizedError(localization.MultidbManagerGetRemoteStorageFailed(ref.Alias, err), err)` | *no unreviewed counterpart* | the corresponding exit now reads `localizedError(localization.MultidbManagerGetRemoteStorageFailed(remote.alias, err), err)` in `buildCompositeStorageSelection`; the scanner does not taint `remote.alias`, so it is not in the current population (see Limitations) |

New-only keys are the three structural carriers created by the split
(`415b6840…`, `e951dcff…`, `94ed48b5…`) plus
`7e8b6a81…` `composite.remotes = append(composite.remotes, remoteConstituentSelection{alias: ref.Alias, databaseName: ref.DatabaseName, ref: resolved})`,
which replaces the pre-move inline factory call inside the constituent loop.

## Classification of every new entry

All ten are internal runtime plumbing, not transport/log/persistence egress. None of the recorded
expressions formats a URL, endpoint or credential value; none reads `authToken`.

* `8fa12856…`, `24930d25…`, `47f84a44…` — in-memory construction of the composite wrapper's
  constituent map, name map and access-mode map. Values are the catalog alias, the resolved local
  database name (from `resolveDatabaseInternal`, which returns names or `ErrDatabaseNotFound`) and
  the access-mode literal; the engine value is the same handle the pre-move code stored.
  `storage.NewCompositeEngine` consumes exactly these maps.
* `7e8b6a81…` and `94ed48b5…` — capture the precomputed `*runtimeConstituent` snapshot (and the
  alias/catalog database name) so the configured factory runs after the metadata lock is released.
  `storageSelection`, `compositeSelection` and `remoteConstituentSelection`
  (`pkg/multidb/storage_view.go:48-76`) are private, non-serialized structs used only inside
  `storage_view.go`/`GetStorageWithAuth`; nothing logs, formats or marshals them, and the same
  pointer was previously passed directly to `getRemoteStorageResolvedInternal`
  (`pkg/multidb/manager.go:731-763`), which redacts factory failures with
  `observability.RedactError(err, ref.redactionValues…, authToken)` before they leave that owner.
  Holding the snapshot in a struct for the duration of one call is the same information flow as the
  pre-move local variable.
* `e951dcff…`, `415b6840…` and `56bee82d…` — return an engine, a selection wrapper around an
  already-resolved engine, or a manager error. `databaseViewLocked` (`pkg/multidb/manager.go:695`)
  only produces `ErrDatabaseOffline`, `ErrDatabaseNotFound` or namespace/engine construction errors;
  no endpoint text is composed on either path.
* `691e38d3…`, `2a85d177…` — `localizedError` (`pkg/multidb/localized_errors.go:8`) wraps the cause in
  `nornicerrors.NewLocalized` with the `localization.Multidb…` message. The message `Fallback` embeds
  `cause.Error()` (`pkg/localization/messages_multidb_operations.go:44-89`), exactly as the two
  pre-move expressions did; the causes here are `resolveDatabaseInternal`'s `ErrDatabaseNotFound` and
  `databaseViewLocked`'s catalog/namespace errors, neither of which carries a URI or credential.

No expression in the new population was approved by copying a sibling's reason: the two entries whose
values are prose in the pre-move manifest were replaced by the exact printed expression (neither
contains `authToken`), and everything else is classified from the statement it replaced.

## Manifest changes

* `pkg/observability/testdata/url_sink_inventory.json`: removed the ten stale
  `manager.go:*DatabaseManager.GetStorageWithAuth` keys and added the ten current
  `storage_view.go` keys in sorted position — 10 deletions, 10 insertions, key count unchanged at
  893.
* `pkg/observability/testdata/url_sink_non_egress.json`: unchanged (no current key is classified
  there, so no classification became stale).
* `pkg/observability/url_sink_structure_test.go`, `pkg/multidb/manager.go`,
  `pkg/multidb/storage_view.go`: unchanged — the scanner logic, assertions and production source are
  byte-identical to the published head.

## Verification (this worktree, macOS arm64)

| command | result |
|---|---|
| `go test ./pkg/observability -run TestCredentialURLSinkStructure -count=1 -v` | PASS 1.17 s; `ENDPOINT_INVENTORY functions=20058 raw_egresses=893 string_helpers=40 classified_non_egress=76` |
| `go test ./pkg/observability -count=1` | `ok github.com/orneryd/nornicdb/pkg/observability 16.667s` |
| `go test ./internal/cyphertext -run TestFoundationLexicalPopulation -count=1` | FAIL at this head (3.027 s; 2.77 s in-test): 4097 discovered candidates against 4095 reviewed. **Not** pre-existing drift and **not** caused by this manifest change — corrected below and reconciled in the follow-up. |
| `go test ./internal/cyphertext -count=1` (after the reconciliation below) | `ok github.com/orneryd/nornicdb/internal/cyphertext 2.368s`; the focused run reports `checked 4097 complete scanner/numeric/output candidates against the reviewed population` |
| `go test ./pkg/observability -run 'TestCredentialURLSinkStructure\|TestCredentialFieldPolicyPopulation' -count=1 -v` (after the reconciliation) | `--- PASS: TestCredentialURLSinkStructure (0.87s)` with `ENDPOINT_INVENTORY … raw_egresses=893`; `--- PASS: TestCredentialFieldPolicyPopulation (0.00s)`; `ok … 1.278s` |
| `go test ./pkg/multidb -count=1` | `ok github.com/orneryd/nornicdb/pkg/multidb 37.477s` |

No full suite, race or stress run was performed (out of scope for this change).

## Correction: lexical population drift (follow-up)

An earlier revision of this document reported the `TestFoundationLexicalPopulation` failure as
"pre-existing, unrelated" drift on this branch, inferred from the failure message being byte-identical
with and without the manifest edit. **That attribution is retracted.** Restoring HEAD's manifest while
keeping the published head's runtime and scanner only shows that the URL manifest was not the cause; it
does not establish release-base behaviour, because the compared tree was never the release base.

Parent evidence at the immutable revisions:

| tree | `go test ./internal/cyphertext -run TestFoundationLexicalPopulation -count=1` |
|---|---|
| `e8cd436e540ed421327bb72478d4d98479f7a2ac`, clean tracked source | PASS 3.082 s, 4095 candidates (`NOR183-LEXICAL-BASE-PASS.log`) |
| published NOR183 runtime, `027814f2ae9f3ca4ee637212089b496fae69fdbf` | FAIL 3.027 s (`NOR183-LEXICAL-POPULATION-FAIL.log`) |

So the drift belongs to the published NOR-183 storage-view split, not to a stale release baseline and
not to the URL bookkeeping.

Actual membership change, from the explicit discovery mode
(`NORNICDB_LEXICAL_POPULATION_DISCOVERY=1 go test ./internal/cyphertext -run TestFoundationLexicalPopulation$ -count=1 -v`):
exactly one member removed and three added, net +2 (4095 → 4097), every other member unchanged.

| old member (removed) | mechanisms / shared / disposition |
|---|---|
| `pkg/multidb/manager.go:*DatabaseManager.GetStorageWithAuth` | `credential-field-output`, `credential-output` / true / `credential-output` |

| new member (added) | mechanisms / shared / disposition |
|---|---|
| `pkg/multidb/storage_view.go:*DatabaseManager.GetStorageWithAuth` | `credential-field-output` / true / `credential-runtime` |
| `pkg/multidb/storage_view.go:*DatabaseManager.buildCompositeStorageSelection` | `credential-field-output`, `credential-output` / true / `credential-runtime` |
| `pkg/multidb/storage_view.go:*DatabaseManager.resolveStorageSelection` | `credential-output` / true / `credential-runtime` |

Each new owner carries its own reason derived from its source and its own evidence tests; the single
carried-over `credential-output` verdict with the shared boilerplate reason ("configured endpoint
sink/display/runtime route") was not copied onto the split owners. Behavioural check: only the two
owners that appear in the reviewed egress manifest keep `credential-output`; the public entry no longer
owns an egress expression and is classified as runtime credential plumbing, with the policy-name hit
being its `authToken` parameter and the returned error. Its redaction owner is unchanged
(`getRemoteStorageResolvedInternal`). No assertions, scanner rules, dispositions of other members, or
production source were touched; the reconciled file has 4097 entries and no other member's
`mechanisms`, `shared`, `reason` or `tests` changed.

## Limitations

* The pre-move remote-failure exit (`f1ffa301…`) has no tainted counterpart in the new code: the
  scanner's taint is name-based and `remote.alias` (a plain field read) does not propagate, while the
  pre-move `ref.Alias` did. The expression still exists and its error information is unchanged (the
  cause is the already-redacted error from `getRemoteStorageResolvedInternal`), so this is a scanner
  population change caused by the ownership split, not a relaxation of the test or a new egress. The
  parent may want a follow-up scanner improvement; this change does not attempt one.
* Only the drifted owners were reconciled. The rest of the manifest population and the non-egress
  classifications were left untouched, and the focused run reports no other stale or unreviewed key.
* The two `return storageSelection{}, err` occurrences (dotted-constituent and standard-namespace
  path) share one identity because the scanner sets `line` only inside a `RedactClientError` boundary;
  one key therefore covers both exits, matching how the pre-move `return engine, err` pair was
  recorded.
* The lexical reconciliation is membership- and metadata-only inside the already reviewed extractor.
  It does not re-examine the other 4094 members, does not add behavioural tests for the split owners,
  and its `tests` entries list existing evidence tests (existence is asserted by the population test;
  it does not re-demand their outcome in this change). No full suite, race or stress run was performed.
