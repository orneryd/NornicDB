# Provider lifetime correction for #1487

The maintainer requested the product lifecycle correction on #1487, superseding
its earlier test-only scope and deferral to 1.5.0. The synchronized log buffer is
retained; DB-side embeddings are enabled in the locale test and its queue
publication wait is removed.

## Causal boundary

The DB owns each constructed provider before health checking and retains
accepted/replaced/cached/per-database providers until their actual users return.
The existing owner from #1465/#1469 is reused, with their unresolved background
batch review corrected. There is one resource release path, not a second server
model closer. The CLI closes the DB after setup failure as well as normal exit.

| Operation | Owner / shutdown contract |
| --- | --- |
| Construction, health, retries | DB-admitted initializer; DB lifetime cancels it and Close joins it |
| HTTP Stop | Cancel initializer and join before successful return; a caller deadline may return an error while DB Close still joins |
| Installation | Closed-state admission under DB lock prevents late publication |
| Failed/rejected attempt | Same idempotent resource owner releases before retry |
| Foreground embedding/chunking, retained snapshots | DB admission and cancellation; join before model release |
| Cypher scopes / MCP discover | Share the binding's DB lifetime guard |
| Queue ordinary/typed batches | Call provider synchronously inside the joined worker; cancellation cannot detach a native call |
| Queue document batches / property batches | Existing synchronous provider calls stay under the worker join |
| Native warmup | Join only a launched worker, outside the model mutex; concurrent native Close joins the same once |

Close may wait for a provider/constructor that ignores cancellation. Returning
while native code still uses freed state would violate the ownership contract;
no timeout is treated as successful resource quiescence.

## Reproduction and verification

Original #1487 head: `d06bcf152dc87a9228c9b62edecdb968dc6c4c74`.
Prior lifecycle owner imported from #1465 head
`6665a99fead163751897f1481523e9e0949458de`; develop counterpart #1469.

- Before runtime edits, the temporary `TestClosedDBRejectsLateProviderPublication` probe fails on
  the original head: the closed database publishes the late provider. Its assertion
  is retained in the stronger existing `TestDB_SetEmbedder_IgnoredAfterClose`
  regression, rather than keeping two tests of the same contract.
- On the prior lifecycle owner, `TestDBCloseJoinsActualBackgroundProviderCall`
  fails for ordinary and typed batches: provider Close runs with one active call.
  The test uses an isolated temporary on-disk DB, explicit embedding enablement,
  channel-controlled cancellation and release, and never calls a native model.
  Initial in-memory probe attempts did not reach the provider and are not counted
  as reproductions. The storage pending-index gate differs on that fixture.
- With synchronous actual calls and the shared owner, both regressions and the
  no-wait locale test pass. The first focused run passes nornicdb in 2.211s and
  server in 2.170s (`-tags noui,nolocalllm`, `-count=1`).
- Native warmup/Close checks pass with `-race -tags noui,localllm -count=10`
  in 1.519s. No real GGUF model was loaded by these deterministic lifecycle tests.

## Canonical APIs and consumers

`embed.NewEmbedder(ctx, config)` and `embed.NewLocalGGUF(ctx, config)` are the
canonical context-taking APIs. Redundant context-suffixed and forwarding
constructors are removed; all Go callers, factory injection and examples migrate
to the canonical APIs. The DB supplies its lifetime context.

The Local GGUF factory branch forwards that context to
`NewLocalGGUF(ctx, config)`. This constructor migration belongs in these
implementation notes; the pre-existing Local GGUF implementation plan under
`docs/advanced/embeddings/local-gguf.md` is left unchanged by this PR.

Single-flight creation and waiting assertions share one test owner with both
cases retained. Native warmup launch, periodic work and completion share one
implementation. DB.Open uses the existing encryption.GenerateSalt owner.
Database HTTP and standalone MCP share bounded HTTP shutdown; the MCP regression
reproduces an active connection surviving canceled Stop before the correction.
Handler and resolver tests use one database-manager fixture that closes the
owning DB before shared storage. Both public cleanup regressions are retained.
Subscription creation tests cover available and missing brokers as explicit
cases, preserving event-delivery and error assertions.

## Verification evidence and limits

Focused race checks passed: DB provider ownership repeated ten times (66.141s),
MCP discovery repeated ten times (9.037s), retained snapshots (17.024s), and
English/Spanish initialization logging under both parsers, repeated thirty times
(default 77.428s; ANTLR 78.257s). Native warmup/Close tests use deterministic fake
resources rather than real GGUF inference. After constructor consolidation,
native warmup/Close passed ten repeated race checks in 1.488s. Single-flight
creation/wait cases passed ten repeated race checks in 2.101s. Shared HTTP
shutdown checks passed ten repeated race checks through both public Stop paths
(MCP 1.864s; server 13.663s; helper 1.339s).

Final frozen-head full verification results are recorded in PR result comments.
Earlier checks overlapping source integration, canceled runs, and blocked runs
are not final-source passes. Initial full verification exposed release-wide
localization-catalog and documentation-link failures; #1540 corrects that shared
root and is integrated here. Synthetic Git verification fixtures are isolated
from the developer's global hooks with GIT_CONFIG_GLOBAL=/dev/null within the
verification process. Actual source commits use the enabled live duplicate hook.
No model-inference, GPU throughput or general performance gain is claimed.

## Documentation placement and build verification (2026-10-20)

TJ deleted the development write-up in `2a0474eb8`. Its lifecycle contracts and
historical evidence are retained here, recovered from that commit's parent;
the deleted published path stays deleted. This follow-up is based on
`631e8fce0`, which preserves TJ's commit and the subsequent release merge.
The Local GGUF constructor note is retained above rather than changing the
pre-existing implementation plan. The package README remains package-level
developer documentation, and the changelog's stray blank line is removed.

An isolated TrueNAS container ran `mkdocs build --strict` with Python 3.12 and
the repository's `requirements-docs.txt` (MkDocs 1.6.1, Material 9.7.7,
minify 0.8.0, pymdown-extensions 10.21.3). The release base `8c2fc6e6e`,
reviewed head `537c27704` with the development README, head `2a0474eb8` after
its deletion, and the final staged documentation all exited 0 with no MkDocs
warnings or errors. The merge from `2a0474eb8` to `631e8fce0` changes none of
the docs inputs. The final `docs/` tree is identical to the release base.
These are documentation builds; no Go checks were run for this follow-up.

Static inspection found all 200 nav entries (197 distinct paths) present, no
references to the deleted path, and no snippet include directives. The root
changelog, package README and OpenSpec are outside the default `docs/` source
directory; `docs/changelog.md` links to the root changelog on GitHub rather
than including it. No missing-link or missing-anchor diagnostics were emitted.
MkDocs 1.6.1 defaults treat missing nav/link targets as warnings and omitted
nav pages, missing anchors and unrecognized relative links as INFO. The
reviewed README added one INFO-only omitted page to the release base's 80;
six existing unrecognized-relative-link INFO messages are unchanged. Thus
the suspected strict-build failure was not reproduced, but development notes
are now kept out of the published documentation as requested.
