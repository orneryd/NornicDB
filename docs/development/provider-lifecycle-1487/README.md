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

Repeated race checks, package checks and current-release integration results
will be recorded before publication. Canceled, blocked and skipped checks are
not passes. No broad throughput or startup/shutdown percentage gain is claimed.

## Local cleanup

Before work, three clean inactive worktrees were removed without deleting
branches, 123 dirty worktrees (and their containing parents) were preserved,
and 912 prior-task temporary artifacts were removed (3,173,918,515 bytes).
Logs/profiles were archived at
`~/Library/Logs/NornicDB/cleanup-20261018/prior-task-evidence.tar.gz`.
The primary checkout and IDE's active code-intelligence/HNSW worktrees remain.

Live NornicDB search in `nornicdbcode` located the initialization, batch retry,
and pending-index owners. Its ingested source predates the PR; candidates were
verified against the checkout. No NornicDB/Graphify MCP tool is exposed in this
session, so the configured authenticated HTTP search API was used read-only.

## Resumed current-release integration

On October 18 the maintainer resumed #1487 and restored mandatory global commit
checking. Current release `714b95e326cddc6bba449437b1cec542eecb8060` is merged
locally at `f93fe58ef`. Conflict resolution retains statement-cache disposal and
provider quiescence: DB foreground calls join before cache disposal and provider
release; HTTP Stop retains its cache cleanup and joins initialization.

Fresh integrated server lifecycle and English/Spanish locale checks pass with
`-race -tags noui,nolocalllm -count=1` (60.969s). Repeated DB provider-owner checks
pass in 66.141s (`-count=10`). MCP discovery has a separate explicit repeated
check; the earlier combined package command selected no MCP tests and is not
counted as MCP lifecycle evidence. Native warmup/Close checks pass in 1.519s
(`-race -tags noui,localllm -count=10`) after copying the existing ignored native
linker artifact from the local macOS build. No actual GGUF inference was run.

The initial broad package run overlapped release integration and failed the
runtime-built plugin fingerprint check; it is superseded, not a pass. A fresh
integrated package run and both-parser repeated locale checks are running.

Both `make verify-list BASE=origin/release/v1.4.4` and
`make verify BASE=origin/release/v1.4.4` fail at the verifier self-test: synthetic
Git fixture `plan` inherits the mandatory global hook and has no ingested graph.
The hook remains enabled. `VERIFY_BASE=origin/release/v1.4.4 bash scripts/verify.sh`
is running the underlying checks separately; this is not a passing make verify
or self-test result.

The live mandatory hook checked repository `orneryd/NornicDB`, database
`nornicdbcode`, and blocked the pre-integration staged snapshot after 107.75s.
It reports NewEmbedderContext versus its existing NewEmbedder forwarding wrapper,
NewLocalGGUFContext versus its existing NewLocalGGUF forwarding wrapper,
startWarmup versus warmupLoop (launcher versus worker), and DB.Open versus
GenerateSalt (11% code overlap). Its ingested source is `7e90401c6`, older than
the checked source. These findings require resolution; no bypass, source commit,
push or readiness claim is made for this resumed correction.

The maintainer explicitly authorized deletion of dirty /tmp worktrees after
rejecting relocation. All 150 existing temporary worktrees were removed (42
clean directly, 108 after the interrupted relocation), branches preserved, and
327 old task artifacts deleted. No registered /tmp worktrees remain. A root-owned
installer log could not be deleted without administrator authentication.

## Mandatory wrapper removal

At the maintainer's direction, forwarding constructor APIs were removed rather
than retained as compatibility layers. NewEmbedder and NewLocalGGUF each have one
context-taking implementation (with the existing build-tag native/stub boundary).
All callers and DB constructor injection use that API; the DB supplies its
lifetime context. Constructor, DB-owner and server-init focused checks pass on
this source; native and full-package verification must be refreshed after this
source change. Earlier completed results do not establish this final revision.

## Remaining finding correction

The single-flight creation/stats and pre-existing-flight assertions now live in
one test owner as two cases; neither set of assertions was removed. Its
10-repeat race check passes in 2.101s. Model warmup launch, periodic work and
completion now have one implementation instead of a startup function forwarding
to a separate loop. Native race checks pass in 1.488s.

The database HTTP server and standalone MCP now share bounded HTTP shutdown.
The public MCP cancellation regression fails before correction because Stop
returns with an active connection still open. The corrected MCP case, existing
main-server deadline/force-close case, and no-active-request helper case pass
10 repeated race checks (MCP 1.864s, server 13.663s, util 1.339s).
These do not establish a final frozen-head full-verification pass.
