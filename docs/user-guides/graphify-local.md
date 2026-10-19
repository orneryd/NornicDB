# Local code intelligence and optional Git hooks

The repository-local importer defaults to the existing `nornicdbcode` database. It does not write
to the server's default database (`nornic` on a standard local installation).
`NORNICDB_DATABASE` explicitly overrides the target; `codeIntelligence.database`
in local Git configuration takes precedence for the runner. Global installation
defaults to the repository name from origin, falling back to the checkout folder. Nodes are keyed by `(id, repo)`,
so a refresh reuses the repository graph instead of creating a new database or a
new copy for every commit.

## Setup

Install the same extractor version used by the reference code-intelligence action:

```sh
uv venv .venv-code-intelligence
uv pip install --python .venv-code-intelligence/bin/python -r scripts/requirements-code-intelligence.txt
export NORNICDB_URI=http://localhost:7474/
export NORNICDB_DATABASE=nornicdbcode
```

Export `NORNICDB_TOKEN`, or both `NORNICDB_USER` and `NORNICDB_PASSWORD`, using your
secret manager or a private environment file. There is no built-in URL, username,
password or token. Missing configuration fails with setup guidance. Keep credentials
out of command arguments and URLs. `.env.code-intelligence` is ignored by Git, but
scripts do not load it automatically: source a trusted private file in your shell
if you use one. Remote connections require HTTPS; local loopback permits HTTP.
An HTTP endpoint supplied as a CLI override does not inherit an environment token
configured for another endpoint.

Hooks inherit the environment of the process running Git. Exports in `.zshrc` are
available to interactive shells; an IDE launched elsewhere may need its environment
configured separately. Set the same variables for your own NornicDB instance.
`CODE_INTEL_REPO` can override the identity derived from the `origin` remote.

## Update the existing graph

```sh
.venv-code-intelligence/bin/python scripts/code_intelligence.py ingest
```

This archives the selected commit (`HEAD` by default), extracts its graph with
`graphify extract . --code-only --no-cluster`, and incrementally imports it. Unstaged
and untracked files are excluded. Use `--revision <commit>` to select another commit
or `--force` to refresh an already recorded commit. No external LLM is required for
extraction. Embeddings use the provider configured on the NornicDB server.

The importer follows the reference action's ingestion format:

- Full symbol source is stored in `body`, with comments/docstrings separated into
  `comments`; rationale and source metadata are preserved. Optional embedding caps
  participate in content hashes. Bodies are not truncated by default.
- Content hashes avoid rewriting and re-embedding unchanged nodes. Edges are merged
  and stale importer-owned data is removed within the repository scope.
- A deletion guard rejects a refresh removing over 50% of the repository graph.
  Investigate extraction errors before changing that limit.
- A `CodeRepository` node records the commit only after successful ingestion. Failed
  runs may have written partial changes; rerunning converges incrementally.
- The configured main entry point receives `:Main`. The local default is
  `cmd/nornicdb/main.go:main`; `CODE_INTEL_MAIN=none` disables it.

The original lower-level entry point remains available:

```sh
graphify extract . --code-only --no-cluster
.venv-code-intelligence/bin/python scripts/graphify_local.py --graph graphify-out/graph.json
```

Use `--out-graph enriched.json` for an offline enriched export. The `/graphify` UI
can browse the imported database. Updating the code database does not automatically
clean historical imports from another database: cleanup must identify their ownership
and preserve unrelated graphs and memories.

## Enable or disable the optional hooks

From the checkout containing these scripts:

```sh
git config --local codeIntelligence.python "$PWD/.venv-code-intelligence/bin/python"
bash scripts/install-git-hooks.sh --with-code-intelligence
```

The installer copies the repository's `pre-commit`, `post-merge`, `post-rewrite`
and `reference-transaction` hooks into Git's configured hook directory, replacing those hook files. Review and
combine any custom hooks first. It honors `core.hooksPath` and linked worktrees.
Running the installer without an option installs hooks without enabling duplicate
checking. Once enabled, the check is mandatory: repair configuration, readiness
or duplicate-code findings before committing; do not disable it to proceed.

The installer records an absolute fallback runner path so hooks can also run on
older branches without these scripts. Keep that tooling checkout and its Python
environment available, or reinstall from another checkout. The enable flag is local
Git configuration, not a setting imposed on every contributor.

### What happens on commit

The existing Go formatting hook still formats staged Go files and refuses partially
staged Go files with unstaged edits. When enabled, duplicate analysis then archives
`git write-tree`: it examines the exact staged content, not a potentially different
working copy. It neither stashes changes nor updates the database. Documentation-only
commits need no database connection.

The detector uses the reference action's analysis, not just an embedding threshold:

1. Select changed functions/methods with at least four code lines, 40 tokens and
   at least one changed code line. Generated paths are excluded. The local
   guard deliberately checks small edits too; the reference action defaults to
   requiring 50% added body lines.
2. Make one ordinary hybrid search request per candidate with `limit: 1000`.
   No cursor, continuation mode, catalog scan or cursor cleanup is requested.
   The server applies its configured relevance filtering. The HTTP endpoint has
   no per-request similarity-threshold field; the hook additionally verifies each
   returned finite RRF score and compares only scores at least **0.01** (the
   search engine's default RRF floor). RRF is distinct from cosine similarity
   and the code-similarity threshold below. Missing/malformed scores, failed
   requests and malformed response lists block the commit.
   This is a bounded candidate search, not proof of collection exhaustion:
   matches beyond the highest 1,000 returned candidates are not checked.
   Database/repository scoping, credentials and duplicate blocking are unchanged.
   The console reports search/comparison time separately from extraction.
   Compare code tokens, normalized structure and receiver-normalized code; a
   similarity of at least 0.8 is a code match.
3. Also detect semantic standouts using chunk similarity relative to the result
   population: at least five scored hits, a 0.15 lead over the median and a 0.05
   lead over the next hit. These defaults come from the reference detector; model
   changes can affect calibration and should be evaluated on real examples.
4. Exclude the candidate itself, touched/retired symbols and recognized moves.
   Test code and production code are compared separately.

**Both a single existing match (reuse candidate) and multiple matches (refactor
target) block the commit.** Fold the implementation into the existing owner and
rerun the check. Authentication, extraction, missing-baseline and incomplete-analysis
failures also block; they do not count as a clean result. The local wrapper removes
the action's candidate-count cap. The time budget covers pagination and comparisons and defaults to 300 seconds; increase
`CODE_INTEL_DEADLINE_SECONDS` when a large staged change cannot finish.

Run the same public check manually with:

```sh
.venv-code-intelligence/bin/python scripts/code_intelligence.py check-staged
```

This is a heuristic against the stored graph, not proof that a change has no
duplicates. Small helpers below the size thresholds and comment-only changes are not
checked. Two new functions absent from the stored graph are not compared directly
to each other. A stale or different-branch baseline limits results; refresh the
intended baseline explicitly. Review is still needed for ownership and design.
Local hooks can be bypassed with Git's normal mechanisms and are not server-side
policy enforcement.

### What happens after a pull to main

The `reference-transaction` hook reacts to committed changes of
`refs/heads/main`: pulls, merges, rebases, local commits, resets and explicit ref
updates. It ingests the new immutable commit even when another branch is checked
out, without changing that worktree. Updates to `origin/main` alone do not refresh:
main must actually change locally. Deletion of main is ignored.

`post-merge` and `post-rewrite` remain compatibility refresh paths for pulls on
main; a successful commit marker makes subsequent calls no-ops. Hooks wait for the
refresh; a large initial ingest can take substantial time. These are local Git
hooks, not new GitHub Actions triggers, and do not start CI.

The Git update has already happened when these hooks run. If extraction, credentials
or ingestion fail, the hook reports that the graph refresh failed; it cannot roll
back the pull. Fix the reported problem and rerun `scripts/code_intelligence.py ingest`.
A successful commit marker allows unchanged refreshes to be skipped.

## Install globally before the PR merges

The global installer copies the runtime outside the repository, so installed hooks
do not depend on the PR being merged or on keeping the tooling checkout around.
Create a standalone environment and install:

```sh
uv venv "$HOME/.local/share/nornicdb-code-intelligence/venv"
uv pip install --python "$HOME/.local/share/nornicdb-code-intelligence/venv/bin/python" -r scripts/requirements-code-intelligence.txt
CODE_INTEL_PYTHON="$HOME/.local/share/nornicdb-code-intelligence/venv/bin/python" bash scripts/install-git-hooks.sh --global-with-code-intelligence
```

This sets global `core.hooksPath`, `codeIntelligence.enabled`, the Python/runtime
paths and `codeIntelligence.databaseFromRepo=true`. It chains a previously configured
global hooks directory, or each repository's native common-dir hooks when there was
none, including hooks unrelated to code intelligence. Managed NornicDB hooks already
call the runner and are not checked twice. An existing repository-local
`core.hooksPath` overrides Git's global setting; incorporate these hooks there or
remove that override deliberately. Global installation does not rewrite those
repository settings.

A database defaults to the origin repository name; with no remote, the checkout
folder supplies both identity and database name. Override a repository explicitly:

```sh
git config --local codeIntelligence.database nornicdbcode
```

Keep that override in NornicDB to reuse its established graph. In global mode, the
main entry point is auto-selected; configure `codeIntelligence.main` or
`CODE_INTEL_MAIN` when an explicit entry point is needed.

Repositories with committed source need a successful baseline ingest before
qualifying source commits can pass.
Run the installed runner from that checkout:

```sh
"$HOME/.local/share/nornicdb-code-intelligence/venv/bin/python" "$HOME/.local/share/nornicdb-code-intelligence/runtime/code_intelligence.py" ingest
```

Linked worktrees share the primary repository's identity and ingestion database.
The origin supplies `owner/repository`; without an origin, the primary checkout's
folder name is used, never a linked worktree's folder. The same resolver serves
ingestion and duplicate checking. Repository database overrides are shared too,
so NornicDB worktrees reuse `nornicdbcode`. The commit check never creates a database.

An unborn branch can be checked against an existing ingested graph without a Git
`HEAD`. A missing database or ingest record still blocks source commits. An empty
database is never accepted as a substitute for the intended graph. Search must be
ready for the bounded candidate search. Failed
authentication, unavailable service, timeouts and incomplete searches block commits.

**Mandatory check:** Do not override, bypass or disable the hook to get a commit
through. Fix the database selection, baseline ingestion, search readiness or
duplicate/divergent code before proceeding. Failures include this warning and
identify the repository and target database so configuration can be repaired.

Historical cursor-mode regression evidence (2026-10-17; superseded by the bounded search policy above): from the `NornicDB-1406` linked worktree,
the installed global hook searched `orneryd/NornicDB` in `nornicdbcode`. A staged
copy of `scripts/graphify_local.py:comments_for_node()` renamed the function and
changed its empty-result behavior instead of reusing the owner. The real commit
was rejected with a reuse finding at 99% similarity and the mandatory warning.
`HEAD` and the staged tree stayed unchanged; the staged probe was then removed.
Test and production sources remain separate comparison populations, as requested.

The ranked-only follow-up retained that rejection with a 0.01 RRF floor: four
successful search/continuation requests took 2.81 seconds on the server. Local
search preparation and comparison completed in 13.58 seconds; the whole hook,
including staged-tree extraction, took 54.07 seconds. These are one live probe's
measurements, not a whole-repository latency guarantee.

Global hooks require URL/credentials in the Git process environment; installation
stores no credentials. Export them in your shell startup configuration and ensure
your IDE's Git process inherits them. Missing configuration produces setup guidance.

The commit check does not require server cursor support or depend on cursor TTL.
`CODE_INTEL_DEADLINE_SECONDS` bounds search and comparison after extraction
(default 300 seconds). Query embeddings must be available for semantic behavior
matching; full-text retrieval still supports lexical/structural code matching.
A high result limit does not guarantee exhaustive duplicate detection.

Rerun the global installer to update its copied runtime. To undo global installation,
restore the previous `core.hooksPath` (recorded as
`codeIntelligence.previousHooksPath`) or unset global `core.hooksPath` if none existed.
The native hook files are preserved.

## Verification and upstream alignment

`scripts/code-intelligence-upstream.json` records hashes of the reference importer
and detector plus local adaptations. Matching logic is shared in `pr_duplicates.py`;
`graphify_local.py` is the single importer used by both manual and hook paths.

The integration tests exercise actual Git commits and pulls against an HTTP fixture,
including staged-versus-unstaged content, both blocking classifications, missing
configuration, missing baselines, disabled checks and main refreshes. The command
isolates unrelated user-global Git configuration; global-hook tests install and
exercise their own real global configuration in temporary repositories:

```sh
GIT_CONFIG_GLOBAL=/dev/null .venv-code-intelligence/bin/python -m unittest scripts.test_code_intelligence_hooks scripts.test_pr_duplicates scripts.test_graphify_local scripts.test_graphify_main scripts.test_graphify_comments scripts.test_graphify_errors scripts.test_graphify_embedding_cap scripts.test_graphify_http_auth scripts.test_graphify_divergence
```
