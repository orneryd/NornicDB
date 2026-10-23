# Local T3 and NornicDB workflow harness

This is the implementation experiment for PR 1648. The workflow service lives in
a pinned local T3 extension; NornicDB stores its records through existing Cypher
APIs. No custom database plugin or Git hook is installed.

## One command on Windows and macOS

From this PR checkout, `make swarm` sets up and starts the harness; `make
swarm-setup` installs without starting, and `make swarm-doctor` checks local tools
and service reachability. On Windows these call the existing PowerShell/NVIDIA
scripts. On macOS the Python launcher uses an existing remote NornicDB and
Ollama host; it does not run the CUDA Compose file on Apple Silicon. The T3
checkout and all runtime state are outside the repository. Press Ctrl-C to stop
the foreground T3 process. T3 prints a private pairing URL; open it in a browser
to use the UI. The normal web address is `http://localhost:5733`.

### macOS with local and remote workers

Install Git, Python 3, Node/npm, [Vite+](https://viteplus.dev/),
[uv](https://docs.astral.sh/uv/), and [llama.cpp](https://github.com/ggml-org/llama.cpp).
Local inference uses Ornith 1.5 Compact through `llama-server`; local Ollama is
not required. The remote 2080 Ti host keeps Ollama for the 9B intent router.

```sh
brew install llama.cpp
make swarm-setup
```

The default permanent tools/state directory is
`~/.local/share/nornicdb-swarm`; set `SWARM_TOOLS_ROOT` to change it. The setup
pins T3 to `2c8bdf6177159d8fcd12a897d7c9311f6eb93237`, applies this PR's
patch, installs frozen T3 dependencies, agent runtime 1.4.0 and the
code-intelligence Python requirements. It downloads the pinned 17.44GB Ornith
Compact GGUF and verifies its SHA256 before starting it. It preserves existing
T3 provider instances and does not change global Git hooks.

Put machine-specific endpoints in a private `env` file under that directory
(mode 0600); shell environment values take precedence. For the current testing
host use:

```dotenv
SWARM_NORNICDB_URL=http://192.168.1.30:7474
SWARM_REMOTE_OLLAMA_URL=http://192.168.1.30:11434
SWARM_LOCAL_MODEL_URL=http://127.0.0.1:11435
SWARM_LOCAL_MODEL=nornic-ornith
SWARM_LOCAL_CONTEXT_WINDOW=0
# Optional if codex is not on PATH:
SWARM_CODEX_BINARY=/absolute/path/to/codex
```

`SWARM_NORNICDB_URL` is deliberately separate from `NORNICDB_URI` so an existing
local NornicDB setting in your shell cannot redirect this harness. If the server
requires authentication, set `NORNICDB_TOKEN` or `NORNICDB_USER` and
`NORNICDB_PASSWORD` in the shell or private env file. Set `LINEAR_API_KEY` there
only when using Linear. Never put keys in the tracked model JSON or patch.
Authenticate Codex separately with `codex login`; the Nornic/Ollama workers do not
need cloud API keys. The code graph remains in `nornicdbcode`; the launcher
creates or reuses `t3workflow` for workflow records and keeps T3's own SQLite
state locally. It does not reingest the remote graph.

```sh
make swarm-doctor
make swarm
```

The agent is named **Nornic** in T3. Its local entry is **Nornic Local · Ornith**,
with **Nornic default** as the model selection; the remote entry is
**Nornic Remote 9B**. The underlying runtime dependency remains
`@earendil-works/pi-coding-agent@1.4.0`. Its `pi` executable,
`PI_CODING_AGENT_DIR` environment variable, T3 adapter package path and `pi`
driver identifier are technical dependency names, not the agent's UI name.

New chats default to **Nornic Local · Ornith / Nornic default**. Within the
Codex provider, the model dropdown defaults to **GPT-6.1 Sol** with **Low** reasoning.
Choose **Nornic Local · Ornith** to use automatic intent routing. Selecting
**Codex Frontier** directly starts it without the intake classifier. Automatic
intake routing uses `ollama_local`; Codex is the frontier provider. Coding workers
use distinct entries, `nornic_local_worker` and `nornic_remote_worker`, described
below. The older standalone `ollama_remote` entry still uses the local-only intake
hook and is not the remote delegated-worker target.
The remote 9B model classifies intent and complexity. `intent.route`
may automatically delegate frontier work only to the exact `gpt-6.1-sol` model,
starting at low reasoning. After a frontier handoff, the local agent only reports
the child-thread receipt: its model request disables tool selection and its tool
hook refuses workflow and graph calls with that handoff reason. Failed routing
uses the same control and reports the failure. The next admitted user turn resets
this state. A Linear issue identifier belongs to `linear.issue`; it is not a graph
node ID for `nornic_recall`.

Plans, reviews, and very uncertain classifications use
medium reasoning. If Sol is not advertised and ready, routing returns
`SOL_UNAVAILABLE` without choosing a different model. Astra requires an explicit
user-started T3 session; MCP agents cannot delegate to it, create Astra threads,
send Astra threads new work, or schedule Astra runs. For terminal use, run
`make swarm` and keep the terminal open; T3 prints the
browser pairing URL there. To launch from any directory, add a shell alias such
as `alias t3-swarm='make -C /absolute/path/to/NornicDB-swarm swarm'` to your
shell configuration. On a different host, update the private URLs and use
`make swarm-doctor` before starting.

### Local model lifetime and context

The macOS setup installs `com.nornicdb.swarm-model` in the user's LaunchAgents.
It starts at login, restarts after failure, and runs independently of the T3
terminal. `--sleep-idle-seconds -1` keeps the model warm after requests. Logs are
under `~/.local/share/nornicdb-swarm/logs/local-model.log`; the listener is
loopback-only. The service enables the bundled MTP head using `--spec-type
draft-mtp` and uses the GGUF's native chat template with tool support.

The local service uses `--ctx-size 0`, meaning the model's native context
(262,144 tokens for this Ornith checkpoint), with one server slot. An explicit
`SWARM_LOCAL_CONTEXT_WINDOW` can select a smaller token count. The standalone
remote worker uses a separate `nornic-agent-remote` alias with its own
`SWARM_REMOTE_CONTEXT_WINDOW` (262,144 by default). The remote `nornic-coder`
intent router and read assistant alone request 32,768-token contexts. Codex
receives no context-size override; its runtime manages the frontier model's
native window and compaction. Catalog context sizes must agree with the serving
runtime; advertising a larger window alone does not increase server capacity.

The artifact is `mudler/Ornith-1.5-35B-A3B-APEX-MTP-GGUF`, revision
`4c326713c52d13fdb40983a2a09a46cca8e88739`, file
`Ornith-1.5-35B-A3B-APEX-MTP-Compact.gguf`, SHA256
`a7373f724cf3693e8a6a5f8209ae9a06ff8071ae2af60659bdbdf8ff6f6c10df`.
The quantization card reports no throughput benchmarks for these files; MTP and
long-context performance require measurements on the actual host.

The remote test Compose configuration has authentication disabled and publishes
HTTP/Bolt on its LAN interfaces. Restrict those ports to the trusted test
network; see `docs/.flagged_security.md` before putting real data there.

## Prerequisites (Windows / NVIDIA)

Install Docker Desktop with WSL2 GPU support, current Git for Windows, Node/npm,
[Vite+](https://viteplus.dev/), [uv](https://docs.astral.sh/uv/), and
[Ollama](https://ollama.com/). Docker and Ollama must be running. Verify
`nvidia-smi`, `docker info`, and `git worktree list --porcelain -z` in this checkout.
Git 2.35 is too old for the worktree command used by the importer.

Clone this PR's branch, then run from the repository root:

```powershell
./scripts/setup-swarm-local.ps1 -BuildImage
```

The default tools/state directory is `../.swarm-local`, outside the repository.
Override it with `-ToolsRoot D:\tools\nornic-swarm` on every script invocation.
An existing machine can keep custom startup paths in the ignored
`docker/.swarm-local.paths.json` (`T3Root`, `StateRoot`, `CodeIntelEnvironment`,
`CheckerRepository`, `GitDirectory`). Explicit script arguments take precedence;
passing `-ToolsRoot` selects that installation instead of the private overrides.
Setup clones T3 at `2c8bdf6177159d8fcd12a897d7c9311f6eb93237`, applies
`testing/swarm-local/t3-nornic.patch`, installs its frozen dependencies, Nornic 1.4.0
and the existing code checker's pinned Python requirements, and creates the
`nornic-coder` Ollama alias. Existing T3 settings are preserved. Use a fresh tools
directory when changing the pinned patch; do not apply it to arbitrary T3 releases.

The Docker image is built from release/v1.4.4 commit
`ba8d6d13b48452ec4e38d826869ad63a64063848` using
`timothyswt/llama-cuda-libs:v0.4.1`; the CUDA libraries are downloaded, not rebuilt.
The image currently reports 1.4.3 from the release branch's version metadata.
Omit `-BuildImage` if `nornicdb:1.4.4-cuda-local` is already available.

## API key and models

Set the following in **`docker/.env.swarm-local`**, which setup creates and Git
ignores. Only the empty example is committed. Never put your key into the patch,
T3 model configuration, or a command committed to the repository.

```dotenv
OPENAI_API_KEY=your-key-here
```

NornicDB uses OpenAI `text-embedding-3-small` (1536 dimensions), with **4 concurrent
embedding workers**. The model's default dimensions are documented in the
[OpenAI embedding guide](https://developers.openai.com/api/docs/guides/embeddings).
Code submitted for embeddings goes to OpenAI. Embedding/search indexes live in
system RAM; graph data and vectors persist in Docker's data volume.

The remote Ollama router uses Qwen 3.5 9B at 32K context. Its standalone
worker alias has a separate native-size context. On the test RTX 2080 Ti it ran entirely on GPU. Confirm using `ollama ps`;
do not infer GPU residency from the model name. BGE is no longer required or loaded
by this configuration. The larger Ornith 35B quantization was not selected for this
11 GB GPU.

Authenticate a frontier provider in T3. Setup registers Codex if `codex` is on
PATH, or pass `-CodexBinary C:\path\to\codex.exe`. Run that executable's `login`
command. Other enabled/authenticated T3 frontier instances can be selected by
`NORNIC_FRONTIER_INSTANCES` in the startup script. API embedding credentials and
frontier authentication are separate.

For Linear, add `LINEAR_API_KEY=...` to the same ignored env file. The startup
script passes it only to the local T3 server; Docker does not need this key.
The tracker adapter uses [Linear's GraphQL API](https://linear.app/developers/graphql).
`linear.assigned` lists issues assigned to the authenticated API-key owner,
returning that person's identity and `pageInfo`. It defaults to 25 uncompleted
issues in the configured `NORNIC_LINEAR_TEAM` (default `NOR`); `first` accepts
1–50, `after` continues the returned cursor, and `includeCompleted` includes
completed/canceled issues. Do not assume that an integration key belongs to the
human asking: report the returned account and resolve any identity mismatch.
For example, call `nornic_workflow` with
`{"operation":"linear.assigned","requestId":"assigned-1"}`. If
`pageInfo.hasNextPage` is true, continue with `after=pageInfo.endCursor` and a
new request ID before claiming a complete list.

`linear.issue` reads the current issue; `linear.bind` attaches the exact current
scope and external version to an assigned task. Binding invalidates its previous
plan/claims/duplicate receipt, so plan again against the actual issue. Bound tasks
refresh Linear before planning, claiming, writing and validating; changed external
versions block admission until reconciled. This protects human scope and assignee
edits. The current runtime performs no external issue mutations. Full status and
Delivery projection remains a target requirement until the lifecycle produces
the required independent evidence; a duplicate pass cannot mark Test Complete.

## Start and ingest

```powershell
./scripts/start-swarm-local.ps1
```

Open the pairing URL printed by T3 (web UI normally `http://localhost:5733`). Its
backend normally uses port 13773. Add the repository as a T3 project and select
**Nornic Local / Nornic default**. Use a **new worktree** for implementation tasks;
the constrained file gateway refuses writes to the project's primary checkout.
State, provider sessions and settings remain isolated under the tools directory.
NornicDB listens only on loopback ports 7474/7687 with local test authentication
disabled. Do not expose those ports on a shared network.

The extension stores coordination state in **`t3workflow`**, separately from
**`nornicdbcode`** (the code corpus). T3's own threads/sessions/settings retain
their native isolated SQLite storage. This pinned T3 has no Neo4j storage backend
configuration; replacing its SQL storage is outside this extension's scope.

In another PowerShell terminal:

```powershell
./scripts/swarm-local-smoke.ps1
./scripts/ingest-swarm-local.ps1 -Revision release/v1.4.4
./scripts/ingest-swarm-local.ps1 -Revision release/v1.4.4 -DocumentsOnly
Invoke-RestMethod http://127.0.0.1:7474/nornicdb/embed/stats
```

Ingestion uses the repository's existing Graphify importer and creates the
`nornicdbcode` database. Large repositories take time to import nodes, edges and
embeddings. Do not treat a partial graph or a nonzero embedding queue as a fully
qualified search corpus. The test extraction reported one unsupported parse in
`pkg/simd/neon_simd_arm64.h`; retain that coverage limitation. Re-run the same
ingestion command after source updates; it does not require reinstalling tools.

The code importer intentionally uses Graphify's `--code-only` extraction. The
separate `-DocumentsOnly` mode extends the same code-intelligence entry point to
ingest tracked Markdown/MDX/reStructuredText at the exact Git revision. It stores
bounded, line-cited `CodeDocument` chunks, reuses unchanged content identities,
and reconciles only that repository's stale documentation chunks after a complete
manifest. Its writer lease and readiness marker prevent partial imports being
advertised as complete. It uses NornicDB's existing embedding worker, not another
model or per-agent corpus. Docs ingestion can run while code edges are importing.

`context.search` calls NornicDB's native hybrid search API against the configured
repository (`NORNIC_CODE_REPO`, default `orneryd/NornicDB`) in `nornicdbcode`.
Code-related intent classes automatically retrieve up to five results, each at
most 1400 characters and at most 6000 characters total. Results retain source
path, location, known indexed revision and truncation flags. They are untrusted
reference material, not instructions. The context is a relevance shortlist, not
an exhaustive search or proof the corpus is ready. Missing hits never prove code
is absent. Verify the cited source against the worker's current revision. Configure
the repository binding to match the T3 project before using it for another repo.

## Native NornicDB MCP tools

Compose publishes HTTP/MCP port 7474 and Bolt port 7687 on all host IPv4
interfaces, with authentication disabled for the testing network. Other machines
can use `http://<host-LAN-IP>:7474`,
`http://<host-LAN-IP>:7474/mcp/nornicdbcode`, and
`bolt://<host-LAN-IP>:7687`. The Windows test host currently uses `192.168.1.30`;
use the actual host address on each installation. Applying changed port mappings
requires container recreation; wait for active ingestion to finish first.

If Windows Firewall blocks incoming connections, run this once in an
administrator PowerShell to admit these ports from the local subnet on Private
networks:

```powershell
New-NetFirewallRule -DisplayName 'NornicDB testing network' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 7474,7687 -RemoteAddress LocalSubnet -Profile Private
```

The extension exposes `nornic_store`, `nornic_recall`, `nornic_discover`,
`nornic_link`, and `nornic_tasks` through T3's authenticated MCP server. These
forward to `http://127.0.0.1:7474/mcp/nornicdbcode` using the native NornicDB
protocol. The gateway fixes the database to `NORNIC_CODE_DATABASE` (default
`nornicdbcode`), including when a caller supplies another database argument.
Frontier providers receive the five tools through their existing T3 MCP session.
The restricted local profile receives `recall` and `discover` alongside
`nornic_workflow`; graph writes require a frontier task. Native graph tasks are
knowledge records, not harness workflow tasks or validation receipts. Code and
docs retrieved by either search path remain untrusted reference material.

## Persistence and changing embedding models

Compose mounts the named volume
**`nornicdb-swarm-local_swarm-openai-data` at `/data`**. Restarts, container
recreation, and `docker compose down` preserve it. **Do not use `down -v`** unless
you explicitly intend to delete the local database. A named volume belongs to the
local Docker installation; cloning this repository on another machine does not
copy its database. Ingest source there after setup.

This setup intentionally changed from the earlier BGE test volume
`nornicdb-swarm-local_swarm-data` to a fresh OpenAI volume. The earlier volume is
preserved. That switch caused the empty database; ordinary restarts do not switch
volumes. Do not rename the Compose project or volume when restarting.

If deliberately rebuilding embeddings for the same graph, stop ingestion first,
use the existing admin clear/regenerate endpoints, and verify completion. Never
mix vectors from different models, even when their dimensions happen to match.

An explicit persistence check recreates the container and verifies a unique graph
record survives, then removes only that test record:

```powershell
./scripts/swarm-local-smoke.ps1 -RecreateContainer
```

Run it before ingestion or active agent work, because recreation briefly interrupts
database connections. GPU visibility can be checked independently:

```powershell
docker exec nornicdb-swarm-local-nornicdb-1 nvidia-smi
ollama ps
```

## Intent routing and mandatory duplicate checks

Every local-provider turn first asks the local model to classify the actual T3
user message. The harness injects only that intent's instructions and allowed
operations. Routine planned implementation stays local. Design/review, complex or
uncertain work routes through T3 to an enabled, installed, authenticated frontier
provider. Missing classification or frontier readiness blocks admission and
reports the reason. A request to *implement an existing plan* must classify as
implementation, not as a request to create a plan.

For simple local information requests, a bounded read assistant on the routing
model can call `linear.assigned`, `linear.issue` and `context.search` directly.
It can make up to three reads, inspect results, and ask a specific clarification
question when user input is missing. Up to four preceding conversation messages
are available to resolve clarification replies. Repeated requests, oversized
evidence and missing evidence stop the shortcut rather than fabricating an
answer. The selected local model displays its answer or question with further
tool calls disabled; this is not a direct UI response that avoids all local-model
inference. Complex requests still follow the existing Sol routing rules.

## Skills and repository guidance corpus

Clone the skills library once, then publish its Markdown documents together with
NornicDB's `AGENTS.md`, this harness guide, and their recursively linked local documentation:

```sh
git clone git@github.com:tommyvo/useful-prompts.git ~/src/useful-prompts
make swarm-help
```

The importer uses committed Git snapshots. Commit intended guidance changes
before refreshing. It does not run the skill repository's installers or execute
retrieved instructions. `SWARM_SKILLS_ROOT` and `SWARM_GUIDANCE_ROOT` can select
different local checkouts. Connection URLs and credentials come from the private
swarm env file or environment. For the explicitly unauthenticated LAN test
server only, avoid inheriting credentials for a different local server:

```sh
NORNICDB_TOKEN= NORNICDB_USER= NORNICDB_PASSWORD= make swarm-help
```

On Windows, `make swarm-help` uses `SWARM_TOOLS_ROOT` (default
`../.swarm-local`); configure `NORNICDB_URI` or the private `env` file in that
directory before running it. Native Windows execution has not been verified.

The existing document publisher owns one leased, reconciled manifest under
`nornic/intent-help`, in `nornicdbcode` by default. It deletes stale chunks only
from that help repository. `NORNIC_HELP_REPO` and `NORNIC_HELP_DATABASE` can
override that scope consistently for ingestion and T3. Each chunk retains its
original repository, path, source revision and line range. A manifest with source
revisions, missing local links and external references is written to
`intent-help-manifest.json` in the tools directory. External URLs are not fetched;
binary files are not ingested. Embedding readiness is separate from publication.

Normal intent routing retrieves bounded relevant help from this corpus alongside
applicable code context. The read assistant can retrieve it on demand. Explicit
help search is available as
`{"operation":"context.search","contextScope":"help","query":"review checklist","requestId":"help-1"}`.
Retrieved guidance is cited reference material. It cannot grant tool access,
authorize external changes, or override the current user's scope and approvals.

The opt-in local profile disables native shell/edit/write tools. Its only workflow
tool supports assigned tasks, versioned plans, expiring resource claims and a
bounded existing-file gateway. Writes require an isolated worktree, planned scope,
live claim generation and the current file SHA256. Pending write records fence
unresolved outcomes. The prototype does not silently clear those records.

`workflow.validate` runs the existing `scripts/code_intelligence.py check-staged`
against an isolated Git index containing the exact candidate tree. It preserves
duplicate advice and blocks on checker errors, incomplete results, confirmed
duplication, or source changes during checking. It does not change the user's Git
index or install a hook. The agent must resolve findings and revalidate. A duplicate
pass is a specific receipt, not proof that unit tests or independent review passed.

## Embedding progress and failures

Check both `/nornicdb/embed/stats` and `/nornicdb/embed/failures?limit=10`.
`stats.failed` is a cumulative failed-attempt counter, including retries; it is
not a count of currently failed nodes. `parked` and the failures endpoint expose
terminal failures. `pending_nodes` is unfinished work, and `total_embeddings`
should grow as it drains. `running` reflects in-flight work at the instant of the
sample, rather than whether the worker pool is enabled.

The initial bulk import hit OpenAI's 1,000,000-token/minute account limit with ten
workers, producing HTTP 429 responses, plus one transient HTTP 500 batch. These
were retryable; the observed 36,912 failed attempts were accompanied by zero
terminal failures. Do not reset the graph or clear embeddings to reset this
counter. Wait for pending work to drain and check for terminal errors before
qualifying the complete corpus. The default is now four workers to reduce
concurrent API pressure. Worker count does not override the account's API
limits. To rotate the key, save it in the private env file and recreate the
container after ingestion finishes; ordinary container restart retains its old
environment.

## Extension checks and current limits

From the patched T3 directory, with Docker/Ollama running:

```powershell
$env:NORNICDB_LIVE_TEST='1'
$env:NORNIC_INTENT_LIVE_TEST='1'
$env:NORNIC_LINEAR_LIVE_TEST='1' # optional; loads NOR-53 read-only
# Supply LINEAR_API_KEY from the private env file in this process for that check.
vp test apps/server/src/nornic packages/provider-pi/src/server/mcpExtensionSource.test.ts
```

Tests cover ownership, contention, stale sessions, replay, path/hash checks,
duplicate-gate outcomes, intent admission and live local classification/persistence.
The service deliberately reports `strictEnforcementReady: false`: this experiment
does not implement PR 1648's entire procedure catalog, universal provider process
isolation, independent test/review evidence, Linear projection, or full lifecycle
completion. Frontier providers retain their existing T3 execution facilities;
the restricted Nornic profile's tool admission must not be described as universal
enforcement. Do not claim an agent's prose summary is an execution receipt.

The full duplicate integration check requires an indexed fixture. From this repo:

```powershell
python ./scripts/create-swarm-fixture.py ../.swarm-local/swarm-workflow-fixture
./scripts/ingest-swarm-local.ps1 -Repository ../.swarm-local/swarm-workflow-fixture -Revision main -Main none
```

Then, from patched T3, set these paths to your tools directory and repository:

```powershell
$env:NORNIC_DUPLICATE_LIVE_TEST='1'
$env:NORNIC_DUPLICATE_FIXTURE_ROOT=(Resolve-Path ../swarm-workflow-fixture).Path
$env:NORNIC_DUPLICATE_PYTHON=(Resolve-Path ../code-intel/Scripts/python.exe).Path
$env:NORNIC_DUPLICATE_RUNNER='C:\path\to\NornicDB\scripts\code_intelligence.py'
$env:NORNIC_GRAPHIFY=(Resolve-Path ../code-intel/Scripts/graphify.exe).Path
$env:NORNICDB_USER='local'
$env:NORNICDB_PASSWORD='local'
vp test apps/server/src/nornic packages/provider-pi/src/server/mcpExtensionSource.test.ts
```

The integration test makes its own temporary Git clone, introduces a known copy,
requires blocking advice naming the canonical owner, removes the copy and requires
a pass. It also checks that the user's Git index is unchanged. The fixture
command refuses to overwrite an existing directory.

Qualification observed on the initial Windows/RTX 2080 Ti setup:

- 42 focused T3/Nornic tests passed with live NornicDB, Ollama, Linear and duplicate
  fixtures enabled; server type checking and focused lint passed.
- 25 Python document-ingestion and existing code-intelligence hook tests passed
  in a disposable Linux container. No hooks were installed in the development repo.
- A real local T3 thread called `nornic_discover` twice and returned an indexed
  source path. A separate bounded worker fixed `square` in an isolated worktree,
  obtained an actual duplicate-pass receipt, and passed independent unit tests.
- A complex request routed to the authenticated configured Codex frontier and
  completed there. Server-generated completion messages do not trigger another
  delegation.
- Full Linear write-back, independent review evidence, and the complete target
  procedure catalog remain unimplemented. The live Linear check reads NOR-53;
  binding invalidation/ownership/version behavior is covered by state tests.

These observations do not replace `make verify` for the NornicDB PR. Report its
actual result and platform/baseline failures separately.


## Offload bounded work from Sol

The setup adds two worker provider instances backed by the existing model servers:

| Provider instance | T3 label | Serving model |
| --- | --- | --- |
| `nornic_local_worker` | Nornic Local Worker · Ornith | Local llama.cpp, `nornic-ornith` |
| `nornic_remote_worker` | Nornic Remote Worker · Qwen 9B | Remote Ollama, `nornic-agent-remote` |

These workers are **subagent targets**, not the default chat selection. Start with
Nornic intake or Sol, ask it to inspect `orchestrator_capabilities`, then give it
one bounded delegation. For example:

```json
{
  "task": "Update docs/example.md to explain the existing flag. Read AGENTS.md, edit only that file, run the documented Markdown checks, and return changed files and actual results. Do not commit or publish.",
  "title": "Document the existing flag",
  "target": { "providerInstanceId": "nornic_local_worker", "model": "default" },
  "isolation": "worktree",
  "mode": "async",
  "clientRequestId": "document-existing-flag-1"
}
```

Call `delegate_task` with that object. Use `nornic_remote_worker` for a smaller
Qwen task. Always inspect readiness in the live catalog; an unavailable worker
is a blocker, not permission to choose Astra or another paid model. Prefer one
active task per model server. Keep the parent brief concise: children receive the
supplied task rather than the parent's entire conversation. Include file scope,
acceptance checks, dependencies and what evidence to return.

T3 creates the child's isolated checkout from the parent's **committed HEAD**;
uncommitted parent edits are not copied. `worker.admit` checks the authenticated
provider, live child run, subagent lineage, bound worktree and matching active
app-owned task in the parent projection. The task prompt comes from that record,
not model-supplied admission fields. Admission does not invoke the classifier,
read assistant, retrieval, or frontier. A standalone worker chat, shared-checkout
child, unconfigured provider or canceled/mismatched assignment stays blocked.
`NORNIC_WORKER_PROVIDER_INSTANCES` configures the worker provider allowlist; its
default is the two IDs above.

Admitted workers use the runtime's native read/edit/write/bash tools and load the
checkout's AGENTS guidance. The same T3 permission-mode hook still applies:
Supervised requests approval for writes/commands; Auto-accept edits requests
approval for commands; Full access executes admitted tools directly. The gateway
also exposes read-only code/help/Linear lookups. Workers cannot call routing,
delegation or workflow mutations through that gateway. Admission is checked again
on each new agent turn. This is tool admission plus an isolated Git checkout,
**not an operating-system sandbox** or enforcement of arbitrary shell filesystem
access. The brief remains the authorized task boundary.

The parent collects the task receipt with `task_status`, reviews the child's diff
and evidence, and integrates the result. Workers do not merge, publish or change
git hooks. This path supports authoring code, docs and tests and running bounded
checks; it does not claim the complete lifecycle/claim enforcement spec finished.

## Connect Sol to Linear's official tools

The harness's `LINEAR_API_KEY` supports its read-only intake operations. A full
permission token does not add write operations to that API. To let the Codex/Sol
parent use Linear's official MCP tools independently, run under the same account
that starts T3:

```sh
codex mcp add linear --url https://mcp.linear.app/mcp
codex mcp login linear
```

If the server already exists, update its existing configuration rather than
creating a duplicate. Finish the browser authorization for the intended workspace
and restart the T3 Codex agent. Verify an assigned-issues read before requesting
organizational changes. This setup does not expose Linear mutations to the Nornic
worker tool allowlist; leave those operations with the authorized parent.
See [Linear's official MCP guide](https://linear.app/docs/mcp) and
[OpenAI's MCP configuration guide](https://developers.openai.com/codex/mcp).


## LAN pairing

`make swarm` now starts the backend on `0.0.0.0` and the Vite frontend on all
IPv4 interfaces. The frontend remains the single origin for HTTP and
WebSocket traffic; its existing proxy connects to the backend over loopback.
The launcher sets both `--host`/`--dev-url` and Vite's supported `.env.local`
`HOST=0.0.0.0` setting, preserving unrelated entries. The approved pinned Vite
patch lets HMR derive its destination from the browser URL for wildcard binds,
so both `localhost` and the LAN address work. Setting only the backend bind does
not make the development UI reachable remotely.

Set `SWARM_T3_ADVERTISE_HOST` in the private harness env file to this Mac's actual
LAN IP, for example `192.168.1.231`. Without an explicit value, the launcher uses
the local IPv4 route to the configured NornicDB host and rejects a loopback result
with setup guidance. The default URL is `http://<LAN-IP>:5733`; backend port is
13773. A configured `T3CODE_PORT_OFFSET` moves both ports by that offset. Both
ports are passed explicitly so the advertised URL cannot silently point to a
port abandoned by automatic free-port selection. If occupied, stop the previous
harness before starting another against the same state directory.

Restart T3 after changing the address. The console prints an authenticated
pairing URL; keep that link private. Network access should report
`remote-reachable`, and an unauthenticated `/api/auth/session` still reports
`authenticated: false`. This does not change the local model's loopback listener
or disable T3 authentication. A request from this Mac's own LAN address verifies
binding and routing; another device's firewall/network reachability is a separate
check.

To repeat the controlled worker checks on this Unix harness:

```sh
python3 testing/swarm-local/worker_smoke.py
```

The script materializes the current extension, starts an explicit local MCP
admission fixture, and runs each real model against a disposable Python coding
task. It verifies native editing and test execution and requires only
`worker.admit`, never `intent.route`. Its JSON report is written outside the
repository in the persistent harness directory. It does not create actual T3
parent/child receipts or exercise a live frontier model; the public workflow
regressions separately test authorization against T3 projections.


Recorded worker smoke evidence is in
[`testing/swarm-local/worker-smoke-results.json`](../../testing/swarm-local/worker-smoke-results.json):
the latest controlled runs completed in 29.74 seconds on Ornith and 79.22 seconds
on remote Qwen, each fixing the function and passing its existing tests using
native editing and commands. These durations are individual smoke runs, not
comparative performance benchmarks. The harness unit suite passed 11 tests; the
focused T3/provider/dev-runner suite passed 234 tests with 9 opt-in skips.
Provider-package typecheck passed. Server-package typecheck still exits 2 on
repository-wide Effect diagnostics, with no TypeScript error diagnostics in the
checked run. Native Windows and a live frontier-to-worker orchestration run were
not exercised by these checks.
