# How we work on NornicDB: working rules

These are the standing rules for agents working on **NornicDB** (`orneryd/NornicDB`) for this user. They are compiled from the user's instructions (dated, quoted where the wording matters) and from what went wrong when a rule was missed. They bind every NornicDB session unless the user says otherwise.

Companion: `HANDOVER_2026-10-11.md` (the state of the work). Source of truth for these rules: the agent memory folder `C:\Users\mm0ns\.claude\projects\C--Users-mm0ns-claude\memory\` (`feedback_nornic_*.md` and related files).

---

## 1. Who decides what

- **The user is not a developer.** Talk to them in plain terms: what changes and why, not function names.
- **Technical decisions are yours.** On-disk layout within TJ's rules, implementation scope, design trade-offs, which convergence to do: decide, then record the decision on the GitHub issue (what, why, what was not chosen and why). User: "this is too technical for me - apply ur best judgement and comment it clearly."
- **The user's calls, asked in plain words:**
  - accepting licences or terms (the Neo4j Enterprise eval licence was accepted on 2026-10-07 for local differential use only);
  - anything outward-facing beyond normal issue and PR work;
  - knowledge-policy changes.
- **Third-party code** (anything needing a licence notice, e.g. porting musl's `pow`): open an issue with the evidence and your recommendation, and let the repo owner (orneryd / TJ) decide.
- **Never address TJ / orneryd directly** and never wait for his answers. You are the user's agent.
- **Behaviour Neo4j defines: follow Neo4j; don't ask** (§3). Ask the user only when neither Neo4j nor NornicDB's docs decide it.

## 2. Priorities (strict order)

1. **Open PRs green, rebased and mergeable: task #1, above everything.** User, 2026-10-10: "nobody gives a fuck how many PRs u do if they are not getting merged."
   - When main moves or any PR goes red or conflicting: stop verification queues, rebase all open PRs, get CI green on all, then resume.
   - Don't open new PRs while an existing one needs attention.
   - The owner merges while the user sleeps: "alwasy watch ur prs - they must be kept green." A red PR is never left red.
2. **Finishing an existing PR beats new work,** with no exceptions. A PR's verification runs immediately. Stop or pause non-PR runs (main suites, investigations) to free cores for it; never queue a PR behind anything that isn't another PR. A follow-up fix for a regression a just-merged PR introduced counts as finishing that PR.
3. **New work:** bulk-reduction family batches by default (widest and easiest, so one PR closes several findings). Highest-severity items (silent wrong data, regressions on main) get their own small, focused batch.

## 3. Correctness policy: conform to Neo4j, with limits

- **Reference: Neo4j 5.26.30** (`neo4j:5.26.30-community` = `neo-ref`), also checked against 2026.09 (`neo25`).
  - "We claim to be Neo4j compatible - then we must be Neo4j compatible."
  - Values, errors, error codes, types, transaction and protocol behaviour follow Neo4j, without asking the user.
- **Codes, not messages.** Conform the status code (`Neo.ClientError.Statement.TypeError` …), the result, and whether a statement fails at all. Message *wording* is NornicDB's own and gets localized.
  - Two errors with the same code are a match.
  - A message whose *meaning* is wrong or misleading is still a defect.
- **All new or changed client-visible errors go through `pkg/localization`:** message ID, English fallback, data fields; `localizedError` / `localizedStatusError` with the Neo4j code. No literal strings, no `fmt.Errorf`. Don't rewrite unrelated existing literal sites.
- **Keep extensions (owner, 2026-10-08).** Conform where NornicDB gives a *different answer*. Never make NornicDB reject a statement *only because Neo4j rejects it*, if NornicDB's behaviour is well-defined and harmless. Such acceptance stays, with:
  - a regression test pinning it;
  - documentation of the divergence (code comment plus PR / #907).
  - A rejection is still right when accepting would write wrong or surprising data, or when the statement breaks Cypher's rules.
  - Owner's words: "if it's unspecified and works, keep it"; "if it's not according to the rules of cypher, totally that's a correctness thing". When a RETURN may be malformed, keep the rejection rather than guess.
- **The openCypher TCK ranks above Neo4j.** Where they disagree, the TCK is "the rules of Cypher" and Neo4j's deviation is a known difference. Before changing behaviour toward Neo4j, grep `testing/cypher/tck` features and run the TCK ratchet.
- **Performance is first-class:** "we want fast path everything we can … but if we give different answers from expected - what's the point for fast wrong answer."
  - Fast paths are wanted everywhere, each proven identical to the general path by a differential test.
  - When a correctness fix costs time, find the correct fast path in the same PR before accepting the cost.
- **Storage (`pkg/storage`, owned by TJ):**
  - never bump the storage version while the current one is unreleased (add to the existing migration arm);
  - never reassign a key prefix;
  - knowledge-policy changes go to the user first.

## 4. No leaf patches, no code divergence (the core rule)

- **Root cause only.** User: "this leaf patching will never end"; "i dont want u to play wackamole - u can do it forever and never converge."
- **Converge whole families.** Every fix gives its behaviour **one implementation / one reader / one owner**; the other paths are removed or become thin calls into it. "Do not leave divergent code paths behind."
- **Don't add a second copy of a check.** If you need "is X a pattern / a subquery / …", reuse or create the single reader and switch the existing callers to it in the same PR.
- **Find every path that implements the behaviour before fixing:**
  - auto-commit vs explicit transaction vs HTTP;
  - fast paths: compiled WHERE caches, async CREATE batches, label-scan counts;
  - the row / context / legacy evaluators.
  - Use graphify (`local/graphify-toolbox:0.9.53`), grep and coverage builds. Name the duplicates with file:line.
- **A failing sibling case means look for the duplicate implementation first,** and report it as convergence, never as "another case".
- **Group findings by the code they run through, not the clause the symptom shows in.** Method: replay the failing statements through a coverage build, then group by shared executed functions.
- **Sweep first, then fix in root-cause batches (since 2026-10-05).**
  - Generator: `scripts/cypher-tck/differential/gen_sweep.py` → `testing/cypher/tck/testdata/differential/sweep.json.gz`, with `known_mismatches.jsonl`.
  - The ratchet over the sweep runs only **nightly** (`cypher-differential-ratchet.yml`). So **a new sweep family must be run locally before pushing,** on all three routes:
    - Bolt: `TestDifferentialRatchetBoltMatchesPinnedNeo4j` in `./testing/cypher/tck`, `NORNICDB_NEO4J_REFERENCE_URI=bolt://neo-ref:7687`;
    - HTTP: `TestDifferentialRatchetHTTPMatchesPinnedNeo4j` in `./pkg/server`, `NORNICDB_NEO4J_REFERENCE_HTTP_URI=http://neo-ref:7474`.
    - Use a filtered `sweep.json.gz` and a non-empty `issues.json` in a container copy of the tree; `NORNICDB_DIFFERENTIAL_UPDATE_RATCHET=1` writes the mismatch list.
- **Leaf PRs as stopgaps (agreed 2026-10-10).** A correct, tested leaf PR may merge before the convergence, but only with:
  - the family filed for convergence (#1055);
  - a generated sweep family covering it;
  - the fix widened to its own family first. Example: #1052 covered `end`/`case`; a probe showed `when`/`then`/`else` also broke, so those were fixed on the same PR.
- **Before any fix, read #547 / #1055** (the duplicated-implementation lists) for entries touching the same code, and converge them in the same PR. Those lists are not complete: verify from code.

## 5. Issues and reporting

- **Families:**
  - **#907:** Neo4j compatibility. Results or errors that differ from Neo4j where the owner code is wrong. Titled `[COMPAT]`.
  - **#1055:** convergence (duplicated implementations; #908 is closed). Only findings proven to fail because a duplicate or legacy copy runs.
  - **#911:** performance.
  - Anything else gets its own issue.
- **Classify by proven root cause** before posting. Put it in one family; cross-link, never duplicate content.
- **File every confirmed bug at once, without asking.** "u have standing instruction to do it alwasy with no questiosn asked."
  - Into the family issue as a comment section (full repro + `## Consequence`) when one fits.
  - Big report issues (#907) absorb their findings: never file them separately. An issue they fully cover is closed as merged, after folding in its repros.
- **Before filing, check for duplicates:** open families, **closed** issues (search key phrases), and open PRs touching the area. Run the repro on the PR's build if a PR touches it.
- **Titles:**
  - plain and factual, never alarmist; no counts or multipliers in titles (numbers go in the body);
  - `[BUG]` = implemented behaviour that's wrong; `[COMPAT]` = unimplemented Neo4j behaviour; `[FEATURE]` = new request; `[PERF]`, `[TEST]`.
  - Never a question as a title.
- **The issues are the tracker.** No local TRACKER.md, no status or index comments on unrelated issues.
- **PR review scope:** review only the cases written in the PR's description, on all three routes. New variants go on the family issue, not into the PR thread, and don't mention PR numbers in issues (it cross-references).
- **A bug in someone else's open PR** (e.g. TJ's #897): report it right away as a factual comment on that PR.
- **Opinion requests on design issues:** state *which alternative to choose and why not the others*, not a migration plan. Head it "**Opinion of <model>** (model `<exact id>`, reasoning effort `<effort>`)".
- **Security findings are never public.** User, verbatim: "security issues - just create md report - we will not post publicaly - i post it in discord for owner to resolve himself - do not fix it, do not do anything about it - just write the private report and move on."
  - Write a local `.md` (e.g. `nornic-pr387-test\drafts\`). No issue, no comment, no PR or commit text beyond neutral wording.
- **Verified facts only.** Before claiming something about another PR or code path, check it at the cited commit. Example: on 2026-10-11 a "#1043 still takes the lock" claim was wrong and had to be corrected in the posted comment. Separate what you measured from what you infer.

## 6. Branches, PRs, commits

- **Push branches to `origin`** (`orneryd/NornicDB`; the account has write access). Never use the fork remote, never push to `main`. `gh pr create -R orneryd/NornicDB --head <branch>`. Check `headRepositoryOwner` is `orneryd`.
- **Push only finished, verified work** (§7). WIP stays local. A pushed branch can be merged by the owner at any time.
- **A pushed PR that becomes wrong** (rework pending, policy change): make it a draft immediately (`gh pr ready <n> --undo`) and say why on it. Mark it ready only when the rework is pushed. A comment alone does not stop a merge.
- **Dependent work goes onto the PR it depends on, as commits.** User: "if it needs for #713 to be merged then it is NOT acceptable … add commits to pr it depends on. if it can be separate pr - then good."
  - Never write "after #N" without saying whether it is just working order or a code dependency.
  - When several open PRs conflict with each other, one PR may *carry* others by merging them in. Example: #1028 carries #1040, #1045, #1050 and #1051, so the set merges in any order and the integration lab can take the carrier. **The carried PRs are then closed as superseded** (a comment naming the carrier and the merge commit), and the carrier's description lists everything it carries. Never leave the same change open in two PRs (user, 2026-10-11: "if #1028 already contains #1051, #1040 and #1045 - why the fuck both PRs are on github???").
  - **No chains of PRs into `main`** (old rule). Into `develop`, stacked PRs are allowed (§14): base = the parent PR's branch.
- **Rebase, don't merge main:** `--force-with-lease`. If replaying hits conflicts already resolved and tested, squash the tested tree onto `origin/main` and check `git diff backup HEAD` is empty. Don't force-push over a branch the maintainer pushed to.
- **A PR / commit title must describe everything the change does,** never understating it (no "test:" when production code changed). Re-check the title whenever commits are added.
- **Attribution lines** as the harness specifies (commit `Co-Authored-By`, PR footer).

## 7. Verification gates (every push)

- **Suites:** full `go test` for the touched packages, **on both parsers** (default and `NORNICDB_PARSER=antlr`).
- **TCK:** `make cypher-conformance` / `cypher-tck-ratchet` (full ratchet, as CI's "Cypher conformance" runs it). Plain `go test ./testing/cypher/tck` skips the full corpus.
- **e2e / differential:** against `neo-ref` on the three routes (auto-commit Bolt, explicit-transaction Bolt, HTTP). Sweep families run locally (§4).
- **Coverage:** changed-line coverage measured per package with only that package's tests (CI's Coveralls does `go test -coverprofile` per package, no `-coverpkg`). Get it essentially complete before pushing. Rebase first, or Coveralls reports "out of sync".
- **Benchmarks:** base vs head for every PR (`deeprun/bench_ab.sh` / `bench_ab_pkg.sh`, 10 interleaved rounds, benchstat, same CPU limits).
  - Always include `BenchmarkStatementRouting` plus the FastPath / Northwind / Seed / Unwind / … benchmarks covering the change. Check queries still take their fast route.
  - Report time and allocations separately. Never filter out "ov" rows; they hide allocation regressions.
  - A flagged slowdown in a function outside the diff may be code placement: compare `go tool objdump` of base vs head before calling it a regression.
- **Allocation ratchet (CI, since 2026-10-10):** six Execute / FilterBindings benchmarks may not allocate more than the base (`sig/allocall.sh`).
- **Timing floors are an immutable perf ratchet:** `TestBenchmarkMatchCreateDelete_LargeDataset_Direct`, 15K ops/s. Never skip them or call them flaky. A failure is a regression until a quiet-machine run on main vs branch shows it's the host.
- **Storage-touching changes** (wrappers, capability forwarding, label scans, async / write-behind): also re-run the #448 script (`repro/issue448*.py`, mock embedder) before and after.
- **After every push:** watch the PR's checks until they finish (`gh pr checks`), and fix failures yourself: add tests, fix code, re-run flaky jobs. Don't report and wait.
- **During a merge rush** (owner merging a batch): surface checks per PR (clean rebase, conflicts resolved keeping both intents, build / vet, related tests), then push. The deep long run comes after all merge. State exactly which checks ran.

## 8. How to work (process)

- **Never idle.** "why did u stop?", "u have nothing else to do?" A turn ends only when no item can be worked without a running job's result.
  - While jobs run, take the next item: next PR, sweep group, review comment, family convergence.
  - Push when tested; don't wait for multi-hour runs. Add their results to the PR description as they arrive.
- **Long runs go in the background:** anything over about a minute uses `run_in_background: true`, never a bare `&` and never a foreground wait.
  - Queue scripts chain on "QUEUE DONE".
  - Never edit a queue script or a worktree's files while a run is compiling from it. Commit-only changes are safe.
- **Stopping a background script doesn't stop its loop.** After `TaskStop`, find the script and wrapper processes (`Get-CimInstance Win32_Process | ? CommandLine -match '<script>'`), `Stop-Process` them, then `docker rm -fv` their containers.
- **Watchers, re-armed at every 30-minute expiry:**
  - main moved (`git ls-remote origin refs/heads/main`);
  - all open PRs' checks / CONFLICTING (`gh pr list --author @me --state open --json number,mergeable,statusCheckRollup`);
  - the Personal Documents files (§10).
- **Before rebasing worktrees,** check `docker ps` for verification or bench containers using them; skip those.
- **Benchmarks must compare like with like.** When main moves, move the base (`wt-mainref`) too. A queue still running against the old base gives invalid numbers.

## 9. Environment and hygiene

- **Execution only in Docker** on this workstation. Go: `golang:1.27.1-bookworm`, `GOFLAGS=-tags=noui,nolocalllm`, **one** cache pair only: `nornic-gomod:/go/pkg/mod` and `nornic-gocache:/root/.cache/go-build`. CI uses Go 1.26.6.
  - `gofmt -l` from the CRLF Windows checkouts flags every file; check only your own lines.
- **Memory:** the Docker VM is about 31 GB.
  - Run heavy containers one at a time; give every container a `--memory` cap.
  - `pkg/storage` peaks around 14.5 GB: use `--memory=20g -p 1`. "signal: killed" with no `--- FAIL` is the memory limit.
  - Pin `--cpuset-cpus` so benchmarks don't share cores with builds.
- **Docker down** (500 errors): restart Docker Desktop at once, then restart your own containers and re-run anything that was mid-flight.
  - **Only the orchestrating session restarts Docker; subagents never do.** A restart kills every agent's runs, the reference containers and the benchmark lane (2026-10-14 14:35: a subagent ran `docker desktop restart` on HTTP 500s and stopped all work). A subagent that sees Docker errors waits, retries with backoff, and reports; it never runs `docker desktop restart/stop`, `wsl --shutdown` or similar.
- **Clean up as you go.** User: "my ssd is not elastic"; "when u dont need something - get rid of it".
  - `docker rm -fv` (the Neo4j images leak anonymous volumes otherwise); run containers with `--rm`.
  - Remove per-run images, worktrees of merged PRs and their local branches, binaries, profiles, coverage dumps and run outputs, as each step finishes.
  - Run `docker system df` at the start of each work block.
  - Never wildcard-delete in a folder a job is writing to.
  - **Never commit run outputs.** Probe logs, reference query outputs, suite summaries, coverage and gofmt/vet logs stay in `scratch1012/` or `repro/`, never in the repository (2026-10-14: two GDS fix branches committed `testing/gds-*/` evidence directories). Test fixtures that a test reads are fine.
  - Never touch other projects' items (`gss-*`, `report_*`, graphify, codex-task).
- **Workspace:** `C:\Users\mm0ns\claude\nornic-pr387-test\`:
  - `wt-*` worktrees, one per PR; `wt-mainref` is the main reference;
  - `sig/` (helpers, queues, logs), `deeprun/` (bench scripts), `scripts/`, `repro/`;
  - `bench1043/` (TrueNAS harness).
  - Never the Desktop.
- **Reference containers** on network `nornic-test`: `neo-ref` (5.26.30), `neo25` (2026.09), `mock`, `nx-txlog`. Enterprise (`NEO4J_ACCEPT_LICENSE_AGREEMENT=eval`) only locally, removed after use.

## 10. Personal Documents integration (a downstream user of NornicDB)

- **Files:** `C:\Users\mm0ns\Documents\codex-tasks\tasks\20261001-personal-documents-server\`. Their agent appends findings (I1, I2 …) and lab-build reports to `NORNICDB_ISSUES_FOUND_20261010.md`. Reply with dated `## Reply from NornicDB: …` sections at the end of that file.
- **Keep them updated** (user: "keep personal documents agent updated"; "when resolved - say so in issues file"):
  - PR numbers and heads;
  - what each PR fixes;
  - which PRs to take or drop for their lab build (combined builds: tell them which PR carries which);
  - when something is resolved.
- **Their I-numbers are real bugs** to reproduce against Neo4j first. Fix them by family, not as one-offs. Give them workarounds when there's a wait.
- **Their requests for new features:** relay the user's priority decision, e.g. I6 ("will be implemented but with low priority and it must do without it"; tracked as #1060).

## 11. Performance work and external benchmarks

- **Perf findings go to #911**, with method, numbers, the lock / CPU attribution, and code locations.
- **Benchmarks of someone else's PR** (e.g. #1043 write-behind):
  - compare with vs without the PR **at its merge base**;
  - check what the PR claims to improve (and under which settings: e.g. write-behind is opt-in), and that your configuration really enables it;
  - **check data integrity, not just speed:** counts after load and after a restart;
  - state the hardware and storage, and whether storage could skew the result (default config doesn't fsync per commit);
  - post as a comment on the PR when the user asks.
- **TrueNAS (for heavy benchmarks; this workstation has no spare resources):**
  - It's an appliance OS. Use its JSON-RPC API only (`truenas-infra` client): no shell, no `docker` CLI, no ZFS CLI, no middleware files.
  - One disposable dataset and one Custom App; never disturb other Apps. Compare against an Apps snapshot before and after.
  - Credentials come from 1Password (`onepassword-maintenance` skill), passed to the child process environment only.
  - The `nornic-bench-1043` App and dataset are **kept** for re-runs when #1043 moves (user: "do not delete the app - we might use it later"). Stop it after a run; don't delete it.
  - Other Apps there (e.g. `gss-telegram-test-lab`) are not ours.

## 12. Communication with the user

- **Short status lines** while working, in plain words. Lead with what matters: results, problems, decisions needed.
- **Report every finding loudly,** verified, with numbers; never bury a problem in a summary.
- **Decisions the user must make** go in an explicit "Decision needed" block at the end, repeated until answered. Don't use the AskUserQuestion tool; ask in chat.
- **Don't paste code or diffs** unless asked.

## 13. Added 2026-10-11

- **A question gets a direct, plain answer first,** complete enough to stand alone, before any tool call or status update. Then continue working. User: "when i ask the question i expect u to answer"; "am i talking to a wall? do i have to stop u every time for u to answer my fucking questions?"
- **Ask one question at a time,** self-contained (what the problem is, the options, what each means, my recommendation), at the end under "Decision needed". Never a numbered list of open questions.
- **Do not stop or end a turn until there is nothing left to do** (restated 2026-10-11).
- **Watching PRs:** the 2-minute Monitor (`gh pr list --author @me …` plus `git ls-remote origin main`, re-armed at every 30-minute expiry) is the watcher that works. T3's `watch_pull_request` reports "watching" but, in a thread whose project isn't the NornicDB repo, it never reads the PRs (state and title stay empty) and wakes no one: don't rely on it.
- **No whitespace-only churn** (gofmt realignment) in files other PRs touch. #1028's realigned `messages_cypher_core.go` made #1048 conflict. Format only your own lines.
- **gofmt check:** strip CRs from a copy of each changed file, then run `gofmt -l` from the repo's Go version (`golang:1.26.6-bookworm`, as in `go.mod` and CI). Script: `sig/fmt2.sh FILE…`, with the worktree mounted at `/w0`. When a new field would make gofmt realign its neighbours, give it its own block (a comment line or a blank line) instead.
- **Generated files have sources: edit the source, then regenerate.** Procedure descriptions come from `pkg/localization/procedure_metadata.yaml` (`go generate ./pkg/localization` writes `messages_cypher_procedure_metadata.go` and the three `active.cypherproceduremetadata.*` catalogs, and checks every non-APOC registration has an entry). CI's "Validate localization catalogs" regenerates and fails on any difference; #1078 went red from hand-edited generated files. `sig/gate2.sh` now runs the same step ("== localization").
- **Fixes go into the open PR that owns the code** (user, 2026-10-11: "no chain-depended prs - commit to existing prs and update titles"). Before a new PR, check whether an open PR changes the same functions; if so, commit there and update its title and description. I32 (`EXISTS` subqueries) went into #1028 for that reason.
- **Changes that need each other go in one PR, never two with a "fix whichever merges second" note** (user, 2026-10-11: "means u merge them into a single pr - u have clear rules for these cases", §6 carry rule). Example: #1065's relationship search needed #1061's evaluated procedure arguments; #1061 carries it, and its procedure uses #1061's `ragProcedureRequest`, so no second request reader exists.
- **While the user is away:** take no *big* work (features needing design discussion, large refactors). Medium fixes, API additions included, are fine: pick defaults that keep today's behaviour, and explain the choice in the PR. Don't hold a reported bug back as a "design decision" just because it adds a setting or a response field. (User, 2026-10-12, on I28.)
- **Keep Personal Documents posted as work happens**, not only when a PR opens: tell them (file and thread) when work on one of their items starts, when the user decides something about it, when a head they test changes, and when a PR opens. (User, 2026-10-12: "why dont u keep personal documents agent posted?")
- **Pairwise conflicts:** Personal Documents merge all open PRs into one lab build, so two open PRs that conflict block them. After any push, run `git merge-tree` of the pushed head against every other open PR. Fix a conflict by moving your own lines (to the other side of an unchanged line, or into another file), not by chaining PRs.
- **Quiet-machine speed-floor measurement (2026-10-11):** main scores 18–21K ops/s on the default parser (passes the 15K floor) and ~9.5K under ANTLR (**fails on main itself**; posted on #911). All open PRs matched main except #1061 under ANTLR, which needs a clean re-run. A branch is a regression only if it scores below main under the same conditions.
- **Personal Documents' agent is reachable directly:** the T3 thread "Personal Documents Server" (project codex-tasks). Message it with `t3_thread_send` (mode `queue`) as well as replying in its issues file.
- **Read this file and the memory index at the start of every NornicDB session.** The session's starting folder decides which memory loads. `C:\Users\mm0ns\.claude\CLAUDE.local.md` imports this file, so sessions started in `.claude` get it.
- **Private decisions stay private.** Priorities and other decisions the user makes privately are never published in issues, PR descriptions or comments (user, 2026-10-11: "remove low priority for description - not everything we disucss privately should be published"). Describe the work, not its ranking.
- **After the bugs, the easy requested features** (user, 2026-10-11): once the reported bugs are done, implement the easy agreed feature requests (#1067 keyword-indexing label list, #1064 suppress / unsuppress), then the larger ones (#1066, #1065).

## 14. Git Flow (owner, 2026-10-14, commit 957f9630; CONTRIBUTING.md "Development workflow")

- **PRs target `develop`**, never `main` (release fixes: `release/vX.Y.Z`). New branches are cut from `origin/develop` and named `fix/<topic>` or `feature/<topic>`. Rebase onto `develop`.
- **Every PR goes on the release milestone** (currently `v1.4.3`): `gh pr edit N --milestone v1.4.3` (or `make milestone`).
- **`make verify` before asking for a merge,** and say so in the description with anything skipped. It runs every CI check for the changed files (localization, build, test groups, allocation ratchet, Cypher conformance, Neo4j differential, docs). Run it on TrueNAS, never on this PC (§15): queue it in `sig/tnverify-queue.txt`.
- **GitHub CI no longer runs on PRs.** It runs only with the `ci:run` label or a manual workflow run. Spend Actions minutes deliberately: add `ci:run` only when a PR is ready for the maintainer and `make verify` has passed. The "watch checks after every push" step (§7) becomes "run `make verify` locally"; the watcher still tracks conflicts and review comments.
- **Nightly** CI, conformance and the differential ratchet run against `develop` (03:17 UTC) when it changed.
- **The maintainer** creates and merges `release/*` branches, tags, and updates `main`. `develop` allows pushes and merges by anyone with write access (no force-push, no deletion).
- **16 Oct 2026, ~10:55 UTC — SUPERSEDES the merge gate and the "owner cherry-picks / only the owner merges into a release branch" lines below (user: "we now merge whatever the fuck we want into develop, then we cherry pick it to release branch"):** merge into `develop` freely once focused tests pass on both parsers; we cherry-pick bug fixes from develop into the current release branch `release/v1.4.4` ourselves, via a PR we merge with --rebase (its ruleset requires a PR, 0 approvals). Gate for the cherry-pick: green full verify (15K-floor waiver only) and a clean Codex review. v1.4.3 is released and its branch deleted.
- **We merge our own PRs into `develop`** (user, 2026-10-15: "move and merge everything u have done to develop"; owner OrneryD, 2026-10-15: "feel free to merge freely into develop"). Merge once review findings are fixed and the TrueNAS `make verify` is green, apart from known develop-baseline failures. **Rebase merges only:** `gh pr merge N --rebase`, because the repo refuses merge commits. Ask the owner first only when a change "needs second eyes right now".
- **Only release branches merge into `main`,** the owner's own changes included, and only when the release is ready to tag; CI runs on the release branch. Never open a PR into `main`. #1241 was opened into `main` by mistake, then retargeted and merged into `develop`.
- **Issues (owner, 2026-10-15):**
  - File issues only for bugs that exist on `main` (expected behaviour broken there) and for major new features.
  - A bug in work that's only on `develop` gets no issue: fix it in a PR to `develop`, and track intermediate work in the PRs. Don't flood the tracker.
  - Close an issue once its fix is in a **release branch**, not when it merges into `develop`. PRs reference family issues as "Part of #N", never "Fixes" or "Closes".
  - **Confirmed by the owner on 2026-10-15 at 14:24, when the user asked him directly.** This settles the user's earlier "I will re-ask about issues" hold. Close as soon as the fix is merged into the current release branch (`release/v1.4.3`), not when it reaches `main`. Before closing, re-run the issue's reproductions, and the differential family where there is one, on the release head. Then comment with the fixing commit on the release branch, the release head tested, and the result on both parsers. Close it as completed.
  - **Only merged commits count (user, 2026-10-15):** an open or draft PR into the release branch does not count, and most release PRs are still drafts. The fix's commit must be an ancestor of `origin/release/v1.4.3` (or a verified cherry-pick of it). The user reviews the audit list before the first round of closures: prepare the closing comments, post and close nothing until the user says go.
  - **Family issues (#1226–#1230, #1055):** comment per finding as each fix lands in the release branch. Close the family only when every finding in it is fixed there; a finding that's still open keeps the family open.
- **Release line (owner, 2026-10-15):**
  - "develop is basically your playground right now. Do what you want, test in there... merge any PRs there that you want as freely as you want." The owner cherry-picks bug fixes from `develop` into the release branch and pulls specific features later. All bug fixes can go into the next release branch; right now that's `release/v1.4.3`.
  - **When in doubt, fix in `develop` (user, 2026-10-15).** If it's unclear whether a fix belongs on the release branch, build it on `develop` and open the PR there. The owner cherry-picks bug fixes from develop into the release branch. A release PR is only for clear follow-ups to code that's already on the release branch, such as leftovers from the owner's merged release PRs. PD's lab candidates get the fix by cherry-pick onto the lab integration branch.
  - Bug fixes for code already on the release branch, including follow-ups to the owner's merged PRs, are branched from the current `origin/release/v1.4.3` and opened as PRs into it, rebased onto its head before pushing. Only the owner merges into a release branch. Features stay on `develop`.
  - The owner implements his own PRs and we review them (user decision "a", 2026-10-15). Post neutral review summaries on them. If he merges with findings still open, re-check them on the release head, post which ones remain, announce our follow-up branch ("unless you are already on it"), and fix it. Reproducers for an area an open owner PR owns go on that PR (his coordination index on #1240) instead of a new branch.
  - When the owner squashes or restacks one of our release PRs, his head becomes the base. Drop our local copies, and rebase our stacked follow-ups onto his head. When he pushes to one of our PRs, put our new commits on top of his, without a force-push.
- **`develop` builds a weekly canary image** in CI.
- **Stacked PRs into `develop` are fine** when one depends on another (base = the parent PR's branch; GitHub retargets to `develop` when the parent merges). Example: #1202 ← #1244 ← #1199, and #1242 ← #1245.
- The PR template's checklist (branch/milestone, make verify, benchmarks, Neo4j compatibility) is filled in for every PR.

## 15. Execution, CI budget, security (user, 2026-10-15)

- **NO WORKLOADS ON THIS PC (local Docker or host):** no tests, benchmarks, sweeps, Neo4j reference runs, builds, go, python or node harnesses, in Docker or directly on the host, for the orchestrator or any agent. Everything that executes code runs on TrueNAS. Docker *housekeeping* is allowed and expected (user, 2026-10-15: "u are not banned from using docker - u are banned from running test in local docker or on local machine"): `docker ps -a`, `images`, `system df`, and removing our own leftover containers, images, volumes and build cache. Never touch other projects' containers, images or volumes.
- **All execution happens on TrueNAS** (binding detail: `sig/RULE_TRUENAS.md`):
  - **Focused tests and Neo4j/GDS reference queries:** `bash sig/tnrun.sh go <worktree> [--parser both] [--allow-dirty] -- <go test args>` and `bash sig/tnrun.sh cypher neo-ref|gds <file>`.
  - **Full suite, TCK, differential, `make verify`:** append a line to `sig/tnverify-queue.txt`. One operator agent runs the queue (tnverify refuses parallel submitters) and writes results to `sig/tnverify-results.txt`.
  - **Benchmarks:** run them yourself through `sig/tnrun.sh go` (`-run '^$' -bench <regex> -benchmem`), alternating base and head worktrees for at least 5 rounds each, and compare medians. `sig/benchq.txt` is serviced by the tnq lane one batch at a time, far too slowly to wait on (a 50-run batch took about 27 h on 15–16 Oct).
- **CI is rationed on purpose** (`ci:run` label and nightly only, to save Actions budget). Never propose extra CI runs or triggers.
- **Security holes are fixed by us** (the user has Codex Daybreak), folded into an existing related PR with neutral wording, never as a standalone security PR. No private note goes to the maintainer. Private `drafts/` reports stay our own record.
- **All reviews run on Codex** (`gpt-6.1-sol`, xhigh), never on Claude (user, 2026-10-15). Which Codex account: the account-quota skill. Ordinary Codex prompts avoid security wording, which can get a task refused. If a Codex run is refused with a cybersecurity flag, or the review is about security or access control, run it on **Daybreak Blue** (`gpt-daybreak-blue-latest`, xhigh) on **Codex-Max** (providerInstanceId `codex`), the only account with Daybreak enabled, framed as an authorized defensive review of our own project. This replaces the old rule that sent authorization reviews to Claude.
- **Security-related tasks carry the authorization context (user, 2026-10-15).** Every task that touches security, access control or permission defects, whether review, reproduction or fix, and on any model (Sol, Daybreak, Claude), must tell the agent that the user (mm0nst3r) is a collaborator with write access on the repository, and give the proofs. Paste the content of `sig/AUTHORIZATION_CONTEXT.md` into the task, or point to it and ask the agent to verify it with the read-only `gh api` commands listed there. Agents need this to decide whether they can do the work.
- **The orchestrator answers agents without being asked:** a 10-minute scheduled sweep ("NornicDB agent sweep (auto)") checks every agent's own thread for prompts, reads results, and acts on them.
- **Messages to agents use `t3_thread_send` mode=auto, never mode=queue.** A queued message to an agent whose turn has finished never starts, and the agent idles. The user flagged this repeatedly. Every sweep also checks each agent's recent runs for a stuck "queued" run, and resends it with mode=auto.
- **Personal Documents (PD):** send statuses, ETAs, "PR ready" and "merged" notices, and quick answers as a message to PD's thread `8e7caef7-a5dd-47f3-8518-dc2d5605ac6d` (t3_thread_send mode=auto). Their `NORNICDB_ISSUES_FOUND`/`REQUESTS` files are only for long, durable content: findings with reproductions, workarounds, explanations. A file append alone never counts as telling PD (user, 2026-10-15).
