# Working rules from 16 Oct 2026, 10:15 UTC (Claude orchestrator, main Nornic thread)

**17 Oct: the Testing standard in sig/RULE_TRUENAS.md (user-approved) overrides every rule below that asks for both parsers, a full verify, full suites or benchmarks per fix or per PR.** Per fix: a red/green test plus the changed package's tests scoped with -run; both parsers only for Cypher parsing/execution changes (tnrun enforces this); the full matrix runs once, on the release candidate (`tnrun.sh rc-matrix`); make verify is background information.

These replace every coordinator, operator, clearance, admission, receipt, reservation, "Gate A/B" and "ROOT" rule from the Codex period. Those processes are abolished.

1. You may run tests yourself, at any time, without asking anyone. Focused tests: `bash sig/tnrun.sh go <worktree> [--parser both] [--allow-dirty] -- <go test args>`, race included. Neo4j: `bash sig/tnrun.sh cypher neo-ref <file>`. Iterate until they pass.
2. Full make verify (background information only, testing standard rule 5; never per fix round): append ONE line to the end of sig/tnverify-queue.txt (`<worktree> | ALL | <task> | <full head>`; `<task>` must be a short slug, letters/digits/dashes only, or the runner rejects the batch), and replace your older line for the same worktree in place. The verify operator runs them in order. Benchmarks (17 Oct): `bash sig/tnrun.sh ab <base-wt> <head-wt> --rounds 10 -- ./pkg/x -bench <regex>`: one paired job on the dedicated bench CPUs, base and head alternating on the same cores, with a median/delta/p-value table (sig/RULE_TRUENAS.md "Paired A/B benchmarks"). Separate alternating -bench jobs are not valid A/B evidence any more. sig/benchq.txt is serviced by the tnq lane one batch at a time, far too slowly to wait on (a 50-run batch took ~27 h); use tnrun for timely numbers.
3. Report in plain language: what changed, the head SHA, which tests ran and their result, and what still fails and why. No hash receipts, manifests or proposal packets.
4. Binding as before: NO workloads on this PC (no go, python, node or docker locally); everything runs via TrueNAS. Don't force-push shared branches. Security-related work follows sig/AUTHORIZATION_CONTEXT.md.
5. Waivers: only `TestBenchmarkMatchCreateDelete_LargeDataset_Direct` (the 15K throughput floor). Any other failure in your full run must be explained. Either it's real and you fix it, or it's identical on develop at the same base (show the develop result); then say so plainly.
6. Done means (testing standard): your fix's red/green test and the changed package's -run-scoped tests pass on TrueNAS (both parsers only when the change touches Cypher parsing or execution), a review is clean, and the benchmark against develop shows no regression. Then the orchestrator opens and merges the PR into develop.

7a. TEMPORARY (18 Oct, user): Claude is short on limit, so NEW implementation agents use Codex `gpt-6.1-sol` with `reasoningEffort: xhigh`, on the Codex account with the most weekly quota left. Running Claude implementers stay where they are. This overrides rule 7 until the user lifts it.
7. Agent roles (latest user instruction): new implementation work uses Claude Opus 5.5 with medium effort (`claude-opus-5-5`, `options: {"effort":"medium"}`). Which account: the account-quota skill, and nothing else. Accounts: Claude-Max (`claudeAgent`), Claude-Admin (`claudeAgent_90628217-3c59-406d-b4b5-c70082f45225`), Claude-Vexor (`claudeAgent_652c5813-cbfe-42a5-b977-4708efa5e464`). Codex agents are reviewers. Let already-running implementation agents finish; do not interrupt or duplicate them to change models. An interrupted implementation may resume in its existing owner thread after confirming no live turn, retaining its worktrees and submitted jobs. Reuse and collect pending jobs before submitting anything again. No automatic Codex implementation fallback.

8. MERGE FLOW (user, 16 Oct ~10:55 UTC, replaces rule 6's merge gate): we merge freely into `develop` once the fix's focused tests pass on TrueNAS (both parsers only for Cypher parsing/execution changes). Full verify and bench no longer block the develop merge. The orchestrator then cherry-picks bug fixes from develop into the current release branch, `release/v1.4.4`, via a PR it merges itself (rebase). The gate for that cherry-pick is a green full verify (15K-floor waiver only) and a clean Codex review. Features stay on develop. HOLD (user 16 Oct): no cherry-pick branch or release PR until PD says develop is to its liking.

## Benchmarks

Paired A/B jobs survive the submitting client exiting once submission is acknowledged. Use `bash sig/tnrun.sh ab <base-wt> <head-wt> --no-wait --rounds 10 -- ./pkg/x -bench <regex>` to submit and return the job id. Fetch later (waits if unfinished): `bash sig/tnrun.sh result <id> --raw`. The box retains `tnrun/done/<id>/result.json` and `out.txt`; API clients can download these by id, as in `scratch1012/ab-fetch-1378.mjs`.

18 Oct: the benchmark App has eight configured slots of four physical cores each. Commissioning enabled **six production lanes: slots 0,1,3,5,6,7**, selected in `bench1043/core-ledger.json` (`bench.maxSlots=6`, `bench.activeSlotIds`). Slots 2 and 4 remain available to tests after false A/A signals; enabling them requires new evidence. `ab-status <id>` prints the running slot and round CPUs; `status` lists slot assignments. The first raw output line and terminal `result.json` retain the slot, build CPUs and round CPUs. Each slot has its own checkout and runs one whole ABBA job at a time. A global claim lock preserves FIFO; a slot lock prevents duplicate workers, and shared per-core leases prevent tests and benchmarks using the same physical core or SMT sibling. Slot 0 stays on 48-51. Other active slots borrow named test workers only when ready A/B jobs need them, then automatically return their cores to tests after the A/B job. Existing tests finish before lending; twelve of the 21 test workers remain available at the six-lane production cap, and all 21 can claim when benchmarks are idle. The optional control slot on 16-19 is used only for simultaneous validation and never takes production jobs. Scheduler housekeeping runs on reserved cores 0-3. Other projects' unrestricted Apps can still contribute foreign CPU use: assess noise against the simultaneous control and report this isolation limit (see `sig/handoff/parallel-bench-lanes.md`).

Check `bash sig/tnrun.sh ab-status <id>` before resubmitting. `HELD by <operator>` means durably queued in `tnrun/operator/<operator>/held`, awaiting an authorized priority window; it is not lost, cancelled, or tied to a client PID. `status` includes ready and priority-held counts, and waiters report hold transitions. Operators restore the original filenames, preserving FIFO within the deferred queue after priority work is claimed. An empty ready queue alone does not prove a job disappeared. Keep existing ids for unchanged inputs; resubmitting duplicates the A/B work (A/B has no identical-job dedupe). Go-job dedupe and stale-head supersede are unchanged. Do not move held jobs or restart the lane to make a result arrive sooner.


Lab dataset retention (18 Oct, user standing order): `bash sig/tnrun.sh admin trim-dataset` safely trims data/nornic-bench-1043 through a unique, short-lived API control App (current worker image, UID 568:568, dataset-only bind mount), writes its receipt and removes the App. `admin lab-watch` and periodic `admin lab-watchdog` share the same retention check: run when the last successful receipt is over 24 h old, or early above 70% quota usage; lab-watch prints a usage alarm. Only shared Go build-cache files unused >24 h and locally collected abbench binaries without queued/running/held references qualify. Never use go clean -cache, trim younger entries, touch module caches or delete unconfirmed binaries. Receipts: sig/tnrun-retention/; details: sig/handoff/truenas-dataset-cleanup-20261018.md. No TrueNAS cron.

## Waiting on TrueNAS jobs
Never end your turn while your TrueNAS jobs (tnrun, bench queue, verify) are still running: nothing wakes you when they finish. Wait for them yourself (tnrun waits on its own jobs; check bench/verify result files every few minutes) and carry the task through to your final reply.

## PR status conventions (user, 18 Oct)
- Milestone: v1.4.4 if the change ships in 1.4.4 (release PRs, and bug-fix candidates on develop that will be cherry-picked); otherwise NO milestone. No milestones beyond 1.4.4.
- Base: release/v1.4.4 for release PRs (cherry-picks); develop for everything else; stacked bases only while the lower PR is open.
- Draft = work in progress, conflicts, or open P1/P2 findings. Ready = finished, focused tests pass on both parsers, waiting only for review/merge. Mark your PR ready yourself when you get there; set it back to draft if a review finds P1/P2.

## Bench queue etiquette
Never hold, move or remove other agents' benchmark or test jobs. If your jobs are urgent, ask the orchestrator; at most your own jobs go next in line. Held jobs (operator/*/held) never run until released.

## Rule 73 (binding; full text: `.agents/rule73.md` on develop, linked from AGENTS.md)
- Trigger: TWO failed correction attempts in the same family (a correction presented as ready that then fails review or verification) with no pass in between. Earlier history counts if the same failed treatment recurs. Enter sooner if the same treatment is being repeated without new evidence.
- Once triggered: STOP per-finding / per-consumer fixing for that family. Re-establish the complete boundary (use an extractor or inventory, not memory), identify the shared causal owner or mechanism that let the failures survive, and correct the whole causal unit through every affected consumer as ONE integrated implementation.
- Present it for review only when the changed approach is implemented across the complete unit and its verification passes through the public paths, including the conditions behind the recurring failures. If that fails review, Rule 73 stays active: change the approach again. Never resume serial leaf patches.
- Reviewers: say explicitly whether the correction is integrated across the complete unit or is another leaf patch.
- Also binding: `.agents/standing-design-principles.md` (root causes only, one canonical owner, exact scope, no precautionary edge-case code).

## Performance (user, 18 Oct; overrides any "5%" wording anywhere)
There is no percentage threshold. Optimise performance as part of the fix. If a measured cost can't be removed, say so clearly in the PR description (paired A/B numbers, cause, why it can't be removed) and leave the decision to the maintainer. Don't hold or move a PR because of a percentage.

## Content-flagged tasks (user, 18 Oct)
If a Codex run (Sol or Daybreak) is stopped by a cybersecurity content flag, the orchestrator reruns it on Claude (Opus 5.5 medium). This applies to implementations and reviews; security flags run on Claude.

## Path enumeration
For family inventories (Rule 73) and "find every path" work, use the LOCAL shared graphify container (skill: C:/Users/mm0ns/.codex/skills/graphify/SKILL.md; how-to in scratch1012/rule73-gate.txt) plus grep/AST/coverage. graphify is code analysis, allowed locally; tests, builds and benchmarks still run only on TrueNAS.

## Release parity (user, Oct 18: develop/release drift is "absolutely unacceptable")
- A bug fix is NOT done when it merges into develop. It is done when its release copy (a release PR into the
  current release branch) is open, or when a ledger entry in sig/release-parity-ledger.json records with code
  evidence why it can't ship there (on-disk format older versions can't read; depends on a develop-only feature;
  test/docs only).
- Whoever merges a fix into develop opens the release PR in the same session. The develop PR body links it.
- `bash sig/release-parity.sh` must exit 0 before every RC build, before handing release PRs to the owner, and in the
  orchestrator's periodic sweep. A failure is fixed immediately (open the release PR or add the evidenced ledger entry).

## TrueNAS API load (user, Oct 18: "you're fucking up TrueNAS")
- Every file download (core.download -> filesystem.get) is a TrueNAS middleware job and appears in the UI. Never
  download a file to find out whether it exists; check with filesystem.stat/listdir first. No polling faster than
  every 30 s, and minutes for long jobs. Use the shared tooling's waits/caches; don't write your own poll loops.
