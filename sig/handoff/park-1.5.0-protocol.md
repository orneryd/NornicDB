# Parking 1.5.0 work gracefully (user, Oct 18): no usable work may be lost

Applies to every agent whose work moved to 1.5.0 (#1391, #1412, #1468, #1469). 1.4.4 has the lab now.

1. Stop starting new work: no new TrueNAS jobs, no new benchmarks.
2. TrueNAS jobs you own:
   - QUEUED and not started: cancel them (`bash sig/tnrun.sh admin cancel-ids <ids>`, yours only). List them in the handoff so they can be resubmitted later.
   - RUNNING: let each finish, then collect and record its result. Exception: if it won't finish within about 30 min, stop it with `admin term-stale <id>` (yours only), keep its partial output, and record that it has to be rerun.
   - Queued A/B benchmarks: cancel them (yours only) and list them.
3. Code: commit EVERYTHING that's useful (production changes, probes, generators, matrices, scripts), including uncommitted provisional edits, with honest WIP subjects. Push it to a branch on the remote your PR uses, named `wip/1.5.0-<PR>-<slug>`. Don't move the PR head to unfinished code unless the PR already is that WIP. Keep your local worktrees too; never delete them.
4. Handoff file: sig/handoff/park-1.5.0-<PR>.md, containing:
   - PR(s), heads, the pushed WIP branch and commit;
   - the approved plan / Rule 73 gate status, with links to the analysis files;
   - what is done, what's half-done (file:line), and what's next, in order;
   - probes and tests and where they live; every job ID with its result; cancelled jobs to resubmit;
   - open questions and cross-agent dependencies (for example the #1385 scope-owner API).
5. A short GitHub comment on the PR: "Parked for 1.5.0 on 2026-10-18", the state in 3-5 lines, and the WIP branch name. No local paths, no secrets. The PR stays draft with milestone v1.5.0.
6. Report to the orchestrator: the handoff path, the WIP branch/commit, cancelled job IDs and anything at risk. Then end your turn. You are done.
