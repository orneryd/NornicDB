PARKED FOR 1.5.0: code-intel labelled count and relationship identity scans
Decision: orchestrator/user, 2026-10-18. This supersedes earlier 1.4.4 plans.

Issues (author mm0nst3r, label bug, milestone v1.5.0):
  #1496 https://github.com/orneryd/NornicDB/issues/1496 -- labelled count
  #1497 https://github.com/orneryd/NornicDB/issues/1497 -- identity scan
Family: #1343 / draft PR #1412. No implementation PR was created for these
two bugs. No production source was changed. Do not move #1412's PR head to
this archive. Its entity-holder WIP is a different branch.

Branch: wip-1.5.0-1412-codeintel-materialization, based on develop 16923aef7.
Canonical shared handoff: sig/handoff/park-1.5.0-1412.md in the orchestrator
workspace. Other local worktrees and the release probe copy are retained.

Why parked
  Both failures share MATCH's retained PathResult/binding-map boundary ahead
  of aggregation or output projection. They belong to the 1.5.0 F-MEM
  retained-entity/memory family. The tool currently has a correct, usable
  unlabelled-count workaround for bug 1, so neither bug needs an isolated
  1.4.4 path. Only the orchestrator may reopen this release decision if the
  tool agent reports that its workaround blocks usable operation.
  Do not revive the earlier standalone-count plan without a new decision.
  No general MATCH streaming, ownership redesign, graph-expression/scope,
  relationship-reader, equivalence, statement-deadline or lifecycle work is
  authorized by this parking record. F-MEM's existing Rule 73 status remains.

Evidence
  RC 39e3f7647e034ebe7bf23b067cd161a915700b98, isolated scratch server:
  54,000 indexed CodeIntel nodes, 4096-byte bodies, 1,300,000 CALLS edges,
  5 GiB cap, GOMEMLIMIT=4GiB. Unlabelled count succeeds with 1,300,000 in
  0.0064575 s. Labelled count dies at 76.5857 s, exit 137, OOM-kill increment.
  Identity query returns no rows before failure at 95.9202 s, exit 137,
  OOM-kill increment. The source query and cgroup readings are preserved in
  compare-repros-39e3f7647-without-server-log.json; server-log text alone was
  omitted from the archived receipt. compare-repros.py.txt is the original
  remote harness; restore its .py suffix only when explicitly resuming.

  Exact queries:
    MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r) AS n
    MATCH ()-[r]->() RETURN startNode(r).id AS a,endNode(r).id AS b,
      type(r) AS t,r.site AS site,r.props_hash AS hash

  Count plan: NodeByLabelScan -> Expand -> EagerAggregation.
  Identity plan: AllNodesScan -> Expand -> ProduceResults.
  count_store.go correctly declines a counter for both labelled endpoints.
  pipelineApplyInitialTraversalMatch retains traversal paths/results and
  joined maps before aggregation/projection. Bolt fetch size cannot bound
  upstream retention. Endpoint .id reads also load ordinary node properties.
  Develop 16923aef7 retains this barrier despite endpoint-read improvements;
  no separate develop large-query runtime/peak was measured here.
  The supplied original repro handoffs retain v1.4.3 evidence and caveats.

Acceptance to add to the integrated 1.5.0 correction
  1. Count: exact labelled query succeeds with 1,300,000 on the full shape
     under the isolated 5 GiB cap, with allocation/retained-memory assertions
     through public Cypher on both parsers. No PathResult/binding-map list
     proportional to edge count may be needed for a scalar count. Compare
     increasing edge populations at fixed nodes/body size. Verify arbitrary
     different endpoint labels, direction, self-loop cardinality, namespace,
     and transaction visibility. An unlabelled total is not a general answer
     for arbitrary labelled endpoint intersections. Paired A/B on resumption.
  2. Identity: exact query successfully returns ALL scalar identities within
     that cap, checking row count and values. Stop the consumer at its first
     fetch and assert retained heap/peak or allocations as graph size grows:
     retention must follow the fetch window plus explicitly bounded working
     caches, not total relationships or node bodies. Preserve namespace,
     transaction visibility and normal cancellation. A graceful memory-limit
     rejection alone is not successful bounded-memory streaming acceptance.

Probe state (NOT measured red/green, NOT formatted/compiled/accepted)
  codeintel_relationship_read_test.go.txt contains a public fixture, count
  allocation probe (256 nodes, 8192 edges, 4096-byte bodies; provisional
  32 MiB TotalAlloc ceiling), semantic controls, and a count benchmark.
  codeintel-identity-working-set-test.go.txt is an extracted test outline
  using that fixture plus executeStreamed from result_stream_test.go; its
  provisional 8 MiB retained-heap ceiling has NOT been measured.
  These are text archives, so ordinary package tests cannot discover them.
  On resumption, integrate into the family tests, format on TrueNAS, establish
  meaningful red baselines and calibrate the memory assertions before making
  a pass claim. Do not submit parked-work jobs now.

Jobs: every one cancelled while QUEUED; none ran and no results exist
  Initial mixed count/identity probes, cancelled when bug 2 was deferred:
    i20261018154359-92312-e2a4  develop/default
    i20261018154400-92312-2e33  develop/ANTLR
    i20261018154417-47712-cc42  release/default
    i20261018154420-47712-9190  release/ANTLR
  Count-only probes, cancelled at the pause:
    i20261018154547-79344-7995  develop/default
    i20261018154548-79344-1e01  develop/ANTLR
    i20261018155021-101336-e063 release/default
    i20261018155023-101336-4da6 release/ANTLR
  The cancellation API returned all under cancelled, none under notQueued.
  Own waiting local clients were stopped after PID/command-line verification.
  No owned test, benchmark or verification job remains queued or running.
  Rework/resubmit only after the 1.5.0 family is explicitly resumed.

Next and dependencies
  Orchestrator resumes the integrated F-MEM correction under its existing
  Rule 73 gate/owner, adopts these acceptance cases, then collects public
  red/green and paired A/B evidence through the shared TrueNAS tooling.
  There is no unfinished production implementation to restore here.
  Bug 3's relationship-property seek is owned by the separate implementer
  fix-nornic-rel-index-seek-20261018; do not duplicate it. Earlier hook-order
  notes are historical because no count hook was written.
  Nothing useful is intentionally discarded; worktrees, probes, source
  findings, original reproductions, executable-harness text and receipt are
  retained. No pending job or release PR needs handoff.
