# Parked for 1.5.0: one reader for the inside of a relationship pattern (#1473 → #1485 / #1486)

Parked 2026-10-18 by user decision: the convergence moves to 1.5.0. #1472, the release copy of #1407, stays in the 1.4.4 candidate and is unaffected.

## PRs, heads, branches

- **#1485** (develop, draft, label bug), branch `fix/relationship-pattern-reader`, head `69b9a43ae`. It carries #1473's four commits plus `8b1756e73` (convergence, PROFILE rows) and `69b9a43ae` (edge contracts, gofmt). Milestone moved to v1.5.0.
- **#1486** (release copy, draft), branch `fix/relationship-pattern-reader-v1.4.4`, head `53cdbf8b2`, based on #1472's branch `cherry/1407-1.4.4`. Same commits plus the release-only `e35015dd6`: a `[` after a map literal subscripts the map, the rule develop has from #1214. Milestone moved to v1.5.0. For 1.5.0 the release copy is moot, because develop will become the next release. Close or retarget it when 1.5.0 is cut.
- **#1473** was closed at 13:33Z as superseded by #1485, before the park decision. #1485 holds all of its code. It stays closed, with a pointer comment. Reopen it only if you want the old number.
- **WIP branch** `wip-1.5.0-1473-relationship-readers`, commit below. It is #1485's head plus `parked/1473-relationship-readers/`, which holds every artefact: extractor outputs and scripts, the 311/415-statement matrix and its generator, Neo4j and NornicDB logs, probes (as `.go.txt`), graphify scripts and outputs, step-2 logs, and the partial review logs. Local `.go` probes were renamed `.go.txt` so that `./...` builds stay clean.

## Rule 73 gate and plan

- **Step 1** (inventory, Neo4j results, cause, plan): `sig/handoff/fix-rel-readers.md`, also in the WIP folder. The orchestrator approved the gate at about 12:15Z.
- **Step 2 is implemented on both heads.** Every reader in the step-1 list goes through `relationshipDeclarationOf` / `relationshipDeclarationHead`, which now also reads inline `WHERE`. Bracket-crossing scanners skip a relationship's inside through the shared bracket finder. Unreached readers are deleted.
- Two more root causes were found and fixed during step 2:
  - PROFILE data rows were being rewritten by the client-text restore (`keyword_scan.go` `restore`, now EXPLAIN-only);
  - release lacked the map-receiver subscript rule.
- **Decision on (c), user/orchestrator, 18 Oct:** follow Neo4j 5.26.30. A dynamic type on a legacy `*` range filters nothing. This is documented in the comment in `relationshipElement` (label_expression_rewrite.go) and in #1485's description as apparently a Neo4j defect, changeable in that one place.

## Done

- Oracle test `TestRelationshipPatternReaderMatchesNeo4j`: 415 statements, auto-commit and explicit transactions. Expected output in `pkg/cypher/testdata/relationship_pattern_oracle.json`, from Neo4j job j20261018122655-75088-eaef.
  - **Red:** develop i20261018123110-13160-01e6; #1473 head i20261018123114-28052-80c2 (69 statements differ); release #1472 head i20261018132002-62416-81b1 (26 panics).
  - **Green:** develop i20261018132504-88952-65f8; release i20261018131806-69468-cb61. Zero differences in the family, both parsers.
- **Full `pkg/cypher`, both parsers:**
  - develop `69b9a43a`, j20261018132720-41064-eccd: only `TestEvaluateWhereForContext_Branches` fails on each parser. That is the develop baseline (#1384 → #1468).
  - release `53cdbf8b`, j20261018132733-49512-005c: only the waived `TestBenchmarkMatchCreateDelete_LargeDataset_Direct` fails on each parser.
- vet i20261018130044-50888-dce1. gofmt was clean after `69b9a43a` (i20261018132133-102764-81d3 flagged one blank line, which `69b9a43a` fixes).

## Not done / next, in order

1. **Paired A/B** for the final head: `a20261018132748-28960-3638` (`wt-relpat-base@16923aef` vs `wt-relpat@69b9a43a`), RUNNING on bench slot 7 when parked. Fetch it with `bash sig/tnrun.sh result a20261018132748-28960-3638 --raw`, or rerun it on the 1.5.0 base. The command is in `parked/.../s2` and the PR body.
2. **Independent review.** Codex Sol xhigh on Codex Admin was cancelled mid-run (no verdict). Its partial logs are `sig/handoff/reviews/sol-1485-r1-*.log` (also in the WIP folder). Restart round 1 from scratch on the rebased head.
3. **Rebase onto develop at 1.5.0 time.** Regenerate the oracle if develop's behaviour moves. The generator is `matrix/gen.sh` → Neo4j `tnrun cypher neo-ref matrix.cypher` → `norm.sh neo` → `mkoracle.awk`.
4. **Put the measured numbers into #1485.** Mark it ready only after the review.
5. **Deliberately left as is (documented in fix-rel-readers.md §2):**
   - the over-approximating prefilters (`mayUse*`, `mayRepeatRelationshipVariable`, `relationshipPatternMayFail`);
   - the lookup-index regex and the NornicDB-own decay DDL reader, which accept only plain names by grammar;
   - node-side readers with the same dynamic-label `{` issue: a separate family, not filed yet.

## Lab jobs at park time

- **Collected:** develop full j20261018132720-41064-eccd and release full j20261018132733-49512-005c (results above).
- **Cancelled (queued, never started):** reviewer job i20261018134041-81356-91a4 (`rev1486-r1@53cdbf8b`, `-run ^TestReviewReleaseMapSubscript$`).
- **Running, owned by the cancelled reviewer, short focused tests:** i20261018133748-56324-84db (`rev1485-r1@69b9a43a`) and i20261018134011-93396-b79a (`rev1486-r1@53cdbf8b`). Left to finish (a few minutes). The results only feed the cancelled review.
- **A/B jobs that tnrun cannot stop.** `admin term-stale` reports them missing, because it covers Go workers only. For the lanes agent:
  - `a20261018131514-72572-18b2`: `wt-relpat-base@16923aef` vs `wt-relpat@8b1756e7`. Superseded by the next job; **drop it**.
  - `a20261018132748-28960-3638`: `wt-relpat-base@16923aef` vs `wt-relpat@69b9a43a`. The final head. Keep the result if it finishes; otherwise drop it and rerun at 1.5.0.

## Worktrees kept (never delete; clean up when the PRs merge)

wt-relpat, wt-relpat-rel, wt-relpat-wip, wt-relpat-base, wt-relpat-red, wt-relpat-rel-red, wt-rel-inv, wt-rel-inv-rel, wt-rel-inv-1473 (the last three hold untracked probe files, which are copied into the WIP folder). Graphify: repo copy `Desktop/codex/builds/repos/nornic-wt-rel-inv` and graph root `/graphs/nornic-wt-rel-inv` (local shared container).

## Open questions and dependencies

- **(c) is decided** (follow Neo4j). If the maintainer objects, the change is one block in `relationshipElement` plus the oracle's range statements.
- **`TestEvaluateWhereForContext_Branches`** is fixed by #1468 (also parked for 1.5.0). Rebase after it lands.
