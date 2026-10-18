# Graph-expression release priority measurement

18 October 2026. This is measurement only; the integrated convergence continues on develop for 1.5.0. #1459 is closed. Neither baseline contains a product overlay.

**Within the complete existing family matrix, shipping release base without #1459 makes nothing worse than v1.4.3.** All four head/parser cells have 816 matches and 438 differences from live Neo4j 5.26.30, out of 1,254 comparisons (1,221 query cases and 33 graph-effect checks). The 438 differing IDs are identical, and their NornicDB outcomes are identical under error-code equivalence and unordered graph-snapshot rows. No release-only difference or changed failing outcome was found. This conclusion is scoped to this matrix and the pinned release base, not a claim about every change in 1.4.4.

| Baseline | Commit | Default match / differ | ANTLR match / differ |
| --- | --- | --- | --- |
| release/v1.4.4, without #1459 | `24d08776cdb9cb624da1073007da437306518426` | 816 / 438 | 816 / 438 |
| v1.4.3 | `f2ddd27cfae3788076778a0a0d48c827df046a3b` | 816 / 438 | 816 / 438 |

All seven original phases were selected separately: BASE (864 cases), SUPPLEMENT (57), PARAMS (27), PROJECTION (77), TX_STATE (15), CODEPATHS (19), REWRITES (162). No default-plus-supplement double-counting. One route, Bolt autocommit, as requested; actual-driver parameter and statement-sequence adapters are unchanged. The harness checks the live oracle version is 5.26.30. The generator is the same file used for the gate; only an all-phase test wrapper and the single-route selection were added.

## The ten concrete failures

“Yes” means the same documented failure occurs, on both parsers. “No” means that member matches Neo4j in the complete phase report (the harness emits every difference and the completed comparison counts are checked).

| # | Concrete failure | Query ID | Release without #1459 | v1.4.3 |
| --- | --- | --- | --- | --- |
| 1 | Comprehension loses correlation after WITH renaming | `019f370bef7572d53fc2` | Yes | Yes |
| 2 | Imported null becomes a local graph scan | `09bf5a9881b509d0c3c7` | Yes | Yes |
| 3 | WITH * endpoint property map incorrectly returns false | `45c0ce713a25679a615f` | **No** | **No** |
| 4 | Nested comprehension projection loses its current node | `e7763c11f8f617a74224` | Yes | Yes |
| 5 | UNION ALL COUNT loses imports after WITH | `05c617080b3abc75b5bb` | Yes | Yes |
| 6 | Valid CALL import disappears | `14604bebf4c98709c319` | Yes | Yes |
| 7 | CREATE/MERGE/SET stores false instead of true | See below | Yes | Yes |
| 8 | NOT EXISTS returns extra rows | `4435c5697643cd4f611c` | Yes | Yes |
| 9 | Normalisation accepts an unbound named endpoint | `bdce0002f06bfa9aa5a9` | Yes | Yes |
| 10 | OPTIONAL cardinality loses correlation | `accb5a19275a7575bae6` | Yes | Yes |

The endpoint-map failure is present at the reviewed #1468/#1459 gate heads but absent in both baselines; this report does not claim a bisect identifying which PR commit introduced it.

## Stored graph effects (U02/G02)

The three wrong graph effects occur **without #1459**, and already occur in v1.4.3, identically on both parsers:

- G02 SET, `7e7d58f6bd14589e1e2b` and `:graph`: `MATCH (a:Q) SET a.flag = EXISTS { WITH 1 AS k MATCH (b:Q) WHERE (a)-[:R]->(b) } RETURN a.id AS id,a.flag AS v ORDER BY id`.
- U02 CREATE, `b350dc67dd6be48d24d6` and `:graph`: creates `(:Gate {key:a.id,found:EXISTS { WITH 1 AS local MATCH (b:Q) WHERE (a)-[:R]->(b) }})` for each a.
- U02 MERGE/SET, `178a156e2d6d9a70c059` and `:graph`: merges `(:Gate {key:a.id})` and sets the same expression as `out.found`.

For the fixture Q(1)-R->Q(2)-R->Q(3), Neo4j stores true for keys/ids 1 and 2 and false for 3. Both NornicDB baselines store false for all three. The post-write graph snapshots prove actual stored values, not just returned expressions. Three failing graph checks appear in each of the four cells; their unordered contents are identical across cells.

## Jobs and reproducibility

All jobs are terminal. Exit 1 records Neo4j differences; each phase completed and emitted its count (not a build failure or skip).

| Job | Baseline / parsers | Result |
| --- | --- | --- |
| `i20261018124351-39084-d1d6` | release base / default + ANTLR | 7 phases each, 816 / 438 each |
| `i20261018124351-67092-4bbb` | v1.4.3 / default | 7 phases, 816 / 438 |
| `i20261018124802-31396-6263` | v1.4.3 / ANTLR | 7 phases, 816 / 438 |

The client's test-scope rule reduced v1.4.3's initial `--parser both` to default because the probe overlay touches no pkg/cypher file. The explicit ANTLR job completed the requested population. Its first local submission failed the shared tnrun self-check (read-cache fixture, 2 calls versus 1) and created no job; retry used the same unchanged inputs after the check passed. No acknowledged job was resubmitted or cancelled.

Commands are `fix-1384-priority-{release,v143,v143-antlr}.sh`. Logs are the corresponding `.log`, with `-antlr-retry.log` for the successful ANTLR submission. Detached worktrees `wt-fix-1384-priority-{release,v143}` contain just the gate generator and test harness overlays; tracked production diffs are empty. The summariser `fix-1384-priority-summarize.ps1` checks all seven phase totals per cell, joins every differing ID, compares all outcomes, and emits:

- `fix-1384-priority-totals.csv`, `fix-1384-priority-differences.json`, `fix-1384-priority-jobs.json`;
- `fix-1384-priority-outcome-changes.json` (empty: zero changed outcomes);
- `fix-1384-priority-ten-failures.csv`, `fix-1384-priority-stored-effects.json` (full raw outcomes, queries and stored graph snapshots).
