# Current follow-up evidence

This file records successive candidates. The latest checkpoint and convergence
sections below define the current scope; earlier passes do not qualify later edits.

Behavioral source: merged release `04c87de3e232fece79ebf3eb5af78e5b3fe5e09f`; follow-up head `ce4dd07bc75d46fa72479aeb5a9829e7447f2439` changes only tracking documentation. No source changed during these runs.

| Check | Result | Seconds |
| --- | --- | ---: |
| `NORNICDB_PARSER=nornic GOFLAGS=-count=1 go test ./cmd/nornicdb -run '^TestRemoteConstituentCredentials$' -count=1 -v -timeout=8m` | PASS | 66.613 |
| Same command with `NORNICDB_PARSER=antlr` | PASS | 74.589 |
| Focused credential persistence/runtime/wire tests in pkg/multidb, pkg/fabric, pkg/storage | PASS | 4.462 |

The public matrix builds and starts actual authenticated remote and coordinator processes. It checks explicit and URI credentials over HTTP/Bolt remotes, HTTP/GraphQL/Bolt ingress, implicit success/error endings, explicit HTTP/Bolt commit/rollback, and direct constituent selection. Remote wrong-password controls reject authentication. Every HTTP transaction phase must retain correct authentication. Native inline remote DDL supplies persisted setup for the ANTLR runtime restart; this does not claim that native extension is in the ANTLR administration grammar.

The focused command selects TestSolReview1513PersistedCredentials, TestRemoteCredentialPersistenceEveryWriter, TestRemoteCredentialPersistenceInput, TestRemoteCredentialPersistenceUpgrade, TestRemoteCredentialPersistenceNewInputRequiresKey, TestRemoteRuntimeCredentials, TestRemoteRuntimeCredentialErrorsAreRedacted, TestCredentialUsernameOnlyCatalogPersistence, TestCatalogRemoteRuntimeCredentials, TestRemoteResolvedAliasesKeepDistinctCredentials, TestRemoteAuthenticationPreservesAdmittedCredentialBytesOnWire and TestRemoteEngineBoltAuthConstruction. Normal go test success proves only selected tests that exist; no blanket migration/reference qualification follows.

DeepSeek read-only review completed in74.201s. Parent source review rejected proposed redundant helper tests: real Bolt URI composition already runs through an authenticating remote, and the wire fixture rejects mismatched Basic/Bearer credentials. The supported explicit V3-to-V4 upgrade contract supersedes the earlier startup-rewrite warning wording. The oidc transaction fixture is lifecycle evidence, not proof of user/password preservation. Successful authenticated remote writes with durable commit/rollback outcomes remain a separate item; the read matrix does not prove those writes.

Graph refresh and the enabled duplicate checker remain pending. The first importer attempt failed at the restarting endpoint; the second targets the exact immutable merged release revision. No performance or complete-AC pass is claimed.

## Authenticated write-outcome extension

The existing real-server matrix now includes28write cases: explicit/URI credentials × HTTP/Bolt remotes × implicit HTTP/GraphQL/Bolt ingress and explicit HTTP/Bolt commit/rollback. Existing read cases remain intact. Transaction helpers accept the statement, and an independent authenticated remote read requires exactly one marker row after commit and exactly zero rows after rollback. No production API or storage layout changes.

- Native focused matrix: PASS65.010s.
- ANTLR focused matrix: PASS74.559s.
- Fresh `GOFLAGS=-count=1 make test`: PASS121packages, exit0, 971.769s, both lint gates pass.
- Go/module source SHA256 before/after: `0e36d7d14a7512aa5322367851dfc8ff650226f79ad530aaf14fc69406572a12`. No behavioral source changed after the run.
- Corrected applicable lexical/fixture/URL-sink inventories pass. An earlier structural command failed on two nonexistent package paths; its census selector matched no tests and is not credited.
- New source remains pending enabled checker verification against completed release ingestion; no checker pass claimed.

Backup review found no combined legacy native backup/credential-upgrade proof. A new fixture must avoid upgrading the V3 source merely to create its backup; no new production version accessor or extra migration arm is needed. This remains open.

## Native backup acceptance extension

The existing credential migration test now runs direct and native-backup routes for all five forms. Native read-only Badger Backup captures the constructed ASCII V3 source without opening it through the upgrade gate; source logical digest stays unchanged. A fresh unattached engine restores the native stream, closes, and the existing authorized startup V4 owner opens it. Existing runtime-resolution, stable encrypted descriptor on second ordinary open, and wrong-key rejection assertions run unchanged on the restored copy. Backup-file SHA256 remains identical after restore/upgrade/reopen.

The worker focused run passed (2.570s package time). Parent race run passed (3.837s package time); all five backup forms and five original direct forms pass. Every other test function in the file is byte-identical; fixture-constructor inventory passes without regeneration. No production storage version/layout/key/payload change or API accessor was introduced. This is a constructed ASCII V3 native-backup population, not qualification against every released binary's backup artifact.

The preceding full-suite result predates this new test extension. A new combined frozen full run is required; no unchanged-source full pass is claimed for the combined candidate yet.

## Additional current acceptance qualification

- Combined frozen `GOFLAGS=-count=1 make test` passed121packages in891.616s, source fingerprint before/after `a01ebe4c5f01f015bed6bc38277cb9c2556c37f082a751c0c14f55a7cec67bb0`. This predates the later53-line GraphQL-only test extension.
- CLI R2 public administration output check passes39.444s. Catalog publication/drop/receipt tests pass under race (multidb9.313s). Public CREATE OPTIONS atomicity, catalog request contexts, decoded identity and remote dynamic-mode/counter tests pass under race (server43.422s; see focused receipt).
- ANTLR rejection/redaction set passed80.496s elapsed; the later direct GraphQL rejection extension passed native38.724s and ANTLR68.532s package time. GraphQL requires actual resolver errors with path `["cypher"]`, nonempty messages and no synthetic password. Existing HTTP checks remain unchanged.
- Actual released-source V3 qualification passed: source-built v1.4.3 f2ddd27c creates12aliases (CREATE/ADD × HTTP/Bolt × explicit/URI/both), direct and native-backup routes each pass24authenticated alias workflows across native/ANTLR modes. Both end atV4, preserve the local marker, leave original backup bytes unchanged and have zero visible native values containing the known synthetic passwords. This is bounded ASCII/source-built qualification, not every released binary artifact or secure-erasure proof.
- Initial strict rejected-open digest differed only because `wal-property-identity` was added. Every pre-existing key/value and V3 marker was unchanged. The user permits new metadata keys; no production migration correction was justified. The original failing expectation and exact key diff are retained externally.
- Strict ANTLR SHOW CONSTITUENTS is not accepted by its grammar in this qualification. Native SHOW plus supported HTTP database-info verify display nondisclosure; both modes' runtime alias queries pass. No strict-grammar display pass is claimed.
- Existing security advisories were independently confirmed as two unpublished private drafts, neither with a CVE ID assigned; released-source evidence was added to the credential-persistence draft.

The full graph refresh remains unsuccessful: canonical ingestion exhausted MVCC retries on one existing CALLS edge. Exact2000-row inputs are captured; all endpoint pairs are unique. The same batch passes on isolated current and releasedv1.4.3 databases, but the remote fails even for the one-row update. Its precise native state/build needs investigation; HTTP reports1.4.3/dev, not an immutable commit. No fresh graph/checker pass is claimed.

## October 27 continuation

The operator updated the remote binary, but the exact one-row update still returned Transaction.Outdated. The operator then authorized dropping and recreating only `nornicdbcode`. The DROP client timed out at60seconds; a subsequent catalog read confirmed that database absent. CREATE succeeded, and immutable release04c87de3 extraction/import restarted through the installed Graphify virtual environment. Other databases were not dropped. A first launch with system Python failed on missing ijson before any ingestion; it is not counted as a running import or successful check. The new import has independently reached22000nodes; completion and the baseline marker remain pending.

The parser stream now selects byte or indexed lookahead once while retaining one cursor, offset-resolution, source-mapping and cancellation owner. DeepSeek's package test/race/vet passed; parent review corrected its Unicode unwind fixture to construct actual Unicode boundaries, preserving the existing assertions. Controlled LongASCII ABBA comparisons against the preserved experiment-before binary (24samples per arm/case) measured background +4.01%/+3.59% and cancelable +0.075%/+0.041%; cancelable allocations decreased27to26. This saved baseline binary's exact source tree is not reconstructible, so these are qualified experiment comparisons rather than pristine-release claims.

A72-workload ABBA comparison against the saved merged-implementation binary (8samples per arm/workload) measured cancelable LongASCII Lex -7.32%/-5.38%, with no workload over5% slower. The earlier Unicode regression against the older experiment binary remains separately open. The broad experiment's first summary hit division by zero on zero-allocation warm cases; all72completed raw workloads were analyzed without replaying the benchmark. A subsequent source-boundary preallocation experiment is under qualification: its mixed Unicode/surrogate allocation test failed with4allocations before the correction and passes at1afterward. Normal cyphertext/parser packages pass1.880s/11.271s after explicit lexical discovery and review of the removed LA owner and two replacement coordinate/lookup owners. This is not a final full-suite or performance pass.

## Current convergence corrections

The credential/parser checkpoint passed uncached `make test`:121packages,
1,034.95seconds, unchanged Go/module fingerprint
`c110a8c30c8ad9e9cc3b44b6f0c23b21737865cbceafacf624d00d936515fdb0`.
It finished normally before an attempted stop; no cancellation is attributed to
that pass. It predates the following corrections, whose final gate is pending.
A later run was deliberately interrupted after formatting preflight found two
nits; its logs are retained and it is not a pass. Formatting now gates launch.

- DF06 / batch5-113: public `DROP COMPOSITE DATABASE a b` deleted the catalog
  entry `ab`. The original failing test is retained. Both DROP routes now use
  `executeDatabaseDrop`; their private duplicate handlers are retired. Complete
  name/suffix admission precedes mutation, including rejection of extra text
  after standard `IF EXISTS`. Thirty public native/ANTLR cases pass, with valid
  plain/quoted names, keyword spacing, trailing comments, rejection without
  target deletion, and preservation of the other database and its marker.
  Original-handler/router overlays also reproduce the invalid-suffix failures.
- DF09 / batch5-126: USE/AND projections reuse `indexTopLevelKeyword`, and all
  Fabric keyword predicates reuse `keywordAt`; `hasKeywordAt` is retired.
  Exact consumer controls pass before/after. The first reuse experiment slowed
  USE by30.22% and was corrected before retention. Final ABBA12samples/workload
  compares matched current dependencies/harness with only executor/planner
  overlaid from immutable41220381 for the baseline: AND564.05→376.45ns,
  USE147.4→140.9ns, imported rewrite2050.5→1575.5ns; B/op and allocations are
  unchanged. Public planner return/use/call/nested medians improve17.67%,12.26%,
  8.07%,5.68%; the composite-scope control is+0.60%. CPU profiles/raw logs remain
  with the saved binaries. These are named workload measurements.
- Batch5-119: one reference-driver fixture now owns the generated and USE
  matrices. Separate `header-matrix`/`header-use` executors,618generated shapes,
  54USE shapes, two unresolved controls, and the original transaction/prefix
  coverage remain. USE case bodies are byte-identical apart from indentation.
  The optional Neo4j reference URI is absent here; no fresh reference-server
  qualification is claimed. The merged matrix passes3.194s via overlay and
  3.287s after integration.
- Batch5-169: `Open` is the data-directory assignment owner. The CLI passes the
  same config pointer and explicit directory into it without a redundant write;
  no intervening consumer reads that field.
- Batch5-53/54/98: self-service guard order, typed JSON decode, subsequent actor
  lookup and exact role projection now have shared private owners. Mutation,
  permissions, audit and response paths remain unchanged. Optional role updates
  retain nil-as-no-change. Focused handler/guard controls pass3.383s/2.625s;
  DeepSeek reviewed the integrated diff with no finding in that supplied slice
  (text review, not worker test execution). A conditional raw-credential question
  was checked against `ClientCredentials`: those raw scalars are redaction inputs,
  retained by the canonical collector; no exposure was established.

The focused integrated race command passes cypher21.373s/fabric2.302s/server21.174s;
it includes the original24DROP cases, before the six trailing-comment controls
were added to the normal matrix. Reviewed lexical/source/credential populations
pass2.000s/3.598s/0.927s. Retired owners are explicitly migrated in the inventory;
no assertion or checker rule is weakened.

Final LongUnicode Lex ABBA24samples/arm shows raw background−0.169%, raw
cancelable−0.629%, accepted background+0.021%, accepted cancelable−0.536% versus
the preserved experiment binary. Memory is+0.35–1.03%; background allocation
counts are unchanged and cancelable cases use five fewer allocations. The exact
old source tree is still not reconstructible; this is qualified saved-binary
evidence. The72-workload boundary comparison has no >5% time or >10% byte
regression; short Unicode/projected lexing improves roughly2–6% with fewer
allocations. No ANTLR dependency patch or storage layout/version change is made.

The enabled refreshed-graph checker, remaining audit population dispositions,
and the frozen post-correction `make test` remain required before readiness.

## Further convergence corrections (not yet full-gate qualified)

The later corrections postdate the 972.599-second integrated suite pass. A new
frozen `make test` and the refreshed-graph checker are required for this candidate.

- Quoted policy names now consume the existing quoted-string reader directly.
  Eleven controls preserve bare/quoted input, offsets, empty and unterminated
  names, and the old escape behavior. The original-reader overlay and new
  implementation both pass the contract controls.
- Optimized RETURN shapes share clause and alias admission in the existing
  projection owner, preserving their separate accepted shape grammars.
- Both literal classifiers are retired into one typed-policy owner. The fixture
  captures 41 exact inputs under both original policies (82 outcomes), including
  the intentionally distinct parenthesized scalar behavior. Public SET/type
  rejection checks pass. Twelve paired benchmark cases retain allocations;
  the largest measured timing increase is 2.42% against the saved old-owner binary.
- Source and quoted-name presentation share one tree and outer-result owner;
  four adapter methods are retired. Controls cover numeric precision, typed nil
  containers, diagnostic coordinates, EXPLAIN plans and mutation of every copied
  container layer without changing cached inputs. Focused controls pass in 2.770s.
  Paired outer-result measurements differ by +0.43%, +1.18% and +0.63% for quoted,
  changed-source and unchanged-source fixtures, with allocations unchanged.
- General and property-type CSV constraint origins use one renderer. Original
  admission rules remain: the general payload cannot supply a property type;
  the dedicated payload renders its stored expected type. Focused checks pass
  in 0.867s. The four-constraint export workload changes from 2,455 to 2,485ns/op
  (+1.22%), retaining 1,336B/op and 58allocs/op.
- Ordered-index collection has one private typed-mode owner. The explicit
  non-null form retains its WHERE property spelling for index selection; the
  general form retains bounds and incomplete-window fallback. Ordering/traversal
  controls pass. The existing tied-page benchmark changes from 840,949 to
  840,269ns/op (-0.08%), with unchanged allocation range.

These comparisons qualify the named workloads against their saved pre-correction
implementations. They do not replace the original immutable-baseline performance
criteria, close the complete audit inventory, or establish a published-head pass.
No storage layout, storage version, ANTLR dependency, checker or hook change is
included in these corrections.

CREATE DATABASE now shares one typed-mode admission owner. A controlled overlay
of the original implementations reproduces flexible-whitespace replacement
failures in both parsers and native acceptance of malformed IF NOT EXISTS tails
for both missing and existing targets. Corrected initial public controls pass
in1.126s; expanded cases assert catalog metadata and an independent keeper marker
remain unchanged. Valid IF NOT EXISTS OPTIONS remains accepted. Replacement
retains its original DROP-then-CREATE contract and does not gain OPTIONS support.

Inline/overflow buffer access now has one generic physical-slot owner, retaining
the existing capacities and struct layouts. Seven paired evaluator/public MERGE
workloads retain allocation counts, with timing changes between-0.98% and+1.28%.
CPU profiles and raw ABBA logs are retained. No storage representation changes.

A fresh frozen-source uncached `make test` is in progress. The two prepared
security advisories were rechecked as private drafts with no assigned CVE IDs.

## Frozen integrated full-suite result

`GOFLAGS=-count=1 make test` passed in995.596seconds, with121passing
package result lines and no failures. The run includes all current convergence
corrections and expanded CREATE target-preservation controls. Go/module hash
`f3b82fc3297b107bba940ae27af44b661d5cffa04ad164ae7f39658f80e7a968` and runtime fixture hash
`894e9c3bf1f9fb075bf7d664fa09c719913a1e23e917922fe44e7dd06b04cf01` were unchanged before/after.

This proves the staged implementation at the docs-only HEAD41220381 plus its
reviewed changes; publication is still pending. The preceding972.599-second pass
is archived separately and is not substituted for this run. Refreshed-graph
checker and remaining workload acceptance qualification are still open. Broader
verification remains for CI under the requested make-test-only gate.

## Completed graph and final workload comparisons

The recreated nornicdbcode import exits0 with63,412nodes and2,319,155relationships.
An independent CodeRepository.name lookup confirms revision04c87de3e232fece79ebf3eb5af78e5b3fe5e09f
and branchrelease/v1.4.4. No recurrence of the original edge conflict was observed;
its underlying cause remains unproven. The operator identified the database binary
as pre-1515 d663ce8f, verified as the merge's sole parent (operator report, not an
independent executable hash).

Paired named workloads on Apple M2Max/darwin-arm64/Go1.26.9:

| Workload | Timing versus its stated baseline | Allocation outcome |
| --- | --- | --- |
| Warm alias lookup,1/128aliases,registered/folded,rebuilt immutabled663 | 72–95%faster | 128B/3alloc to0 |
| LongASCII public lexer,raw/accepted,background/cancelable | -0.012%to+0.599% | unchanged |
|72lexer/validator workload population | no >5%time increase | no >10%byte increase |
| Warm remote dispatch,no import/import | 13.96%/16.98%faster | three fewer allocations |
| Cold dispatch,no import | 4.34%faster | three fewer allocations |
| Cold imported dispatch | +2.35%,overlapping sample ranges | +7.83%bytes,+4.43%allocations |
| SHOW with cache disabled and identical124-row/callback/digest control | +2.67%,overlapping ranges | four fewer allocations |
| MCP nil/Spanish unknown-tool response,identical controls | 8.89%/4.06%faster | one fewer allocation each |

The cold-import cost is retained for actual emitted-source policy validation;
its measured bytes stay within the project's10%budget and time within noise.
This is an explicit scoped tradeoff, not a claim of zero overhead. Dispatch uses
an actual compiler→Fabric→RemoteEngine encode/decode path with a synthetic
RoundTripper; its prior-policy arm reconstructs the old policy in the same
integrated binary. It is not a pristine historical-dependency or network-latency
comparison. Lexer comparison uses the preserved experiment binary; SHOW/MCP uses
the saved2ddf47c program. Raw logs, binary hashes and CPU profiles are retained.

The actual stored-user mapper comparison exposed a populated-read regression:
2,497.5ns/2,186B/44alloc versus817.6ns/714B/7alloc for the saved codec. The corrected
shared JSON binding uses typed decoding on valid input and ordered accepted-prefix
handling on type errors. Metadata's default empty map is allocated only on absent
or rejected input; JSON null stays nil. The immutable344-case golden fixture,
malformed/prefix/timestamp controls and a permanent7-allocation regression pass.
The original-source overlay fails that regression at44allocations.

Eight paired samples per arm after correction: populated read853.1ns versus825.35ns
(+3.36%,overlapping ranges),both714B/7alloc. Empty read is62.33%faster with1versus5alloc;
empty write23.99%faster with13versus19alloc; populated write is within noise
(-2.02%)with unchanged22alloc. The reference executes saved codec bodies against
the current shared JSON/storage/toolchain; it does not claim pristine dependencies.
No stored key,payload or storage-version change is made. No existing assertion,
golden fixture,checker or hook was weakened. A new full suite is required after
this last mapper correction; the995.596-second pass is preserved as preceding
integration evidence.

### Refreshed graph audit and allocation-test correction

The full cumulative checker retry completed against release graph04c87de3: 2,650 analyzed functions, 69 refactor targets and281 reuse candidates, with no capped skips. The first transport-timeout run is preserved separately; the retry did not time out. The checker and hooks remain enabled and unmodified. All350 rows are retained in the evidence inventory; none is waived by the test result.

The subsequent integrated `make test` failed only `TestSelectedPathCountedCompilation`. Fresh-process reproduction failed19/20 times because its public-execution allocation measurement included first-use shared ANTLR initialization; its direct counted-plan budget did not fail. The test now isolates its process-wide allocation counters, initializes shared parser state with a distinct small statement, and asserts that the measured large statement and plan remain uncached. Its128KiB compiler,8MiB execution, context and elapsed-time limits are unchanged. Twenty fresh-process runs pass; an overlay adding9MiB to the measured public execution fails the original8MiB assertion. No production parser change or allocation-threshold change was made. A new uncached full-suite run remains pending.

The first ten audit rendering-test findings are implemented as one catalog-message matrix: ten original named cases,21 renders and73 constructor/render assertions. Original message arguments, exact strings, reported language tags and language order are preserved. Focused matrix and affected-package compile checks pass. These rows remain pending integrated verification and audit review.

### Accepted syntax-view consolidation overhead

The shared syntax view preserves the named public node/token types,16-byte footprint, source-coordinate/text contracts and allocation counts. Eighteen direct public Span/Text workloads (ASCII, Unicode, mapped original/accepted coordinates, hidden trivia and empty EOF) passed independent byte/extent controls in both arms. Eight alternating samples per arm compare the two original source files against the current owner with the same dependencies/harness: measured timing increases1.96–18.16%, with0B/0alloc unchanged. CPU profiles include the benchmark process's untimed initialization and are retained. The maintainer explicitly accepted this ANTLR tradeoff and instructed continuing readiness. This is an accepted measured overhead, not a performance-improvement claim or proof that every cost is unavoidable.

## Current integrated gate and public corrections

The subsequent uncached `GOFLAGS=-count=1 make test` failed after1251.003s:119package passes and failures in internal/testfixture, pkg/cypher, pkg/observability and testing/storagecensus. Preserved evidence: integrated-owner-wave-failed-make-test.log and its receipts under the existing PR1515 evidence directory. Reviewed corrections update only the deliberately changed constructor count, static-owner spelling witness, relocated URL owners and schema-version codec owner location. No checker rule or product assertion was disabled. The unchanged storage mutant test subsequently passed283.913s; its original filtered full-run failure is not explained or waived. A new full gate is required.

The configured default namespace now comes from the supplied resolved configuration once, for memory and persistent engines. Fourteen source/backend cases and retained-data controls pass; two actual HTTP cases fail with the prior source and pass with the correction. Existing keys and payloads are unchanged. Four embedding configuration methods moved unchanged into db_embedding_configuration.go solely for cohesion/file size; their separate publication convergence remains open.

Browser raw-Unicode framing passes32 shared statement fixtures,3 exact UTF16 offset controls and6 actual Zustand editor execution contracts. Existing malformed-source handling is preserved. DeepSeek is independently assessing the defensive impact using an owned HTTP/database instance; no vulnerability or affected-release conclusion is claimed before that evidence is reviewed.

## Subsequent integrated uncached pass

`GOFLAGS=-count=1 make test` passed125test packages in1408.077s. Go/module source fingerprint stayed `b58ef62c337fdae96cfb306b68923fd7f9e7b15ac8613829e143bc4e22a80bb4`. Both lint gates passed. Original receipt records broader runtime fingerprint mismatch; independent reconciliation proves the sole difference was three unimported worker-created UI `.proposal.mjs` files. Archiving only those files restored the exact pre-run broader hash `6ff501a8c4954be789079a1ed48227f054e65d73e2d66876750859be8d9356e1`. Original receipt/log remain unchanged; see integrated-owner-wave-pass-reconciled-receipt.json and worker-lease-recovery/receipt.json. The prior storage mutant failure did not reproduce in this full run; its original cause is still unproved.

This pass qualifies that frozen Go source and imported runtime checkpoint; subsequent renderer/defensive/Fabric/WAL/fixture proposals remain unapplied and need integrated verification after adoption. It does not close the audit inventories or establish final PR readiness.

## Lookup renderer owner integration after the full checkpoint

The existing schema renderer now also owns LOOKUP index CREATE output; the separate lookup renderer is retired and its sole SHOW consumer migrated. Lookup still ignores labels, properties and config; other kinds preserve their existing schema patterns and configuration output. Isolated frozen old/current comparison passes9408outcomes plus independent node/relationship/range/fulltext expected strings (0.499s). Existing public SHOW/schema/lookup suites pass with proposal overlays in native1.494s and ANTLR1.306s; the same scoped suite passes after actual integration. Lexical census3902→3901 removes exactly the retired declaration; all other annotations and the unchanged guard are retained, wholecyphertextPASS2.445s. Evidence: lookup-renderer-owner-proposal/. This is subsequent source and requires a new final integrated gate.

## Existing WAL schema owner qualification

Audit329's related encoder/decoder use one existing walPropertyEpochFrame schema and jsontext codec. The outbound envelope and received-control admission intentionally own distinct directional contracts; forcing encoder validation or deriving output from decoder caches would change persisted behavior. Production and all existing tests remain unchanged. The29existing tests cover real append/read/recovery/seal/reopen/retention/compaction and rejection without rewriting, qualified by the unchanged-source full storage pass527.146s. Four new narrow cases pin independent literal data/header/LElength bytes and CRC32C, finite uppercase/leadingzero spellings independent of stale read caches, and returned partial overflow fields plus exact NumError/ErrRange/error precedence. Focused2groupsPASS0.788s. Source hashes/proofmap/log/receipt: wal-codec-parent-recovery/. Unsafe incomplete worker refactor was rejected. No storage-layout/payload/version/migration change; final integrated gate/checker pending.

## Receipt fixture, cluster issuance and named-argument test owners

The shared receipt helper lives in the existing storagefault subpackage because the resource-owning testfixture base deliberately has no storage dependency. Both local finish methods are retired;12calls across CREATE/catalog publication/durable-drop fixtures now preserve the same prepared pointer, receipt owner and outcome including applied ErrConflict acknowledgement. Four real native fixture cases pass1.363s; actual CREATE/publication/durable-drop matrices pass2.936s/6.740s, canceled completion Sync passes0.911s. Constructor inventory passes unchanged17.798s. Initial compiler failures exposed4additional consumers; they were migrated without assertions changed, and original failed logs are retained. No production storage changes.

One typed-purpose cluster issuance owner now preserves default versus explicit expiry/error/audit behavior. Eight independently expected public contract cases pass original0.894s and current2.965s together with4existing cluster suites. The underlying issueUserJWT signer is byte-identical toHEAD. No validation, API, schema, role, crypto or storage change. One new conservative credential-field census candidate was explicitly reviewed: lexical3901→3902; guard/extractor unchanged.

Storage loader/validation named-argument tests share one table, separate managers percase, original message factories/arguments and Spanish/pseudo expectations; affected public-error suitesPASS1.220s. Evidence directories: receipt-fixture-parent-integration/, cluster-token-parent-integration/, storage-catalog-test-convergence/. Final combined gate remains required after these source changes.

### Latest HTTP and browser source qualification

HTTP catalog DROP shared-owner selected public/cross-protocol/cache-error checks passed (4.160s). Browser malformed-source opaque framing passed the immutable native witness on both parsers, preserving the original request and native error with zero unintended writes; browser scripts passed 32 shared, 3 coordinate, 1 pinned malformed span and 6 store contracts. Private security draft (private advisory evidence retained outside the repository) remains unpublished with no CVE request. These edits postdate the 125-package `make test` checkpoint and still require final integrated verification. Fabric COLLECT and projection-fact convergence remains in progress. Saved audit now records {'implemented-pending-integrated-verification-and-checker': 121, 'open': 229}. No findings are finally closed.

### Integrated Fabric projection and collect owner

The selective eight-file correction retires six private projection/alias helpers into one typed facts reader and integrates collect with the existing count/row plan, query renderer, batch transport and scheduler. Original named input/expected tables remain byte-identical; 40 explicit contract cases also passed the original source. Isolated package (2.243s), current-root overlay (2.347s), race (7.530s), and actual-root package (2.215s) passed. Whole lexical ownership guard passed (1.861s), with reviewed census3911→3908 and unchanged extractor/minimum rules. No benchmark result is claimed. A new uncached `GOFLAGS=-count=1 make test` is running on frozen runtime source; the older full pass does not qualify these edits. Saved audit:133 candidate implementations pending integrated verification/checker,217 open; zero final closures.

### Current integrated gate failures under investigation

The new full run is still active and has reported constructor-ownership and quotation-family guard failures. Both enumerate copied completed-worker source trees under ignored `.codex/local-models/logs` proposal directories. Those proposal copies must be preserved outside the repository, with unchanged guard rules and a full rerun. No test is being skipped or weakened. This is a failed/incomplete run, not qualification of the latest source; any additional failures will be retained and corrected. The source remains frozen while this run finishes.

### Lint-fixture isolation and bounded source modules

The second complete uncached run failed in1082.203s:124packages passed; only `TestCLISecretFamilyPublicOutput/root-help-env` failed because the compiler enumerated a temporary Go sentinel that a parallel lint falsifiability test removed from the live checkout. Before/after fingerprints were equal; they cannot establish absence of transient mutations between snapshots. The correction preserves the actual read-only repository lint baseline and original sentinel bytes/assertions, runs the actual Makefile rule against a private temporary tree with all ten scanned paths, and retains clean→offender rejection→recovery proof. No Makefile, CLI assertion, retry policy or skip was weakened. Combined affected public qualification passed: observability1.464s, CLI secret-output123.614s, permanent HTTP profile/token product+domain audit cases2.041s.

Cohesive declaration moves preserve384original declaration spans and leading comments at the move phase. Subsequent explicitly tracked fixture/GPU test convergence retains every original assertion and actual nine-request population; each saves one line. All modified/new Go files are below2500lines. Exactly three lexical ownership paths moved from Bolt server.go to session_hello.go; the unchanged extractor and minimum still match3908rows with no missing/extra/reordered structural members. Integrated lexical/constructor/Fabric/Bolt focused guards passed. No benchmark or broader local verification result is claimed. Saved audit:146candidate implementations/existing owners pending final integrated verification and checker,204open, zero final closures. A new full gate remains required.

### Focused reference qualification still open

The retained local Neo4j 5.26.30 reference is available again. The current focused
Fabric generated-name run reproduces real NornicDB failures for queries admitted
by that reference, including CALL keyword bindings and projected keyword names.
The run is preserving every failing query and reference rejection separately;
its partial counts are not a final result. These failures must be corrected before
acceptance. No current full-suite pass or merge-readiness claim applies to the
later convergence source.

The completed focused reference run failed (package 329.017s): 541 failing leaf
cases, 29,128 passing nested cases, and 90 cases excluded after actual Neo4j
syntax/semantic rejection. No reference-unavailable skips occurred. Remote apply
completion, union trace, graph-name and typed-parameter top-level tests passed;
generated names, generated separators and scoped CALL tests failed. The original
queries, outputs and unchanged test-source hashes are preserved in the external
reference-final-consumers evidence. The failures remain open for correction.

### Combined native correction reference result

Original seven consumer groups completed against Neo4j5.26.30:493of541recorded failing leaves now pass,48remain,zero new failures and zero unobserved original cases. Six groups pass; generated-name qualification remains failing. All five original test source files are unchanged;90existing oracle-rejected spelling exclusions remain, with no reference-unavailable skips. Focused allocation contracts and the reviewed3912-member lexical population pass. This is a failed focused qualification, not merge readiness or a final make-test pass. Remaining corrections are tracked before the final full gate.


### Final integrated publication gate

`GOFLAGS=-count=1 make test` passed, exit 0 in 949.025s: 125 packages pass, zero failing packages. The checked source starts at HEAD `41220381d6fff9a2236eef2a7c8bd55b340adaaf` with staged tree `e1867265794fbf6ed1217025011966e6fbc43212`, against release base `04c87de3e232fece79ebf3eb5af78e5b3fe5e09f`. Staged tree, Go/module fingerprint `5858d7f44ec5ec30a93cb9e9339314154a2a21900d78e87b91e04e5f40d1fc1f`, runtime fingerprint `83e3f9d09a91d8e235c5dc798568530477e75d016535c994d28be01ebd563913` and all 5389 physical Go inputs remained unchanged. The raw test output is retained separately from the Makefile output filter. Earlier failed/canceled runs remain historical evidence.

Publication afterward updates only this Markdown status record, tasks and current disposition; runtime source is unchanged from the passing gate. The complete saved duplicate audit is used under explicit maintainer authorization; no enabled fresh-checker pass is claimed. Broader verification and new timing benchmarks are deferred. Native parsing remains scannerless recursive descent, with zero-allocation guards on the scoped source products; this is not a claim that complete query execution allocates nothing.
