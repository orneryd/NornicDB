# Prepared-candidate and hydration review

Review population: the prepared node-candidate dispatch, its former collectors,
four ID-to-node loops, traversal admission, and fulltext acquisition. This is a
source review of these named candidates, not a completed repository-wide checker
run. Remote Graphify retrieval returned HTTP 400 (HTTP sent to an HTTPS port),
and the local checker entry point lacked `ijson`; neither failure means no
duplicates exist.

| Candidate | Actual behavior and decision | Proof and limits |
| --- | --- | --- |
| Inline lookup versus canonical candidate dispatch | The old `lookupPatternCandidatesUsingPropertyIndex` had only test callers and bypassed the canonical inline admission. Remove it and migrate its tests to the dispatch. | Intersected inline properties and cancellation/retry remain asserted; public traversal controls verify admission. |
| Equality, IN, OR and compound collector entry points | Twelve old collectors called one another but had no production-root consumers. Retire them; component cases use their own existing planners, resolve the selected plan and read it. Keep the ordered-read collector because traversal still calls it. | Full symbol/reference review; twenty-five component cases preserve recognition, parameters, eligibility, cancellation and results. The lexical extractor loses exactly the twelve retired declarations, with no changed continuing members. |
| Four candidate hydration loops | All read listed node IDs and apply route-specific admission. Consolidate the loop with one typed policy request. Keep each route's labels, properties, equality, store and cancellation semantics. | Focused race checks and all ten ordinary benchmark arms retain baseline allocation counts. The first aggregate request shape added 896 bytes/one allocation; separating the engine argument removes that cost. |
| Alleged duplicate identity-property filter | The identity switch cases return before the general residual-filter tail. They do not apply both filters. | Current control-flow inspection and public list-property identity regressions; no extra filtering change. |
| Identity property equality versus Go comparison | Different dynamic list representations compare unequal with Go interface equality; equal uncomparable types can panic. MATCH must use the canonical value comparison. | Preserved public fail-first empty-result evidence on both parsers, then exact-result controls. Do not describe every pair of list values as a panic. |
| Candidate count versus admitted count | The raw count guides traversal orientation; the selected path still hydrates and admits candidates before producing results. It is an estimate, not result authority. | Selected/unselected, empty, singleton and both-orientation public controls. No claim that every estimate equals the final cardinality. |
| Slice admission versus one-node admission | `admitTraversalStartNodes` owns slice copy-on-rejection and invokes `pipelineNodeMatchesPattern`, which owns the predicate. Removing either responsibility would discard required ownership or admission. | Source call relationship plus public pattern/admission tests; no predicate copy retained. |
| Fulltext corpus acquisition versus generic token postings | The current query dialect includes literal substrings, field-scoped matching and its own TF/DF corpus semantics. Generic token postings have not been shown to be a candidate superset. Replacing acquisition with them can drop matches before scoring. | Release-oracle tests cover `hotel`, `ote`, `ot`, phrases, fields, negation, wildcards and ranges with exact scores/results. No generic-postings substitution or completed postings-performance qualification is claimed. |

The fulltext row-window kernel is shared by unranked node and relationship
queries. Ranked acquisition retains the corpus required by scoring; ranked
zero-limit behavior was not changed by the unranked page correction.

Remaining PROFILE work and final-head CI/census are unresolved acceptance
items. The original two census decision-flow findings were corrected under the
approved scoped change; that correction is not a final-head pass or a clean
formal duplicate-checker result.


### Vector cache qualification checkpoint

Fulltext and vector cache-image tests share one configured fixture and the same
detached-image mutation/assertion owners. Vector setup alone installs distinct
managed chunk vectors through the existing sidecar writer; no new read adapter,
production fallback or vector-stripping rule was added. The optional HNSW
first-miss diagnostic runs only after the original public assertion would fail;
it is evidence collection, not a retry or replacement success condition.
The two retired lexical declarations have no continuing source members or
callers; removing their records changes neither the extractor nor its rules.


### GDS catalog read convergence

The pre-existing lookup, project existence check and two drop-phase map reads
now reuse `readGraphProjectionLocked`. It owns acquisition/result accounting;
callers retain their required read/write locks, atomicity and error policies.
Node/edge streams reuse `queryprofile.EntityVisitor`; no parallel counter loop
or copied storage scan was introduced. Shared replacement-policy tests cover
both newly reviewed registrations. A read-only reviewer identified a test that
split inner-read evidence across collectors; it was replaced with a real
canonical catalog read in the same public Execute collector, plus a separate
PROFILE opacity assertion. The earlier test is not claimed as that proof.


### Fulltext literal candidate ownership

The new bit-sliced signature index implements conservative byte-substring
exclusion, not token matching or scoring. The existing generic fulltext postings
are stemmed/tokenized and cannot exclude all literal/field/phrase matches; the
release oracle now verifies the retained dialect on both parsers. Document and
query byte grams share `addLiteralText`; Boolean exact evaluation and lowering
share `ftBooleanNode.occurrence`; candidate visits reuse the existing matcher,
scoring, sort, page and entity-hydration owners. There is no second hydration or
copied exact-match implementation. Complete-corpus body frequencies use the same
selector and then their original exact predicate, never final-hit statistics.
Slot updates and retirement stay under the projection's existing lease/cache
owners. Broader formal duplicate retrieval remains unavailable (HTTP sent to an
HTTPS endpoint), not a clean duplicate-check result.

Integrated public/component race checks pass. Warm fixed-count samples improve
latency/bytes/allocations; latest cold construction costs +6.6% time and +1.6%
bytes, explicitly still open in performance qualification. Do not substitute
the earlier +3% sample or claim every read path is faster.


### Procedure catalog acquisitions and registration

Three repeated catalog point-read sites now use one counted point owner; reload
and list use one counted label-population owner. Both use existing generic
`queryprofile` result/population accounting, and the canonical storage-membership
owner decides whether a transaction adapter already accounts for mutation
requests. The generic matcher/identity admission readers retain their distinct
contracts; this catalog lookup returns the requested stored record without
MATCH predicates or identity-prefix reinterpretation. The unused registration
forwarding function is removed; all five names register with the canonical typed
spec owner. Actual out-of-pipeline invocation reuses the existing runtime record
owner rather than adding a second collector. Full PROFILE race controls pass,
including opaque replacements. Ordinary listing allocation counts remain
113/4,494 at 0/128 records in paired three-sample runs.


Fulltext lowering now reuses the existing immutable Cypher syntax instead of
allocating a second selector tree. One generic candidate compiler accepts both
that syntax reader and explicit selector data. Their adapters only describe
syntax; neither owns selection, storage access or scoring. The existing
candidate index owns a bounded cleared plan arena, borrowed exclusively during
selection and removed while callbacks run so nested selection cannot overwrite
its caller. Its capacity is charged to retained memory; large requests do not
leave oversized scratch behind. Compiler-level cancellation and error controls
cover readers independently of the Cypher adapter. Prepared literal/compound
selection and syntax lowering assert zero allocations, including under race
instrumentation. No duplicate-checker rule or analyzer exemption changes.


Final direct-reader source now has a fresh paired qualification: cold +2.71%
latency/+1.60% construction bytes, warm -71.16% latency with overlapping
allocation ranges. The earlier +6.6% and later +8.1% variants remain recorded,
but are not the final measured source. Cached chunk hits now merge their
immutable signature directly, avoiding initialization of an unused temporary.
The owner, cross-boundary grams and cancellation policy are unchanged.

Suppression converges node/relationship changes through one typed operation and
one actual request counter, then the existing search-refresh owner. The deferred
value retains IDs/kind/desired state only: no statement context, frame or entity
snapshot. A compact operation derives the refresh request when executed instead
of storing duplicate IDs. Explicit COMMIT work belongs to COMMIT; the earlier
CALL reports no mutation request it did not yet perform. Rollback runs nothing.
The observer does not mutate an already-returned CALL plan after COMMIT.
