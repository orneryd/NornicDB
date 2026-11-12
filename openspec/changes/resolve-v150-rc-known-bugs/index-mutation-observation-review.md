# Index mutation PROFILE ownership review

Eight native procedure names share the existing fulltext/vector create, drop,
and vector-property setter owners. Their registrations now report the reads
those owners actually execute instead of marking every invocation opaque.

- Create admission shares the constraint/index catalog readers. Rejection stops
  before later catalogs or publication; failed admission leaves schema unchanged.
- Drop uses the same fulltext/vector definition readers as queries and DROP
  INDEX. A missing or wrong-kind name reports its attempted lookup, never a
  successful removal or service-population lookup.
- Vector setters report actual point attempts and reuse transaction mutation
  request accounting. Supplied images create no synthetic point reads. Unchanged
  vectors create no write or refresh. Actual user mutation callbacks stay opaque.
- Population reports delivered node-label or relationship-type members before
  filtering, plus actual refresh and settled-state reads. Search's existing
  node-image reader now owns both metadata and full-image acquisitions; callers
  select their required image without forwarding implementations.
- Search temporal eligibility records one logical provider request and one
  returned decision, including false. Its native scan fallback separately
  reports the complete physical label acquisition before namespace/key filtering.
  Private history/point helpers remain implementations of the temporal request,
  rather than double-counted logical seeks.

## Accepted work lifetime

The preexisting post-commit refresh used WithoutCancel on the service context,
which also discarded shutdown cancellation. A blocked public refresh regression
fails before correction. The refresh now completes independently of the original
request but stops with its actual service. Observation transfers freeze only a
collector and immutable clause ID; they retain no request values, frame pointer
or statement snapshot. A second transfer preserves attribution and replaces the
work lifetime. Unprofiled transfers allocate zero times.

## Verification

Public both-parser tests cover all six create/drop names, ready-service
reconciliation, ID/supplied node/relationship setters, unchanged images, actual
callback entry, missing/wrong-kind drops, invalid arguments, failed create
admission, missing entities and preserved schema/entity state. Native Badger
current/historical versions additionally cover scalar temporal lookup and the
write-behind list-key scan (four records before filtering).

Integrated focused race run:
`go test -p 1 -race -ldflags=-w -tags=noui,nolocalllm ./pkg/search ./pkg/cypher ./pkg/cypher/fn ./apoc -run '<PROFILE,refresh,policy,inventory selection>' -count=1`
passes on the final runtime source: search 1.858s, Cypher 19.692s,
functions 1.552s, APOC 1.560s.
Raw evidence: `/tmp/v150-index-lifetime-source-final-race.log`.
This is focused verification, not make test or full verification.

Earlier failures remain recorded: missing temporal observations on both parsers;
a component fixture that leaked its backing store and unintentionally retained a
temporal provider; unordered schema-list comparisons; and an old continuation
expectation missing four newly observed temporal decisions. Fixture ownership,
capability masking and unordered comparisons were corrected without weakening
product or acquisition assertions.

## Ordinary refresh costs

Existing public refresh benchmarks, 3 x 1,000 fixed iterations, CPU-profiled
and serialized on this Mac, compare unchanged runtime checkpoint 883c6e521
(guard-only descendant of 6b878f157) with this source:

| Workload | Baseline median ns/op | Current median ns/op | Baseline/current B/op medians | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Committed SET with search refresh | 79,940 | 75,450 | 46,002 / 46,003 | 437 / 437 |
| Relationship create/delete with refresh | 326,455 | 322,289 | 122,067 / 122,070 | 1,460 / 1,460 |

Byte ranges overlap. These small-workload results do not establish production
throughput. The initial transfer added one allocation per refresh; allocation
profiling identified WithoutCancel.Value boxing at the observer lookup. Actual
commit completion retains admittedCtx; refresh receives the original context
only for metadata attribution and independently binds reads to the captured
service. This removes the extra object without dropping service cancellation.
Intermediate samples remain evidence, not the accepted final benchmark.
Raw logs/profiles: /tmp/v150-refresh-owner-{baseline,source-scope}-bench.txt/.cpu;
/tmp/v150-refresh-owner-allocation.mem and allocation-source.txt.

Explicit unchanged lexical discovery adds one reviewed data-grammar row and
retires two records for removed raw-syntax test-only setter wrappers. Their
ordinary cases now exercise public Execute; injected custom-engine update
failures call the typed canonical setter directly. The added row is:
profileReadContext.Access constructs immutable PROFILE ledger identity and shares
the collector's counter storage; it does not inspect credentials or lex queries.
The scoped and root Observer methods implement distinct attribution contracts
without copying acquisition/counter logic. Exact-head CI remains pending.

After retiring the two test-only wrappers, the public setter branch and complete
index/lifetime selection passes under race instrumentation (Cypher 3.810s,
/tmp/v150-index-canonical-setter-final-race.log). Runtime acquisition and
completion code is unchanged from the integrated 19.692s PROFILE pass.
