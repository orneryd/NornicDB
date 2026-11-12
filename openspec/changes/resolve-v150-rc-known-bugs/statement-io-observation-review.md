# Statement and I/O observation ownership

Eight native I/O procedure registrations now declare the observations made by
canonical owners: JSON/JSON-array/CSV loading, JSON/CSV graph and query export,
and JSON import. Local file rows are not database hits. Graph export records
its actual node and relationship acquisition populations, including empty
populations. Import uses the existing transaction request counters. Query export
retains its statement-execution declaration and delegates reads to the executor.

Configured HTTP client and host-resolution callbacks are marked opaque only when
actually invoked. Denied access invokes neither provider and records no invented
callback. The default client does not become a custom database-read callback.
Public both-parser controls cover exact local rows, empty/nonempty exports,
constant/MATCH query exports, import requests, actual custom calls and rejection.
Custom registrations retain opaque policy even when replacing these names.

Four dynamic-query and three periodic registrations reuse actual nested statement
observations; their wrappers add no guessed population. Invocation summaries and
transaction-scoped folds remain canonical, including independently committed
units and rollback. Public controls check all seven names against a known two-node
population. Ordinary and nested opaque callbacks still report their actual entry.

## Schema runtime contract

Pinned Neo4j 5.26.30 executes PROFILE schema commands with the SCHEMA runtime and
unknown database-access totals. Nornic keeps that explicit unknown result instead
of claiming zero, or inventing a new maintenance-observation contract. The earlier
temporal-maintenance API proposal is withdrawn, with no Go API migration.

The reference also rejects CREATE INDEX inside apoc.cypher.doIt: data-statement
procedures do not acquire schema execution authority from their caller. Nornic
previously accepted that command. Admission now uses the existing resolved
procedure mode at the existing statement-authorization boundary. The invocation
frame owns that fact in the same-sized allocation as its statistics collector;
transaction scopes forward the same fact, and a nested procedure cannot expand
its enclosing invocation's authority. Schema-mode procedures remain supported.
Both-parser public tests verify refusal before schema mutation and declared
schema-mode success. This changes no storage or wire format and adds no new
query classifier.

Reference evidence: /tmp/v150-native-schema-reference2.log and
/tmp/v150-apoc-schema-reference.log. The latter establishes doIt's behavior,
not separate reference execution of every historical alias. Local regression
coverage includes all four dynamic aliases and both query exporters.

## Measured ordinary cost

Generated native Badger in-memory fixture, result cache disabled, 128 nodes;
three CPU-profiled samples of 500 public executions. Baseline is frozen 2e280ce03,
prior to the final schema-admission addition; a final-source check remains needed.

| Public workload | Baseline/current median ns/op | B/op | Allocations |
| --- | ---: | ---: | ---: |
| Local JSON load | 56,900 / 52,329 | 11,603 / 11,603 | 144 / 144 |
| JSON graph export | 178,069 / 172,492 | 223,249 / 223,119 | 1,642 / 1,642 |
| CSV graph export | 129,795 / 132,066 | 199,979 / 199,979 | 1,265 / 1,265 |
| JSON query export | 401,739 / 394,605 | 333,728 / 333,704 | 4,468 / 4,468 |

Byte ranges overlap. These bounded controls do not establish production throughput.
Raw paired evidence: /tmp/v150-native-io-{baseline,candidate}-bench.txt and .cpu.
All final integrated checks and exact-head CI remain separate gates.

## CI fixture and generated-source corrections

Two new temporal test fixtures called Own without registering their underlying
memory engines with Track. Both now use the canonical fixture lifetime owner;
focused race execution passes in 1.825 seconds. No product assertion is removed.
The generated stored-entity census refresh comes from the failed 2e280ce03 CI
output. Normalizing only badger_temporal_index.go line numbers makes its JSON
identical to the previously committed artifact: populations, rules and decisions
are unchanged. The compiled source has the same storage operations; final CI
must still verify the integrated artifact, rather than assuming a current pass.


Final targeted integrated race checks pass: search 1.962s, Cypher 12.585s,
covering exact fulltext results/lifecycle, direct-reader/scratch invariants,
all new I/O and statement observations, schema runtime/mode admission, and
suppression request/refusal/custom-callback/COMMIT/ROLLBACK behavior. Separate
loader-provider, both-parser tests pass in 2.283s. Relationship declaration,
16/17-cap/isolation and public 128/4096/32768 result controls pass (storage
3.062s, Cypher 4.785s). These are focused checks; no full local sweep is claimed.
An earlier overly broad PROFILE/APOC selection was canceled after 2m9s at about
1.9GB RSS and is incomplete (/tmp/v150-statement-io-final-race.log), not a pass.

Four factual lexical inventory additions describe invocation-mode metadata
transport in the existing stats frames and their constructor. Each uses the
unchanged extractor, data-grammar disposition and public schema-admission tests;
no rule, selector or exclusion changed. The generated census artifact refresh
is byte-identical between 2e280ce03 and 7368982 CI outputs. Its line-only
changes leave writer populations and decisions intact.


After migration, the old localized, literal-description and administrator opaque
registration factories have no production callers. All three private factories
are retired; their three factual lexical records are removed. Existing negative
registration fixtures define their own stubs and remain independent. Canonical
registerProcedure owns metadata completion and every surviving registration's
explicit read policy; no forwarding compatibility factory remains.

Final ordinary I/O samples include the completed schema-mode invocation frame:
JSON load 50,985ns/144 allocations; JSON export 170,431ns/1,642; CSV export
125,947ns/1,265; query export 390,593ns/4,468. All four allocation counts match
the frozen baseline; byte ranges overlap. Three fixed 500-operation samples,
CPU-profiled, /tmp/v150-native-io-final-source.txt/.cpu.
