# Temporal equality and equivalence oracle

`generate_temporal_contracts.go` generates inputs, never expected semantics.
Run it on TrueNAS (`sig/tnrun.sh go <worktree> --allow-dirty -- run
pkg/cypher/testdata/generate_temporal_contracts.go`). Its JSON output contains
the cases and two reference queries. Run `neo` through
`sig/tnrun.sh cypher neo-ref <file>` and `apoc` on Neo4j/APOC 5.26.30.
The latter extends the reviewer's `sol-temporal-r2-apoc-reference.cypher`,
including the split procedure and its partition count.

Record the APOC JSON rows in `temporal_contract_oracle.json`, joined to the
generated name/a/b inputs. Check every row's equality, equivalence, DISTINCT,
grouping and UNION results against the separate `neo-ref` output. Do not derive
set expectations from equality: named-zone overlap datetimes are equal but
belong to different equivalence classes. Null lookup and nested null comparison
also differ from the three-valued `=` operator.

The generator retains the reviewer's 71 valid mixed/scalar families and the
accepted temporal normalization cases. It adds both directions of overlaps in
London, New York, Berlin, Lord Howe (30-minute change), and Auckland; same-offset
controls; region/fixed-offset pairs; every ordered cross-temporal pair; scalar,
list and map/list nesting; and independent null and fresh-NaN policies. Separate
NaN expressions avoid Neo4j's special identity treatment of the same bound NaN.

The permanent test exercises all eleven native collection consumers plus
DISTINCT/grouping/UNION, encoded equivalence and value-lookup adapters, with
literal, WITH, typed parameter and pointer forms, and both expression engines.
The TrueNAS runner selects default and ANTLR parsers. Raw duration and TIME
parameters additionally preserve representations that constructors normalize.
Allocation tests cover both contracts and all fourteen temporal carriers.

`TestCartesianTemporalContracts` also sends the complete oracle population
through public cartesian MATCH predicates (both directions) and a three-node
MATCH ... CREATE equality cycle, then reads back the persisted relationships.
The cycle covers index construction, both lookup directions and the final
constraint check. Its projection control evaluates equality without a join
predicate. Legacy candidate pushdown is checked separately for true values.
Cartesian keys select equality; DISTINCT, grouping and UNION select equivalence.
The 80 overlap scalar/list cases were also executed as stored-property MATCH,
CREATE and relationship readback queries on Neo4j 5.26.30, using
`WHERE coalesce(a.t=b.t,false)` to retain predicate evaluation. Neo4j's extracted
`WHERE a.t=b.t` join itself drops overlap rows, although projected equality and
the coalesced predicate return true. NornicDB's join must preserve the required
equality contract rather than reproduce that optimizer inconsistency.
Nested map properties
are a NornicDB storage extension, so their expectations use the direct-value
reference oracle; Neo4j cannot store maps as node properties.

The complete permanent matrix has 564 cases: the original 396 plus all 168
independent round-3 reviewer samples. The additional fixture retains twelve
explicit reference dispositions. Those core DISTINCT/grouping/UNION observations
collapse overlap occurrences while APOC sets retain both; the required stable
equivalence contract keeps both recursively. The fixture records the observed
values alongside the required values and reasons. Two Chicago deep samples also
differ between the reference instances (eight core-field differences, recorded
in `sol-temporal-r3-reference-comparison-final.log`). These are disclosed reference
divergences, not a claim of uniform Neo4j plan/hash compatibility.

TIME projection preserves the offset at the source instant, even when a region
had another offset on the placeholder date. St John's scalar/list/map/deep
samples cover the producer through all collection and query consumers.
`apoc.map.clean` uses equivalence for its removal set while preserving its
separate numeric-kind boundary recursively; its public tests include overlap,
nested null and integer/float controls.

Every constructor, comparison, hash and index operation requires an explicit
contract argument. There is no zero/default index contract or implicit key
wrapper. `TestValueContractInventory` mechanically enumerates call sites,
including generic instantiations, and checks the declarations/reasons in
`value_contract_calls.json`; `value_contract_calls.md` lists production sites.
Missing/changed/new sites, absent contract arguments and constructor aliases
fail the guard. To inspect a proposed inventory, run the test on TrueNAS with
`--env NORNICDB_VALUE_CONTRACT_INVENTORY=1 -v`; review each required contract and
reason before updating the registry, then run the normal guard without that
environment setting. Inventory mode alone is not guard verification.

Large-inexact integer/float equality is not assigned a uniform expectation:
Neo4j's constant folding and runtime comparison already differ. The prior
review's independent numeric baseline characterization remains unchanged.
