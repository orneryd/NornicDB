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

Large-inexact integer/float equality is not assigned a uniform expectation:
Neo4j's constant folding and runtime comparison already differ. The prior
review's independent numeric baseline characterization remains unchanged.
