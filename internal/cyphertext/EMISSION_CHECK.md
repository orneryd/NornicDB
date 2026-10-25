# Cypher name emission checks

[Author: #1486 lane (Codex). Contracts, scope and verification interpretations in this document are the lane’s.]

Decoded relationship types, labels, property keys, variables and aliases are data.
The Go writers routed in `name_emission_inventory.json` delegate to
`cyphertext.QuoteName` or `NameText`, which double embedded backticks. Adding
backticks around an unescaped value does not preserve the name.

## Owners and integration seam

Name emission, relationship declaration/provenance, and installed private
constraint definitions with detached projections remain three separate owners.
DDL property-list admission and installation both consume
`pkg/cypher/schema_property_reference.go:readDDLPropertyReferences`: ordered
source spans with decoded qualifier/key identities and an explicit legacy-bare
mode. Dots, commas and parentheses inside quoted keys are data.

The credentials foundation owns the identifier product and scanners in
`identifier.go` and `name.go`. Name emission is in `name_emission.go` and consumes
that product. The old lower scanner and `name_primitive_adapter.go` have been
removed at the actual credentials restack. The remaining Cypher-layer
`scanSymbolicName` only projects the original written extent returned by
`ScanIdentifier`; its consumers require written source and a cursor before
later decoding. It contains no independent scanning or decoding implementation.
The current credentials parent still has a separately tracked binding-metadata
reader correction; composition verification remains required.

The maintained JS/TS peer is `ui/src/utils/cypherNames.mjs`, imported by the
browser API and both JS migration CLIs. The Python peer is
`scripts/migration/cypher_names.py`. All three runtimes consume
`semantic_fixtures.json`; the browser property SET and Qdrant derived
label/schema/upsert/read builders also have actual production-body subjects.
Knowledge-policy names keep their distinct string-literal grammar and delegate
to the existing string writer/reader, rather than symbolic-name quotation.

## Population admission

`name_emission_inventory.json` retains the 79 historical source groups and
their protocol dispositions. It contains no test-to-production execution map.
The lab recorder generates the execution ledger from observed production call
sites. Unrecorded sites remain not exercised, including when a test symbol
exists. Source dispositions cannot set the exercised column. See
`internal/testutil/cyphernames/RECORDING.md` for the recorder and its controls.

`TestNameEmissionInventory` reconciles repository source cues with that mapping.
It scans all maintained source roots and supported runtime files, including
global initializers and templates. Exact cues are pinned, so new constructions
inside already admitted files also require reconciliation. Additional conservative
hits have explicit source dispositions. Missing groups, dispositions, sources and
unadmitted or removed cues fail. Independent omission controls inject a browser
property writer, same-file global alias, Python label, Go template and obvious
standalone colon/backtick concatenations in .mjs/.ts/.py through
the same discovery/admission operation.

This lexical census detects its documented query/name-construction cues. It
cannot detect arbitrary data flow, computed fragments or indirection; shell and
PowerShell are outside its language set. It proves no universal new-emitter
detection claim.
The semantic subjects establish their named operations and roles, not every
field of every statement or an untested live transport.

## Static smoke check

`TestCypherNameEmissionBoundary` is a **smoke check**, not the safety boundary.
It discovers the active production Go packages with `go list`, populates compiler
identities and checks ordinary literal `fmt.Sprintf` (`%s`, `%v`, `%%`) and string
concatenation at common `:`, `[:`, `-[:` and node/relationship colon slots. Canonical
writer calls and transparent wrappers are recognized by compiler identity. Five
independent negative controls cover these common forms. Discovery/type failures
fail the test rather than silently producing an empty scan.

The check does **not** cover `IS`, whitespace variants, formatter widths or argument
indices, templates, builders, package initializers, other build selections, arbitrary
data flow or other languages. Passing it cannot establish that an emitter is safe.
Do not extend it into a model of arbitrary Go formatting or template execution.
There is no emitter-file allowlist and no claim of fail-closed source analysis.
Bare colon fragments use conventional type-source names; pattern slots do not
depend on variable spelling. Opaque syntax helpers are outside this check.

## Behavioral property

`cyphernames.Names` supplies the common generated population: pipes, colons,
dots, commas, parentheses, embedded backticks, spaces, keywords and Unicode.
`cyphernames.RoundTrip` imports
the **actual emitted relationship bracket**, then reads it through a typed MATCH
and compares stored types and keys. Marker variants preserve the emitter's type
and property bytes: they never decode and re-quote a broken name. Six spellings
cover `:`, whitespace after/before it, `IS`, tabs and newlines. Schema reimports
compare installed types and property keys through public SHOW. The property
exposed a quoted key ending in a space being trimmed during schema decoding;
that adapter now trims only written trivia before canonical identifier decoding.

The property runs over export/ToString, schema export and SHOW recreation,
value/path and label/dynamic rewrites, all eight remote type writers, GraphQL,
Fabric, Heimdall suggestions and Go/Node/Python migration source/target writers.
Tests also retain the public GraphQL execution paths, actual remote transport
statements, migration property updates, legacy migration labels/keys and APOC
label/hint contracts. Construction controls demonstrate that IS, spaced colon,
formatter width, template and initializer implementations need the same property.
Saved production controls remove quoting in the previously exercised families;
their scope is recorded with those receipts. Current browser and Qdrant controls
replace the shared writer with identity in the actual production bodies. They
observe rejected execution or a schema identifier that no longer reparses as
the complete expected name; the native outer DDL grammar's permissiveness is
explicit. Projection, SHOW alias,
path-property and remote-parameter subjects observe non-relationship roles too.

A new emitter must join this behavioral population, including its source
parameters and stored key expectations. Static discovery is useful inventory,
not completeness proof for code that has not been exercised.

Migration brackets retain their actual type, `_neo4j_id` and row expressions,
using visible anonymous endpoints. The pre-existing `_Migrated` visibility policy
and disallowed system graph statements prevent a full migration claim. Peer
adapters execute actual Node/Python writer bodies with driver loading stubbed;
no live external Neo4j/Bolt transfer is claimed. Heimdall's quoted-property EXISTS reader now consumes the shared property-reference reader. Its public quoted-name red/green belongs to the decoded-name verification evidence.
