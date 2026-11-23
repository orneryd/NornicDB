# Prevent lossy CSV label exports

NOR-280 confirms that joining a delimiter-bearing label into the Neo4j CSV
`:LABEL` field changes one label into several during public re-import.
The approved ticket allows an explicit actionable export error when a compatible
representation cannot preserve a label. Do not invent an escape or alter
unrelated property-array semantics.

The public owner is `adminimport.ExportNeo4jCSV`, reached directly by
`nornicdb-admin database export neo4j-csv`. Its `exportNodesCSV` boundary must
validate the entire node population before the shared writer opens/truncates
the node file. Keep the existing CSV writer and ordinary/custom-delimiter
round trips. Check importer normalization at the same boundary, and reproduce
any additional loss before extending validation.

Other APOC CSV section/flat formats have different headers and consumers;
trace their contracts before deciding whether they share this selected owner.
No generic CSV redesign or unrelated exporter change is authorized by similarity.

Primary format references checked during this task:

- [Neo4j import header format](https://neo4j.com/docs/operations-manual/current/import/header-format/): LABEL fields use the array delimiter.
- [Neo4j full import options](https://neo4j.com/docs/operations-manual/current/import/full-import/): the array delimiter is configurable. These sources do not establish an escaping scheme for an embedded delimiter; rejection follows the ticket's approved fallback.

Base: `43a39d64f9374dce08ea5fd17b87b17c268137e9` (release/v1.5.2).
Original historical proof and test source remain inline on both Plane records.
