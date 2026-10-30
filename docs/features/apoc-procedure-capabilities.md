# APOC procedure capability audit (v1.4.3)

The Go APOC library catalog contains 983 names. Its historical list of 118
procedure-only entries is **excluded from the optional plugin's scalar function
exports**; the other 865 entries are scalar functions. The plugin exports 815:
50 registered scalar names belong to the query engine's 52-name native APOC
catalog and are left to their native implementations. `pkg/apocmeta` owns that
catalog once; Cypher's function catalog and `apoc.PluginFunctionNames` both derive
from it. The two native catalog names absent from the Go library do not
change its export count. All ten native collection functions (`toSet`, `union`,
`intersection`, `subtract`, `contains`, `containsAll`, `containsAny`, `indexOf`,
`occurrences` and `frequencies`) are excluded from the real bundled plugin.
Removing a false function declaration does not implement its `CALL` procedure.
Genuine scalar functions, including `apoc.math.sigmoid`, remain function exports
and are not promoted to procedures.

Of those 118 historical entries, 16 have a registered native procedure contract
(including the new `apoc.cypher.doIt`); 102 have no bundled procedure declaration.
The table names every remaining gap. A registered contract is not certification
of full Neo4j/APOC parity. The new `doIt` read/write, parameter/result, exact-case,
and caller-rollback controls were checked independently against the reference.
The generic plugin loader accepts canonical `cypher.ProcedureHandler` and the
existing legacy ABI, so a separately implemented procedure plugin can add real
contracts. It does not synthesize them from function registrations.

## Evidence and classification

- Core procedure/function kinds were queried from Neo4j **5.26.30** Community
  (`sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c`)
  with [APOC Core 5.26.30](https://github.com/neo4j/apoc/releases/tag/5.26.30),
  JAR SHA-256 `4c60fdab264230d60745d31d55d6cb668e66061daba7c5fa6777ed457b49b798`.
  `SHOW PROCEDURES` returned 190 APOC procedures and `SHOW FUNCTIONS` 246 APOC
  functions: 100 of the historical entries were exact procedure matches and none
  were functions.
- Fifteen additional procedure kinds are confirmed from official
  [APOC Extended 5.26.4](https://github.com/neo4j-contrib/neo4j-apoc-procedures/tree/5.26.4/extended/src/main/java/apoc)
  `@Procedure` declarations in `cypher/CypherExtended.java`, `load/LoadCsv.java`,
  `load/LoadDirectory.java`, `load/LoadHtml.java`, `load/LoadLdap.java`,
  `load/LoadParquet.java`, `load/jdbc/Jdbc.java`, and `log/Logging.java`.
  Their kind is a source audit, not a claim that their external services were run.
- Three historical spellings are mixed-case aliases of actual Core procedures:
  `apoc.import.graphML` → `apoc.import.graphml`, `apoc.neighbors.atHop` →
  `apoc.neighbors.athop`, and `apoc.neighbors.toHop` → `apoc.neighbors.tohop`.
  These aliases are not genuine scalar functions either. Use the reference's
  canonical spelling when its procedure is available.
- The NornicDB column comes from the actual initialized native procedure registry
  on the candidate containing the `doIt` fix. No optional user procedure plugin
  was installed for that audit.

## Historical 118-entry disposition

| Historical catalog name | Reference kind / canonical spelling | Bundled NornicDB `CALL` contract |
| --- | --- | --- |
| `apoc.algo.aStar` | Core procedure | Registered native contract |
| `apoc.algo.cover` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.algo.dijkstra` | Core procedure | Registered native contract |
| `apoc.atomic.add` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.atomic.concat` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.atomic.insert` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.atomic.remove` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.atomic.subtract` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.atomic.update` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.coll.split` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.convert.setJsonProperty` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.convert.toTree` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.addLabels` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.node` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.nodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.relationship` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.removeLabels` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.removeProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.removeRelProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.setProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.setProperty` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.setRelProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.setRelProperty` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.uuids` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.create.vNodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.cypher.doIt` | Core procedure | Registered native contract |
| `apoc.cypher.mapParallel` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.cypher.parallel` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.cypher.run` | Core procedure | Registered native contract |
| `apoc.cypher.runFile` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.cypher.runMany` | Core procedure | Registered native contract |
| `apoc.graph.from` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.graph.fromCypher` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.graph.fromData` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.graph.fromDocument` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.graph.fromPath` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.graph.fromPaths` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.import.csv` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.import.graphML` | Core procedure: `apoc.import.graphml` | **Not implemented by bundled runtime/plugin** |
| `apoc.import.json` | Core procedure | Registered native contract |
| `apoc.load.csv` | Extended procedure | Registered native contract |
| `apoc.load.directory` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.driver` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.html` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.jdbc` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.jdbcUpdate` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.json` | Core procedure | Registered native contract |
| `apoc.load.jsonArray` | Core procedure | Registered native contract |
| `apoc.load.jsonParams` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.ldap` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.parquet` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.load.xml` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.lock.all` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.lock.nodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.log.debug` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.log.error` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.log.info` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.log.stream` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.log.warn` | Extended procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.data` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.graph` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.graphSample` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.nodeTypeProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.relTypeProperties` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.schema` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.stats` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.meta.subGraph` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.neighbors.atHop` | Core procedure: `apoc.neighbors.athop` | **Not implemented by bundled runtime/plugin** |
| `apoc.neighbors.toHop` | Core procedure: `apoc.neighbors.tohop` | Registered native contract |
| `apoc.nodes.collapse` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.nodes.cycles` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.nodes.delete` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.nodes.get` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.nodes.group` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.nodes.link` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.path.expandConfig` | Core procedure | Registered native contract |
| `apoc.path.spanningTree` | Core procedure | Registered native contract |
| `apoc.path.subgraphAll` | Core procedure | Registered native contract |
| `apoc.path.subgraphNodes` | Core procedure | Registered native contract |
| `apoc.periodic.cancel` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.periodic.commit` | Core procedure | Registered native contract |
| `apoc.periodic.countdown` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.periodic.iterate` | Core procedure | Registered native contract |
| `apoc.periodic.list` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.periodic.repeat` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.periodic.submit` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.periodic.truncate` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.cloneNodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.cloneSubgraph` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.cloneSubgraphFromPaths` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.collapseNode` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.deleteAndReconnect` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.extractNode` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.from` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.mergeNodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.mergeRelationships` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.normalizeAsBoolean` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.refactor.setType` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.schema.assert` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.schema.nodes` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.schema.relationships` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.search.node` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.search.nodeAll` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.search.nodeReduced` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.stats.degrees` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.text.phoneticDelta` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.add` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.drop` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.install` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.list` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.pause` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.remove` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.removeAll` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.resume` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.trigger.show` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.util.sleep` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.util.validate` | Core procedure | **Not implemented by bundled runtime/plugin** |
| `apoc.warmup.run` | Core procedure | **Not implemented by bundled runtime/plugin** |

## Go handler binding is a separate question

The typed invoker removes the 18-signature dispatcher ceiling and checks arity,
conversions, nulls, numeric bounds, returned errors, cancellation and panics. It
accepts the reported scalar/list/map signatures. This is not a claim that all 983
library entries implement portable Cypher or Neo4j APOC semantics.

For example, this actual catalog contains the following Go callback/function
contracts. Standard Bolt/HTTP Cypher values cannot supply callbacks or carry
function results; these need explicit adapters/capability implementations:

| Catalog entry | Go signature |
| --- | --- |
| `apoc.coll.partition` | `func([]interface {}, func(interface {}) bool) [][]interface {}` |
| `apoc.import.batch` | `func([]interface {}, int, func([]interface {}) error) (*apocimport.ImportResult, error)` |
| `apoc.import.filter` | `func([]interface {}, func(interface {}) bool) []interface {}` |
| `apoc.import.transform` | `func([]interface {}, func(interface {}) interface {}) []interface {}` |
| `apoc.json.filter` | `func(string, func(interface {}) bool) string` |
| `apoc.json.map` | `func(string, func(interface {}) interface {}) string` |
| `apoc.json.reduce` | `func(string, interface {}, func(interface {}, interface {}) interface {}) interface {}` |
| `apoc.lock.withLock` | `func(*lock.Node, func() interface {}) (interface {}, error)` |
| `apoc.lock.withReadLock` | `func(*lock.Node, func() interface {}) (interface {}, error)` |
| `apoc.log.timer` | `func(string) func()` |
| `apoc.merge.conflict` | `func(*merge.Node, map[string]interface {}, func(interface {}, interface {}) interface {}) *merge.Node` |
| `apoc.nodes.batch` | `func([]*storage.Node, int, func([]*storage.Node) interface {}) []interface {}` |
| `apoc.nodes.filter` | `func([]*storage.Node, func(*storage.Node) bool) []*storage.Node` |
| `apoc.nodes.map` | `func([]*storage.Node, func(*storage.Node) interface {}) []interface {}` |
| `apoc.nodes.reduce` | `func([]*storage.Node, interface {}, func(interface {}, *storage.Node) interface {}) interface {}` |
| `apoc.util.partition` | `func([]interface {}, func(interface {}) bool) [][]interface {}` |

Private APOC entity types, I/O interfaces, and multiple non-error return values
also require explicit contracts rather than an automatic claim of transport
compatibility. The 102 missing procedure declarations above remain capability
gaps after the dispatcher, classification and `doIt` fixes; this audit does not
mark them fixed or establish that issue #1167 is wholly resolved.

## Rechecking the plugin export boundary

The plugin source and its regression test use `//go:build ignore` because this
is a separately built Go plugin, not an ordinary package in `go test ./...`.
`make plugin-apoc` runs the export test explicitly before building the plugin.
The focused standalone command (also usable with `-race`) is:

```sh
go test apoc/plugin-src/apoc/apoc_plugin.go apoc/plugin-src/apoc/apoc_plugin_test.go
```
