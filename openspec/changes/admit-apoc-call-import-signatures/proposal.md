# Admit the registered import and runMany signatures through exported `apoc.Call` (N152-56 / NOR-175)

`apoc.Call(name, args)` answered through the published registry but with
`registry.APOCArguments`, and that policy's plan table (`apoc/registry/invocation_policy.go`)
had no entry for either registered signature of the import family or of the dynamic-Cypher
runners. Every one of the ten registered names sharing those signatures was therefore
refused before handler entry with

```
unsupported function signature: the implementation was not invoked:
the 1 argument policy cannot express func(string, map[string]interface {}) (...)
```

even though `apoc/plugin_exports.go` publishes each of them and the same callable answers
`registry.CypherArguments`. The reported calls (`apoc.import.cypherData("CREATE (:A)", {})`
and `apoc.cypher.runMany("CREATE (:A)", {})`) are two members of that population, not two
spellings.

The correction extends the one canonical APOC plan owner with the two declared shapes: the
source text or path stays strict text, the parameters stay a string-keyed map, and each
handler keeps its own result and error. Handler identity, the `GoArguments` and
`CypherArguments` policies, the closed root surface for undeclared shapes, and the
pre-entry rejection of bad arity or argument types are unchanged.

The affected handlers are argument-derived summaries and placeholders. They do not import
data and do not execute queries; typed-policy success and the result counts reported by
this change are not evidence of database writes.

## Scope

- In scope: admission of the two registered signatures through the exported Go boundary,
  and the regression that pins the complete population.
- Out of scope: implementing CSV/GraphML/JSON imports, query execution, or any transport
  route; adding plans for other undeclared signatures; changing the Cypher or
  plugin-typed policies.
