# Operator-Declared GraphQL Schema - Superseded Plan

Status: **Superseded, 2026-09-08.**

This live-managed SDL design has been replaced by the
[generated GraphQL schema plan](generated-graphql-schema-plan.md). The active
plan generates a deterministic SDL artifact with `nornicdb-admin`, loads it
only at server startup, and requires an operator-controlled restart to activate
changes. The material below is retained as historical design context only.

## Problem

NornicDB's GraphQL endpoint today is, in effect, a second way to invoke
Cypher. Clients send an opaque query string, the server runs it, and the
result is returned as JSON. There is no GraphQL schema in the
GraphQL-spec sense: no introspectable types, no field-level
authorization, no relationship traversal that a client can discover, no
filter inputs auto-derived from properties. A consumer that wants
graph-shaped data still has to know Cypher.

The goal is to give operators a way to expose a real, typed,
introspectable GraphQL schema over their graph that is more useful than
"another way to execute Cypher" — without paying the cost of a fully
auto-inferred schema (which picks wrong types when properties are
mostly null, surprises consumers when a stray write introduces a typo'd
label, and turns every DDL into a schema-rebuild storm).

## Goal

Operators declare the GraphQL schema as an SDL document. NornicDB
validates it against actual graph topology, builds an executable schema,
and serves read-only Query types with relationship traversal. Mutations
and auto-inference are explicitly out of scope for this cut.

## Non-goals

- **No automatic schema generation.** Topology is never converted into
  GraphQL types behind the operator's back.
- **No mutations.** No `Mutation.create<L>` / `update<L>` / `delete<L>`.
  Writes still go through Cypher (HTTP `/db/*/tx/commit`, Bolt RUN, or
  the bolt-over-WebSocket UI driver).
- **No subscriptions.** Live-query support is a separate initiative
  (and would need a write-feed hook in the storage layer that doesn't
  exist today).
- **No schema overlay / merge with inferred fields.** The SDL is
  authoritative or absent; there is no third "partially declared"
  state.
- **No automatic schema rebuilds on data writes.** Schema changes are
  driven exclusively by an admin endpoint. A typo'd label inserted by
  an ingestion bug never affects the GraphQL surface.

## Design

### Source of truth: a single SDL document in the system database

The operator's SDL is stored as a single document in the `system`
database under key `graphql.schema`. This puts it on the same
durability + replication track as the rest of the operator's
configuration (auth users, RBAC roles, retention policies). It also
means a fresh boot from a backup automatically has the schema present.

The document carries:

```go
type schemaDocument struct {
    SDL          string    // raw SDL text the operator submitted
    UpdatedAt    time.Time // last successful PUT
    UpdatedBy    string    // username from the auth context that pushed
    Version      int64     // monotonically incremented per accepted PUT
    LastValidated *validationResult // attached at write time
}

type validationResult struct {
    Status  string // "ok" | "rejected"
    Errors  []validationError // empty when Status == "ok"
    GraphTopologySnapshot string // hash of the topology at validate time
}
```

`Version` is the rebuild epoch. The runtime swap below uses it to
reject stale rebuild requests when two admin PUTs race.

### When the schema is missing

`/graphql` returns **HTTP 503** with body
`{"error": "GraphQL schema not configured. POST SDL to /admin/graphql/schema."}`.
This is the explicit-declaration philosophy: no SDL means no GraphQL.
Health probes are unaffected (the endpoint is `/graphql`, not `/`).

### SDL contract

The operator writes idiomatic `@neo4j/graphql`-style SDL. Three custom
directives bind GraphQL types to the graph:

```graphql
"""
@node binds a GraphQL type to a label in the graph. Required on every
type that backs node data. Without it, the type is treated as a pure
GraphQL scalar wrapper or a relationship payload.
"""
directive @node(label: String!) on OBJECT

"""
@property binds a GraphQL field to a property key on the node. Required
on every scalar field of a @node type. Defaults: key = field name.
"""
directive @property(key: String) on FIELD_DEFINITION

"""
@relationship binds a GraphQL field to a relationship traversal.
Required on every field that returns another @node type or a list
thereof.
"""
directive @relationship(
  type: String!
  direction: RelationshipDirection!
) on FIELD_DEFINITION

enum RelationshipDirection {
  OUT
  IN
  BOTH
}
```

Example operator schema:

```graphql
type Person @node(label: "Person") {
  id: ID! @property(key: "id")
  name: String! @property(key: "name")
  age: Int @property(key: "age")
  email: String @property(key: "email")

  knows: [Person!]! @relationship(type: "KNOWS", direction: OUT)
  managedBy: Manager @relationship(type: "REPORTS_TO", direction: OUT)
}

type Manager @node(label: "Manager") {
  id: ID! @property(key: "id")
  name: String! @property(key: "name")
  reports: [Person!]! @relationship(type: "REPORTS_TO", direction: IN)
}

type Query {
  person(id: ID!): Person
  people(filter: PersonFilter, limit: Int = 50, offset: Int = 0): [Person!]!
  manager(id: ID!): Manager
}
```

The operator does **not** write the `PersonFilter` input — the schema
builder synthesizes it from the `@property` fields on `Person` (see
"Filter input synthesis" below).

### Filter input synthesis

For every `@node` type, the schema builder emits one filter input with
mechanical operators per scalar property type:

| GraphQL type    | Operators emitted                                                                |
| --------------- | -------------------------------------------------------------------------------- |
| `String` / `ID` | `<f>_eq`, `<f>_ne`, `<f>_in`, `<f>_contains`, `<f>_starts_with`, `<f>_ends_with` |
| `Int` / `Float` | `<f>_eq`, `<f>_ne`, `<f>_in`, `<f>_lt`, `<f>_lte`, `<f>_gt`, `<f>_gte`           |
| `Boolean`       | `<f>_eq`                                                                         |
| Enum            | `<f>_eq`, `<f>_in`                                                               |
| List of scalar  | `<f>_includes`, `<f>_excludes`                                                   |

Plus three logical combinators: `AND: [PersonFilter!]`,
`OR: [PersonFilter!]`, `NOT: PersonFilter`.

Synthesis is pure: SDL → AST → filter inputs → enriched AST → executable
schema. No graph IO.

### Validation

Validation happens at two boundaries: at PUT time (operator's SDL),
and at boot (existing SDL re-checked against current topology). The
validator walks the parsed SDL AST and verifies:

| Check                                                                                                                         | Failure mode                     |
| ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| Every `@node(label:)` references a label that has at least one node OR appears in the label registry                          | `unknown_label`                  |
| Every `@property(key:)` is in `PropertyKeyDict` (or matches the field name when `key` is omitted)                             | `unknown_property`               |
| Every `@relationship(type:)` is a known relationship type with at least one edge OR appears in the relationship-type registry | `unknown_relationship_type`      |
| Every relationship field returns a `@node` type                                                                               | `relationship_target_not_a_node` |
| Every scalar field on a `@node` type carries `@property`                                                                      | `missing_property_directive`     |
| Every type referenced from a `@relationship` target type is itself a `@node` type                                             | `relationship_target_unbound`    |
| `Query` field arguments are valid (no unknown filter keys, `limit`/`offset` are non-negative)                                 | `invalid_query_argument`         |
| The SDL parses as syntactically valid GraphQL                                                                                 | `syntax_error`                   |

Validation **does not** require that data exist for every label. An
operator declaring a `type Tombstone @node(label: "Tombstone") { ... }`
on an empty graph is valid; the GraphQL queries against it just return
empty results until data is written.

Validation errors carry line numbers from the SDL parser so operators
can find the offending line in their editor.

### Resolution strategy: SDL → Cypher transpilation

For each GraphQL query, the resolver compiles the operation + selection
set into a single Cypher statement, runs it through the existing
Cypher executor, and shapes the result back into the GraphQL response
shape.

Why Cypher instead of direct storage scans:

- The Cypher planner already does label scans, property filters,
  index selection, and relationship traversal.
- All existing access-control checks (per-DB read mode, per-principal
  RBAC) fire at the Cypher boundary; reusing them avoids re-implementing
  authorization in the GraphQL layer.
- Future Cypher-planner improvements (e.g., new index types, better
  filter pushdown) propagate to GraphQL automatically.

The cost is the planner's per-query overhead, which is the existing
dominant cost for any non-trivial query.

#### Translation table

| GraphQL                                  | Cypher                                                                           |
| ---------------------------------------- | -------------------------------------------------------------------------------- |
| `Query.person(id: $id)`                  | `MATCH (n:Person {id:$id}) RETURN n`                                             |
| `Query.people(filter: {...}, limit: 10)` | `MATCH (n:Person) WHERE <filter-cypher> RETURN n LIMIT 10`                       |
| `person { name age }`                    | `RETURN n {.name, .age}`                                                         |
| `person { knows { name } }`              | `MATCH (n)-[:KNOWS]->(m:Person) RETURN n, collect(m {.name}) AS knows`           |
| `person { managedBy { name } }`          | `OPTIONAL MATCH (n)-[:REPORTS_TO]->(m:Manager) RETURN n {.id, .name}, m {.name}` |

For nested traversals deeper than one hop, the transpiler emits a
single Cypher query with `OPTIONAL MATCH` per level and `collect()`
aggregation per list field. This avoids the N+1 problem that would
otherwise dominate any traversal-heavy query.

#### Filter compilation

`PersonFilter { name_contains: "ali", age_gt: 30 }` becomes
`WHERE n.name CONTAINS 'ali' AND n.age > 30`. Logical combinators
nest: `{ AND: [{name_eq: "Alice"}, {OR: [...]}] }` becomes
`WHERE (n.name = 'Alice' AND (...))`.

The transpiler emits parameterized Cypher, never string-interpolated
values. Filter values flow through `$param0`, `$param1`, etc.

### Runtime swap

`Server.graphqlSchema atomic.Pointer[*graphqlRuntime]`, where
`graphqlRuntime` bundles the parsed schema + the version + a built
type registry the resolvers consult.

- **Boot**: read SDL from system DB → validate → build →
  `Store(runtime)`. If SDL is missing, store nil; `/graphql` returns
  503 until the operator pushes one.
- **PUT**: validate the submitted SDL → build into a new runtime →
  CAS-store. The CAS prevents two racing PUTs from both accepting; the
  loser retries (returning 409 to the client with "schema changed
  during your write, GET and merge").
- **DELETE**: store nil. Future GraphQL requests get 503.
- **GET**: `/graphql` reads the current runtime atomically. No locking
  on the request path. `/graphql/schema.sdl` returns the stored SDL
  text (separate from the executable schema so it stays valid even if
  the build step ever produces a non-deterministic representation).

### Admin endpoints

| Endpoint                         | Method | Auth        | Effect                                                                                                                   |
| -------------------------------- | ------ | ----------- | ------------------------------------------------------------------------------------------------------------------------ |
| `/admin/graphql/schema`          | GET    | `PermAdmin` | Returns `{sdl, updated_at, updated_by, version, last_validated}`                                                         |
| `/admin/graphql/schema`          | PUT    | `PermAdmin` | Body is SDL text. Validates + builds + swaps. 200 on success, 422 with errors on validation failure, 409 on CAS conflict |
| `/admin/graphql/schema`          | DELETE | `PermAdmin` | Clears the schema. `/graphql` returns 503 until next PUT                                                                 |
| `/admin/graphql/schema/validate` | POST   | `PermAdmin` | Validate without persisting. Lets operators check SDL before they push it. Returns the same `validationResult` shape     |

The PUT response body on success carries the validation result so
operators can see warnings (declared types with no data backing yet)
even on a successful save.

### Public endpoints

| Endpoint              | Method | Auth            | Effect                                                                                                                        |
| --------------------- | ------ | --------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `/graphql`            | POST   | Any auth'd user | Executes a GraphQL query against the current schema. 503 when no schema is configured                                         |
| `/graphql/schema.sdl` | GET    | Any auth'd user | Returns the current SDL. Useful for tooling that wants to type-generate clients                                               |
| `/graphql`            | GET    | Any auth'd user | Returns introspection result (the GraphQL spec's `__schema` query result) when called without a body. Same 503 when no schema |

The introspection endpoint is what makes the dynamic schema "real" —
GraphiQL, GraphQL Code Generator, Apollo Studio, and anything else
that speaks GraphQL discovers the operator's types automatically.

### Per-database scoping

GraphQL queries are scoped to one database per request. The HTTP layer
takes a `?database=<name>` query param (or, if absent, uses the
default database from discovery). The Cypher transpiler emits queries
against that database via the existing `USE <db>` mechanism.

The SDL is **per-database**: the system DB key is
`graphql.schema:<dbname>` so different databases can expose different
shapes. The admin endpoints take an optional `?database=<name>` to
target one; absent, they target the default DB.

This matches how `dbconfig` already namespaces per-DB settings and
keeps the GraphQL surface coherent when an operator runs multiple
graph schemas on one cluster.

## Call sites

| File                                             | Change                                                                                                                    |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `pkg/graphql/sdl_store.go` (NEW)                 | `SDLStore` interface; `systemDBSDLStore` impl reads/writes per-DB SDL documents in system DB                              |
| `pkg/graphql/sdl_store_test.go` (NEW)            | Round-trip, version-bump, missing-key, multi-DB isolation                                                                 |
| `pkg/graphql/validator.go` (NEW)                 | `Validate(sdl, topology) []validationError`                                                                               |
| `pkg/graphql/validator_test.go` (NEW)            | Unknown-label / unknown-property / unknown-rel / syntax-error / valid-but-empty-data cases                                |
| `pkg/graphql/filter_synth.go` (NEW)              | `SynthesizeFilters(parsedSDL) parsedSDL` — emits `<Type>Filter` inputs from `@property` fields                            |
| `pkg/graphql/filter_synth_test.go` (NEW)         | Per-scalar-type operator coverage; AND/OR/NOT nesting                                                                     |
| `pkg/graphql/transpiler.go` (NEW)                | `Transpile(operation, schema) cypherStatement` — selection set → Cypher RETURN clause                                     |
| `pkg/graphql/transpiler_test.go` (NEW)           | One-hop and two-hop traversal; filter compilation; parameterization assertions                                            |
| `pkg/graphql/runtime.go` (NEW)                   | `graphqlRuntime` bundle (parsed schema + version + type registry); `BuildRuntime(sdl, topology) (*graphqlRuntime, error)` |
| `pkg/graphql/runtime_test.go` (NEW)              | Boot path; CAS swap on PUT; nil after DELETE                                                                              |
| `pkg/server/server_graphql.go` (MODIFY)          | `/graphql` POST routes through transpiler when a runtime is loaded; 503 otherwise. `/graphql` GET introspects             |
| `pkg/server/server_admin_graphql.go` (NEW)       | `/admin/graphql/schema` GET / PUT / DELETE / `validate`                                                                   |
| `pkg/server/server_router.go` (MODIFY)           | Register the four admin endpoints + the public introspection/SDL endpoints                                                |
| `pkg/server/server.go` (MODIFY)                  | Server struct gains `graphqlSchema atomic.Pointer[graphqlRuntime]`. Boot loads schema per default DB                      |
| `cmd/nornicdb/main.go` (MODIFY)                  | Plumb `pkg/graphql.NewBuilder(...)` into server construction                                                              |
| `docs/operations/configuration.md` (MODIFY)      | New "GraphQL Schema" section: how to push SDL, what's validated, what 503 means                                           |
| `docs/user-guides/graphql.md` (MODIFY or CREATE) | Operator walkthrough: write SDL, push via curl, query via GraphiQL                                                        |

## Phasing

**Phase 1 — primitives**

- `pkg/graphql/sdl_store.go` + tests
- `pkg/graphql/validator.go` + tests (without filter synthesis or transpilation; just AST walk + topology check)
- Admin GET/PUT/DELETE endpoints serving the SDL store; PUT runs the validator. No `/graphql` changes yet, so submitting SDL doesn't affect anything live.

This is the smallest landable unit. Operators can save and validate
SDL before any execution path exists.

**Phase 2 — filter synthesis + executable build**

- `pkg/graphql/filter_synth.go` + tests
- `pkg/graphql/runtime.go` + tests
- `Server.graphqlSchema atomic.Pointer[graphqlRuntime]`; boot loads schema; PUT triggers swap.
- `/graphql` GET returns introspection; POST returns 501 ("transpiler not yet wired").

**Phase 3 — transpiler + execution**

- `pkg/graphql/transpiler.go` + tests
- `/graphql` POST executes via transpiler → Cypher executor → response shaping.
- One-hop traversal; multi-hop nested selection sets.

**Phase 4 — docs + driver verification**

- Update `docs/operations/configuration.md` with the schema-push protocol.
- Update `docs/user-guides/graphql.md` with operator walkthrough.
- Verify with GraphiQL in a browser tab against a running NornicDB.
- Verify with `graphql-code-generator` produces typed TypeScript clients
  from the SDL — that's the practical "more useful than Cypher" end
  state.

## Test scenarios

### SDL store

ST1. Boot with no SDL document → store returns `not found`; runtime is nil.
ST2. PUT then GET round-trips text-identically.
ST3. Two concurrent PUTs: first wins, second fails with CAS conflict.
ST4. Multi-DB: PUT to `?database=foo`, GET on `?database=bar` returns 404 (independent schemas).
ST5. DELETE then GET returns 404.

### Validator

V1. Unknown label in `@node(label:)` → `unknown_label` error with the SDL line number.
V2. Unknown property key in `@property(key:)` → `unknown_property`.
V3. Unknown relationship type → `unknown_relationship_type`.
V4. Relationship field returns a non-`@node` type → `relationship_target_not_a_node`.
V5. Scalar field missing `@property` directive → `missing_property_directive`.
V6. Syntax error → `syntax_error` with line + column.
V7. Empty graph + valid SDL → `ok` (data presence is not required).
V8. Two `@node` types with the same label → `duplicate_label_binding` error (clarity, not technical correctness — the second one is unreachable).

### Filter synthesis

F1. `String` field → emits `_eq`, `_ne`, `_in`, `_contains`, `_starts_with`, `_ends_with`.
F2. `Int` field → emits `_eq`, `_ne`, `_in`, `_lt`, `_lte`, `_gt`, `_gte`.
F3. List-of-scalar field → emits `_includes`, `_excludes`.
F4. AND/OR/NOT combinators present on every emitted filter type.
F5. Nested filter type for relationship targets → does NOT auto-emit (operators query relationships through traversal, not filtered).

### Transpiler

T1. `person(id: "x")` → `MATCH (n:Person {id:$0}) RETURN n {.id}`. Selection set drives the RETURN map projection.
T2. `people(filter: {age_gt: 30}, limit: 10)` → `MATCH (n:Person) WHERE n.age > $0 RETURN n {.id, .name, .age} LIMIT 10`.
T3. `person(id:"x") { knows { name } }` → one Cypher round-trip with `OPTIONAL MATCH (n)-[:KNOWS]->(m:Person)` + `collect(m {.name}) AS knows`.
T4. Two-hop nested traversal `person { knows { knows { name } } }` → `OPTIONAL MATCH ... -[:KNOWS]->()-[:KNOWS]->...` with `collect(collect(...))` aggregation. Bounded by GraphQL's max depth (configurable, default 6).
T5. Direction `IN` → `MATCH (n)<-[:R]-(m)`. Direction `BOTH` → `MATCH (n)-[:R]-(m)`.
T6. Filter values are parameterized; assert no string interpolation by checking the emitted Cypher contains `$0`/`$1` and not the literal value.
T7. AND/OR/NOT compile to parenthesized Cypher boolean expressions.

### Runtime + endpoints

R1. PUT valid SDL → 200; subsequent GET `/graphql` introspection succeeds.
R2. PUT invalid SDL → 422 with line numbers; previous schema (if any) still active.
R3. POST `/admin/graphql/schema/validate` with valid SDL → 200 + validation result; nothing persisted.
R4. POST `/admin/graphql/schema/validate` with invalid SDL → 200 + errors (NOT 422; the validate endpoint always returns 200, the body conveys the result).
R5. DELETE → 200; subsequent `/graphql` POST returns 503.
R6. Schema not configured → `/graphql` POST returns 503; introspection returns 503; `/graphql/schema.sdl` returns 404.
R7. Per-DB isolation: schema set on `db_a` does not affect `/graphql?database=db_b`.

### Manual / smoke

M1. **GraphiQL discovery**: load GraphiQL pointed at `/graphql`, confirm types appear in the docs panel without manual config.
M2. **Type-generation**: `graphql-code-generator --schema http://host:7474/graphql` produces a TypeScript client; sample query compiles + runs.
M3. **Cypher fallback**: confirm Cypher endpoints (`/db/*/tx/commit`, Bolt) still work for writes when GraphQL is configured.

## Risks / open questions

1. **Cypher injection via filter values.** The transpiler must emit
   parameterized Cypher exclusively. Tested explicitly (T6). Any
   future shortcut that string-interpolates a filter value is a
   security regression.

2. **Selection-set depth bombing.** A GraphQL query with deeply nested
   traversal can produce a Cypher statement the planner refuses or
   that runs unbounded. Mitigated by a configurable max depth
   (default 6). Beyond the depth, the resolver returns a GraphQL error
   field-level, not a 500.

3. **Schema staleness vs. data shape.** The validator runs against
   topology at PUT time. If the operator deletes the last node of a
   label after PUT, the schema still references it. This is
   intentional: GraphQL is a contract, not a mirror. Querying through
   the schema returns empty results; the schema stays declared.

4. **Authorization granularity.** This cut applies the existing
   per-DB read mode + per-principal RBAC at the Cypher boundary. It
   does NOT add field-level authorization (`@auth(rules: [{...}])`).
   That's a Phase-5 expansion if there's demand.

5. **Performance ceiling.** Compile-to-Cypher per request adds the
   Cypher planner's per-query overhead on top of GraphQL parsing.
   Acceptable for read-heavy interactive workloads; for tight loops
   the operator should still use Bolt directly.

6. **Validator schema drift.** When new property keys are added by
   data writes, an existing schema declaring those properties is
   unaffected; declaring NEW properties requires an SDL update. There
   is no "incremental discovery" mode — that would amount to inference
   and contradicts the design.

## What stays the same

- All existing Cypher endpoints (`/db/*/tx/commit`, Bolt, Bolt-over-WS).
- All existing auth + RBAC checks.
- All existing observability (request latency, error rates, query duration).
- The HTTP server's middleware stack.

## Definition of done

- Operator can `PUT /admin/graphql/schema` with an SDL referencing
  `@node` / `@property` / `@relationship` directives, get 200 on
  acceptance, 422 with line numbers on failure.
- `GET /graphql/schema.sdl` returns the operator's SDL byte-for-byte.
- `POST /graphql` with a query against a configured schema executes via
  Cypher transpilation and returns a GraphQL response shape.
- `GET /graphql` (introspection) returns a `__schema` document compatible
  with GraphiQL and `graphql-code-generator`.
- Filter inputs are auto-emitted from `@property` fields; per-scalar
  operator coverage matches the table above.
- Relationship traversal (1-hop and N-hop within max depth) compiles
  to a single Cypher round-trip via `OPTIONAL MATCH` + `collect()`.
- Schema is per-database; per-DB isolation tested.
- All filter values pass through Cypher parameters; injection-test (T6)
  passes.
- Schema not configured → `/graphql` returns 503 with a clear message.
- Documentation:
  - `docs/operations/configuration.md` has a "GraphQL Schema" section.
  - `docs/user-guides/graphql.md` has an operator walkthrough +
    SDL example + curl push command.
- All existing GraphQL tests still pass; Cypher tests unaffected.

## Compatibility statement

This implementation is independent of `@neo4j/graphql` but borrows its
directive vocabulary (`@node`, `@property`, `@relationship`,
`RelationshipDirection`) so operators familiar with that library find
the SDL idiomatic. It is NOT a drop-in replacement: NornicDB does not
implement `@neo4j/graphql`'s full directive set (no `@auth`, no
`@cypher` custom resolvers, no `@populated_by`, no temporal directives).
The cut covered here is the read-only + traversal slice that delivers
the most consumer value with the smallest surface area.
