# Generated GraphQL Schema - Implementation Plan

Status: **Draft, 2026-09-08.**

## Problem

NornicDB's current GraphQL API exposes generic `Node` and `Relationship` types whose application properties are JSON. It also exposes arbitrary Cypher entry points. That API is useful for administration, but clients cannot discover domain types such as `Person`, `Document`, or `KNOWS` through GraphQL introspection.

NornicDB already has enough metadata to generate a useful initial schema:

- label and relationship-type indexes identify graph entity kinds;
- the property-key dictionary identifies durable property names;
- `db.schema.nodeTypeProperties()` and `db.schema.relTypeProperties()` associate properties and observed types with labels and relationship types;
- constraints and indexes provide stronger identity and type hints where they exist.

The first implementation turns that metadata into a reviewable GraphQL SDL artifact. Operators generate the artifact explicitly, place it in a configured read-only directory, and restart NornicDB to activate it. Normal data writes never rewrite the public GraphQL contract.

## Goals

1. Add an admin CLI command that connects to a running NornicDB database, introspects graph metadata, and writes deterministic GraphQL SDL.
2. Validate generated SDL with the same parser and semantic checks used by the server.
3. Load one SDL file per database at server startup and build an executable, read-only typed GraphQL schema.
4. Document exactly where operators place files, how they regenerate them, and that a server restart is required.
5. Preserve the current generic GraphQL API and Cypher endpoints during the initial rollout.

## Non-goals

- No schema regeneration on ordinary graph writes.
- No live reload, file watcher, admin PUT endpoint, or runtime CAS swap.
- No persistence of SDL in the system database.
- No generated mutations or subscriptions in the first release.
- No inference of non-null fields from samples. Observed properties remain nullable unless a declared constraint proves otherwise.
- No field-level authorization directives. Existing database read permission remains the minimum authorization boundary.

## Operator workflow

### 1. Generate a candidate schema

The generator connects to a running server with a principal that has read access to the target database and permission to call schema procedures:

```bash
nornicdb-admin graphql introspect nornic \
  -o ./graphql/nornic.graphql
```

The connection defaults to `NORNICDB_URI`, then `bolt://localhost:7687`.
Credentials use the admin CLI's standard environment variables or credential
store. `--uri` may override the endpoint, but passwords must not be accepted in
a command-line argument that can be exposed through process listings.

Neo4j calls this operation **introspection**. Its current GraphQL Library
provides the programmatic `@neo4j/introspector` API and documents calling
`toGraphQLTypeDefs(sessionFactory)` before writing `schema.graphql`; it does not
provide a shorter official CLI command. NornicDB keeps the same terminology but
provides the native one-line command above.

Generation is deterministic: identical metadata produces byte-identical SDL. Types, fields, directives, and enum values are sorted before rendering. The command writes atomically through a temporary file followed by rename and refuses to overwrite an existing file unless `--force` is passed.

For review workflows, stdout is supported:

```bash
nornicdb-admin graphql introspect nornic -o - \
  > nornic.graphql.candidate

diff -u /etc/nornicdb/graphql/nornic.graphql nornic.graphql.candidate
```

### 2. Validate the artifact

```bash
nornicdb-admin graphql validate ./graphql/nornic.graphql
```

Validation parses the SDL and checks NornicDB directives, generated query roots, duplicate names, GraphQL identifier normalization, relationship target types, filter inputs, and reserved-name collisions. With `--uri` and `--database`, validation also compares the artifact with current database metadata and reports drift without modifying the file.

### 3. Install the schema

Configure a schema directory:

```yaml
graphql:
  schema_dir: /etc/nornicdb/graphql
```

Equivalent environment variable:

```bash
NORNICDB_GRAPHQL_SCHEMA_DIR=/etc/nornicdb/graphql
```

Place one file per canonical database in that directory:

```text
/etc/nornicdb/graphql/
  nornic.graphql
  analytics.graphql
```

Database aliases do not get separate files. The HTTP layer resolves an alias to its canonical database before selecting the schema. The directory should be mounted read-only in containers. NornicDB does not create or modify these files.

### 4. Restart NornicDB

NornicDB reads and validates configured schema files during startup. A missing file means that database has no generated typed endpoint. An invalid file fails startup with the file name and SDL line/column rather than silently serving an old or partial schema.

Regeneration does not affect a running process. Operators review and replace the artifact atomically, then restart NornicDB to activate it.

## Endpoint model

The database must be selected before GraphQL parses and validates an operation because each database can expose a different type system. Typed schemas are therefore served at:

```text
POST /graphql/{database}
GET  /graphql/{database}/schema.graphql
```

The path database may be an authorized alias, but schema lookup uses its canonical name. Authentication and database-scope checks run before schema selection. Unknown, inaccessible, and unconfigured databases must not leak which schema files exist.

The existing `/graphql` endpoint remains unchanged in the first release for backward compatibility. Arbitrary Cypher remains available only through existing authorized API surfaces; generated types give application clients a typed alternative.

## Generated SDL contract

The artifact uses a small NornicDB-owned directive vocabulary:

```graphql
directive @node(label: String!) on OBJECT
directive @property(key: String!) on FIELD_DEFINITION
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

Example output:

```graphql
type Person @node(label: "Person") {
  id: ID @property(key: "id")
  age: Int @property(key: "age")
  name: String @property(key: "name")
  knows: [Person!]! @relationship(type: "KNOWS", direction: OUT)
}

input PersonFilter {
  id_eq: ID
  id_in: [ID!]
  name_contains: String
  age_gt: Int
  AND: [PersonFilter!]
  OR: [PersonFilter!]
  NOT: PersonFilter
}

type Query {
  person(id: ID!): Person
  people(filter: PersonFilter, limit: Int = 50, offset: Int = 0): [Person!]!
}
```

## Metadata and inference rules

Metadata precedence is explicit:

1. Declared constraints and schema contracts.
2. `db.schema.nodeTypeProperties()` and `db.schema.relTypeProperties()` observations.
3. Durable label, relationship-type, and property-key registries.

Rules:

- A label becomes one GraphQL object type.
- A relationship type becomes fields only when source and target labels can be determined. Ambiguous topology is emitted as a warning and omitted from typed traversal in the first release.
- GraphQL names are normalized deterministically. The original database name is always retained in a directive argument.
- Name collisions after normalization are errors, not silently numbered names.
- A property observed with one compatible scalar family maps to that scalar.
- Mixed integer/float observations widen to `Float`.
- Incompatible observations map to `JSON` and produce a warning.
- Unknown or absent properties remain nullable.
- Existence constraints may produce non-null fields after their semantics are verified for the target label/type.
- Unique identity constraints select generated singular lookup arguments.
- Internal embedding and implementation properties are excluded by a stable, documented denylist unless explicitly included by a future override file.

The generator emits warnings as diagnostics and comments at the top of the candidate file. Warnings never change ordering or make output nondeterministic.

## Runtime architecture

An SDL file is not directly executable by the current gqlgen-generated server: gqlgen compiles field resolvers into Go. The typed endpoint therefore needs a generic runtime that executes an `ast.Schema` using metadata-backed field descriptors.

The runtime is built once at startup:

```text
SDL file
  -> gqlparser parse and validate
  -> NornicDB directive validation
  -> immutable type/field registry
  -> generic executable schema
  -> per-database HTTP handler
```

The generic executor translates typed GraphQL selections into parameterized Cypher and runs them through `StorageExecutor`. It must not open storage directly, bypass canonical Cypher authorization, or interpolate argument values into query text.

Start with singular and plural node queries, scalar projections, filters, pagination, and one-hop relationships. Deeper traversal follows only after complexity accounting and query-shape tests are established.

## Package and file layout

| File                                           | Responsibility                                                                                                                    |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `pkg/graphql/schemaintrospect/catalog.go`      | Collect normalized labels, relationship topology, properties, types, constraints, and indexes through a read-only metadata client |
| `pkg/graphql/schemaintrospect/catalog_test.go` | Metadata precedence, mixed types, empty labels/types, and deterministic normalization                                             |
| `pkg/graphql/schemagen/generator.go`           | Pure catalog-to-SDL generation                                                                                                    |
| `pkg/graphql/schemagen/generator_test.go`      | Golden files and byte-for-byte determinism                                                                                        |
| `pkg/graphql/schemagen/validate.go`            | Shared SDL and directive validation used by CLI and server                                                                        |
| `pkg/graphql/schemagen/validate_test.go`       | Syntax, collision, relationship-target, and filter validation                                                                     |
| `cmd/nornicdb-admin/graphql_schema.go`         | `graphql introspect` and `graphql validate` Cobra commands                                                                        |
| `cmd/nornicdb-admin/graphql_schema_test.go`    | Flags, stdout, overwrite protection, atomic output, and diagnostics                                                               |
| `pkg/config/config.go`                         | `graphql.generated_schema_dir` and environment binding                                                                            |
| `pkg/graphql/dynamic/runtime.go`               | Startup-built immutable schema registry and generic execution entry point                                                         |
| `pkg/graphql/dynamic/transpiler.go`            | Typed operation to parameterized Cypher                                                                                           |
| `pkg/graphql/dynamic/handler.go`               | Per-database GraphQL HTTP handler and SDL endpoint                                                                                |
| `pkg/server/server_router.go`                  | Authenticated `/graphql/{database}` routing                                                                                       |
| `docs/operations/configuration.md`             | File location, permissions, configuration, restart, and failure behavior                                                          |
| `docs/user-guides/graphql.md`                  | Generate, review, install, restart, introspect, and query walkthrough                                                             |

Do not modify `pkg/graphql/generated` manually. The existing gqlgen schema and resolvers continue to own `/graphql`.

## Delivery phases

### Phase 1 - generator and validator

- Build the metadata catalog client.
- Implement deterministic catalog-to-SDL generation.
- Add `nornicdb-admin graphql introspect` and `graphql validate`.
- Add golden fixtures representing multi-label nodes, relationship topology, mixed property types, constraints, unsafe names, and empty databases.
- Document generation and review. The generated file is not served yet.

This phase is independently useful: operators can generate a stable schema for review, client design, or use with external GraphQL tooling.

### Phase 2 - startup loader and introspection

- Add configuration and startup-only directory loading.
- Parse every configured file and build immutable per-database registries.
- Register `/graphql/{database}/schema.graphql`.
- Implement GraphQL introspection against the loaded runtime.
- Fail startup on malformed configured files.

### Phase 3 - typed read execution

- Implement parameterized translation for generated root node queries, projections, filters, pagination, and one-hop relationships.
- Execute only through the canonical authorized Cypher executor.
- Add depth, field-count, result-limit, and query-time limits.
- Keep writes unavailable from generated schemas.

### Phase 4 - operational hardening

- Verify read-only container mounts and canonical path handling.
- Add startup metrics and structured diagnostics for loaded schema count, schema hash, load duration, and validation failures.
- Verify GraphiQL and GraphQL Code Generator against a running server.
- Add drift reporting to `graphql validate <file> --uri ... --database ...`.

## Required tests

### Generator tests

1. Seed a real test database with representative nodes, labels, properties, relationships, indexes, and constraints.
2. Run the same catalog collector used by the CLI.
3. Generate SDL and compare it with a checked-in golden file.
4. Generate a second time and assert byte-for-byte equality.
5. Parse generated SDL with gqlparser and run shared semantic validation.
6. Exercise awkward names, normalized-name collisions, mixed scalar types, missing endpoints, empty databases, lists, temporal values, and JSON fallback.

### CLI tests

1. `--output -` writes only SDL to stdout; diagnostics go to stderr.
2. Existing output is rejected without `--force`.
3. Output replacement is atomic and leaves no partial file after failure.
4. Authentication failures and missing schema-procedure permissions return non-zero without printing credentials.
5. Alias input is resolved and the output file uses the canonical database name.

### Startup loader tests

1. No configured directory preserves current behavior.
2. A valid `<database>.graphql` file loads and supports introspection.
3. Invalid SDL fails startup with file, line, and column.
4. Unknown database files fail startup rather than being silently ignored.
5. Alias requests select the canonical database schema.
6. Replacing a file does not change a running server; restart activates the replacement.
7. A non-readable file fails startup without exposing its contents.

### Execution and security tests

1. Generated singular and plural queries return typed scalar fields.
2. One-hop relationship fields execute in one parameterized Cypher request.
3. Filter values containing Cypher syntax remain parameters.
4. A principal lacking database read permission is denied before schema lookup or storage routing.
5. A schema for one database cannot be selected for another database.
6. Introspection and SDL download enforce the same database authorization.
7. Mutation operations are absent and rejected during GraphQL validation.
8. Depth, complexity, result, and timeout limits fail closed.

### Operator acceptance test

The end-to-end release gate uses the actual tool and restart workflow:

1. Start NornicDB and seed a test database through Bolt.
2. Run `nornicdb-admin graphql introspect <database>` against it.
3. Stop the server.
4. Install the generated file in a temporary configured schema directory.
5. Start a fresh server process with that directory configured.
6. Run GraphQL introspection and a typed query through `/graphql/{database}`.
7. Add a new property while the server is running and verify the active schema does not change.
8. Regenerate the file, restart, and verify the new field appears.

## Security requirements

- Generation requires only metadata read access; it never requests write, schema, or admin permission unless a metadata procedure already requires it.
- Database authorization occurs before schema-file selection to avoid exposing database or filesystem existence.
- Configured schema paths are rooted under one canonical directory; database names never become unchecked filesystem paths.
- Symlinks and traversal outside the configured root are rejected.
- SDL errors do not include graph values or credentials.
- Every generated Cypher value is a parameter. Labels, property keys, and relationship types come only from the validated immutable registry and are escaped with the canonical Cypher identifier helper.
- The loaded schema is immutable for the process lifetime.

## Definition of done

- The generator produces deterministic, parseable SDL from a real database.
- Operators can validate and review the generated artifact before deployment.
- Documentation names the configuration key, environment variable, file naming convention, permissions, and restart requirement.
- NornicDB loads valid per-database artifacts at startup and fails clearly on invalid configured artifacts.
- The typed endpoint supports introspection and read-only node queries without requiring clients to send Cypher.
- Existing `/graphql`, HTTP transaction, Bolt, and Bolt-over-WebSocket behavior remains compatible.
- The end-to-end test exercises generation, installation, restart, introspection, typed query execution, schema stability, regeneration, and activation after restart.

## Compatibility statement

The generated SDL borrows familiar `@neo4j/graphql` vocabulary but is a NornicDB contract, not a drop-in implementation of that library. Unsupported directives such as `@auth`, `@cypher`, and `@populatedBy` are rejected. The SDL artifact is intended to be checked into source control and reviewed like an API contract; graph metadata provides a candidate schema, not an automatically changing public API.
