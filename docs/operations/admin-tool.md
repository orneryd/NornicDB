# nornicdb-admin

`nornicdb-admin` is the offline operator CLI for NornicDB. Use it for bulk CSV import and other database administration tasks that should not go through the live Bolt server.

## When to use it

Use `nornicdb-admin` when you need to:

- load a database from CSV into an empty target
- import Neo4j-style node and relationship files
- import a whole Neo4j-compatible CSV package from a directory
- export a NornicDB database as a Neo4j-compatible offline package
- run an import that should build search artifacts after load
- verify the resulting database by namespace after import

## Build

```bash
make build-admin
```

This produces `bin/nornicdb-admin` on the current platform.

## Basic Usage

```bash
nornicdb-admin database import full mydb \
  --nodes=Person=people.csv \
  --relationships=KNOWS=relationships.csv \
  --data-dir=./data \
  --schema=./schema.cypher \
  --report-file=./import-report.json
```

Example `people.csv`:

```csv
:ID,name:string,age:int,:LABEL
u1,Alice,34,Person;User
u2,Bob,29,Person
```

Example `relationships.csv`:

```csv
:START_ID,:END_ID,:TYPE,since:int
u1,u2,KNOWS,2024
```

## Import From A Directory

If you already have a folder containing Neo4j-compatible CSV files, use `--from-path` instead of repeating `--nodes` and `--relationships`.

```bash
nornicdb-admin database import full mydb \
  --from-path=./neo4j-export \
  --data-dir=./data
```

The directory importer:

- scans for `*.csv`, `*.csv.gz`, and single-member `*.zip` CSV files
- classifies node vs relationship files from their CSV headers
- automatically uses `schema.nornic.json` (preferred) or `schema.cypher` from that same directory when present and `--schema` is not set

This makes it possible to import a full offline package produced by `database export neo4j-csv` with a single command.

## Multi-File Sources

The first file in a source must contain the header. Additional files in the same source are read as data files.

```bash
nornicdb-admin database import full mydb \
  --nodes=Person=people-header.csv,people-part-2.csv \
  --relationships=WORKS_AT=works-at-header.csv,works-at-part-2.csv
```

## Different Database Targets

Each database name is isolated inside the same storage directory.

```bash
nornicdb-admin database import full alpha --nodes=Person=alpha.csv --data-dir=./data
nornicdb-admin database import full beta --nodes=Person=beta.csv --data-dir=./data
```

Re-importing into an existing target database is rejected.

## Export A Neo4j-Compatible Package

`database export neo4j-csv` writes a filesystem package that can be imported back with `--from-path`.

```bash
nornicdb-admin database export neo4j-csv mydb \
  --to-path=./neo4j-export \
  --data-dir=./data
```

When schema exists, the export also writes `schema.cypher` alongside the CSV files.

Typical output:

- `nodes.csv`
- `relationships.csv`
- `schema.cypher` when the database has exportable constraints or indexes

Roundtrip back into a fresh target:

```bash
nornicdb-admin database export neo4j-csv mydb --to-path=./neo4j-export --data-dir=./data
nornicdb-admin database import full restored --from-path=./neo4j-export --data-dir=./restored-data
```

## Common Flags

- `--nodes` - Node CSV sources. Repeatable.
- `--relationships` - Relationship CSV sources. Repeatable.
- `--from-path` - Directory containing Neo4j-compatible CSV files for auto-discovery.
- `--data-dir` - Target data directory.
- `--schema` - Cypher file applied after the data load.
- `--report-file` - JSON report written after import.
- `--build-indexes` - Build search indexes after import.
- `--skip-bad-relationships` - Continue past unresolved relationship rows.
- `--skip-duplicate-nodes` - Ignore duplicate import IDs.

## Usage Notes

- The target database must be empty.
- Missing source files fail before any writes begin.
- Composite IDs, labels, vector properties, and named embeddings are supported through the CSV header dialect in the admin import plan.
- `database export neo4j-csv` emits Neo4j-compatible CSV plus `schema.cypher` when the current database schema can be expressed as Cypher DDL.
- After import, verify the loaded data by opening the namespace through the storage API or by starting the database normally.

## Recovery

If import fails, remove or reset the target database directory before retrying. The importer writes in chunks, so partial data can remain after a failed run.

## Related Documentation

- [CLI Commands](cli-commands.md)
- [Data Import/Export Guide](../user-guides/data-import-export.md)
- [Admin Import Plan](../plans/nornicdb-admin-import-plan.md)
