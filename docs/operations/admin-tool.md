# nornicdb-admin

`nornicdb-admin` is the offline operator CLI for NornicDB. Use it for bulk CSV import and other database administration tasks that should not go through the live Bolt server.

## When to use it

Use `nornicdb-admin` when you need to:

- load a database from CSV into an empty target
- import Neo4j-style node and relationship files
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

## Common Flags

- `--nodes` - Node CSV sources. Repeatable.
- `--relationships` - Relationship CSV sources. Repeatable.
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
- After import, verify the loaded data by opening the namespace through the storage API or by starting the database normally.

## Recovery

If import fails, remove or reset the target database directory before retrying. The importer writes in chunks, so partial data can remain after a failed run.

## Related Documentation

- [CLI Commands](cli-commands.md)
- [Data Import/Export Guide](../user-guides/data-import-export.md)
- [Admin Import Plan](../plans/nornicdb-admin-import-plan.md)
