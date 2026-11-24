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

## Value capacity

Full import uses the ordinary server’s high-performance disk profile: each encoded stored value may be at most 256 MiB. A property’s available space is smaller because the encoded value also contains entity metadata. This lets the tool import CSV exports containing values accepted by the ordinary server. The profile also selects the server’s larger memtables and caches; allow memory for those and for the largest CSV record.

The embedded balanced disk profile still has a 128 MiB value limit, and the explicitly selected low-memory profile has a 32 MiB limit. A large value accepted by the ordinary server can be read from an existing store with those profiles, but rewriting it or restoring it into a fresh smaller-capacity store can fail. Use a destination whose per-value capacity holds the original encoded value; snapshot framing does not increase storage capacity. No values are truncated.

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

## Database Storage Layout

NornicDB 1.5.3 and later support isolated, shared, and mixed layouts. On persistent standalone servers, newly created user databases default to their own Badger store and WAL. The bootstrapped default database and `system` use the shared root. Existing databases keep their storage layout. An existing populated shared namespace stays shared when it is first registered. Choose the layout in the **Create database** panel or in Cypher. See the [storage folder diagrams](../architecture/multi-db-architecture.md#storage-folder-layouts).

```cypher
CREATE DATABASE analytics OPTIONS {storage: 'isolated'};
CREATE DATABASE tenant_a OPTIONS {storage: 'shared'};
```

Storage layout is set at creation. Each isolated store has its own caches and background maintenance. Shared storage pools those resources, which can be useful when hosting many small databases. In-memory and replicated deployments use shared storage.

Each dedicated store writes its WAL under `prepared-databases/<store-id>/wal/`. WAL auto-compaction writes recovery snapshots under `prepared-databases/<store-id>/snapshots/` when enabled. On startup, an isolated store with corruption can recover from its own usable snapshot and WAL. A store seeded by import or restore needs a recovery snapshot to cover data written before its first WAL entry. If recovery cannot open a store safely, that database stays offline and the rest of the server starts.

`POST /admin/backup` and Qdrant full snapshots include all local databases and the system catalog. The request is the same for either layout. Shared-only installations use the native Badger file format. Installations with dedicated stores write one versioned archive of native Badger backup streams, including the shared root and each dedicated store. The archive contains logical Badger backup records. Raw WAL files and auto-compaction snapshot files live in their own directories for local recovery. Each backup stream is consistent at the time it is captured. Database creation, publication, and deletion wait for the backup's catalog view to finish.

To recover either native backup format, restore into a new data directory and start the server with that directory.

```bash
nornicdb-admin database restore ./full.backup --data-dir=./restored-data
```

The restore command reconstructs the catalog and stores in a temporary directory and publishes the new data directory after validation. The destination must be absent before restore. Pass the destination server's `--config` or encryption environment settings to create encrypted Badger files with a fresh salt or provider-wrapped key. Archives from encrypted mixed installations require encryption settings at restore time. Native backups contain portable Badger streams. Protect backup files separately from the database directory. The online `/admin/restore` endpoint supports single-store restores into shared-only installations.

```bash
nornicdb-admin --config ./nornicdb.yaml database restore ./full.backup --data-dir=./restored-data
```

When the catalog contains remote database credentials, use the same remote-credential encryption settings on the recovered server. An explicitly configured `NORNICDB_REMOTE_CREDENTIALS_KEY` is independent of the Badger key.

CSV export follows the database's catalog entry and can export a dedicated store while the server is stopped. `database check-ids` checks the root and committed dedicated stores. Supply the server's encryption settings when operating on an encrypted data directory.

## Stage And Publish A Database While The Server Runs

To import into a new database on a persistent standalone server, run the CLI on the server's filesystem with the same data directory the server uses. For an encrypted server, pass its `--config` or matching encryption environment settings so the staged files use the installation key. The CLI prints an import ID after the new Badger store is sealed and closed. Other databases can continue serving requests during the build.

```bash
nornicdb-admin database import full wiki --stage \
  --from-path=./wiki-csv --data-dir=./data
```

Publish with a server administrator credential. Fine-grained authorization uses the `dbms.database.create` permission. `wiki` is the name used during staging and must be available in the catalog.

```bash
curl --fail-with-body -u "$NORNICDB_USER:$NORNICDB_PASSWORD" \
  -H 'Content-Type: application/json' \
  --data '{"database":"wiki","import_id":"<import-id>"}' \
  http://localhost:7474/admin/databases/imports/publish
```

Publication validates the closed store and records it in the catalog. It leaves the graph in the same files and accepts retries with the same database name and ID. The staged importer loads graph data and applies CSV schema using the same native full-import writer. The server builds search artifacts after publication.

## Offline Imports Into The Shared Store

Without `--stage`, stop the server and open the shared storage directory for import.

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

A label cannot contain the selected `--array-delimiter` (default `;`). Export
rejects such a label with exit code 6 before creating or replacing package
files. Choose a delimiter absent from every label and pass it to both export
and import, for example `--array-delimiter='|'` for a graph containing `A;B`
when no label contains `|`.

Empty labels and labels with surrounding whitespace are also rejected because
label import would drop or trim them. Interior spaces and newlines, quotes,
commas and Unicode retain their values when they do not contain the selected
array delimiter. This validation does not introduce a label escape encoding.


Export preserves absent properties as bare empty CSV cells and present empty
strings as quoted empty cells (`""`). Import keeps that distinction;
`--ignore-empty-strings` discards quoted empty strings too. Ordinary float64 lists
use `double[]` headers and keep their precision; float32 vector properties and
named embeddings retain their existing coordinate format.

Older exports that encoded absence and empty strings identically cannot recover
that distinction. Re-export the original database for a lossless round trip.

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
- `--stage` - Build a dedicated store and print its import ID for online publication. The server owns search index generation in this mode.
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

## Check The ID Dictionary

Before #907's I29 fix, a node or relationship created with a fixed ID (a
`MERGE`d relationship's ID is derived from its endpoints and properties) in a
rolled-back transaction, then created again and committed, lost its
ID-dictionary entry at the next restart. Its record is still found by ID,
but it is not traversed, a node is not found by its label, and counts can
leave it out. A restart that rebuilt the MVCC heads may since have given
such an entity a new number while its label and adjacency keys keep the old
one; it then has an entry, but one its index keys don't use, and is listed
apart. Stop the server, then list them (read-only):

```bash
nornicdb-admin database check-ids --data-dir=./data
```

`--repair` restores the original numeric ID of each one the index keys
identify unambiguously (replacing a mismatched entry, and moving its MVCC
head), so the existing index keys are valid again, and recounts the labels
and relationship types. An unlabelled node, or two lost
relationships with the same endpoints and type, can't be identified; they are
listed and left unchanged. Back up the data directory first.

```bash
nornicdb-admin database check-ids --repair --data-dir=./data
```

## Related Documentation

- [CLI Commands](cli-commands.md)
- [Data Import/Export Guide](../user-guides/data-import-export.md)
- [Open Knowledge Format import and export](../user-guides/open-knowledge-format-interchange.md)
