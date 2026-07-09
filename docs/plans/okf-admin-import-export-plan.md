# Open Knowledge Format Admin Import/Export Plan

**Status:** Proposed (format-interchange review 2026-06-16)

## Scope

Add Open Knowledge Format (OKF) import/export support to `nornicdb-admin` as an offline, deterministic interchange path for knowledge bundles. OKF is complementary to the existing Neo4j-compatible CSV admin import path: CSV is the high-volume graph migration format, while OKF is the human-readable, agent-friendly knowledge exchange format.

The target surfaces are:

```bash
nornicdb-admin database import okf <db-name> --from-path ./knowledge-bundle
nornicdb-admin database export okf <db-name> --to-path ./knowledge-bundle
nornicdb-admin okf validate ./knowledge-bundle
```

The implementation prioritizes correctness, validation, export/import round-trip fidelity, merge-safe idempotency, and index-build efficiency over online convenience. The `<db-name>` argument is the OKF bundle namespace. Import opens that database namespace offline, writes graph-search-ready records in batches, suppresses per-row search index maintenance, and builds search indexes once at the end.

Deterministic export is a format requirement, not just a nice-to-have implementation detail. OKF import/export in `nornicdb-admin` MUST define bundle version fields and canonical ordering rules up front so human review, agent-generated patches, and future bundle migrations remain stable and safe.

`--from-path` MUST accept:

- A directory containing an OKF bundle.
- A `.zip`, `.tar`, or `.tar.gz` archive containing exactly one OKF bundle root. Archive bundle support is new work; the existing CSV reader only supports plain files, `.gz`, and single-member `.zip` CSV sources.

The importer MUST reject archives with multiple plausible bundle roots unless `--bundle-root <relative-path>` is supplied.

## Existing Code State

This plan extends the current admin import/export implementation. Do not build a parallel admin stack.

Already implemented:

- `cmd/nornicdb-admin/main.go` owns the Cobra command tree, `--data-dir`, `database import full`, `database export neo4j-csv`, and `exitCodeForError`.
- `pkg/adminimport.ImportFull` already opens a namespaced target via `storage.NewNamespacedEngine(engine, opts.DatabaseName)`.
- `pkg/adminimport.ImportFull` already validates an empty target for CSV full import through the unexported `ensureEmpty`.
- `pkg/adminimport.ImportFull` already writes chunks through `storage.Engine.BulkCreateNodes` and `storage.Engine.BulkCreateEdges`.
- `pkg/adminimport.ImportFull` already applies schema files and builds search indexes once after the load.
- The post-import search build currently uses `search.NewService(target)`, `SetPersistenceEnabled(true)`, `SetFulltextIndexPath`, `SetVectorIndexPath`, `SetHNSWIndexPath`, then `BuildIndexes(ctx)`.
- `pkg/adminimport.ExportNeo4jCSV` already exports a namespaced database through `storage.ExportableEngine` using `AllNodes`, `AllEdges`, and `GetSchema`.
- `pkg/storage.Engine` already exposes `BulkDeleteNodes` and `BulkDeleteEdges`, which OKF `replace` mode can use.
- Storage IDs are generated through the normal NornicDB/admin-import ID allocation path and then namespaced by `storage.NewNamespacedEngine`. OKF import must not synthesize semantic IDs such as `okf:<bundle>:concept:<hash>`.
- Managed embeddings already exist outside the admin import path: `pkg/server` creates the runtime `embed.Embedder`, installs it through `DB.SetEmbedder`, and the server/runtime paths write managed embeddings through `pkg/embeddingutil.ApplyManagedEmbedding`.
- The only current ingest-time embedding path is Cypher against a running server using the `WITH EMBEDDING` extension. `nornicdb-admin` is an offline import tool and must not issue Cypher to a server to generate embeddings.
- `gopkg.in/yaml.v3` is already a direct dependency in `go.mod`.

## Non-Goals

- Replacing the native NornicDB graph model or Cypher import/export surfaces.
- Replacing Neo4j CSV import/export compatibility.
- Defining a NornicDB-specific fork of OKF.
- Enforcing a universal ontology beyond the OKF fields and optional NornicDB profile metadata.
- Lossless export of every arbitrary property-graph shape into plain Markdown. Lossless export is only guaranteed for graph data that already follows the NornicDB OKF profile defined in this document.
- Online OKF import. OKF admin import/export is an offline administrative workflow.
- Inferring typed relationship semantics from prose or ordinary Markdown links. Typed edge creation is supported only through the NornicDB OKF heading extension defined below.
- Generating embeddings during admin import. Embedding generation is handled outside OKF import by existing runtime/server mechanisms, including Cypher `WITH EMBEDDING` against a running server.

## Format Model

An OKF bundle is a directory tree of UTF-8 Markdown files. Every non-reserved `.md` file is a concept document and MUST start with YAML frontmatter delimited by `---` lines. The OKF concept ID is the normalized relative file path with the `.md` suffix removed. For example, `tables/users.md` has concept ID `tables/users`.

Reserved filenames are `index.md` and `log.md` at any directory level. They are not concept documents and MUST NOT be imported as graph nodes.

NornicDB must preserve the source bundle faithfully and use export-compatible rules as the import contract:

| OKF Element                              | NornicDB Representation                                                        |
| ---------------------------------------- | ------------------------------------------------------------------------------ |
| Bundle directory                         | Database namespace named by `<db-name>`                                        |
| Concept file                             | Node with opaque generated ID                                                  |
| Concept ID, e.g. `tables/users`          | `okf_concept_id`, unique inside the database namespace                         |
| Source file path, e.g. `tables/users.md` | `okf_path`                                                                     |
| Required `type` frontmatter              | `type` property                                                                |
| Reserved `labels` frontmatter            | Node labels                                                                    |
| `title`                                  | `title` property                                                               |
| `description`                            | `description` property                                                         |
| `resource`                               | `resource` property                                                            |
| `tags`                                   | `tags` property                                                                |
| `timestamp`                              | `timestamp` temporal property when valid; `timestamp_raw` when preserved       |
| Unknown frontmatter                      | Properties with the same exact key                                             |
| Non-relationship Markdown sections       | Node properties whose keys are exact, case-sensitive heading text              |
| `# Schema`                               | NornicDB block-constraint declarations for labels declared in frontmatter      |
| Cypher-like relationship headings        | Typed relationships with optional properties                                   |
| Ordinary Markdown links                  | Preserved in text properties; no edge creation                                 |
| Directory hierarchy                      | File layout only; no hierarchy nodes or relationships are created              |
| `index.md`                               | Bundle navigation file; preserved on export/generation, not imported as a node |
| `log.md`                                 | Bundle history file; preserved on export/generation, not imported as a node    |

Node labels come from the reserved `labels` frontmatter array and `type` as a single node label, normalized if the label name isn't allowed for labels to match node label semantics. `type` is concatenated with `labels` as the first element in the resulting array to be applied as node labels. on export, we choose whatever the first label is and use that as the `type` with the remaining labels exported as `labels`. The raw OKF type value remains available as the `type` property.

## Format Versioning and Canonical Ordering

Versioning and canonical ordering are part of the interchange contract.

Bundle-level version fields:

- `okf_version`: the upstream OKF format version. For this plan, export writes `"0.1"`.
- `okf_schema_version`: the NornicDB OKF profile/schema version for deterministic export/import behavior. Initial value is `1`.

Rules:

- Root `index.md` is the canonical bundle metadata file when present.
- Export MUST generate or preserve a root `index.md` whose frontmatter contains `okf_version: "0.1"` and `okf_schema_version: 1`.
- Import and validate MUST accept bundles with no root `index.md` for backward compatibility and treat them as `okf_version = "0.1"` and `okf_schema_version = 1` by default.
- Non-root `index.md` MUST NOT contain frontmatter.
- Unknown future `okf_version` or `okf_schema_version` values are validation errors until an explicit migration path is implemented. Silent best-effort import is not acceptable for unknown bundle contracts.

Canonical ordering rules:

- Concept files are processed and exported in ascending slash-normalized `okf_path` order.
- Relationship headings for a concept are exported before ordinary heading-derived content sections.
- Relationship headings are ordered by `(direction, type, target okf_concept_id, original heading text)`.
- Frontmatter keys are emitted in canonical order: `okf_version`, `okf_schema_version` on root `index.md`; then `type`, `title`, `description`, `resource`, `tags`, `timestamp`, `labels`; then remaining user keys in ascending bytewise key order.
- Node labels exported through reserved `labels` frontmatter are sorted ascending.
- `okf_frontmatter_keys` and `okf_heading_keys` are stored sorted ascending.
- Heading-derived properties are exported in ascending bytewise heading-key order, except reserved `# Schema`, which is emitted first when present.
- Markdown table rows in `# Schema` are exported in ascending `Property` order.
- Validation reports emit `errors` and `warnings` in stable order: `(path, line, code, message)`.

These ordering rules are part of the round-trip guarantee. Exporters, validators, and migration tools must produce the same ordering for the same logical bundle contents.

## Validation Rules

OKF v0.1 conformance is intentionally narrow. NornicDB implements one strict validator:

| Rule                                              | Result                |
| ------------------------------------------------- | --------------------- |
| Non-reserved `.md` has parseable YAML frontmatter | Required; error       |
| Non-reserved `.md` has non-empty `type`           | Required; error       |
| `index.md` structure follows OKF v0.1             | Required; error       |
| `log.md` date headings use `YYYY-MM-DD`           | Required; error       |
| Missing optional fields                           | Allowed               |
| Unknown `type`                                    | Allowed               |
| Unknown frontmatter keys                          | Allowed and preserved |
| Broken relationship-heading targets               | Warning, never fatal  |
| Missing `index.md`                                | Allowed               |
| Root `index.md` with `okf_version` frontmatter    | Allowed               |
| Root `index.md` with `okf_schema_version`         | Allowed               |
| Unknown `okf_version` or `okf_schema_version`     | Error                 |
| Non-root `index.md` with frontmatter              | Error                 |

Broken relationship targets are never fatal because OKF consumers must tolerate not-yet-written knowledge. NornicDB records them in the validation report only. The admin importer MUST NOT create placeholder nodes for broken targets because concept files are the only OKF source of database nodes.

## Identity and Idempotency

Identity must be deterministic at the OKF/property layer while storage IDs remain opaque generated NornicDB IDs.

Use:

- Database namespace: the `<db-name>` CLI argument. This is the bundle name for import/export.
- `okf_concept_id`: slash-normalized path relative to the bundle root with `.md` removed, for example `tables/users`.
- `okf_path`: slash-normalized source path including `.md`, for example `tables/users.md`.
- `okf_frontmatter_keys`: sorted list of node property names that came from concept frontmatter.
- `okf_heading_keys`: sorted list of node property names that came from Markdown headings.

Deterministic identity and deterministic ordering are separate guarantees. Opaque storage IDs remain non-semantic, but exported ordering MUST depend only on stable OKF properties (`okf_path`, `okf_concept_id`, relationship metadata), never on storage insertion order or generated IDs.

The admin import path allocates opaque node and edge IDs normally, then the namespaced engine applies the database prefix. Merge and replace discover existing records by scanning/indexing properties inside the target database namespace.

Import modes:

- `--mode=fail-if-exists` (default): fail if any node with `okf_concept_id` already exists in the target database namespace.
- `--mode=merge`: upsert incoming OKF nodes by `okf_concept_id`, refresh OKF-owned relationships for the incoming bundle content, and leave existing concepts that are absent from the incoming bundle untouched.
- `--mode=replace`: delete existing OKF-profile nodes/relationships in the target database namespace, then import.

The implementation MUST support `fail-if-exists`, `replace`, and `merge`.

Storage ID behavior:

- New concept nodes MUST be created with opaque generated IDs from the admin import ID allocator.
- New relationships MUST be created with opaque generated IDs from the admin import ID allocator.
- Export MUST NOT depend on storage IDs. Export paths and relationship headings are reconstructed from OKF properties and relationship endpoints.

Bundle mode write behavior:

```go
func writeOKFBundleByMode(ctx context.Context, target storage.Engine, opts OKFImportOptions, bundle *okfBundle, report *OKFValidationReport) error {
	switch opts.Mode {
	case "fail-if-exists":
		existing, err := findOKFNodesByConceptIDProperty(target)
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			return &Error{ExitCode: ExitOKF, Message: "OKF namespace already contains OKF concepts: " + opts.DatabaseName}
		}
		return writeOKFNodesAndEdges(ctx, target, opts, bundle, report)
	case "replace":
		if err := deleteOKFNamespace(target); err != nil {
			return err
		}
		return writeOKFNodesAndEdges(ctx, target, opts, bundle, report)
	case "merge":
		return mergeOKFBundle(ctx, target, opts, bundle, report)
	default:
		return &Error{ExitCode: ExitUnsupported, Message: "unsupported OKF import mode: " + opts.Mode}
	}
}
```

`deleteOKFNamespace` MUST:

1. Call `target.AllNodes()` and collect nodes with `okf_concept_id` or `okf_path`.
2. Call `target.AllEdges()` and collect edges whose start/end node is in that OKF node set, or whose properties contain `okf_edge_source = "okf"`.
3. Call `target.BulkDeleteEdges(edgeIDs)` before `target.BulkDeleteNodes(nodeIDs)`.

This reuses existing storage APIs and avoids `ensureEmpty`, which remains specific to `database import full`.

`mergeOKFBundle` MUST:

1. Build all incoming OKF nodes and edges with stable OKF properties before writing.
2. Build lookup maps from existing `target.AllNodes()` by `okf_concept_id`.
3. Create missing nodes in batches with `target.BulkCreateNodes`.
4. Update existing nodes with `target.UpdateNode`, replacing OKF-owned labels and properties from the bundle while preserving storage-managed timestamps as follows:
   - Preserve the original `CreatedAt`.
   - Set `UpdatedAt` to the import time.
   - Preserve non-OKF user properties only when they do not conflict with incoming OKF frontmatter or heading-derived properties.
   - Do not preserve stale `ChunkEmbeddings`, `NamedEmbeddings`, or `EmbedMeta` for nodes whose OKF `title`, `description`, `tags`, `resource`, `timestamp`, or `content` changed. The admin tool still must not regenerate embeddings.
5. Refresh OKF-owned relationships for the incoming source set:
   - Collect existing edges with `target.AllEdges()`.
   - Delete existing OKF-created typed edges whose `okf_edge_source = "okf"` and whose `okf_source` belongs to the incoming bundle content.
   - Recreate incoming relationships with generated IDs using `target.BulkCreateEdges`.
6. Leave existing bundle nodes that are absent from the incoming bundle untouched. Operators use `--mode=replace` when removed files must disappear from the database.
7. Never merge across database namespaces. The target database namespace is the bundle boundary.

Reserved files:

- `index.md` and `log.md` are parsed for validation/reporting only.
- They do not create nodes.
- They do not create directory hierarchy relationships.
- Export can preserve or generate them as files.

## NornicDB OKF Profile

The profile is export-first: every imported shape must have a deterministic Markdown representation that NornicDB can export and re-import.

### Frontmatter to Properties

Every top-level frontmatter key maps directly to a node property with the same exact name, except reserved frontmatter keys.

Examples:

| OKF Frontmatter | Node Property |
| --------------- | ------------- |
| `type`          | `type`        |
| `title`         | `title`       |
| `description`   | `description` |
| `resource`      | `resource`    |
| `tags`          | `tags`        |
| `timestamp`     | `timestamp`   |
| `labels`        | node labels   |
| `owner`         | `owner`       |

Rules:

- Property names are case-sensitive. `title`, `Title`, and `# Title` are distinct property names when they originate from different headings/frontmatter keys.
- `labels` is reserved and MUST be a YAML array of strings. It is applied to `storage.Node.Labels` and is not stored as a node property.
- Export MUST write node labels back to frontmatter as `labels`.
- Unknown frontmatter keys are preserved as node properties with the same exact keys.
- If a frontmatter-derived property collides with a schema constraint, the schema constraint applies.
- The raw OKF `type` value maps to the `type` property; it does not create a label.
- Reserved OKF bookkeeping properties are limited to `okf_concept_id`, `okf_path`, `okf_frontmatter_keys`, and `okf_heading_keys`.

### Headings to Properties

Markdown sections become node properties unless the heading is a reserved NornicDB OKF heading.

Rules:

- The property key is the exact heading text after the `#` markers and surrounding whitespace are removed.
- Heading property keys are case-sensitive. `# title`, `# Title`, and frontmatter `title` are distinct only when their exact property names differ by case or spelling.
- A heading-derived property name MUST NOT equal any frontmatter-derived property name in the same concept after heading normalization. This is required so the parser can round-trip the file without guessing whether a property belongs in frontmatter or the Markdown body.
- The property value is the Markdown body under that heading until the next heading of the same or higher level.
- If a heading does not parse as a reserved heading or relationship heading, the heading/body pair is stored as a node property.
- Heading properties are subject to `# Schema` constraints.
- Repeated headings in one file are validation errors.

### `# Schema` Constraint Extension

`# Schema` is a reserved NornicDB extension heading. It describes block constraints for the labels declared by frontmatter `labels`.

The supported format is a Markdown table:

```markdown
# Schema

| Property      | Type      | Description                                         |
| ------------- | --------- | --------------------------------------------------- |
| `order_id`    | STRING    | Globally unique order identifier.                   |
| `customer_id` | STRING    | Foreign key into [customers](/tables/customers.md). |
| `total_usd`   | NUMERIC   | Order total in US dollars.                          |
| `placed_at`   | TIMESTAMP | When the customer submitted the order.              |
```

Import semantics:

- `Property` is the case-sensitive property name.
- `Type` maps to NornicDB property type validation. Initial supported values are `STRING`, `INTEGER`, `FLOAT`, `NUMERIC`, `BOOLEAN`, `DATE`, `DATETIME`, `TIMESTAMP`, `LIST`, `MAP`, and `ANY`.
- `Description` is stored as constraint documentation metadata.
- The schema applies to frontmatter-derived properties and heading-derived properties.
- A schema row for `title` constrains the property named `title`, whether it came from frontmatter or a heading. A single concept cannot define both frontmatter `title` and heading `# title` because that would collide.
- Generated constraints target the labels declared by frontmatter `labels`.
- A concept with `# Schema` and no `labels` is a validation error.
- Invalid values are validation errors.

### Relationship Heading Extension

Typed graph relationships are represented by headings whose heading text is a Cypher-like relationship pattern followed by a Markdown path target:

```markdown
# [:RELATES_TO]->(./path/to/file.md)

# [:ACTED_IN {role: 'Bud Fox', year: 1987}]->(./movies/wall-street.md)
```

Supported grammar:

```text
relationship_heading = "[" ":" type properties? "]" direction "(" target ")"
direction            = "->" | "<-"
type                 = /[A-Za-z_][A-Za-z0-9_]*/
properties           = "{" cypher_literal_map "}"
target               = relative-path | absolute-bundle-path
```

Import semantics:

- `->` creates an edge from the current concept to the target concept.
- `<-` creates an edge from the target concept to the current concept.
- The relationship type is the parsed `type`.
- The relationship properties are the parsed Cypher literal map plus:
  - `okf_edge_source = "okf"`
  - `okf_source = <source okf_concept_id>`
  - `okf_target = <target okf_concept_id>`
  - `okf_heading = <original heading text>`
- The heading body is stored on the edge as `okf_body` when non-empty.
- Relationship targets resolve like OKF links: absolute paths begin at the bundle root; relative paths resolve from the source file directory; `.md` is stripped for concept identity.
- A heading that does not match the relationship grammar is not an edge. It becomes a normal node property using the heading text as the key.
- Ordinary Markdown links inside text do not create edges. They remain part of the containing property value so export stays OKF-compatible.

Export semantics:

- An edge with `okf_edge_source = "okf"` exports as a relationship heading when both endpoints are in the export set.
- Edge properties other than OKF bookkeeping are rendered inside the relationship heading map.
- `okf_body` is rendered as the body under that relationship heading.
- Edges that cannot be represented by the heading grammar are reported as lossy and are not silently converted into prose links.

## Import Pipeline

1. Discover files.
   - Walk the bundle root.
   - Include `.md` files.
   - Ignore hidden directories by default unless `--include-hidden=true`.
   - Normalize paths with `/` separators.

2. Parse frontmatter and Markdown.
   - Require YAML frontmatter with non-empty `type` for every non-reserved concept file.
   - Convert all frontmatter fields to node properties with the same exact key.
   - Apply reserved frontmatter `labels` to `storage.Node.Labels` and omit it from node properties.
   - Record every frontmatter-derived property name in `okf_frontmatter_keys`.
   - Treat `index.md` and `log.md` as reserved files, not graph nodes.
   - Parse root `index.md` frontmatter only when present and only for `okf_version` and `okf_schema_version`; all other root index frontmatter keys are validation issues because reserved files do not create nodes or properties.
   - Reject non-root `index.md` frontmatter.
   - Preserve body bytes after frontmatter, normalized to LF line endings.
   - Parse Markdown headings into ordered sections.
   - Parse `# Schema` as constraint metadata.
   - Parse relationship headings into edge declarations.
   - Parse all non-reserved headings into case-sensitive node properties.
   - Reject any heading-derived property whose normalized heading key exactly matches a frontmatter-derived property key.
   - Record every heading-derived property name in `okf_heading_keys`.

3. Validate.
   - Duplicate normalized paths: error.
   - Missing `type`: error.
   - Invalid YAML: error.
   - Unsupported YAML values: error.
   - Invalid timestamp: error.
   - Frontmatter/heading property collision: error.
   - Broken relationship targets: warning.
   - Invalid `log.md` date heading: error.
   - Invalid `index.md` entry shape: error.

- Unknown `okf_version`: error.
- Unknown `okf_schema_version`: error.

4. Resolve relationship headings.
   - Implement relationship-heading parsing in `pkg/adminimport/okf_relationships.go`.
   - Absolute targets beginning with `/` resolve relative to the bundle root.
   - Relative targets resolve against the source file directory.
   - Targets ending in `.md` resolve to concept IDs by stripping `.md`.
   - Directory targets are warnings because `index.md` is not imported as a node.
   - Heading targets may include fragments; fragments are ignored for target identity and preserved as `okf_fragment` on the edge.
   - Parsed relationship headings create typed edges using the parsed relationship type.
   - Broken relationship targets are warnings and do not create edges or placeholder nodes.
   - Ordinary Markdown links are not resolved into edges.

5. Write nodes.
   - Use the offline bulk write path from the admin import infrastructure.
   - Add only labels from frontmatter `labels`.
   - Do not create directory, index, or log nodes.
   - Avoid per-node search-index updates.
   - Do not generate or attach embeddings. Imported nodes intentionally have no `ChunkEmbeddings`, `NamedEmbeddings`, or managed `EmbedMeta`.

6. Write relationships.
   - Typed edges for parsed relationship headings.
   - Do not create directory hierarchy relationships.

7. Build indexes once.
   - Build fulltext indexes from `title`, `description`, `tags`, and heading-derived content properties.
   - Do not build an OKF-specific vector index because OKF v0.1 does not define embeddings.
   - Do not call `embed.NewEmbedder`, `DB.SetEmbedder`, `/nornicdb/embed/trigger`, or Cypher `WITH EMBEDDING` from admin import.
   - If operators want managed embeddings for imported OKF nodes, they must run the normal server/runtime embedding workflow after import.

8. Emit report.
   - Source path, database namespace, import mode.
   - File counts, concept counts, reserved-file counts, relationship counts.
   - Warnings grouped by file and validation category.
   - Index build timing.

## Implementation Call Sites

Add the commands in `cmd/nornicdb-admin/main.go` next to the existing CSV commands:

```go
importOKFCmd := &cobra.Command{
	Use:   "okf <db-name>",
	Short: "Import an Open Knowledge Format bundle",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runImportOKF(args[0], *dataDir, cmd)
	},
}

exportOKFCmd := &cobra.Command{
	Use:   "okf <db-name>",
	Short: "Export a database as an Open Knowledge Format bundle",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runExportOKF(args[0], *dataDir, cmd)
	},
}

okfCmd := &cobra.Command{Use: "okf", Short: "Open Knowledge Format tools"}
okfCmd.AddCommand(validateOKFCmd)
rootCmd.AddCommand(okfCmd)
```

The run functions MUST mirror the existing `runImportFull` and `runExportNeo4jCSV` style:

```go
func runImportOKF(dbName, dataDir string, cmd *cobra.Command) error {
	fromPath, _ := cmd.Flags().GetString("from-path")
	bundleRoot, _ := cmd.Flags().GetString("bundle-root")
	reportFile, _ := cmd.Flags().GetString("report-file")
	mode, _ := cmd.Flags().GetString("mode")
	buildIndexes, _ := cmd.Flags().GetBool("build-indexes")

	engine, err := storage.NewBadgerEngine(dataDir)
	if err != nil {
		return err
	}
	defer engine.Close()

	_, err = adminimport.ImportOKF(context.Background(), engine, adminimport.OKFImportOptions{
		DatabaseName: dbName,
		SourcePath:   fromPath,
		BundleRoot:   bundleRoot,
		Mode:         mode,
		DataDir:      dataDir,
		ReportFile:   reportFile,
		BuildIndexes: buildIndexes,
	})
	return err
}

func runExportOKF(dbName, dataDir string, cmd *cobra.Command) error {
	toPath, _ := cmd.Flags().GetString("to-path")

	engine, err := storage.NewBadgerEngine(dataDir)
	if err != nil {
		return err
	}
	defer engine.Close()

	target := storage.NewNamespacedEngine(engine, dbName)
	return adminimport.ExportOKF(target, adminimport.OKFExportOptions{
		OutputDir: toPath,
	})
}
```

Add these files under `pkg/adminimport`:

- `okf.go`: public option/report types and `ImportOKF`, `ExportOKF`, `ValidateOKF`.
- `okf_parse.go`: frontmatter/body parsing, YAML conversion, timestamp validation.
- `okf_schema.go`: `# Schema` table parsing and constraint application.
- `okf_relationships.go`: relationship-heading parsing, target resolution, fragment preservation.
- `okf_archive.go`: directory/archive staging and `--bundle-root` resolution.
- `okf_export.go`: OKF bundle writer and index/log generation.
- `okf_test.go`: unit/integration tests.

Refactor existing helpers instead of duplicating them:

- Move the inline search build block in `ImportFull` into `buildSearchIndexes(ctx, target, dataDir, dbName)`.
- Change `writeReport(path string, report Report)` to `writeJSONReport(path string, value any)` and keep `writeReport` as a CSV wrapper if needed.
- Keep `ensureEmpty` unchanged for CSV full import; OKF import must use `writeOKFBundleByMode` because it supports bundle-scoped `fail-if-exists`, `replace`, and `merge`.

The OKF public types MUST be concrete:

```go
const (
	ExitOKF = 7
)

type OKFImportOptions struct {
	DatabaseName      string
	SourcePath        string
	BundleRoot        string
	Mode              string // fail-if-exists|replace|merge
	IncludeHidden     bool
	BuildIndexes      bool
	DataDir           string
	ReportFile        string
	Now               time.Time
}

type OKFExportOptions struct {
	OutputDir     string
	GenerateIndex bool
	GenerateLog   bool
	Overwrite     bool
	ReportFile    string
}

type OKFValidationReport struct {
	Format        string        `json:"format"`
	FormatVersion string        `json:"format_version"`
	SchemaVersion int           `json:"schema_version"`
	Database      string        `json:"database"`
	BundleRoot    string        `json:"bundle_root"`
	Valid         bool          `json:"valid"`
	Counts        OKFCounts     `json:"counts"`
	Errors        []OKFIssue    `json:"errors"`
	Warnings      []OKFIssue    `json:"warnings"`
	StartedAt     time.Time     `json:"started_at,omitempty"`
	CompletedAt   time.Time     `json:"completed_at,omitempty"`
	Duration      time.Duration `json:"duration_nanos,omitempty"`
}
```

Core import skeleton:

```go
func ImportOKF(ctx context.Context, engine storage.Engine, opts OKFImportOptions) (OKFValidationReport, error) {
	opts = opts.withDefaults()
	if engine == nil {
		return OKFValidationReport{}, unsupported("storage engine is required")
	}

	bundle, report, err := loadOKFBundle(opts)
	if err != nil {
		_ = writeJSONReport(opts.ReportFile, report)
		return report, err
	}
	if !report.Valid {
		_ = writeJSONReport(opts.ReportFile, report)
		return report, &Error{ExitCode: ExitOKF, Message: "OKF validation failed"}
	}

	target := storage.NewNamespacedEngine(engine, opts.DatabaseName)
	if err := writeOKFBundleByMode(ctx, target, opts, bundle, &report); err != nil {
		_ = writeJSONReport(opts.ReportFile, report)
		return report, err
	}
	if opts.BuildIndexes {
		if err := buildSearchIndexes(ctx, target, opts.DataDir, opts.DatabaseName); err != nil {
			return report, err
		}
	}
	return report, writeJSONReport(opts.ReportFile, report)
}
```

## Validation Report Schema

Every validation and import command MUST be able to emit this JSON shape:

```json
{
  "format": "okf",
  "format_version": "0.1",
  "schema_version": 1,
  "database": "product-docs",
  "bundle_root": "/absolute/path/to/bundle",
  "valid": true,
  "counts": {
    "concept_files": 42,
    "index_files": 4,
    "log_files": 1,
    "relationship_headings": 91,
    "broken_relationship_targets": 2
  },
  "errors": [],
  "warnings": [
    {
      "code": "broken_relationship_target",
      "path": "tables/orders.md",
      "line": 18,
      "target": "/tables/customers.md",
      "message": "relationship target does not exist"
    }
  ]
}
```

Error and warning `code` values MUST be stable. Initial codes:

- `invalid_archive_root`
- `invalid_utf8`
- `missing_frontmatter`
- `invalid_frontmatter`
- `missing_type`
- `invalid_timestamp`
- `invalid_index_frontmatter`
- `invalid_index_entry`
- `invalid_log_frontmatter`
- `invalid_log_date`
- `duplicate_concept_id`
- `duplicate_heading_property`
- `property_name_collision`
- `path_traversal`
- `broken_relationship_target`
- `invalid_schema_section`
- `invalid_relationship_heading`
- `unsupported_yaml_value`

## Export Pipeline

Export MUST support a lossless OKF-profile round trip and a best-effort generic graph projection.

Command shape:

```bash
nornicdb-admin database export okf <db-name> \
  --to-path ./knowledge-bundle
```

Export steps:

1. Select nodes.
   - Use `storage.ExportableEngine.AllNodes()` and `AllEdges()`, matching the current `ExportNeo4jCSV` pattern.
   - Default: export nodes with `okf_concept_id` and `okf_path`.
   - The database namespace is the bundle boundary.
   - Cypher-driven `--match` export is not part of this OKF admin implementation because the current admin export path does not embed the Cypher executor.
   - Sort the export set in ascending canonical `okf_path` order before writing any files.

2. Compute output paths.
   - Prefer existing `okf_path`.
   - If `okf_path` is missing, derive `<safe-slug>.md` from `title`; if `title` is missing, derive from the storage ID.
   - Every concept output path MUST end in `.md`.
   - Reject path traversal and absolute path outputs.

3. Write frontmatter.
   - Emit frontmatter from properties listed in `okf_frontmatter_keys` using their exact property names.
   - Emit node labels as reserved `labels` frontmatter in deterministic sorted order.
   - Root `index.md` frontmatter MUST begin with `okf_version: "0.1"` and `okf_schema_version: 1`.
   - Emit canonical fields first when present: `type`, `title`, `description`, `resource`, `tags`, `timestamp`.
   - Emit remaining properties listed in `okf_frontmatter_keys` in sorted key order for deterministic output.
   - Do not emit `okf_*` storage properties as user frontmatter, except root `index.md` MAY emit `okf_version: "0.1"` and `okf_schema_version: 1`.

4. Write Markdown body.
   - Emit `# Schema` first when the concept has NornicDB OKF schema metadata.
   - Emit relationship headings for OKF-created edges whose endpoints are in the export set, in canonical relationship order.
   - Emit properties listed in `okf_heading_keys` as Markdown sections using exact property keys as headings, in canonical heading-key order.
   - Do not emit frontmatter-origin properties as Markdown headings because they belong in frontmatter.
   - Do not export `ChunkEmbeddings`, `EmbedMeta`, or `NamedEmbeddings` into Markdown. They are runtime storage concerns, not OKF v0.1 fields.

5. Render relationships.
   - Convert OKF-created graph edges to relationship headings when target nodes are in the export set.
   - Render edge type and non-OKF properties in the heading map.
   - Render `okf_body` as the body under the relationship heading.
   - Emit warnings for relationships that cannot be represented by the relationship-heading grammar.
   - Do not emit directory hierarchy relationships; OKF directory structure is represented by output file paths.

6. Write reserved files.
   - Preserve existing `index.md` and `log.md` files when the source bundle metadata is available, or generate them when requested.
   - Generate `index.md` files for directories when `--generate-index=true` using the OKF v0.1 bullet format: `* [Title](relative-url) - description`.
   - Generate `log.md` from txlog/MVCC metadata when `--generate-log=true` using newest-first `## YYYY-MM-DD` sections.
   - Do not write frontmatter in non-root `index.md`; root `index.md` MAY include only `okf_version` and `okf_schema_version`.
   - Do not write frontmatter in `log.md`.

## CLI Flags

Import:

```bash
nornicdb-admin database import okf <db-name>
  --from-path <path>
  --bundle-root <relative-path>
  --mode fail-if-exists|replace|merge
  --build-indexes=true
  --include-hidden=false
  --report-file import-report.json
```

Export:

```bash
nornicdb-admin database export okf <db-name>
  --to-path <path>
  --generate-index=false
  --generate-log=false
  --overwrite=false
  --report-file export-report.json
```

Validation:

```bash
nornicdb-admin okf validate <path>
  --bundle-root <relative-path>
  --report-file okf-validation.json
```

Exit-code behavior MUST use the existing `cmd/nornicdb-admin/main.go` `exitCodeForError` path:

- Return `nil` for success, which exits as `adminimport.ExitOK`.
- Return `*adminimport.Error` for OKF validation/import/export failures so the CLI preserves the package exit code.
- Add `adminimport.ExitOKF = 7` for OKF parse/conformance failures.
- Reuse `adminimport.ExitUnsupported` only for invalid option values outside the required OKF mode set.
- Let storage open errors continue to exit as `1` unless they are explicitly wrapped in `*adminimport.Error`.

## Storage and Schema Requirements

Recommended constraints/indexes for imported OKF data are derived from labels declared by the reserved frontmatter `labels` array. There is no global `:OKFConcept` label and no all-node OKF schema target. For each declared label that participates in the OKF profile, generate label-scoped schema:

```cypher
CREATE CONSTRAINT okf_<label>_concept_identity IF NOT EXISTS
FOR (n:<Label>)
REQUIRE n.okf_concept_id IS UNIQUE

CREATE FULLTEXT INDEX okf_<label>_content IF NOT EXISTS
FOR (n:<Label>)
ON EACH [n.title, n.description]
```

Property names in storage MUST use underscore keys:

- `okf_concept_id`
- `okf_path`
- `okf_frontmatter_keys`
- `okf_heading_keys`
- frontmatter-derived properties using exact OKF keys

Implementation and docs MUST NOT use dotted OKF property keys.

Concrete concept node construction:

```go
func conceptNode(c okfConcept, opts OKFImportOptions) *storage.Node {
	props := map[string]any{
		"okf_concept_id":      c.ConceptID,
		"okf_path":            c.Path,
		"okf_frontmatter_keys": c.FrontmatterKeys,
		"okf_heading_keys":    c.HeadingKeys,
		"type":                c.Type,
	}
	if c.Title != "" {
		props["title"] = c.Title
	}
	if c.Description != "" {
		props["description"] = c.Description
	}
	if c.Resource != "" {
		props["resource"] = c.Resource
	}
	if len(c.Tags) > 0 {
		props["tags"] = c.Tags
	}
	if !c.Timestamp.IsZero() {
		props["timestamp"] = c.Timestamp
	} else if c.RawTimestamp != "" {
		props["timestamp_raw"] = c.RawTimestamp
	}
	for key, value := range c.UnknownFrontmatter {
		props[key] = value
	}
	for key, value := range c.Sections {
		props[key] = value
	}
	return &storage.Node{
		ID:         c.GeneratedID,
		Labels:     c.Labels,
		Properties: props,
		CreatedAt:  opts.Now,
		UpdatedAt:  opts.Now,
	}
}
```

Concrete relationship-heading edge construction:

```go
func relationshipEdge(rel okfResolvedRelationship, opts OKFImportOptions) *storage.Edge {
	props := maps.Clone(rel.Properties)
	props["okf_edge_source"] = "okf"
	props["okf_source"] = rel.SourceConceptID
	props["okf_target"] = rel.TargetConceptID
	props["okf_heading"] = rel.Heading
	if rel.Body != "" {
		props["okf_body"] = rel.Body
	}
	if rel.Fragment != "" {
		props["okf_fragment"] = rel.Fragment
	}
	return &storage.Edge{
		ID:         rel.GeneratedID,
		StartNode:  rel.SourceNodeID,
		EndNode:    rel.TargetNodeID,
		Type:       rel.Type,
		Properties: props,
		CreatedAt:  opts.Now,
		UpdatedAt:  opts.Now,
		Confidence: 1.0,
	}
}
```

## Tests

Add focused unit/integration tests for:

- `cmd/nornicdb-admin` command tree includes `database import okf`, `database export okf`, and `okf validate`.
- `exitCodeForError` preserves the new `adminimport.ExitOKF` when returned as `*adminimport.Error`.
- Valid minimal bundle imports.
- Required `type` enforcement.
- Missing `type` fails validation.
- Unknown YAML frontmatter maps to same-name properties and round-trips through export.
- Imported nodes record `okf_frontmatter_keys` and `okf_heading_keys` for deterministic export.
- Root `index.md` frontmatter accepts only `okf_version` and `okf_schema_version`.
- Non-root `index.md` frontmatter fails validation.
- Unknown bundle `okf_version` fails validation.
- Unknown bundle `okf_schema_version` fails validation.
- `log.md` rejects frontmatter.
- `log.md` date headings validate `YYYY-MM-DD`.
- Archive import with a single root.
- Archive import with ambiguous roots fails unless `--bundle-root` is supplied.
- Relative relationship target resolution.
- Absolute bundle-relative relationship target resolution.
- Directory relationship targets warn because `index.md` is not imported as a node.
- Fragment preservation for `./file.md#section`.
- Broken relationship target reporting without failing validation.
- Broken relationship targets do not create placeholder nodes.
- Ordinary Markdown links and external URLs remain in heading/property text and do not create edges.
- `index.md` and `log.md` validate as reserved files but do not create nodes.
- Reserved `labels` frontmatter must be an array of strings.
- Reserved `labels` frontmatter is applied to `storage.Node.Labels`.
- Export writes node labels back as sorted reserved `labels` frontmatter.
- Export writes files in canonical `okf_path` order regardless of storage iteration order.
- Export writes frontmatter keys in canonical order.
- Export writes relationship headings in canonical order.
- Export writes heading-derived sections in canonical order.
- Validation reports are stable across repeated runs for unchanged bundles.
- Import does not add `OKFConcept`, `OKFDirectory`, `OKFIndex`, `OKFLog`, or other OKF-specific labels.
- Property-based idempotency across repeated imports using generated storage IDs.
- `replace` mode removes stale files that disappeared from the bundle.
- `fail-if-exists` allows a non-empty database when no nodes with `okf_concept_id` are present in the namespace.
- `fail-if-exists` rejects an existing OKF concept in the namespace.
- `replace` deletes edges before nodes for the target namespace only.
- `merge` creates new incoming concepts while leaving absent existing bundle concepts untouched.
- `merge` updates incoming OKF frontmatter and heading-derived fields and preserves non-conflicting non-OKF user properties.
- `merge` refreshes stale OKF-created typed relationships for incoming source concept IDs.
- Imported nodes and relationships use opaque generated IDs, not OKF hash IDs.
- `merge` clears stale managed embedding fields when OKF text or canonical frontmatter changes, without generating replacement embeddings.
- Frontmatter maps to same-name properties, except reserved `labels`.
- `# Schema` table rows create block constraints for the labels declared in frontmatter.
- `# Schema` with no declared `labels` fails validation.
- Heading properties are case-sensitive and must not collide exactly with frontmatter property names.
- Relationship headings create typed edges with parsed properties and direction.
- Non-matching relationship-like headings are stored as node properties.
- Export/import round trip for the NornicDB OKF profile.
- Export warnings for non-representable graph relationships.
- Index build called once after bulk writes.
- Shared `buildSearchIndexes` helper preserves current CSV import behavior.
- OKF import does not call `embed.NewEmbedder`, server embedding endpoints, or Cypher execution.
- Freshly imported OKF nodes have empty `ChunkEmbeddings`, empty `NamedEmbeddings`, and no managed `EmbedMeta`.

Add benchmarks for:

- Parse/validate throughput on 1k, 10k, and 50k Markdown-file bundles.
- Import write throughput with search index build disabled.
- Import write throughput with post-load search index build enabled.
- Link-resolution allocation profile on dense cross-linked bundles.

## Documentation

Ship these docs with the implementation:

- `docs/user-guides/open-knowledge-format-interchange.md`
- Admin tool usage examples in `docs/operations/admin-tool.md`
- Environment/config notes only if parser limits become globally configurable.
- A release-note entry describing OKF as an interchange profile, not as a replacement for native graph export.

## Acceptance Criteria

- `nornicdb-admin okf validate` produces deterministic JSON reports.
- `database import okf` imports a valid sample bundle into an empty database without online index churn.
- `database import okf --mode=merge` upserts incoming bundle files, refreshes OKF-owned relationships for changed files, and leaves absent bundle files untouched.
- Imported concepts are queryable by Cypher and fulltext search. Vector retrieval is explicitly outside OKF admin import and is handled by normal runtime/server embedding workflows.
- Relationship headings become graph relationships with stable target identity.
- Exported bundles include explicit `okf_version` and `okf_schema_version` metadata and use canonical deterministic ordering for files, frontmatter keys, headings, schema rows, and relationship headings.
- `database export okf` can export and re-import a NornicDB OKF-profile graph without losing canonical OKF fields or Markdown body content.
- Generic graph export clearly reports lossy projections instead of silently dropping relationship information.
- `database import okf` never generates embeddings, never calls the server embedding endpoints, and never issues Cypher `WITH EMBEDDING`.
