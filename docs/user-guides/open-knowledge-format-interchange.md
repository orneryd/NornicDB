# Open Knowledge Format Interchange

Open Knowledge Format (OKF) is a lightweight interchange format for portable knowledge bundles. In OKF v0.1, a bundle is a directory tree of UTF-8 Markdown files. Every non-reserved `.md` file represents one concept, starts with YAML frontmatter, and uses its relative path without the `.md` suffix as its concept ID.

NornicDB treats OKF as a source and exchange format, not as a replacement for its native graph, vector, and MVCC storage model. The practical model is:

- OKF is the human-readable bundle on disk.
- NornicDB is the queryable runtime representation.
- Admin import/export is the bridge between the two.

## Standard Summary

OKF is intentionally minimal. The v0.1 shape is:

- A bundle is a directory tree.
- Concepts are non-reserved Markdown files.
- YAML frontmatter carries structured metadata.
- `type` is the required canonical field.
- Common optional fields include `title`, `description`, `resource`, `tags`, and `timestamp`.
- Standard Markdown links remain Markdown content. NornicDB typed graph edges use the relationship-heading extension.
- Reserved files are `index.md` and `log.md` at any directory level.
- `index.md` is a directory listing for progressive disclosure. It normally has no frontmatter; root `index.md` may declare `okf_version: "0.1"`.
- `log.md` is a newest-first directory update log with `## YYYY-MM-DD` headings and no frontmatter.

A bundle is conformant when every non-reserved `.md` file has parseable YAML frontmatter with a non-empty `type`, and reserved `index.md`/`log.md` files follow their OKF v0.1 structures. Consumers must tolerate missing optional fields, unknown types, unknown extra frontmatter keys, broken cross-links, and missing `index.md` files.

Example concept file:

```markdown
---
type: dataset
title: Customer Events
description: Cleaned customer event stream used for churn analysis.
resource: gs://example-bucket/customer/events.parquet
tags:
  - analytics
  - churn
labels:
  - Dataset
timestamp: 2026-06-16T10:00:00Z
---

# Schema

| Column | Type | Description |
|--------|------|-------------|
| `title` | STRING | Human-readable name. |

# [:RELATES_TO]->(../raw/customer-events.md)

Derived from raw customer events.

# Notes

It is consumed by [Churn Features](../features/churn-features.md).
```

The format is deliberately producer/consumer independent: any tool that can read Markdown, parse YAML frontmatter, and resolve relative links can participate.

## NornicDB Mapping

NornicDB maps OKF bundles into a graph profile that keeps the original files recoverable while making the content searchable and traversable.

| OKF | NornicDB |
| --- | --- |
| Bundle directory | Database namespace named by `<db-name>` |
| Markdown concept file | Node with opaque generated ID |
| Concept ID, e.g. `tables/orders` | `okf_concept_id` property |
| Source path, e.g. `tables/orders.md` | `okf_path` property |
| `type` frontmatter | `type` property |
| Reserved `labels` frontmatter | Node labels |
| `title` | `title` property |
| `description` | `description` property |
| `resource` | `resource` property |
| `tags` | `tags` property |
| `timestamp` | `timestamp` property when parseable |
| Unknown frontmatter | Properties with the same exact key |
| `# Schema` | NornicDB block constraints for labels declared in frontmatter |
| Other headings | Case-sensitive node properties |
| Relationship headings | Typed graph relationships |
| Ordinary Markdown links | Preserved as Markdown text |
| Directory hierarchy | Output file paths only; no hierarchy nodes or relationships |
| `index.md` | Reserved navigation file; not imported as a node |
| `log.md` | Reserved history file; not imported as a node |

The importer preserves unknown frontmatter instead of rejecting it. That keeps NornicDB compatible with OKF revisions and domain-specific profiles.

Frontmatter and headings are property sources. Frontmatter maps directly to same-name node properties except for reserved `labels`, which must be a YAML array of strings and is applied to the node's labels. Markdown headings use the exact case-sensitive heading text as the property key. A heading-derived property name cannot equal a frontmatter-derived property name in the same file, because export must know whether that property belongs in YAML frontmatter or Markdown body. `# Schema` is reserved for NornicDB constraints on those properties, and relationship headings such as `# [:RELATES_TO]->(./target.md)` are the only Markdown syntax that creates graph edges.

NornicDB does not add OKF-specific labels such as `OKFConcept`, `OKFDirectory`, `OKFIndex`, or `OKFLog`. If a node needs labels, author them explicitly in the concept frontmatter:

```yaml
labels: [BigQueryTable, SalesFact]
```

## Import Flow

The admin import command is:

```bash
nornicdb-admin database import okf knowledge \
  --from-path ./knowledge-bundle \
  --mode fail-if-exists
```

Expected behavior:

1. Walk the bundle directory and discover Markdown files.
2. Parse YAML frontmatter and body content.
3. Validate required fields, schema sections, and relationship-heading targets.
4. Create concept nodes with opaque generated IDs, idempotent `okf_concept_id` properties, and labels from reserved `labels` frontmatter.
5. Resolve relationship headings into typed relationships.
6. Build search indexes once after the bulk write. OKF import indexes OKF text for fulltext retrieval and does not generate embeddings.
7. Emit a deterministic import report.

NornicDB uses one strict OKF validator. Missing concept frontmatter, invalid concept frontmatter, missing `type`, invalid reserved-file structure, schema errors, and property-name collisions are validation errors.

Broken relationship-heading targets are never fatal. OKF allows partially written bundles, so NornicDB reports broken targets without creating placeholder nodes.

OKF admin import is deliberately not an embedding path. It does not call a running server, does not use `/nornicdb/embed/*`, and does not issue Cypher with `WITH EMBEDDING`. Imported OKF nodes receive managed embeddings through the normal server/runtime embedding workflow outside admin import.

Import modes are deterministic:

- `fail-if-exists` rejects existing OKF concepts in the target database namespace.
- `replace` deletes the existing OKF graph in the target namespace and imports the supplied bundle.
- `merge` upserts incoming files, refreshes OKF-owned relationships for changed files, and leaves existing bundle files that are absent from the incoming bundle untouched.

## Export Flow

The admin export command is:

```bash
nornicdb-admin database export okf knowledge \
  --to-path ./knowledge-bundle
```

For graphs that were imported from OKF, export is a profile-level round trip:

- Existing `okf_path` decides the output file path.
- Existing `okf_concept_id` decides the concept identity.
- Frontmatter-origin properties become YAML frontmatter using their exact names.
- Node labels become reserved `labels` frontmatter.
- `# Schema` metadata becomes the reserved schema section.
- Heading properties become Markdown sections.
- OKF-created typed relationships become relationship headings when both endpoints are exported.
- `index.md` and `log.md` are emitted without frontmatter, except root `index.md` may emit `okf_version: "0.1"`.

For arbitrary NornicDB graphs, export is best-effort. Property graphs can express relationship types, properties, vectors, labels, and temporal state that plain Markdown cannot represent without a profile. The exporter should warn when a relationship or property cannot be represented losslessly.

## Query Examples

Find a concept by OKF concept ID:

```cypher
MATCH (n)
WHERE n.okf_concept_id = 'features/churn-features'
RETURN n.title, n.type, n.tags
```

Traverse linked concepts:

```cypher
MATCH (n)-[r]->(m)
WHERE n.okf_concept_id = 'features/churn-features'
  AND r.okf_edge_source = 'okf'
RETURN type(r), m.okf_concept_id, m.title, m.type
```

Use imported Markdown as retrieval content:

```cypher
CALL db.index.fulltext.queryNodes('okf_content', 'customer churn features')
YIELD node, score
RETURN node.okf_concept_id, node.title, score
ORDER BY score DESC
LIMIT 10
```

After the normal runtime embedding workflow has embedded imported nodes, semantic retrieval can be combined with graph expansion using the standard vector-search surface. That workflow is outside OKF admin import.

## Interchange Guidance

Use OKF when:

- Knowledge needs to live in Git or another file-based workflow.
- Humans and agents both need to edit the source.
- A bundle must move between tools without requiring a database dump.
- Markdown is the canonical authoring surface.

Use native NornicDB export when:

- You need full graph fidelity.
- Relationship properties and labels are business-critical.
- You need vector payloads, MVCC history, receipts, or exact storage-level restore.
- You are moving a full operational database between NornicDB deployments.

The strongest pattern is to keep OKF as the authored source of knowledge and use NornicDB as the indexed, queryable execution layer.

## Implementation Status

See `docs/plans/okf-admin-import-export-plan.md` for the CLI, validation rules, graph mapping, tests, and acceptance criteria.
