# Open Knowledge Format import

NornicDB imports [Open Knowledge Format (OKF) 0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md) bundles as an offline administration operation. An OKF bundle is a directory of Markdown concepts with YAML frontmatter; ordinary Markdown links between concepts become directed graph relationships.

NornicDB keeps the bundle source as the authority. It stores the complete frontmatter and original Markdown body alongside a query-friendly projection, without generating embeddings or starting a server.

## Import a bundle

```bash
nornicdb-admin database import okf knowledge \
  --from-path ./knowledge-bundle \
  --profile okf \
  --mode fail-if-exists \
  --property-map ./properties.env \
  --data-dir ./data
```

The importer accepts a directory. Every non-reserved `.md` file must start with YAML frontmatter containing a non-empty `type`. `index.md` and `log.md` are reserved navigation/history files and are validated but not imported as nodes. Root `index.md` may declare `okf_version: "0.2"`; nested indexes cannot have frontmatter, and `log.md` cannot have frontmatter.

The default `fail-if-exists` mode protects an existing imported bundle in the target database namespace. `merge`, `replace`, and archive input are planned separately and are not accepted by this importer yet.

`--property-map` is optional. It names an environment-style text file of
`source_property=storage_property` assignments. It applies to every property
written by the adapter, including frontmatter fields and preserved source
fields. For example:

```text
# Keep source fidelity fields under this application's naming convention.
_okf_frontmatter=source_metadata
_okf_body=markdown
type=concept_type
```

With this map, the node contains `source_metadata`, `markdown`, and
`concept_type`; it does not receive second copies at `_okf_frontmatter`,
`_okf_body`, or `type`. The map also applies to relationship properties.

## Export an imported bundle

```bash
nornicdb-admin database export okf knowledge \
  --to-path ./exported-bundle \
  --property-map ./properties.env \
  --data-dir ./data
```

Export accepts only an empty output directory and exports concept nodes that
carry the preserved-source properties from a prior OKF import. Pass the same
property-map used for import when you renamed those properties. The exporter
recreates concept Markdown from preserved frontmatter and body and restores
the reserved `index.md` and `log.md` files retained with the imported bundle.

## Graph projection

Every concept becomes one node with these adapter-owned properties:

| Source | NornicDB property |
| --- | --- |
| Bundle/database name | `_okf_bundle` |
| Concept ID, such as `architecture/system` | `_okf_concept_id` |
| Relative path, such as `architecture/system.md` | `_okf_path` |
| Complete YAML frontmatter | `_okf_frontmatter` |
| Original Markdown body | `_okf_body` |
| Safe scalar frontmatter values | Same-named node properties |
| Unresolved concept links | `_okf_unresolved_relationships` |

The importer always preserves the body; it creates an edge only for a local Markdown link to a present non-reserved concept. Images, external URLs, query-bearing URLs, paths outside the bundle, and links to reserved files remain source text only. Missing targets are warnings, not errors, and do not create placeholder nodes.

With `--profile okf` (the default), each resolved link occurrence becomes an untyped directed edge. No synthetic relationship type is added: OKF specifies a directed link, not a type.

## Property Graph Markdown profile

The opt-in `--profile pgm-0.4-draft` applies the [Property Graph Markdown 0.4.0 Public Draft](https://github.com/property-graph-markdown/specification/blob/v0.4.0-public-draft.1/SPEC.md) interpretation of a normal Markdown link title containing a YAML flow mapping:

```markdown
[Administration](../operations/admin-import.md "{type: DOCUMENTS, scope: import}")
```

The importer preserves each link occurrence. It saves the decoded mapping in `_pgm_properties`; a non-empty string `type` becomes the edge type, while untyped links remain untyped. An invalid brace-leading PGM title is reported as a warning and the original Markdown is still retained.

## Test corpus

`pkg/adminimport/testdata/okf/nornicdb-docs` is a small bundle derived from the NornicDB architecture, administration, and hybrid-search documentation. It covers root and nested reserved files, frontmatter, relative links, untyped and PGM-typed links, external links, images, and an intentionally unresolved relationship. `go test ./pkg/adminimport -run 'Test(ValidateOKF|ImportOKF)'` imports it under both profiles.
