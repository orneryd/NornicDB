# OKF and Property Graph Markdown Admin Interchange Plan

**Status:** Proposed — revised after OKF 0.2 and Property Graph Markdown (PGM) review

## Decision

NornicDB should implement offline OKF import and export. It is still useful: OKF is a portable, human-readable source format, while NornicDB provides indexed storage, Cypher, traversal, and search.

The previous proposal's NornicDB-specific relationship-heading syntax is not necessary and must not be implemented. It would create an incompatible second graph grammar in Markdown.

Instead, the implementation uses these upstream contracts:

- [OKF 0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md) is the baseline bundle format. Each non-reserved Markdown document is a concept; its path is its concept ID; a Markdown concept link asserts a directed, untyped relationship.
- [PGM 0.4.0 Public Draft](https://github.com/property-graph-markdown/specification/blob/v0.4.0-public-draft.1/SPEC.md) is the optional graph profile. It retains every link occurrence and uses a complete YAML flow mapping in a normal Markdown link title for relationship properties and optional `type`.

PGM is still a public draft. NornicDB must name it exactly as `PGM 0.4.0 Public Draft` in CLI output, reports, fixtures, and conformance metadata; it must not claim conformance to a final PGM 0.4.0 standard.

## Why an adapter is still needed

OKF already has graph-like linking, and PGM provides a precise property-graph interpretation. Neither specifies a NornicDB storage adapter, offline bulk workflow, namespace/mode behavior, index build policy, or a projection from the complete PGM value model into NornicDB properties.

| Concern | Upstream authority | NornicDB responsibility |
| --- | --- | --- |
| Concept identity and documents | OKF | Map concepts to nodes and preserve source provenance |
| Ordinary concept links | OKF | Build directed untyped graph projections |
| Typed links and relationship values | PGM | Build typed/multigraph projections without losing occurrences |
| Storage IDs, merge/replace, indexes | — | Offline administration and durable storage |
| Generic graph export | — | Clearly report a lossy projection; never claim source fidelity |

## Compatibility contract

### Versions and parser profiles

The parser validates against OKF 0.2. PGM mode pins the exact upstream OKF revision and hash required by the PGM 0.4.0 Public Draft, rather than a moving branch. Every report records:

- `okf_version: "0.2"`
- PGM profile: `PGM 0.4.0 Public Draft` when selected
- CommonMark parser/version
- YAML 1.2.2 Core Schema support and supported explicit tags
- NornicDB adapter version

An unknown `okf_version` is a warning by default, not an unconditional rejection: OKF 0.2 directs consumers to attempt best-effort consumption. A selected strict profile may reject only behavior it cannot safely interpret.

### Explicit profiles

`--profile=okf` is the default. It imports conformant OKF 0.2 and projects each resolved concept link as one directed, untyped relationship.

`--profile=pgm-0.4-draft` additionally applies the PGM Core rules and Portable Relationship Identification Profile v1. It remains opt-in until PGM is final. A PGM failure is separate from an OKF failure: a bundle may be valid OKF but not satisfy selected PGM requirements.

There is no auto-detection. PGM has stronger parsing, value, diagnostic, and occurrence-preservation obligations and therefore requires an operator choice.

## Source mapping

### Concepts

Every present non-reserved concept becomes one node. `index.md` and `log.md` are validated and preserved as bundle files but never become graph nodes.

| Source | NornicDB node mapping |
| --- | --- |
| Concept ID (`tables/orders`) | `_okf_concept_id` |
| Relative path (`tables/orders.md`) | `_okf_path` |
| Bundle namespace | `_okf_bundle` |
| Complete YAML frontmatter | `_okf_frontmatter`: Canonical PGM Value v1 JSON in PGM mode; canonical JSON in OKF mode |
| Original Markdown body | `_okf_body`, exactly as imported after frontmatter |
| `type` | `type` property; in PGM mode also the derived node graph element type in adapter metadata |
| Safe scalar frontmatter | Same-named properties for query ergonomics |
| Complex/non-native frontmatter | Retained in `_okf_frontmatter`; copied only when exact |

NornicDB must not manufacture `OKFConcept`, `OKFDirectory`, or `OKFIndex` labels. It also must not silently turn OKF `type` into a node label. A future label projection is an explicit adapter option and never replaces the raw `type` property.

The `_okf_` fields are adapter-owned and must never appear as author frontmatter during source-fidelity export.

### Links and relationships

The importer parses Markdown before interpreting links. It uses a CommonMark parser and applies OKF/PGM path rules before any filesystem access.

For every concept-link occurrence:

1. Resolve an absolute link from bundle root or a relative link from the source concept's directory.
2. Reject traversal above the bundle root, malformed percent encoding, external URLs, images, query-bearing links, fragment-only links, and reserved-file paths as graph links.
3. Always retain the original Markdown in `_okf_body`.
4. If the target concept exists, create exactly one directed edge from source to target.
5. If it does not, create no placeholder node; record an unresolved occurrence in `_okf_unresolved_relationships` and emit a warning.

In `okf` mode, a resolved link becomes one directed, untyped edge. The adapter does not invent an OKF relationship type. It records source provenance in storage properties, subject to `--property-map`:

```text
_okf_bundle
_okf_source
_okf_target
_okf_link_text
_okf_link_title          # nullable
_okf_link_fragment       # nullable
_okf_relationship_ordinal
```

In PGM mode, every resolved Concept Link occurrence becomes one edge. The importer stores the complete PGM property map in `_pgm_properties`, together with:

```text
_pgm_relationship_key
_pgm_relationship_id
_pgm_occurrence
_pgm_type                # nullable; derived only from a non-empty string property named type
```

When the PGM `type` property is a non-empty string, the adapter uses it as the native NornicDB edge type. Without that source property, the relationship remains untyped; the adapter must not invent a semantic type.

The complete PGM property map remains authoritative even when native storage cannot exactly represent a source value.

## Commands

```bash
nornicdb-admin database import okf knowledge \
  --from-path ./bundle \
  --profile okf \
  --mode fail-if-exists \
  --property-map ./properties.env
```

`properties.env` uses `source_property=storage_property` assignments, for example `_okf_frontmatter=source_metadata`. The same mapping applies to node and relationship properties.

Initial implementation accepts a directory. `.zip`, `.tar`, and `.tar.gz` support is a later, independently tested phase. It must use the same parser after safe extraction and reject traversal, escaping symlinks, and ambiguous roots.

### Import modes

| Mode | Behavior |
| --- | --- |
| `fail-if-exists` (default) | Implemented. Fails only when the target namespace already contains the mapped concept-ID property. A non-OKF graph may coexist. |
| `merge` | Planned. |
| `replace` | Planned. |

`merge` deletes and recreates outgoing projected edges rather than patching individual occurrences. PGM treats repeated links as separate occurrences, and a duplicate's ordinal may change after edits.

## Import pipeline

1. Resolve input root and enumerate files in slash-normalized lexical order.
2. Parse and validate every concept before changing storage.
3. Build an in-memory concept-ID map, link-occurrence list, warnings, and canonical source records.
4. Begin the selected namespace operation.
5. Write or update nodes in batches using the existing `pkg/adminimport` bulk path.
6. Write resolved edges in batches only after all target nodes exist.
7. Store unresolved relationship records on source nodes; do not manufacture graph nodes.
8. Return a deterministic in-memory report. File reports, schema/index definitions, and search builds are separate future work.

The offline importer must not start a server, invoke Cypher, call `embed.NewEmbedder`, or generate managed embeddings. Existing runtime workflows may embed nodes later.

## Validation and diagnostics

`adminimport.ValidateOKF` uses the same parser and diagnostics as import but writes no database state. Diagnostics are sorted by `(path, line, code, message)` and grouped as follows:

| Category | Examples | Behavior |
| --- | --- | --- |
| OKF error | Missing/invalid frontmatter, missing type, invalid reserved-file structure | Reject |
| PGM warning | Unusable brace-leading PGM title | Preserve the original Markdown and continue |
| Warning | Broken concept link, unknown OKF version, unusable brace-leading PGM title | Continue and report |
| Adapter warning | Valid source value cannot be a native property or edge type | Preserve canonical source; continue with documented fallback |

The report includes effective profile, versions, input root, node count, resolved-edge count, unresolved occurrence count, copied-property count, fallback count, errors, warnings, and adapter warnings.

## Export contract

### `--scope=imported` — initial source-fidelity export

Exports only nodes with `_okf_bundle` and `_okf_path` for the selected database namespace. It writes concept files from `_okf_frontmatter` and `_okf_body` in lexical `_okf_path` order. The original body contains original Markdown links, so this retains unresolved links, repeated links, fragments, ordinary titles, and PGM annotations without reconstructing them from database edges.

This is the supported round trip: `OKF/PGM bundle → NornicDB adapter → bundle` preserves the upstream data model. PGM itself does not preserve presentation-only YAML details such as comments, styles, anchors, aliases, or mapping order when canonicalization is selected.

If a node or adapter-owned edge changed through Cypher after import, export reports source divergence. `--on-divergence=fail|preserve-source|project` defaults to `fail`; `preserve-source` emits the original bundle and reports graph edits as not exported; `project` is deferred until generic projection exists.

### `--scope=project` — later and explicitly lossy

Projects selected native NornicDB nodes and relationships into a new PGM bundle. It is not part of the first release because arbitrary property graphs cannot be losslessly represented as OKF Markdown.

When implemented it must require `--profile=pgm-0.4-draft`, create ordinary Markdown Concept Links with YAML-flow-map titles for relationship properties, emit canonical encodings for values without direct YAML mappings, report every unrepresentable choice, and never advertise the result as source-fidelity export.

## Implementation layout

Extend the existing offline admin stack; do not create a parallel command or storage path.

```text
cmd/nornicdb-admin/main.go     command wiring and exit-code mapping
pkg/adminimport/okf.go         public options, reports, profile/mode validation
pkg/adminimport/okf_parse.go   safe traversal, frontmatter and CommonMark parsing
pkg/adminimport/okf_validate.go OKF and optional PGM validation
pkg/adminimport/okf_project.go Core Result to NornicDB node/edge projection
pkg/adminimport/okf_write.go   fail-if-exists, merge, replace, bulk writes
pkg/adminimport/okf_export.go  imported-scope source-fidelity export
pkg/adminimport/okf_pgm.go     PGM canonical values and relationship-ID adapter
```

Use `gopkg.in/yaml.v3` only with safe decoding and explicit limits. Use a CommonMark-capable parser rather than regular expressions. Document limits for file count, file size, Markdown nesting, YAML aliases/nesting, relationship count, and archive expansion before archive support ships.

## Delivery phases

### Phase 1 — OKF 0.2 validation and source fidelity

- Directory input only.
- Parse concepts, reserved files, paths, and standard concept links.
- Implement library validation and `database import okf --profile=okf --mode=fail-if-exists`.
- Project resolved links as untyped edges; preserve unresolved occurrences in source metadata and reports.
- Never create embeddings or start a server.

### Phase 2 — PGM 0.4.0 Public Draft adapter

- Add explicit PGM profile selection.
- Implement CommonMark link-title YAML flow-map handling, complete canonical property maps, relationship keys, IDs, and occurrence ordinals.
- Add typed edge projection only when the PGM source mapping supplies `type`; keep untyped links untyped.
- Ship upstream PGM fixtures and a NornicDB adapter conformance statement.

### Phase 3 — archives and generic projection

- Add safely extracted archive inputs.
- Add `--scope=project` with explicit lossy-projection reports and fixtures.
- Do not block Phases 1–2 on generic graph export.

## Acceptance tests

- Minimal OKF 0.2 bundles import and export deterministically.
- `index.md` and `log.md` never become nodes.
- Every resolved ordinary concept-link occurrence becomes a directed edge; repeated links remain repeated edges.
- Broken links are warnings, create no placeholder node, remain in source-fidelity export, and are counted in reports.
- Relative and root paths resolve correctly; traversal and reserved-file links never create graph edges.
- `merge` refreshes only adapter-owned outgoing edges for changed source concepts and preserves unrelated graph data.
- `replace` deletes adapter-owned edges before adapter-owned nodes in only the selected bundle namespace.
- Import never starts a server or generates embeddings.
- PGM fixtures prove typed and untyped links, complete property preservation, duplicate occurrence IDs, type fallback, and canonical output.
- PGM adapter failures are reported separately from OKF validation failures.
- Imported-scope export/import preserves concept identity, frontmatter data model, body, and link occurrences.
- Generic projection, once implemented, has explicit lossy-report fixtures and never passes source-fidelity tests.
