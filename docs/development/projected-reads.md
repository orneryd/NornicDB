# Projected reads without a storage migration

Projected reads request named properties instead of materializing every property
of an entity. They work with the ordinary NornicDB node and relationship formats.
They do not mean that Badger reads individual property bytes from a device:
Badger still reads and decodes its normal blocks.

## Default read behavior

Safe indexed-candidate projection, retained property-use analysis and a lazily
widened body cache run automatically. No feature flag or data conversion is
required. These optimizations do not select a writer, change schema versions,
migrate existing data or enable an experimental table codec.

For omitted strings, byte arrays and opaque MessagePack extensions, the read path
checks the span and advances the unbuffered reader without copying payload bytes
into decoder scratch. Selected values retain strict typed decoding; containers
retain the canonical decoder walk. This changes no stored bytes.

`ProjectedNodeReader.GetNodeProjected` takes property names; a nil projection
requests a full entity. Storage wrappers and managed databases forward the
capability. Query planning requests a projection only when it can prove which
properties the read needs. Whole entities, writes and unproven expressions retain
the full-entity path.

The cache keeps one image per entity and physical version. Wider projections
decode missing properties and merge coverage, including known-absent fields.
Results belong to the caller. Snapshot versions, acknowledged write overlays,
invalidation and byte budgets keep their existing owners. Large label scans stop
admitting new projected images when the known label exceeds cache capacity;
entries already held still widen.

## Dependency and format boundary

The module replacement in `go.mod` pins an exact commit from
`orneryd/nornic-badger` branch `feature/database-layout-v1.5.3`. The fork
combines experimental property codecs and columnar SST blocks with ordered
transaction writes. NornicDB selects the experimental encodings only for a
new isolated database created with `OPTIONS {layout: 'experimental'}`.
Ordinary databases use the standard format and receive projected reads
automatically. See the [admin guide](../operations/admin-tool.md#database-storage-layout)
for creation and import limits.

## Qualification

Storage tests exercise ordinary
persisted data, unchanged primary bytes, ordinary table compression, reopening,
cache widening, ownership, snapshot visibility and
scan admission. Query tests cover indexed plans, residual predicates, whole-node
fallback and forwarding through a managed database. Historical benchmark
receipts describe the revisions and workloads they measured. Performance
qualification for the combined branch requires a new comparison.
