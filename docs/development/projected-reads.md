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

The dependency is pinned to `orneryd/nornic-badger` branch
`release/projected-reads-v4.9.6`. That branch starts at upstream v4.9.6 and changes
only its integration note, not Badger runtime code. No fork-side performance gain
or new physical projection primitive is claimed by this integration.

Experimental property compression, integer columns, columnar SST blocks and the
offline conversion command remain on the separate storage-layout branch. They
are not included here. The format-free branch contains none of their encoders
or readers, so projected reads require neither converted files nor
experimental-format data.

## Qualification

This integration is a draft for focused review. Storage tests exercise ordinary
persisted data, unchanged primary bytes, ordinary table compression, reopening,
cache widening, ownership, snapshot visibility and
scan admission. Query tests cover indexed plans, residual predicates, whole-node
fallback and forwarding through a managed database. Full-suite and performance
qualification remain separate; results from the storage-layout experiment do
not establish performance of this format-free branch.
