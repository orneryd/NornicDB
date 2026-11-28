# Projected reads without a storage migration

Projected reads request named properties instead of materializing every property
of an entity. They work with the ordinary NornicDB node and relationship formats.
The default format still reads normal Badger blocks and full value-log values;
only a newly created isolated store with `projected: true` uses bounded property
reads from Badger's value log.

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
`orneryd/nornic-badger` with the bounded value-range API. The fork also
combines experimental property codecs and columnar SST blocks with ordered
transaction writes. NornicDB selects the experimental encodings only for a
new isolated database created with `OPTIONS {layout: 'experimental'}`.
Ordinary databases use the standard format and receive projected reads
automatically. See the [admin guide](../operations/admin-tool.md#database-storage-layout)
for creation and import limits.

## Opt-in physical projection

`CREATE DATABASE name OPTIONS {storage: 'isolated', projected: true}` chooses
the additive projected node format for a new persistent store. The default is
`false`; existing stores keep their bytes and cannot be converted in place.
The catalog option and an on-disk marker must agree when reopening a store.

Each projected node has a small metadata header and a sorted directory from
property key ID to offset, length, and checksum within that node's value.
Property IDs resolve in the same Badger snapshot as the node. Badger follows
its current value pointer and reads only the requested ranges from a value-log
record, including when encryption is enabled. The directory moves with the
record when value-log GC relocates it, so it never stores an absolute SST or
value-log address. Inline values are already present in Badger's block.
When whole-value checksum verification is configured, the range API reads the
whole value to retain that existing integrity check. An eligible finite
projection decodes only selected properties; a full-entity read decodes all.

## Inside the SST block

Both layouts start from the same Badger record structure. The example shows
four node entries with two property tokens. Letters stand for bytes in the
already encoded property values. For eligible experimental values, those
bytes may themselves include a compressed property frame.

```text
Standard block before the ordinary Badger codec

entry 0: [header | key suffix | value prefix | T7: a0 a1 | T9: x0 x1 | node metadata]
entry 1: [header | key suffix | value prefix | T7: b0 b1 | T9: y0 y1 | node metadata]
entry 2: [header | key suffix | value prefix | T7: c0 c1 | T9: z0 z1 | node metadata]
entry 3: [header | key suffix | value prefix | T7: d0 d1 | T9: w0 w1 | node metadata]
footer:  [entry offsets | entry count | checksum | checksum length]

Experimental block when the column transform wins

[mode 2][Zstd of:
  [BCT2 | original block length | span count]
  [span directory: gap, length, record kind, column ID per span]
  [unannotated bytes in original order, including keys and footer]
  [T7 position 0: a0 b0 c0 d0 | T7 position 1: a1 b1 c1 d1]
  [T9 position 0: x0 y0 z0 w0 | T9 position 1: x1 y1 z1 w1]
]
```

The value prefix includes Badger value metadata and expiry, followed by the node format byte, property payload length, and property count. The sketch highlights property spans. NornicDB can also annotate node
metadata by field position and compact relationship or adjacency fields such
as endpoints, timestamps, and type. Badger groups spans by record kind and
column ID. It aligns bytes from similar-length values and keeps skewed groups
sequential. Each block stores a one-byte mode followed by the smallest
candidate payload it tried: raw (0), ordinary Zstd (1), or the transformed
frame compressed with Zstd (2).

On read, a mode 2 block uses the span directory to put the bytes back at
their original offsets. This reconstructs the original Badger block,
including its entry order and footer. NornicDB then decodes the requested
properties from those records. The span directory drives block reconstruction
and does not provide a disk address for fetching one property by itself.

## Qualification

Storage tests exercise ordinary
persisted data, unchanged primary bytes, ordinary table compression, reopening,
cache widening, ownership, snapshot visibility and
scan admission. Query tests cover indexed plans, residual predicates, whole-node
fallback and forwarding through a managed database. Historical benchmark
receipts describe the revisions and workloads they measured. Performance
qualification for the combined branch requires a new comparison.
