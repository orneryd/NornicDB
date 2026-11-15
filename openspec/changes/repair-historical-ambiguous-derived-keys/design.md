# Record-local repair of historical ambiguous derived keys

## Problem

A historical NornicDB writer serialized `WALBufferedCommitData` as a plain JSON
object. `encoding/json` replaces every invalid UTF-8 byte of a Go string with
U+FFFD, so a map key containing one (an unmappable physical key) is written as a
*different* key, and two keys that share that decoded form collapse into one
entry. Derived Badger keys embed big-endian numeric IDs; and a JSON string is
invalid UTF-8 as soon as any byte of that ID is in `0x80..0xFF`. So a historical
buffered commit whose commit body was never applied to Badger can name a numeric
ID no decoded key resolves, and the upgrade used to refuse the whole store.

The V4 upgrade previously refused every such record. This change repairs the
records whose own bytes still determine the damaged entries, and keeps refusing
the rest.

## Scope: the eight re-derivable families

Exactly eight physical key families are a pure function of facts a buffered
commit still names exactly, because those facts are string-keyed or stored bodies
and therefore survive the same encoder:

| family | key layout | facts |
|---|---|---|
| label index | `0x03 label 0x00 nodeNum` | stored node body labels + forward node binding |
| outgoing adjacency | `0x04 startNum edgeNum` | relationship compact header + forward relationship binding |
| incoming adjacency | `0x05 endNum edgeNum` | relationship compact header + forward relationship binding |
| relationship type index | `0x06 type 0x00 edgeNum` | relationship compact header + forward relationship binding |
| between set | `0x11 startNum endNum type 0x00 edgeNum` | relationship compact header + forward relationship binding |
| between head | `0x12 startNum endNum type` | relationship compact header + forward relationship binding |
| node reverse dictionary | `0x1D nodeNum` | forward node binding |
| relationship reverse dictionary | `0x1E edgeNum` | forward relationship binding |

No other family is repaired. A damaged key in an unclassified family, or in a
family the writer knows but cannot re-derive (embeddings, MVCC images, schema,
property-key dictionary, change events, receipts), refuses the record.

## Decision, and why it is record-local

Facts are read only from the record: `0x01`/`0x02` bodies (labels through the
stored-body reader the label index rebuild uses; endpoints and type through the
compact header owner), and `0x1A`/`0x1B` forward numeric bindings. Nothing durable
is consulted. An unapplied commit's entities exist only inside its own record, so
absence from Badger says nothing about recoverability, and a flush receipt says
the commit's writes reached Badger, not that its record became replayable.

`Ops` are never read: their property values are the same lossy projection, and a
lost value must not become authority by being decoded.

A record is refused as a whole, with every reason reported, when:

- a damaged key is outside the eight families, or a damaged key remains
  unproven;
- a damaged key is a *deletion* (which old entry it removed is not a fact the
  record holds);
- the record carries cross-store numeric bindings, so its numeric IDs are another
  store's dictionary;
- an entity body the record writes has no forward numeric binding in the same
  record: the damaged key keeps only the structure of its numeric IDs, so any
  body could be the one it names, and restoring a subset would apply an
  incomplete commit (this is the "record-local facts missing" case; the operator
  route is the old build that wrote the journal, not an inferred repair);
- two relationships of different identity share one between-head key, whose value
  is a write-order artifact.

Because the old map encoding merges every entry of one decoded form into one, a
collision class is restored from the facts that name its members. Counting the
surviving replacement characters understates the loss: a fixture with 146 damaged
keys merged 144 entries away.

## Where the decision lives

`v4UpgradeRecords.convert` is the existing conversion owner shared by validation
and the conversion pass. It normalizes a buffered commit before any ambiguity or
frame-size decision, so the file-only preflight (`ValidateWALFormat`, the boundary
`nornicdb.Open` applies before Badger opens), the V4 validation and the write pass
all take the same pure decision, and a record the upgrade cannot prove is refused
before anything is written. The repaired payload replaces the record's data and
its checksum; the record encoder then plans the frame size from the new payload.
The derivation itself is `repairV4BufferedCommitPhysicalKeys` in
`pkg/storage/wal_v4_buffered_key_repair.go`.

## Encoding losslessness versus named-property authority

Re-encoding a repaired record with the current typed MessagePack codec preserves
physical key bytes, which would otherwise be read as "this record's named
property images are authoritative" and promote the lossy `Ops` property values.
`decodeBufferedWALRecord` therefore reports two separate facts:

- `losslessPhysicalKeys` — whether the payload's *encoding* preserved physical key
  bytes (true for the typed envelope, false for historical plain JSON). The
  ambiguity decision depends only on this.
- `namedPropertiesAuthoritative` — true only when the typed encoding is present
  *and* the new `PhysicalPropertyImages` marker is absent. The marker travels on
  repaired records, so their `Ops` property values stay historical.

Current typed records keep named-property authority unchanged (marker absent).
Legacy plain JSON keeps both facts false.

## Compatibility

No Badger key layout, payload format or storage version changes. The
`WALMessagePack` codec and the framing codec are untouched; no new JSON WAL
writing and no gob path were added.

## Known limits

- A record that does not itself prove the damaged entries still refuses. The
  supported recovery route for a refused store is its own old build, which can
  still read the journal; no repair is inferred from durable state.
- The acceptance fixtures are a synthetic historical codec (a captured buffered
  commit re-encoded the way the old writer encoded it) and are not claimed to be
  bytes from a genuine old producer. The genuine historical fixtures in
  `pkg/storage/testdata/wal-pre-identity` remain the unchanged control.
- Public startup is exercised through `nornicdb.Open` in an external-package test.
  A test-only fixture bridge supplies a disposable legacy journal; public Cypher
  verifies recovered node counts and a relationship after startup and reopen.
- The property-binding guard owner inventory remains unchanged pending explicit
  operator approval for the new physical-write-image producer.
