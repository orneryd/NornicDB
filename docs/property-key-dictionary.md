# Property-Key Dictionary (Storage V2)

## Summary

Storage V2 tokenizes property-key **names** to per-namespace varint IDs inside node and edge bodies. Instead of every record carrying the full string `productName`, `unitPrice`, `description`, etc., each name is allocated a small integer ID once per namespace and the bodies reference the ID. The dictionary lives in Badger under its own prefix, hydrates into memory on engine open, and persists changes atomically with the bodies that reference it.

The on-disk schema version moves from V1 to V2. Upgrade is **forward-only and gated** — operators must explicitly authorize the migration with `--upgrade-storage`, after which the engine eagerly rewrites every node and edge body to V2 format before accepting traffic. Once the migration completes the store is pure V2: no mixed-format coexistence, no per-record format checks on the hot path.

Measured savings: **~17–21% reduction** in encoded body size on a 100-node corpus that shares 8 property keys.

---

## Why

Under V1, `node.Properties` was serialized as a string-keyed msgpack map. Every record paid the full byte cost of every key name it carried. On a Northwind Product node:

```
productID, productName, sku, unitPrice, unitsInStock,
discontinued, description, tags
```

That's ~79 bytes of pure key-name overhead per record. Across a 100k-entity seed, ~30 MB of waste; extrapolated to 1B entities at the same schema shape, ~75 GB.

Neo4j's `PropertyKeyTokenStore` solves this by storing each name once and referencing it by 32-bit token in record bodies. V2 does the same with varint IDs, scoped per-namespace so multi-tenant deployments keep their ID spaces isolated.

---

## How it works

### Dictionary data structure

A single engine-wide `propertyKeyDictionary` struct holds per-namespace forward and reverse maps:

```go
type propertyKeyDictionary struct {
    forward map[string]map[string]uint64  // namespace -> (name -> id)
    reverse map[string]map[uint64]string  // namespace -> (id -> name)
    nextID  map[string]*atomic.Uint64     // namespace -> monotonic counter
    txnCounters map[*badger.Txn]map[string]uint64
}
```

Reads take an RLock; allocation takes a brief write lock to commit a new (name, id) pair. Concurrent goroutines racing to allocate the same name converge on the same ID — the loser's allocation is discarded and never persisted.

### On-disk layout

Three new Badger prefixes, all length-prefixed by namespace to prevent ambiguity between e.g. `("ab", "c")` and `("a", "bc")`:

```
prefixPropKeyForward (0x20):
  + varint(len(namespace)) + namespace
  + varint(len(name)) + name
  -> varint(id)

prefixPropKeyReverse (0x21):
  + varint(len(namespace)) + namespace
  + varint(id)
  -> name

prefixPropKeyCounter (0x22):
  + varint(len(namespace)) + namespace
  -> varint(next_id)
```

The counter is the high-water mark per namespace. It's persisted via the same per-txn batching pattern as the existing id dictionary: N allocations in one transaction produce exactly **one** counter `Set` per dirty namespace at commit, not one per allocation.

### Body layout

**Nodes** gain a leading format byte:

```
[nodeFormatTokenizedV1 = 0x10]
[varint(propsLen)]
[tokenizedProps]                  -- the property-key payload
[standardNodeMsgpackWithNilProps] -- the rest of the Node struct, Properties cleared
```

The `Properties` field is set to `nil` before encoding so it doesn't appear twice in the body.

**Edges** keep the V1 compact codec's fixed-width header (start/end numIDs, timestamps, confidence, flags) but bump the format byte to `edgeFormatCompactV2 = 0x03` and use a tokenized property payload in place of the V1 string-keyed map.

### Tokenized property payload

Hand-rolled rather than `msgpack(map[uint64]any)`:

```
varint(count)
repeated count times:
  varint(id)          -- per-namespace dictionary ID
  msgpack(value)      -- preserves original value type
```

**Why hand-rolled?** msgpack-go encodes uint64 map keys as full 9-byte uint64 forms regardless of value, which would actually *increase* body size compared to short string keys. `UseCompactInts` fixes the keys but also narrows integer **values** (`int64(30)` → `int8(30)`), changing user-observable types on round-trip. The varint+msgpack(value) layout gives compact keys without touching value encoding.

### Encode path

`(b *BadgerEngine).encodeNodeInTxn(txn, namespace, node)` runs inside the caller's Badger transaction so dictionary forward/reverse/counter writes commit atomically with the body that references them. If a property name has never been seen in this namespace, the encoder allocates a fresh ID; if it has, the encoder reuses the existing one.

### Decode path

The hot-path decoder in V2 stores **rejects** any non-V2 body with a hard error. The legacy V1 decoder survives only inside `migration_v1_to_v2.go` for the duration of an upgrade. After migration, no live code path can produce or consume an untokenized body.

If a body references a dictionary ID with no reverse-map entry, the decoder returns `property key id %d not in dictionary for namespace %q` rather than silently dropping the property. Tests assert this error contract on exact message text — callers parse it for diagnostic output.

### Cypher property-name semantics

Tokenization changes WHAT is stored, not WHEN a property exists.

- `MATCH (n) RETURN n.unknownField` against a name no entity has set → `null`, no error. Identical to V1 / Neo4j.
- A name in some namespace's dict but not on this entity → `null`. Dictionary tracks "names ever seen"; presence on a specific record is per-record.
- A body referencing a missing dictionary ID → hard error (corruption signal).

---

## Migration

### Upgrade gate

The engine refuses to open a data directory whose schema version is older than the binary's:

```
storage upgrade required: on-disk version 1 is older than binary version 2;
back up the data directory and restart with --upgrade-storage to authorize
the one-way upgrade
```

`--upgrade-storage` (or `NORNICDB_UPGRADE_STORAGE=1`) authorizes every applicable migration arm to run on next open. Empty stores skip the gate — there's nothing to lose.

### Chained migrations

`RunOnStartMigrations` runs migration arms in order based on the on-disk version, each preserving the prior step's invariants:

| From → To | Trigger             | Action                                                                                          |
| --------- | ------------------- | ----------------------------------------------------------------------------------------------- |
| V0 → V1   | on-disk version 0   | Existing — extracts legacy access state into AccessMeta records. Idempotent, body bytes intact. |
| V1 → V2   | on-disk version 1   | Eager rewrite of every node + edge body to the tokenized codec. Bumps version on clean pass.    |

A V0 store with `--upgrade-storage` runs both arms in sequence: V0→V1, then V1→V2 against the resulting V1-shaped store.

### V1→V2 rewrite

Walks `prefixNode` and `prefixEdge` in bounded batches (default 500 records per batch). For each body:

1. Skip if the body already starts with the V2 format byte (resumability — partial passes pick up where they left off).
2. Decode through the legacy codec. The V1 edge decoder additionally handles pre-compact gob/msgpack-wrapped edges, allocating numIDs through the id dictionary as needed.
3. Re-encode through the tokenized codec, which allocates dictionary IDs as a side effect inside the same transaction.
4. Write back to the same key.

Each batch commits atomically. The schema version bump is the **last** write — a crash mid-pass leaves the store at V1, and the next start-up resumes the rewrite from where it stopped.

### Failure semantics

- Decode error on a body → migration aborts, version stays V1, error names the offending entity ID. Operator restores from backup or fixes the corruption manually.
- Node body whose ID lacks a namespace prefix → migration aborts (every body must be namespaced; this is a corruption signal).
- Edge endpoints with no idDict reverse-map entry → migration allocates the missing numIDs from the body's string IDs and continues.

---

## Engine wiring

```go
type BadgerEngine struct {
    // existing fields...
    propKeyDict    *propertyKeyDictionary  // hydrated on open
    storageVersion int                     // post-migration version
}
```

`storageVersion` is set once in `RunOnStartMigrations` and read-only thereafter. The encode path picks codecs deterministically from this value. There's no runtime atomic toggle.

The transaction commit path flushes both the id-dict counters and the property-key counters in the same place:

```go
if err := tx.engine.idDict.flushTxnCounters(tx.badgerTx); err != nil { ... }
if err := tx.engine.propKeyDict.flushTxnCounters(tx.badgerTx); err != nil { ... }
```

Engine open ordering changed: schema migrations run **before** the edge-between index backfill goroutine starts. If the backfill ran concurrently with V1→V2's many small write transactions, Badger's `DropPrefix` would block migration writes ("Writes are blocked, possibly due to DropAll or Close"). Sequencing migrations first eliminates the race.

---

## What was deleted along the way

The gob-serializer plumbing on the data path is gone:

- `BadgerOptions.Serializer`, `Database.StorageSerializer`, env var `NORNICDB_STORAGE_SERIALIZER`, yaml `storage_serializer` — all removed.
- `SetStorageSerializer`, `currentStorageSerializer`, `ParseStorageSerializer`, `encodeWithSerializer`, `decodeWithSerializer` — all removed.
- `encodeValue` always emits msgpack with the standard NDB header.
- `decodeValue` retains a single gob fallback arm for header-less legacy bodies, used **only** by the in-place gob → msgpack migration tool. Production read paths never hit it.

The legacy V1 node/edge encoders (`encodeNodeV1`, `encodeEdgeCompactV1`, `decodeNodeV1`, `decodeEdgeCompactV1`) survive only because the V1→V2 migration needs them to read pre-V2 bodies. They're not referenced from any hot path, and can be deleted whenever the team is comfortable that no V1 stores exist anywhere in production.

---

## Test coverage

All new files (`property_key_dictionary.go`, `property_codec.go`, `migration_v1_to_v2.go`, V2 codec arms in `edge_compact.go` and `badger_helpers.go`, the upgrade gate in `migration_runner.go`) target deterministic deep-asserting tests:

- Every conditional branch and every error return.
- Specific error messages asserted, not just `require.Error` — the error contract is part of the API.
- Malformed inputs constructed deliberately (truncated bodies, varint overflows, dictionary IDs with no reverse-map entry, mismatched format bytes).
- Migration resumability verified by setting up half-converted stores and asserting the next pass finishes the work.
- The varint reader is tested at every boundary: 9-byte truncation, 10-byte max-uint64 round-trip, 10th-byte continuation overflow, 10th-byte value > 1 overflow.

---

## Operational checklist for upgrades

1. Take a backup of the data directory.
2. Deploy the new binary.
3. First start: pass `--upgrade-storage` (or set `NORNICDB_UPGRADE_STORAGE=1`).
4. Engine runs the migration chain, emits structured progress logs, then begins serving traffic.
5. Subsequent starts don't need the flag — the version is already current.

Downgrade is not supported. Operators who need to revert restore from the pre-upgrade backup.
