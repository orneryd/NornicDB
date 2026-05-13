# Property-Key Dictionary Plan

**Status:** Proposed
**Date:** May 13, 2026
**Scope:** Introduce a per-namespace property-key dictionary that tokenizes property-key _names_ to varint IDs in node and edge bodies. Storage version bumps from V1 to V2. Upgrade is one-way and gated behind `--upgrade-storage`; once invoked the engine runs every applicable migration arm in order (V0→V1, V1→V2, …) and eagerly rewrites all bodies so the store is pure V2 at the end. Old binaries cannot read V2 bodies — operators back up before flipping the switch.

---

## 1. Objective

Eliminate repeated property-key name strings from node and edge bodies. Under the current msgpack encoding, every entity's property map pays ~80–100 B of pure overhead for key names like `productName`, `unitPrice`, `description`. Over a 409k-entity seed this already accounts for ~30 MB of wasted bytes; at 1 B entities the savings project to roughly 75 GB.

The dictionary is **per namespace** (per logical database) so multi-tenant instances keep their key ID spaces isolated. Tenants with schema sprawl cannot exhaust another tenant's ID space, and each dictionary compacts independently. Property-key IDs are **varint-encoded**: the top ~20 keys in any real schema cover >80% of occurrences and fit in one byte, while the ceiling remains effectively infinite at ~2 B distinct keys per namespace.

---

## 2. Problem Statement

A Northwind Product node today occupies ~296 B in its encoded property map. Of that, ~79 B is key-name overhead (`productID`, `productName`, `sku`, `unitPrice`, `unitsInStock`, `discontinued`, `description`, `tags`). Every copy of the same schema pays this cost again.

Neo4j avoids this with a global `PropertyKeyTokenStore`: each key name is stored once and referenced by a 32-bit token inside record bodies. NornicDB's current msgpack encoding has no such indirection — `node.Properties` is serialized as a string-keyed map by the msgpack library, which writes each key name inline.

The savings at scale are material:

- Northwind 100k seed: ~30 MB on property maps alone
- Extrapolated to 1 B entities with the same schema shape: ~75 GB

Beyond byte savings, tokenizing property keys produces a single source of truth for the set of keys a namespace has ever used, which is useful for schema introspection, observability, and future planner decisions (e.g., "does this key exist at all before we plan an index probe").

---

## 3. Compatibility model

Storage upgrade is **forward-only and gated**.

- A binary that knows about V2 cannot ship a V1 reader for V2 bodies — old binaries do not have the new format byte, the new prefixes, or the property-key dictionary keyspace. Forward compatibility is therefore not on the table.
- A V2-aware binary opens a V0 or V1 data directory in **read-only mode** until the operator passes `--upgrade-storage`. With the flag, the engine runs every applicable migration arm in order (V0→V1 if needed, then V1→V2) before accepting writes.
- After V1→V2 completes, the store is **pure V2** — every body has been rewritten to the tokenized format. There is no mixed-format coexistence and no lazy upgrade. The decoder can therefore reject untokenized bodies with a hard error in V2 code paths.
- Operators are expected to back up before passing `--upgrade-storage`. Downgrade requires restoring from that backup.

The `--upgrade-storage` flag is the universal gate. It does not target a specific version — it authorizes the binary to advance the store through whichever migration arms it understands. Today that's "up to V2." A future binary that knows V2→V3 will use the same flag.

---

## 4. Scope

### In scope

- A new `propertyKeyDictionary` mirroring `idDictionary`'s structure: per-namespace forward (name → id) and reverse (id → name) maps, backed by Badger keys.
- Varint encoding of property key IDs inside node and edge bodies, framed by per-record format bytes (`nodeFormatTokenizedV1 = 0x10`, `edgeFormatCompactV2 = 0x03`).
- A storage-version bump from V1 to V2, gated behind `--upgrade-storage`.
- An **eager rewrite migration** that walks every node and edge body, decodes it through the legacy codec, re-encodes it through the tokenized codec, and writes it back. The version bump is the last step so a crash mid-migration leaves the store in a resumable state.
- A `--upgrade-storage` flag at the binary level that authorizes all applicable migration arms to run on open.
- Read-only refusal at engine-open time when the on-disk version is below the binary's version and `--upgrade-storage` was not passed.

### Out of scope

- Tokenizing property _values_. Values have unbounded cardinality and no Zipfian concentration worth exploiting at this layer.
- Tokenizing labels or edge types. Both already get numID'd in secondary index keys via idDict; the inline label/type strings are small relative to property keys.
- Freelist / recycling of property-key IDs. In real schemas property key names are effectively immortal — once `productName` exists, it exists forever.
- Lazy / mixed-format bodies. The eager rewrite eliminates the need for a per-record decoder switch in V2 code paths.
- Downgrade from V2 to V1. Operators restore from backup if they need to roll back.

---

## 5. Migration chain

`RunOnStartMigrations` already runs migrations in order. The new V1→V2 arm appends to that chain:

| From → To | Trigger                                | Action                                                                                                              |
| --------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| V0 → V1   | On-disk version 0 (pre-AccessMeta)     | Existing — extracts legacy access state into AccessMeta records. Idempotent, body bytes untouched.                  |
| V1 → V2   | On-disk version 1, `--upgrade-storage` | Eager rewrite: walk all node + edge bodies, decode-then-tokenize, write back. Bump version to V2 only on full pass. |

If the on-disk version is V0 and the operator passes `--upgrade-storage`, both arms run sequentially: V0→V1 first (preserving its invariants), then V1→V2 against the V1-shaped store.

If `--upgrade-storage` is not passed and the on-disk version is below V2, the engine refuses to start with a clear error message naming the from/to versions and telling the operator to back up first.

---

## 6. Design

### 6.1 Storage version

```go
const (
    storageVersionV1                 = 1
    storageVersionPropKeyDictV2      = 2
    storageVersionCurrent            = storageVersionPropKeyDictV2
)
```

`engine.storageVersion` is set once during `NewBadgerEngineWithOptions` after migrations run. The encode path picks codecs deterministically from this value. There is no atomic toggle and no per-record version check on the encode hot path.

### 6.2 Dictionary structure

A single engine-wide `propertyKeyDictionary` struct holds state for all namespaces. The outer map is keyed by namespace string; inner maps are the forward and reverse indexes for that namespace.

```go
type propertyKeyDictionary struct {
    mu      sync.RWMutex
    forward map[string]map[string]uint64    // namespace -> (name -> id)
    reverse map[string]map[uint64]string    // namespace -> (id -> name)
    nextID  map[string]*atomic.Uint64       // namespace -> monotonic counter

    txnMu       sync.Mutex
    txnCounters map[*badger.Txn]map[string]uint64
}
```

Locks are split: the outer RWMutex guards structural additions; lookups on the fast path take RLock only. Allocation takes Lock briefly to commit a new (name, id) pair.

### 6.3 Badger key layout

```
prefixPropKeyForward  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  + varint(len(key_name))
  + key_name bytes
  -> varint(id)   (value)

prefixPropKeyReverse  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  + varint(id)
  -> key_name bytes   (value)

prefixPropKeyCounter  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  -> varint(next_id)   (value)
```

Length-prefixing the namespace guarantees no ambiguity between e.g. namespaces `"ab"` and `"a"` + key `"b..."`.

### 6.4 Allocation API

```go
func (d *propertyKeyDictionary) resolveOrAllocateInTxn(
    txn *badger.Txn, namespace, name string,
) (uint64, error)

func (d *propertyKeyDictionary) lookup(namespace string, id uint64) (string, bool)
```

Counter batching mirrors `idDictionary`: per-txn high-water marks staged via `recordTxnCounterUse`, flushed once per dirty namespace at commit by `flushTxnCounters`. Concurrent-create races are resolved via map re-check after the Badger writes; the losing goroutine's allocated ID becomes orphaned (safe — nothing references it yet, and there is no recycle).

### 6.5 Open-time hydration

At engine open, after idDict hydration:

1. Hydrate the property-key dictionary — single prefix scan of `prefixPropKeyForward` plus a scan of `prefixPropKeyCounter`, populating the in-memory maps and seeding `nextID` per namespace. On a fresh data directory the scan finds nothing and starts empty.
2. Read the on-disk schema version.
3. If the version is below `storageVersionCurrent`:
   - If `--upgrade-storage` was passed, run all applicable migration arms in order (V0→V1, V1→V2). Each arm bumps the version on its own success.
   - Otherwise, return an error from `NewBadgerEngineWithOptions` — the engine refuses to open.
4. Set `engine.storageVersion` to the post-migration version.

### 6.6 V1→V2 eager rewrite

The migration runs inside a series of bounded write transactions so it can be resumed if it crashes mid-pass. Pseudocode:

```
for each batch of <N> nodes under prefixNode:
    for each node body:
        if body already starts with nodeFormatTokenizedV1: skip
        decode via legacy codec
        encode via tokenized codec (allocates dictionary IDs as a side effect)
        write back under the same key
    commit batch (commits dictionary forward/reverse/counter entries atomically)

repeat for prefixEdge with the edge codec

write storage version 2
```

Properties:
- **Idempotent**: a partial pass leaves some bodies in V1 and some in V2 form; re-running picks up where it left off because the per-batch skip check looks at the body's leading byte.
- **Crash-safe**: each batch commits atomically; the version bump is the last write.
- **Bounded memory**: batch size is configurable (default 1000 records).
- **Progress reporting**: the migration emits structured logs every batch with `{converted, scanned, namespaces_seen}`.

### 6.7 Format bytes (V2 codec)

- Nodes: tokenized body framed with leading `nodeFormatTokenizedV1 = 0x10`. Layout:
  ```
  [0x10][varint(propsLen)][tokenizedPropsBytes][standardNodeMsgpackWithNilProperties]
  ```
  The standard Node msgpack body is unchanged except `Properties` is set to `nil` before encoding — properties live in the prepended section.
- Edges: existing compact-V1 codec extended to V2. Same fixed-width header (start/end numIDs, timestamps, confidence, flags); the trailing properties payload is `msgpack(map[uint64]any)` instead of `msgpack(map[string]any)`.

### 6.8 Decode path in V2

Once V2 is the active version, the decoder requires the tokenized format byte. An untokenized body in a V2 store is treated as corruption. The legacy decode arm survives only inside the migration code (`migration_v1_to_v2.go`) so it can read pre-rewrite bodies during the upgrade. After migration, no live code path can produce or consume untokenized bodies.

#### Cypher property-name semantics

Tokenization changes WHAT is stored on the body, not WHEN a property exists. Cypher property reads (`MATCH (n) RETURN n.unknownField`) continue to return `null` for property names that no entity has ever set — same as Neo4j. The dictionary is implementation detail; the engine does not surface its existence to query authors.

Three distinct cases need explicit test coverage:

1. **Read of a never-allocated property name**: returns `null`. Identical behavior to V1. The engine does NOT error just because the name is absent from the dictionary; an absent name is equivalent to an absent property on every node, and Cypher's missing-property semantics return `null`.
2. **Read of a property name that exists in some namespace's dict but not on this entity**: returns `null`. The dictionary tracks "names ever seen in this namespace"; presence on a specific entity is a per-record question.
3. **Decode of a body that references a dictionary ID we cannot resolve**: hard error with the namespace and ID in the message (`property key id %d not in dictionary for namespace %q`). This indicates store corruption — either the dictionary was wiped without rewriting bodies, or a body was written under a different namespace than the one its key is filed under. Tests assert this returns an error rather than silently dropping the property.

Test plan §7 calls these out as required cases.

### 6.9 Encoding API

`encodeNode` and `encodeEdge` take a namespace argument and a Badger txn so the tokenized codec can resolve property-key IDs inside the same transaction that writes the body. Removing the old single-argument signatures is the forcing function — compilation fails on every missed call site.

```go
// Old
func encodeNode(n *Node) ([]byte, bool, error)
func (b *BadgerEngine) encodeEdgeInTxn(txn *badger.Txn, edge *Edge) ([]byte, error)

// New
func (b *BadgerEngine) encodeNodeInTxn(txn *badger.Txn, namespace string, n *Node) ([]byte, bool, error)
func (b *BadgerEngine) encodeEdgeInTxn(txn *badger.Txn, namespace string, edge *Edge) ([]byte, error)
```

Decoder API stays string-keyed: callers continue to see `map[string]any` property maps. The reverse lookup happens inside `decodeNode` / `decodeEdgeBodyWithID` using the in-memory reverse map — no Badger read on the decode hot path.

---

## 7. Implementation inventory

### New files

| File                                          | Purpose                                                                                   |
| --------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `pkg/storage/property_key_dictionary.go`      | Dict struct, resolveOrAllocateInTxn, lookup, flushTxnCounters, loadFromBadger             |
| `pkg/storage/property_key_dictionary_test.go` | Unit tests: alloc, resolve, hydrate, two-namespace isolation, commit-flushes-counter-once |
| `pkg/storage/property_codec.go`               | Tokenized property encode/decode helpers                                                  |
| `pkg/storage/property_codec_test.go`          | Encode/decode round-trip tests                                                            |
| `pkg/storage/migration_v1_to_v2.go`           | Eager rewrite: walk nodes + edges, decode-then-tokenize, write back, bump version         |
| `pkg/storage/migration_v1_to_v2_test.go`      | Tests: full rewrite, resumable mid-crash, version bump only on success                    |

### Modified files (encoding sites)

| File:Line                           | Current                                             | New                                            |
| ----------------------------------- | --------------------------------------------------- | ---------------------------------------------- |
| `pkg/storage/badger_helpers.go`     | `encodeNode(n *Node) ([]byte, bool, error)`         | `(b *BadgerEngine).encodeNodeInTxn(txn, ns, n)` |
| `pkg/storage/badger_helpers.go`     | `decodeNode(data)`                                  | `(b *BadgerEngine).decodeNode(ns, data)` — V2-only |
| `pkg/storage/badger_helpers.go`     | `decodeNodeWithEmbeddings(txn, data, id)`           | namespace derived from id                      |
| `pkg/storage/edge_compact.go`       | `encodeEdgeCompactWithNums(edge, startNum, endNum)` | `encodeEdgeCompactV2(b, txn, ns, edge, startNum, endNum)` |
| `pkg/storage/edge_compact.go`       | `decodeEdgeCompact(data)`                           | dispatches V1 (legacy, migration only) vs V2   |
| `pkg/storage/edge_compact.go`       | `(b *BadgerEngine).encodeEdgeInTxn(txn, edge)`      | `(b *BadgerEngine).encodeEdgeInTxn(txn, ns, edge)` |

### Modified files (call sites)

Nine `encodeNode` call sites, three edge encode paths — same as before. Removing the old signatures is the forcing function.

### Modified files (engine init)

- `pkg/storage/badger.go` — add `propKeyDict` and `storageVersion` fields; init dictionary alongside `idDict`; refuse to open when `on_disk < binary` and `--upgrade-storage` was not passed.
- `pkg/storage/migration_runner.go` — append V1→V2 arm; set `engine.storageVersion` at end.
- `pkg/storage/badger_transaction.go:Commit` — add `engine.propKeyDict.flushTxnCounters(tx.badgerTx)` next to the existing idDict flush.

### Modified files (CLI / config)

- `cmd/nornicdb/main.go` — add `--upgrade-storage` flag, threaded into engine open.
- `pkg/nornicdb/config.go` — `Database.AllowStorageUpgrade bool`.
- `pkg/storage/badger.go:BadgerOptions` — `AllowStorageUpgrade bool` propagated from config.

---

## 8. Test plan

### Dict-level tests

- Allocate keys in namespace A, confirm forward+reverse round-trip.
- Same key name in namespaces A and B gets different IDs.
- Close engine, reopen, confirm dict state preserved.
- N allocations in one txn → exactly one counter Set per namespace at commit.
- Two goroutines racing to allocate the same key get the same ID back; no duplicate reverse entry persisted.

### Codec tests

- Encode a node with mixed property types (string, int, float, bool, array), decode, verify struct equality.
- Encode + decode an edge with tokenized properties.
- Tokenized body size is within 75–80% of the equivalent legacy body for a 100-node corpus sharing 8 property keys.

### Coverage requirement

All new files (`property_key_dictionary.go`, `property_codec.go`, `migration_v1_to_v2.go`, the V2 codec arms in `edge_compact.go` and `badger_helpers.go`, the upgrade gate in `migration_runner.go`) must hit **100% line coverage** in the storage test suite. This is mission-critical migration code — silent miscoding loses data. Tests must:

- Cover every conditional branch and every error return.
- Assert on specific error messages (e.g. `"property key id %d not in dictionary for namespace %q"`), not just `require.Error`. The error contract is part of the API.
- Exercise deterministic error paths by constructing malformed inputs (truncated bodies, varint overflows, dictionary IDs with no reverse-map entry, mismatched format bytes) rather than relying on incidental failures.
- For the migration: verify resumability by interrupting between batches and asserting the next run picks up where the prior left off.

### Cypher property-name semantics tests

- `MATCH (n) RETURN n.unknownField` against a namespace whose dictionary has never seen `unknownField` returns `null`, no error.
- Same query after some other entity in the namespace has set `unknownField` returns `null` for entities that don't have it set themselves.
- Decode of a synthesized body that references a dictionary ID with no reverse-map entry returns an error of the form `property key id %d not in dictionary for namespace %q`. The error must propagate out of the decode site rather than the missing property silently dropping.

### Migration tests

- Full rewrite: V1 store with N nodes + M edges → run migration → all bodies start with the V2 format byte.
- Resumability: kill the migration mid-pass, restart it, confirm completion.
- Version bump is the last write: corrupting the final version write doesn't invalidate already-rewritten bodies.
- Engine-open refusal: V1 store + binary at V2 + no `--upgrade-storage` → `NewBadgerEngineWithOptions` returns an error naming both versions.
- V0 → V2 chain: V0 store + `--upgrade-storage` runs both V0→V1 and V1→V2 in order.

### Integration

- Full `pkg/storage` + `pkg/cypher` + `pkg/nornicdb` suites pass.
- Bench: data-files bucket on a freshly migrated Northwind 100k corpus drops 20–25%.

---

## 9. Rollout

1. Operators back up their data directory.
2. Deploy the new binary with `--upgrade-storage` set on first start.
3. Engine runs the migration chain, emits progress logs, then begins serving traffic.
4. Subsequent starts do not need the flag — the version is already current.

---

## 10. Risks

| Risk                                                                | Mitigation                                                                                                                                              |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Decode site bypasses the codec dispatch and assumes legacy format   | Removing the old encode signatures forces every call site to update; a single decode entry point per kind concentrates the format check.               |
| Migration takes a long time on large stores                         | Bounded batches, structured progress logs, resumable. Operators can size their maintenance window from a dry-run pass against a snapshot.               |
| Operator forgets to back up                                         | `--upgrade-storage` flag exists precisely to make the irreversibility explicit; documentation calls out the backup requirement.                         |
| Rolling upgrade in a Raft cluster                                   | Each replica is upgraded independently; cluster-wide coordination is out of scope here. Operators pin Raft followers to read-only during the rollout.   |
| Crash mid-migration leaves a half-rewritten store                   | Per-batch atomic commits + leading-byte skip check on resume. The version bump is gated on a full clean pass.                                           |

---

## 11. Expected outcomes

- **Data-file size reduction:** 20–25% on the data-files bucket once the migration completes — applies uniformly to all data because the rewrite is eager.
- **Write path:** marginally faster after migration. One dict lookup + varint emit per property, vs. one string write per property.
- **Read path:** marginally faster. In-memory `map[uint64]string` reverse lookup is hotter than string-interned msgpack headers.
- **Codebase:** the V1 encoder and the `decodeEdgeCompactV1` arm survive only inside `migration_v1_to_v2.go` and can be deleted whenever the team is comfortable that no V1 stores exist anywhere. The hot path is single-codec.
