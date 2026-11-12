# Storage read delivery

Native transaction point reads reuse the existing bounded snapshot-image cache
for complete, small, committed node images after a previous read. The cache is
limited to 128 nodes and 256 KiB; its existing size and sidecar admission rules
also apply to these images. Partial projections never populate that complete
image cache. A first, one-off point read retains its existing allocation path.

Pending updates and deletions take precedence over retained committed images.
The pinned snapshot does not advance when a peer writes. Live suppression and
decay bypass the cached visibility answer. Write-behind reads retain their
existing overlaid read contract instead of using this pinned-image shortcut.

## Mutable results

`CopyNode`, `CopyEdge`, and mutable native read results own their labels,
properties, embedding metadata and any vectors present. `GetQueryNode` can omit
separately stored vectors; resident vectors that it does return belong to its
caller. Mutating a result must not change a retained cache, transaction image or
acknowledged buffered write. Immutable node runtime metadata remains shared and
uses its existing copy-on-write setters.

A native transaction relationship read already returns an independent image.
Its Cypher namespace wrapper therefore copies only the header when removing
prefixes, retaining the property map that this delivery already owns. Buffered
between-edge reads detach through the same edge-copy owner before delivery.
Header-only reads may omit properties, as declared by `AdjacentEdgeRead.Headers`.

## Go custom values

Caller copying and durable normalization have different contracts:

- `OwnedPropertyValue.StoragePropertyValue` belongs to the durable codec. It
  retains its existing normalization and admission behavior.
- An optional `CallerCopy() any` method owns an opaque Go value's mutable
  in-memory state. It must return an independent value of the receiver's exact
  concrete type. A mismatched type is a programming error and panics.
- Otherwise the structural copy owner preserves scalar widths and named
  container/record types, detaching exported fields and mutable containers.
  Unexported state is preserved without interpretation; a type with mutable
  opaque state must supply its own caller-copy method.

For example:

```go
type Opaque struct{ data []byte }

func (v Opaque) CallerCopy() any {
    return Opaque{data: append([]byte(nil), v.data...)}
}
```

A scalar's copy method is selected before its underlying numeric representation
and is called once per selected value, including inside typed lists. Nil
containers stay nil. Regression tests cover recursive map, slice and custom
record references without sharing their mutable state with the source.

This is an in-memory delivery correction. It does not expand Cypher property
admission, change Badger keys or stored payloads, change WAL encoding, or require
a storage-version bump or migration.
