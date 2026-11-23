# Detach in-memory inference store ownership (NOR-279 / N15-82, v1.5.2)

`EdgeMetaStore` and `NodeConfigStore` are public in-memory stores for edge
provenance and per-node inference configuration. Both accepted caller-owned
values and handed retained state back out. A caller that kept its `EdgeMeta`
metadata map, its `NodeConfig` pin/deny lists, label configs or metadata map and
changed it afterwards changed recorded evidence and retained configuration, and
a mutation through a delivered record or config changed the store.

This change gives both stores one ownership rule: a supported mutable nested
value is detached at ingress, and every read, filtered getter and export
delivers detached data. Ordering, scalar values, timestamps, the collection
representation the stores already publish, and export/import round trips are
preserved.

Only these two stores change. Committed storage, inference ranking and the
separate `pkg/inference` evidence buffer are out of scope and untouched.

Nested metadata copying is delegated to the existing shared caller-copy owner
(`storage.CopyValue`) through one typed boundary helper. There is no second deep
copier, no JSON round trip, and no new reflection or cycle policy.

That copy runs the caller-copy protocol, so it never runs under a store lock: a
caller value may read the store from inside `CallerCopy`. Ingress detaches before
the write lock; reads snapshot the store's own fields under the lock and detach
metadata after releasing it.

No caller-visible collection representation changes. An `EdgeMeta` with nil
metadata still delivers nil, and a `NodeConfig` supplied with nil collections is
still delivered as allocated empty collections, so `Get` and `Export` keep
publishing `[]`/`{}`.

Affected issue: NOR-279 (release card N15-82, `v1.5.2`, urgent). Base
`43a39d64`. Already merged and unrelated: the committed-storage ownership work
referenced by the original triage, which this change does not revisit.
