# Design: detached ownership for the two in-memory inference stores

## Established contract

`EdgeMeta.Metadata` and `NodeConfig.Metadata` are `map[string]interface{}`;
`NodeConfig` also holds `PinList`/`DenyList` (`[]string`) and `LabelConfigs`
(`map[string]LabelConfig`, whose `LabelConfig` has only value fields).

| Boundary | Contract after this change |
|---|---|
| `EdgeMetaStore.Append` | retained record is detached from the caller's `EdgeMeta`; `Timestamp`/`EdgeID` defaults unchanged |
| `EdgeMetaStore.Import` | each non-nil imported record is detached before the write lock; append order and count unchanged |
| `EdgeMetaStore.GetHistory`, `GetLatest` | each returned record is detached from the store and from the other returned records |
| `EdgeMetaStore.GetBySignalType`, `GetMaterialized`, `GetByTimeRange`, `GetByOrigin`, `GetBySession` | same detached delivery; newest-first order and `limit` semantics unchanged |
| `EdgeMetaStore.Export` | every exported record is detached; insert order unchanged |
| `NodeConfigStore.Set` | retained config is detached from the caller's value argument |
| `NodeConfigStore.Import` | each imported config is detached, including a config that replaces an existing one |
| `NodeConfigStore.Get`, `GetOrCreate`, `Export` | returned config has detached `PinList`, `DenyList`, `LabelConfigs` and `Metadata` |
| `NodeConfigStore.GetPinnedTargets`, `GetDeniedTargets` | already delivered copies; retained as-is, now covered by a regression |

## Copy placement: the caller's copy never runs under a store lock

`CopyValue` runs the caller-copy protocol: it asks every caller-declared value
for its own copy, so caller code executes inside the store call. That protocol's
consumer contract allows the caller's code to read the store the value is stored
in, and `sync.RWMutex` is not reentrant — its read side also blocks a recursive
read as soon as a writer is queued. The two stores therefore follow one placement
rule:

1. Ingress (`Append`, `Import`, `Set`) detaches the caller's record or config
   **before** taking the write lock, so no protocol call runs under a lock and a
   caller value can read the store.
2. Egress (`Get`, `GetOrCreate`, `GetHistory`, `GetLatest`, the filtered getters,
   `Export`) snapshots the store's own state **under** the lock, releases it, and
   only then detaches metadata, so delivery runs the caller's copy with no lock
   held.
3. Retained metadata is frozen after the ownership copy at ingress: nothing in
   either store writes a retained metadata map afterwards, which is what makes
   the post-lock detach of a snapshot safe without holding the lock.
4. Value fields, `PinList`, `DenyList` and `LabelConfigs` stay inside the locked
   snapshot, because store methods still change them (`RecordEdgeCreation`,
   `AddToNodePinList`, the config setters, `MarkMaterialized`). The lock keeps
   delivering one whole image of those fields.
5. `Timestamp`/`EdgeID` defaults and `UpdatedAt`/`CreatedAt` stamping stay in the
   accepted write phase, now applied to the detached copy under the same lock.
6. A disabled provenance flag still returns before any copy: `Append` runs no
   caller protocol and records nothing.

## Callback reentry: reproduced, then corrected

The reported value shape is a supported caller-declared metadata value: it
implements `CallerCopy`, returns its own type, and reads the store through its
public API while it is copied. In the staged proposal those copies ran under
`EdgeMetaStore.mu` and `NodeConfigStore.mu`:

- Fail-first on that proposal (`nor279_callback_reentry_test.go`; all 13 paths:
  4 ingress, 3 `EdgeMeta` egress paths, 3 filtered getters, 3 `NodeConfig`
  egress paths): the store held its lock in 13/13 cases, and the
  isolated child process never returned — the caller's read blocked inside the
  store call and the bounded parent stopped the child after 5 s. Ingress
  deadlocks on the held write lock; egress blocks because the recursive read
  queues behind the copy while a concurrent writer waits.
- After the correction: 13/13 cases return, and the store holds no lock while a
  caller's value is copied.
- At the pre-change base 43a39 these cases are not applicable: the stores ran no
  copy protocol at all, so a caller value was never asked for a copy. The reentry
  contract exists from this change onward, and the proposal that introduced it
  was corrected before acceptance.

## Canonical owners reused, not duplicated

- `CopyValue` (`pkg/storage/property_copy.go`) is the package's generic
  caller-copy owner. Its documented contract already covers what metadata
  needs: concrete types preserved, nil containers stay nil, scalars keep width,
  cycles and aliases resolved, one `CallerCopy` dispatch. No new copier,
  serialization round trip or reflection policy is introduced.
- `copyCallerMetadata` is the single typed boundary for `map[string]interface{}`
  retention/delivery. It is the only addition to `property_copy.go` and exists so
  both stores reach `CopyValue` through one place instead of repeating the nil
  guard and type assertion.
- `copyNodeConfig` is one pair of owners, `snapshotNodeConfig` +
  `detachNodeConfigMetadata`, applied together: the snapshot copies value fields,
  `PinList`, `DenyList` and `LabelConfigs` (allocated even for a nil input), and
  the detach replaces `Metadata` with a `CopyValue` copy. Ingress applies the
  pair before the lock; egress applies the snapshot under the lock and the detach
  after it, so ingress and egress cannot diverge.
- `copyEdgeMeta` is the same pair for `EdgeMeta` (`snapshotEdgeMeta` +
  `detachEdgeMeta`), with `snapshotEdgeMetas`/`detachEdgeMetas` for delivered
  slices. `EdgeMeta` previously had no copy owner at all: four boundaries
  open-coded `metaCopy := *meta`. The five sites (`Append`, `Import`,
  `GetHistory`, `filterMetas`, `Export`) now share the owner, and `filterMetas`
  takes its own read lock so the filtered getters detach after releasing it.

## Population and consumer trace

- `pkg/storage` is the only definition site; the stores are constructed by
  `pkg/inference` (`edgeMetaStore`, `nodeConfigStore`) and exposed through
  `Engine.GetEdgeMetaStore`/`GetNodeConfigStore`, which is the public embedding
  surface.
- Inference writes call `AppendFromSuggestion` and `MarkMaterialized`, neither of
  which sets `Metadata`, so production inference does not depend on metadata
  aliasing; the provenance record is built entirely by the store.
- `NodeConfigStore` mutators change `PinList`, `DenyList`, `LabelConfigs`,
  counters and `UpdatedAt` only. Nothing in the package or in its consumers
  writes `Metadata` of a retained config or record after retention, which is the
  immutability the post-lock detach relies on. `Cleanup` and `Clear` drop whole
  records; they never edit one.
- No production caller of `Export`/`Import` exists in the repository: those are
  the public backup/import hooks documented for embedders. Their fidelity is
  therefore verified through the public API in tests rather than through an
  internal consumer.
- `pkg/inference/evidence.go` has its own `Evidence.Metadata` with a shallow
  `copyMetadata`; it is a different in-memory structure (evidence buffer, not
  `EdgeMetaStore`) and is explicitly out of the authorized scope.

## Cost, stated as a tradeoff

Ownership requires one deep copy of the accepted metadata at retention and one
per delivered record or config at egress; the work is proportional to the number
of records times the size of their nested metadata. That copy is the mechanism by
which the store owns the values it hands out: the public field type is a plain
`map[string]interface{}`, so a delivered value cannot become immutable or
copy-on-read by itself. Source reasoning for the measured work: each delivered
record adds the outer map plus one allocation per nested map, slice and
slice-of-map value, instead of the single shallow struct copy the unsafe baseline
made. Measured on unchanged fixtures (100 iterations, three samples,
`GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB`):

- `GetHistory` of 64 records with the reported nested metadata:
  `1710-3394 ns/op, 15944 B/op, 69 allocs/op` before;
  `73443-77405 ns/op, 89691-89694 B/op, 837 allocs/op` after the proposal.
  Provenance history is an audit/read API; inference's hot path never reads it.
- `NodeConfig.Get` with the reported nested metadata:
  `341-349 ns/op, 1232 B/op, 7 allocs/op` before;
  `893-957 ns/op, 2048 B/op, 17 allocs/op` after the proposal. `Get` is called
  from inference admission, so this is the cost that matters; it remains
  sub-microsecond per call for the fixture.

The earlier numbers are the cost of the unsafe shared ownership, not a target to
recover. Moving the metadata copy out of the lock does not change the copy work;
it only reorders it, and it adds no allocation to a delivered record. The
corrected measurements are recorded in the result bundle for the accepted head.

## Compatibility risks

- Collection representation is unchanged: `copyNodeConfig` has always delivered
  allocated empty `PinList`/`DenyList`/`LabelConfigs`/`Metadata` for a nil input,
  so `Get` and `Export` keep publishing `[]`/`{}`. A nil-preserving variant is
  not adopted; it would have changed the published JSON to `null`.
- `EdgeMeta` metadata keeps its base shape: a nil metadata map is delivered as
  nil, an empty one as empty.
- Pointer identity of retained records/configs changes only in that each is now
  a private copy; no in-repository caller compares against the caller's pointer.
- Lock placement adds no new lock and no new lock ordering: the store lock is
  released before any caller-declared code runs.
- No API signature, wire format, storage schema, ranking or feature flag
  changes.

## Deletion targets

The four open-coded `metaCopy := *meta` sites in `edge_meta.go` and the
hand-written shallow map/slice copies in `copyNodeConfig` are replaced by the
owners above; nothing else is removed.
