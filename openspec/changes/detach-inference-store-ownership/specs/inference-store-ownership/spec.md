# Spec delta: detached ownership for inference provenance and node configuration

Observable requirements for the two in-memory stores only. Every scenario is
driven through the public Go API.

## ADDED Requirements

### Requirement: Evidence accepted by the store is retained, not aliased

`EdgeMetaStore.Append` and `EdgeMetaStore.Import` SHALL retain the values
accepted by the store, so later mutation of the caller's record cannot change
retained evidence.

#### Scenario: append then mutate the caller's nested metadata

- **WHEN** evidence with nested maps and slices is appended and the caller then
  mutates every reference member of its own `Metadata`
- **THEN** `GetHistory`, `GetLatest` and `Export` still report the appended
  values, and per-key ordering and scalar fields are unchanged.

#### Scenario: import then mutate the source records

- **WHEN** records are imported and the caller mutates the imported `EdgeMeta`
  values, including their nested metadata
- **THEN** the importing store still reports the imported values in append
  order, with the imported record count and materialized count preserved.

### Requirement: Delivered evidence is detached from the store and from itself

Every read, filtered read and export SHALL deliver data whose mutation cannot
change retained evidence or another delivered record.

#### Scenario: mutate one returned record

- **WHEN** a caller mutates the nested metadata of a record returned by
  `GetHistory`, `GetLatest`, `GetBySignalType`, `GetMaterialized`,
  `GetByTimeRange`, `GetByOrigin`, `GetBySession` or `Export`
- **THEN** neither the store nor any other returned record observes the change.

#### Scenario: filtered reads preserve order and limits

- **WHEN** a filtered getter is called with and without a limit
- **THEN** it returns the matching records newest-first, truncated to the limit,
  with the same values as `GetHistory` and `Export`.

### Requirement: Node configuration is detached at ingress and egress

`NodeConfigStore.Set` and `NodeConfigStore.Import` SHALL retain a detached copy
of `PinList`, `DenyList`, `LabelConfigs` and `Metadata`, and `Get`,
`GetOrCreate`, `GetPinnedTargets`, `GetDeniedTargets` and `Export` SHALL deliver
detached data.

#### Scenario: set then mutate the caller's configuration

- **WHEN** a configuration with pin/deny lists, label configs and nested
  metadata is set and the caller then mutates those same values
- **THEN** `Get` and `Export` still report the set values, with list order,
  label entries, scalars and timestamps preserved.

#### Scenario: mutate a returned configuration

- **WHEN** a caller mutates the nested members of a config returned by `Get`,
  `GetOrCreate` (existing config), `GetPinnedTargets`, `GetDeniedTargets` or
  `Export`
- **THEN** the retained configuration is unchanged.

#### Scenario: import replaces an existing configuration

- **WHEN** `Import` supplies a configuration for a node that already has one
  and the caller then mutates the imported value
- **THEN** the replacement is retained detached, the node count is unchanged,
  and the previous configuration is not merged with it.

### Requirement: A caller's copy protocol never runs under a store lock

The stores SHALL NOT hold their own lock while they run a caller-declared copy
(`CopyValue` asking a value for `CallerCopy`), because that caller code may read
the store through its public API.

#### Scenario: a caller value reads the store during its copy

- **WHEN** a supported caller-declared metadata value performs a public store
  read inside its `CallerCopy`, during append, import, get, filtered get or
  export, with a concurrent writer present
- **THEN** the store call and the caller's read both return, and the store is
  never holding its lock while the caller's copy runs.

#### Scenario: delivered value fields stay one image

- **WHEN** a caller reads a record or config while `MarkMaterialized` or the edge
  counters change the store
- **THEN** the delivered record or config is one consistent image of the fields
  the store owns, taken before the caller's copy runs.

#### Scenario: disabled provenance stays a no-op

- **WHEN** edge provenance is disabled and `Append` is called
- **THEN** nothing is recorded and the caller's copy protocol does not run.

### Requirement: Fidelity across collection representation, ordering and round trip

The stores SHALL preserve ordering, scalar values, timestamps, the collection
representation they have always published, and export/import fidelity.

#### Scenario: published collection representation is unchanged

- **WHEN** an `EdgeMeta` supplies nil metadata, or a `NodeConfig` supplies nil
  `PinList`/`DenyList`/`LabelConfigs`/`Metadata`
- **THEN** the `EdgeMeta` metadata stays nil, while the `NodeConfig` collections
  are delivered as allocated empty collections, so `Get` and `Export` keep
  publishing `[]`/`{}` exactly as before this change, stay nil-safe for
  `IsPinned`/`IsDenied`, and remain isolated from retained state.

#### Scenario: export and import round trip

- **WHEN** exported records or configs are imported into a fresh store and the
  exported values are mutated afterwards
- **THEN** both stores report the original values, `UpdatedAt` is preserved by
  the config round trip, and store statistics agree.

### Requirement: Ownership cost is measured, not assumed

The change SHALL report workload-scoped cost for the representative nested
metadata fixture and distinguish unsafe baseline cost from the correct ownership
tradeoff, stating the necessary copy work and the source reasoning for it.
