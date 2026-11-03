# Persisted numeric reader compatibility for PR #1665

The original inventory compared the numeric layer with historical release base
`b1b018d3f407eb7ec9d4b3038667be5dae58a09e`. The owner names and binary-replication
rows below were updated for release `04571a481e8324139b46c82d99c76db71a951ab8`
and retained while rebasing onto `df07c94fb7df5bb05a0c854cdaa0b8b6eae8182a`
(Go 1.27.3 and dependency integration).
Internal replay state and JSON document/backup imports have distinct contracts.

Case **a** means the former internal format already preserved numeric types.
Case **b** means untagged internal JSON was formerly read as float64. **a\***
identifies a different, already-established exact-lexeme reader: it was not a
float64-to-exact change in this PR. **Import** identifies the approved NOR-68
contract, even where its former reader returned float64.

| Store / reader | Former format and reader | Class | Result in this correction |
| --- | --- | --- | --- |
| Schema definitions: engine load/persist, transaction DDL and knowledge policies, transaction publication copies, admin schema sidecars | Envelope version 1 (or absent); plain JSON; `encoding/json.Unmarshal` decoded interface numbers as float64 | b | One owner, `storage.EncodeSchemaDefinition` / `DecodeSchemaDefinition`, handles every persisted read, write and deep copy. Versions 0/1 use `PropertyJSON.UnmarshalLegacyState`; version 2 uses exact PropertyJSON. The shared writer preserves existing declared metadata bytes while dynamic DOMAIN values retain explicit FLOAT kind. Every writer uses version 2 from decoded values, including rewrites of legacy state. Native backup preserves bytes and restore uses the engine loader. |
| Replication commands and snapshots; `storage.EncodeCommandPayload` / `DecodeCommandPayload`, node and edge image codecs | Current release uses typed MessagePack; historical Gob/JSON is confined to the V4 migration reader | a | Keep the current strict MessagePack owner. Historical `replication.decodeV4ReplicationValue` and `storage.TranscodeV4EntityChange` read Gob or legacy JSON only while upgrading old V4 inputs; it is not a current traffic/replay fallback. Retired raw property/query commands remain rejected. |
| WAL outer frames, `wal.go`, `wal_jsonbuf.go`, `wal_entry_codec.go` | Versioned framing; typed sequence, timestamp, operation, checksum and database fields; property payload is opaque `[]byte` | a | No property reader changed in the outer envelope. New JSON projections preserve FLOAT tokens; payload interpretation remains at the recovery codec. |
| WAL redo/undo/bulk/buffered payloads and framed snapshots, `recovery_codec.go`, `snapshot_stream.go` | `storage-msgpack-v1` sidecar or serialization header selects typed MessagePack; native property bodies also retain typed storage values | a | Preserve existing marker/header dispatch and typed reader. New writes continue to carry typed authority. |
| Untagged WAL recovery JSON and legacy JSON snapshots, `decodeRecoveryPayloadAuthority`, `decodeLegacyRecoveryJSON`, `LoadSnapshot` | At the release base, `json.Decoder.UseNumber` plus `legacyRecoveryNumber` already read integral lexemes as int64/uint64 and decimal/exponent lexemes as float64 | a* | Keep that exact-lexeme contract. The shared PropertyJSON replacement preserves this existing policy; do not introduce the schema/replication float64 compatibility policy here. Added a `1152921504606847000` INTEGER control. |
| CDC, `cypher.cdcValue`, `holdsValues`; storage `ChangeEvents` | Change events and transaction metadata are stored through `encodeValue` and decoded through `decodeValue` as typed MessagePack | a | Keep typed persisted values. Shared numeric normalization/equality preserves exact cross-type identity; there is no legacy JSON reader to version. |
| Native Badger entity storage and native backup restore | Versioned typed storage serialization; native backups carry the original key/value bytes, including schema envelopes | a | Entity decoding remains typed. Restored schema envelopes pass through the version-aware schema reader above. |
| JSON backup restore, `nornicdb.DB.Backup` / `Restore` | Version `1.0` plain JSON with untagged interface numbers; former JSON restore returned float64 | Import | All backup versions intentionally restore exact integer token digits. New JSON backups identify format `2.0` and retain FLOAT tokens and negative zero. |
| Combined graph exports and JSONL, `loader.go`, `types.go` | Plain Neo4j-style JSON; former combined/line readers returned float64; `types.go` converts already-decoded values and writes JSON | Import | Preserve exact PropertyJSON imports and new FLOAT-preserving writers. There was no existing graph-export format version field to bump. |
| APOC graph export/import, `apoc/export`, `apoc/import`, `cypher.callApocImportJson`; generic APOC JSON transforms | External JSON documents and exports; old raw JSON readers | Import / external | Preserve the NOR-68 exact-token contract. Do not impose internal-state float64 decoding on unmarked documents. |
| Retention archives / subject exports, `nornicdb.Open` archive callback / `ExportUserData` | Plain JSON documents; these changes affect writers only | Import | New writers retain number kind. Any subsequent document import uses the import contract above. |
| Remote HTTP results and entities, `doRequestToURL`, `decodeRemoteHTTPRowValue`, `remoteHTTPRESTEntity` | Former remote decoder already used `UseNumber` and integral-token int64 decoding; HTTP/Bolt are external value protocols | a* / external | Keep exact persisted-property result decoding and typed temporal/spatial/graph reconstruction. External input follows its separately registered input domain. |
| HTTP/GraphQL/tool/LLM input (MCP keeps release behaviour; 1.5.0 MCP process), load.json, explicit fromJson, migration HTTP reads, JSON redaction | External documents/protocol values, not internal persisted replay state | External | Keep their approved input/property domain contracts. Redaction retains number text using `json.Number`; it does not select an internal-state format. |

For legacy JSON backup/export imports, the old token `1152921504606847000`
restores as INTEGER `1152921504606847000`. An original whole FLOAT
`float64(1<<60)` was exactly `1152921504606846976`; its old shortest integral
token does not carry that FLOAT meaning at the import boundary. This limitation
is intentional under NOR-68. Internal schema/replication state instead restores
the former FLOAT and keeps its behavioural meaning. Exact DOMAIN comparison is
unchanged: it accepts that FLOAT and rejects the different INTEGER token.

The reader population was extracted from every changed production `.go` file
in `git diff b1b018d3f HEAD`, then traced to its former writer/reader. Schema
types, exports, key users and copies were also searched across the repository.
The domain-reader functions (excluding the shared codec owners) are:

- `apoc/convert`: `ToMap`, `FromJsonList`, `FromJsonMap`.
- `apoc/import`: `JsonData`, `ParseJsonLine`.
- `apoc/json`: `Path`, `Validate`, `Parse`, `Pretty`, `Compact`, `Keys`, `Values`,
  `Type`, `Size`, `Merge`, `Set`, `Delete`, `Flatten`, `Unflatten`, `Filter`,
  `Map`, `Reduce`.
- `apoc/load`: `loadJsonFromURL`, `loadJsonFromFile`, `JsonParams`, `GraphQL`, `Rest`.
- `pkg/adminimport`: `applySchemaDefinition` (versioned schema codec).
- `pkg/cypher`: `loadJsonFromURL`, `loadJsonFromFile`, `callApocImportJson`,
  `callDbInfer`, `apocConvertFromJSON`.
- `pkg/graphql/models`: `JSON.UnmarshalGQL`.
- `pkg/heimdall`: `parseActionEnvelope`, `tryParseActionResponse`.
- `pkg/nornicdb`: `DB.Restore`.
- `pkg/storage/entity_payload_codec.go`: current `DecodeCommandPayload`, `DecodeNodePayload`, `DecodeEdgePayload` consume typed MessagePack, not a JSON domain reader. Historical JSON/Gob decoding is restricted to explicit V4 migration owners, including `pkg/replication/wal_v4_payload.go:decodeV4ReplicationValue` and `pkg/storage/wal_v4_gob_entity.go:TranscodeV4EntityChange`.
- `pkg/server`: `decodeTransactionRequest`, `readJSON`.
- `pkg/storage`: `BadgerEngine.loadPersistedSchemas`,
  `BadgerTransaction.Schema` (both use the versioned schema codec), `decodeLegacyRecoveryJSON`,
  `LoadFromNeo4jExport`, `loadNodesFromReader`, `loadRelationshipsFromReader`,
  `decodeRecoveryPayloadAuthority`, `httpTransport.doRequestToURL`,
  `decodeRemoteHTTPRowValue`, `remoteHTTPRESTEntity`.
- `scripts/migrate_neo4j_to_nornic.go`: `fetchNodesFromNeo4j`, `verifyMigration`.

Additional non-domain candidates inspected were CDC's native numeric readers,
WAL's raw typed framing readers, native entity serialization, the removed
remote HTTP decoder, JSON writers in `types.go` and the retention callback,
and the number-text decoder in `observability.RedactText`.

The complete persisted-schema boundary is:

| Site | Operation | Format owner |
| --- | --- | --- |
| `SchemaManager.ExportDefinition` / `exportDefinitionLocked`, all schema mutation callbacks | Build typed definitions from runtime state; no wire encoding | `schema_persistence.go` builds the definition; each persistence callback below uses the codec |
| `BadgerEngine.loadPersistedSchemas` | Startup and native-restore read of `prefixSchema` | `DecodeSchemaDefinition` |
| `BadgerEngine.persistSchemaDefinition` | Engine write of `schemaKey(namespace)` | `EncodeSchemaDefinition` |
| `BadgerEngine.GetSchemaForNamespace` / startup `SetPersister` | Attach the engine writer to ordinary and knowledge-policy mutations | `persistSchemaDefinition` → `EncodeSchemaDefinition` |
| `BadgerTransaction.Schema` | Snapshot read of `schemaKey`, staged write of the same key, deep copy for commit | `DecodeSchemaDefinition`, `EncodeSchemaDefinition`, then `DecodeSchemaDefinition` |
| `BadgerTransaction.Schema` / `StageSchemaChanges`, Cypher `prepareSchemaTransaction` / `mutateSchema` | Ordinary DDL transaction and runtime backfill snapshot | `Schema(KnowledgePolicyChanges)`; staged typed snapshot comes from its codec-produced copy |
| `publishKnowledgePolicySchemaLocked` | Publish committed schema/knowledge-policy snapshot | Consumes the codec-produced isolated definition; no serialization |
| `finishKnowledgePolicyMutationLocked` / `persistLocked` | Direct and transactional knowledge-policy mutations | Registered engine or transaction persister above |
| `adminimport.exportNornicSchemaDefinition` | Schema sidecar export | `EncodeSchemaDefinition` |
| `adminimport.applySchemaDefinition` | Schema sidecar import | `DecodeSchemaDefinition` |
| `BadgerEngine.Backup` / `Restore`, namespaced/WAL backup wrappers | Copy native key/value bytes, then reload schema | No alternate codec; restore calls `loadPersistedSchemas` |
| `BadgerEngine.dropNamespaceSchema` / namespace deletion | Delete schema key; no schema values read or written | No encoding |

`json_data_paths.json` records the engine, transaction and sidecar codec calls
as schema-domain paths and the codec's PropertyJSON calls separately. Raw schema
JSON has no typed-schema exception. The ownership guard also rejects a raw
reader/writer referencing `SchemaDefinition` or `ExportDefinition` even when a
frozen exception is supplied; mutation controls exercise that rejection and
each registered schema codec call.

Regression coverage added or extended:

- `TestNumericWireSchemaDomainValuesSurviveRestart`: legacy unversioned/v1 and
  exact v2 definitions; restart-to-write for nodes and relationships in direct
  and explicit transactions; original large FLOAT acceptance and different
  INTEGER rejection; 2.0 and signed-zero controls; exact integers beyond 2^53,
  signed limits and uint64 max; rewrite followed by another restart.
- `TestNumericWireSchemaTransactionRewrite`: unversioned/v1 large whole FLOATs
  (`float64(1<<60)` and `1e20`) and an exact v2 INTEGER beyond 2^53; ordinary
  schema and knowledge-policy commits, restart, allowed/rejected writes, native
  backup/restore of original and rewritten schemas, and restored-store restart.
- `TestNumericWireSchemaDDLTransactionRestart`: public Cypher `BEGIN`, unrelated
  `CREATE INDEX`, `COMMIT`, restart and DOMAIN enforcement on both parsers.
- `TestNumericWireSchemaSidecarRoundTrip`: versioned sidecar import, export and
  reimport; `TestSchemaDefinitionCodec`: exact deep copy and version/error controls.
- `TestNumericWireReplicationNumbersSurviveReplay`: typed MessagePack
  commands persisted to WAL, adapter reopen and replay into fresh storage;
  node/relationship properties, set-property values and Cypher parameters;
  replication snapshot restore; large whole FLOAT, 2.0, negative zero and
  exact integer/uint64 controls.
- `TestNumericWireReplicationJSONFallbackKeepsLegacyMeaning` and
  `TestPropertyJSONLegacyStateKeepsFloat64Meaning`: former rounding, nested
  numbers, signed zero and complete-document parsing.
- `TestJSONIngestionBackupRoundTrip` and
  `TestNumericWireBackupLegacyIntegralTokensRestoreExactly`: v2.0 FLOAT/sign
  preservation and the explicitly requested legacy import limitation.
- `TestLegacyRecoveryIntegerRangeAndMalformedValues`: retain the release's
  already-exact legacy recovery token interpretation.

The original PR's TrueNAS lab receipts and round-13 results are historical
proof, not qualification of this rebased source. Current PR #1665 qualification
runs on the maintainer's Mac with Go 1.27.3. Its PR result comment identifies
the actual local head/base, full gate result, cached checks and limitations.
The transaction and sidecar controls above cover the original omitted rewrite
paths; current source inventories are independently regenerated/reviewed.

## Typed integer envelopes and FLOAT-preserving writers

The final-gate `commit_sequence:0.0` failure came from a historical WAL test
fixture, not a production commit. `TestHistoricalEdgeOverwriteWALKeepsCounts`
encoded `WALBufferedCommitData` with the old `encoding/json` writer, decoded the
whole object into `map[string]any` to insert `replaces_existing`, then sent that
map to the current writer. The intermediate decode changed
`MVCCVersion.CommitSequence` from `uint64(0)` to `float64(0)`. The current writer
correctly emitted a FLOAT token that the typed recovery reader rejected. The
fixture now decodes with `json.Decoder.UseNumber` before the edit, preserving
the old writer's numeric tokens. Integer readers remain strict.

The original boundary audit used its release graph and registered writer list.
That graph is stale relative to the current release and does not certify this
rebase. Current review uses the integrated source, `json_data_paths.json`, exact
numeric ownership guards and the public format/recovery tests. Current WAL
framing and MessagePack owners below supersede the original JSON/Gob paths;
historical conversion remains confined to the existing V4 startup migration.

| Family member | Integer producer through writer and consumer | Why FLOAT conversion cannot occur |
| --- | --- | --- |
| WAL entry/frame | Sequence is allocated as uint64; checksum is uint32. Ordinary/batch/forced-flush appends construct typed `WALEntry`; `processWALEntryPayload` writes the current JSON metadata header plus opaque binary Data. Ordinary readers reject superseded framing, which belongs to startup migration. Property epoch format/version is int and its control uses its own typed JSON marshaler. | Interface arguments retain concrete integer kinds. No whole-entry decode into an untyped map occurs. Data remains opaque bytes. |
| Buffered WAL / MVCC | `allocateMVCCVersion` -> `commitBufferedLocked` -> `bufferedCommit` -> `AppendBufferedCommit` -> `walBufferedCommitPayload` -> the typed `valueEncoder.recovery` serialization owner; recovery decodes `WALBufferedCommitData`. Node/edge counters, property IDs/counters and commit sequence are uint64. | The version and counters are copied as typed fields and typed maps throughout. Native MVCC heads/version bodies use binary/MessagePack, not JSON. |
| WAL markers, redo/undo/bulk | `AppendTxCommit` uses typed int `WALTxData.OpCount`; checkpoint's map receives uint64 directly from `sequence.Load()`. Engine/batch writers construct typed entity payloads -> `marshalWALRecoveryPayload`. | No intermediate float64 decoding. Entity JSON projections retain typed metadata; the existing MessagePack sidecar remains authoritative for properties. |
| Snapshots | `WriteSnapshot` and `writeSnapshotCollection` write binary uint64 sequence and typed uint64 footer counts through `writeSnapshotFrame` -> `encodeRecoveryPayload`; `ReadSnapshot` strictly decodes `snapshotCompletion`. | Counts originate from uint64 counters or explicit uint64 length conversion. Entity frames are typed; legacy snapshot reads do not feed an untyped whole-envelope rewrite. |
| Replication | `ApplyCommand` builds typed `replicationWALRecord.Timestamp` from `UnixNano()` (int64), used unchanged by async and synchronous append. Current supported commands, entity images and snapshots use typed MessagePack writers; the V4 conversion owner alone reads historical JSON/Gob. | Current values retain typed authority. Historical conversion is separate from normal traffic; timestamps, positions and counts never pass through an untyped JSON map. |
| CDC | `ChangeEvent` integer sequence/timestamp fields are encoded/decoded with native MessagePack; `callDbCdcQuery` (`pkg/cypher/call_cdc.go`) projects sequence integers directly to external results. | No internal JSON rewrite. FLOAT normalization is confined to value leaves; it does not modify the typed event envelope. |
| Schema | Typed `SchemaDefinition.Version` (int) and constraint/index metadata reach the sole `EncodeSchemaDefinition` owner, including transactional publication and sidecars. Versions 0/1 decode into that typed envelope with legacy semantics for interface leaves only. | The writer sets version 2 as an int; old interface FLOAT values occur only inside allowed-value/property fields, not integer metadata. |
| Catalog / database metadata | Database metadata remains `json.Marshal(map[string]*DatabaseInfo)` -> JSON string -> typed `json.Unmarshal`; index/access metadata uses its existing MessagePack owner. | These writers are unchanged and outside the FLOAT-preserving writer route. The frozen typed-catalog exception is retained, not broadened. |

Other registered writers emit external documents, responses or request values;
they do not rewrite these internal typed envelopes. JSON imports/backups decode
their typed envelope separately from numeric property leaves, as listed above.

Direct regressions are `TestNumericWireWALIntegerEnvelopes` (real append routes,
uint64 limits, int marker, map checkpoint and whole FLOAT property),
`TestNumericWireWALFrameIntegerFields` (current framing, rejection of superseded versions 1–5 and strict FLOAT rejection), and
`TestNumericWireSnapshotIntegerMetadata` (public streaming/materialized routes).
`TestNumericWireReplicationNumbersSurviveReplay` also asserts an exact signed
nanosecond timestamp through WAL reopen and uint64-max snapshot position.
Existing schema rewrite/restart, CDC metadata, native MVCC, redo/undo/bulk and
legacy recovery regressions cover the remaining inventoried consumers.
