# NornicDB Metrics Reference

Auto-generated from `pkg/observability/catalog_*.go`. Total: 56 metrics.

## Auth

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_attempts_total` | counter | - | Authentication attempts by result (closed enum: success, failure, denied) and protocol (closed enum: bolt, http, grpc). No `user`/`user_id`/`email`/`ip` labels — Phase 3 D-03a forbidden-label panic is the registration-time gate. Per-protocol observation site lives in the protocol adapter  |
| `nornicdb_attempts_total` | counter | - | Authentication attempts by result (closed enum: success, failure, denied) and protocol (closed enum: bolt, http, grpc). No `user`/`user_id`/`email`/`ip` labels — Phase 3 D-03a forbidden-label panic is the registration-time gate. Per-protocol observation site lives in the protocol adapter  |

## Auth_test

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_attempts_total` | counter | - | PII drift attempt — must panic. |

## Bolt

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_bolt_connections_total` | counter | - | Bolt connections terminated by result. Result enum closed (CONTEXT D-11: success, error, timeout). |
| `nornicdb_bolt_messages_total` | counter | - | Bolt messages dispatched by op and result. Op enum closed (CONTEXT D-11a: hello, run, pull, begin, commit, discard, reset, goodbye, route, ack_failure). Result enum closed (success, error). |
| `nornicdb_bolt_packstream_decode_errors_total` | counter | - | Packstream decode failures classified by closed reason enum. Reason enum closed (CONTEXT D-11c: truncated, invalid_marker, wrong_type, oversize). Free-form err.Error() MUST NEVER reach this Vec — classification via reasonFromError() at decode boundary. |
| `nornicdb_connections_active` | gauge | - | Number of currently-active Bolt protocol connections. |

## Cache

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_cache_evictions_total` | counter | - | Cache evictions per cache and reason. Reason enum closed (CONTEXT D-12b: lru, ttl, capacity, manual). |
| `nornicdb_cache_misses_total` | counter | - | Cache misses per cache. Cache enum is closed (CONTEXT D-12). |
| `nornicdb_cache_size_bytes` | gauge | - | Approximate cache size in bytes per cache (gauge). |
| `nornicdb_cache_uptime_seconds` | gauge | - | Wall-clock seconds since the bag was constructed (≈ process start). |
| `nornicdb_hits_total` | counter | - | Cache hits per cache. Cache enum is closed (CONTEXT D-12). |
| `nornicdb_hits_total` | gauge | - | Cache hits per cache. Cache enum is closed (CONTEXT D-12). |
| `nornicdb_process_build_info` | gauge | - | Build identification (constant 1; metadata in const labels). Labels: version, commit, go_version, backend. |

## Cypher

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_cypher_active_transactions` | gauge | - | Currently-open Cypher transactions. Inc on Begin; Dec on Commit/Rollback (deferred pair). |
| `nornicdb_cypher_planner_cache_hits_total` | counter | - | Cypher planner cache hits. D-12a: planner cache lives under the cypher subsystem, not the cross-cutting cache subsystem. |
| `nornicdb_cypher_planner_cache_misses_total` | counter | - | Cypher planner cache misses. D-12a: planner cache lives under the cypher subsystem, not the cross-cutting cache subsystem. |
| `nornicdb_cypher_planner_cache_size` | gauge | - | Current Cypher planner cache occupancy (entries). D-12a. |
| `nornicdb_cypher_slow_queries_total` | counter | - | Cypher queries that exceeded slow_query_threshold_seconds. Matches the Phase 2 D-04c slow-query log emission gate. |
| `nornicdb_cypher_slow_query_threshold_seconds` | gauge | - | Configured slow-query threshold (seconds). GaugeFunc — reflects cfg.Logging.SlowQueryThreshold() on every scrape; no event wiring required for config reload (D-15b). Returns 0 on callback panic (RISK-8 mitigation). |
| `nornicdb_cypher_transaction_conflicts_total` | counter | - | MVCC conflicts surfaced from storage at the Cypher transaction wrapper (D-16: storage detects, Cypher counts; storage layer never imports observability — preserves AGENTS.md §8 separation). |
| `nornicdb_queries_total` | counter | - | Cypher queries by op_type (closed enum: read, write, schema, admin, fabric, parse_error). database label included when tenant-labels-enabled per D-08. |

## Cypher_test

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_queries_total` | counter | - | would-be cardinality bomb |

## Embed

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_embed_cache_hits_total` | counter | - | Embedding cache hits (in-process LRU keyed by FNV-1a hash of input text). Bridges into the cross-cutting Cache bag at the call site. |
| `nornicdb_embed_cache_misses_total` | counter | - | Embedding cache misses (compute-and-store path). Bridges into the cross-cutting Cache bag. |
| `nornicdb_embed_ffi_panics_total` | counter | - | Recovered CGo / purego panics from local llama.cpp FFI call sites. mode = build-tag-derived backend at panic time (closed enum: gpu, cpu, cuda, metal, vulkan). Per D-09: server stays up — counter increments and panic converts to error. |
| `nornicdb_embed_processed_total` | counter | - | Embedding processing outcomes by provider (closed enum: ollama, openai, local, other), model (open-ish; ceiling 250), result (closed enum: success, failure, cached), and mode (closed enum: gpu, cpu, cuda, metal, vulkan). Cardinality bounded by RESEARCH §Q11. |
| `nornicdb_embed_worker_running` | gauge | - | Embedding worker lifecycle indicator (1 = running, 0 = stopped). Set at StartWorkers/Stop hook sites per D-15b binary-state distinction; NOT a GaugeFunc. |
| `nornicdb_queue_depth` | gauge | - | Embedding queue depth (nodes pending embedding) at scrape time. GaugeFunc — reads EmbedProbe.QueueLen() on every scrape; returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_queue_depth` | gauge | - | Embedding queue depth (nodes pending embedding) at scrape time. GaugeFunc — reads EmbedProbe.QueueLen() on every scrape; returns 0 on probe panic (RISK-8 mitigation). |

## Http

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_http_in_flight_requests` | gauge | - | Number of HTTP requests currently being processed. |
| `nornicdb_http_requests_total` | counter | - | HTTP requests by method, path_template, status_class. Same label-set as request_duration_seconds (database when D-08 flag enabled). |

## Http_test

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_requests_total` | counter | - | would-be cardinality bomb |

## Mvcc

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_mvcc_active_readers` | gauge | - | Count of currently-open MVCC reader snapshots. Returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_mvcc_oldest_reader_age_seconds` | gauge | - | Wall-clock age (seconds) of the oldest active MVCC reader snapshot. Returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_mvcc_pinned_bytes` | gauge | - | Cumulative bytes pinned by all active MVCC reader snapshots (RISK-2 accessor on storage.BadgerEngine). Returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_mvcc_pinned_bytes` | gauge | - | Cumulative bytes pinned by all active MVCC reader snapshots (RISK-2 accessor on storage.BadgerEngine). Returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_pressure_band` | gauge | - | MVCC pressure band indicator gauge per CONTEXT D-14. Active band = 1, others = 0. Closed band enum: normal, warn, high, critical. Thresholds 0.50 / 0.75 / 0.90. |

## Replication

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_replication_apply_index` | gauge | - | Last applied log index. Set at apply-advance log sites (D-15a). Per-cluster — no labels. |
| `nornicdb_replication_commit_index` | gauge | - | Last committed log index. Set at commit-advance log sites (D-15a). Per-cluster — no labels. |
| `nornicdb_replication_lag_bytes` | gauge | - | Per-peer replication lag in bytes. peer label sources from PeerConfig.ID (RISK-3 fix); fallback to PeerConfig.Addr. Mode-aware cardinality ceiling: ha_standby=8, raft=16, multi_region=64 (D-05a). Stale peers GC'd by peer_metrics_gc lifecycle.Component (D-05b). |
| `nornicdb_replication_lag_entries` | gauge | - | Per-peer replication lag in log entries. Same peer-label discipline as lag_bytes. Mode-aware ceiling per D-05a. |
| `nornicdb_replication_last_contact_seconds` | gauge | - | GAP-1 keystone — wall-clock seconds since the last successful AppendEntries with each peer. Operators alert on `time() - nornicdb_replication_last_contact_seconds{...} > N` per Phase 9 Helm/Grafana plan. Per-peer; mode-aware ceiling per D-05a. |
| `nornicdb_replication_leader_changes_total` | counter | - | Total leader-boundary transitions (non-leader→leader OR leader→non-leader). Keystone alert metric for cluster instability. Increments at lifecycle log sites (D-15a). |
| `nornicdb_replication_term` | gauge | - | Current Raft term. Set at term-change log sites (D-15a). Per-cluster — no labels. |
| `nornicdb_role` | gauge | - | Current replication role as numeric enum (-1=unknown, 0=follower, 1=candidate, 2=leader, 3=standby). Set at the same lifecycle log sites that emit 'became leader' etc. (D-15a). Per-cluster — no labels. |
| `nornicdb_role` | gauge | - | Current replication role as numeric enum (-1=unknown, 0=follower, 1=candidate, 2=leader, 3=standby). Set at the same lifecycle log sites that emit 'became leader' etc. (D-15a). Per-cluster — no labels. |

## Search

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_requests_total` | counter | - | Search requests by [database], mode (closed enum: vector, bm25, hybrid), and result (closed enum: success, no_results, error). database label gated by tenant flag (D-08). |
| `nornicdb_search_index_size_bytes` | gauge | - | Search index size in bytes by kind (closed enum: hnsw, bm25). Populated by Set() at index-build/load events; GaugeFunc fallback reads SearchProbe.IndexSizeBytes(kind) on every scrape (D-15b; defer-recover returns 0 on panic per RISK-8 mitigation). |
| `nornicdb_search_index_size_bytes` | gauge | - | Search index size in bytes by kind (closed enum: hnsw, bm25). Populated by Set() at index-build/load events; GaugeFunc fallback reads SearchProbe.IndexSizeBytes(kind) on every scrape (D-15b; defer-recover returns 0 on panic per RISK-8 mitigation). |

## Storage

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nornicdb_nodes_total` | gauge | - | Total node count in storage. GaugeFunc — reads StorageProbe.NodeCount() on every scrape; returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_storage_bytes` | gauge | - | Storage size in bytes by kind (closed enum: nodes, edges, index, wal, search). Populated every 30s by the bytes_metrics_sweeper lifecycle.Component (D-07). |
| `nornicdb_storage_compactions_total` | counter | - | BadgerDB compactions by level and result. Bounded by BadgerDB's compaction-level surface — no user input flows to label values. |
| `nornicdb_storage_edges_total` | gauge | - | Total edge count in storage. GaugeFunc — reads StorageProbe.EdgeCount() on every scrape; returns 0 on probe panic (RISK-8 mitigation). |
| `nornicdb_storage_index_rebuild_total` | counter | - | Index-rebuild events by [database], index (closed enum: label, edge_between, temporal, embedding, user_created), and result (success, failure, aborted). D-13c: arbitrary user index names bucket to user_created — closed-enum discipline prevents cardinality bombs. |
| `nornicdb_storage_wal_lag_bytes` | gauge | - | Heuristic estimate of WAL backlog (vlog size minus LSM size from badger.DB.Size()). Best-effort; alerting should use 5-minute trends, not single scrapes (RISK-6). |

