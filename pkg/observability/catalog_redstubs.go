// Package observability — minimal type stubs for catalog bags whose GREEN
// implementations land in Plans 04-03..04-06 (Cypher, Storage, MVCC, Embed,
// Search, Replication, Auth).
//
// These stubs exist to keep pkg/observability compiling while the per-bag
// RED tests sit in their `t.Skip("RED: pending Plan 04-NN")` state (Plan
// 04-01 Wave-0 RED-first cadence). Each stub:
//
//   - Declares the struct fields the RED tests reference (e.g. `Bytes`,
//     `IndexRebuild`, `LagBytes`, `AuthAttempts`, `FFIPanicTotal`).
//   - Provides a constructor that PANICS at runtime — the RED test's
//     leading `t.Skip` ensures the constructor is never called from a
//     running test, so the panic is purely a load-bearing guard against
//     accidental production usage before the GREEN bag lands.
//
// When Plan 04-NN ships its real `NewXxxMetrics` constructor + bag, that
// plan REPLACES the stub here with the production bag (split into a
// per-subsystem catalog_<sub>.go file per CONTEXT D-02a).
//
// Plan ownership map:
//
//	NewCypherMetrics        — Plan 04-03 (SHIPPED — see catalog_cypher.go)
//	NewStorageMetrics       — Plan 04-04 (SHIPPED — see catalog_storage.go)
//	NewMVCCMetrics          — Plan 04-04 (SHIPPED — see catalog_mvcc.go;
//	                          RISK-2 PinnedBytes accessor lives on
//	                          *BadgerEngine in pkg/storage/badger_mvcc.go)
//	NewEmbedMetrics         — Plan 04-05 (SHIPPED — see catalog_embed.go)
//	NewSearchMetrics        — Plan 04-05 (SHIPPED — see catalog_search.go)
//	NewReplicationMetrics   — Plan 04-06 (with RISK-3 PeerConfig.ID + GAP-1
//	                          last_contact_seconds + per-mode cardinality)
//	NewAuthMetrics          — Plan 04-06 (GAP-6 / MET-15)
//
// Plan 04-02 (this plan) DELIVERS NewHTTPMetrics + NewBoltMetrics — those
// live in catalog_http.go and catalog_bolt.go respectively (NOT in this
// stubs file).
//
// As of Plan 04-06 all bags have shipped — the stub bodies are empty and
// this file remains as a historical pointer to the per-subsystem catalog
// files (one entry per <plan ownership map> row).
package observability

// ----- Cypher (Plan 04-03) — GREEN bag lives in catalog_cypher.go ---------

// ----- Storage (Plan 04-04) — GREEN bag lives in catalog_storage.go -------

// ----- MVCC (Plan 04-04) — GREEN bag lives in catalog_mvcc.go -------------

// ----- Embeddings (Plan 04-05) — GREEN bag lives in catalog_embed.go ------

// ----- Search (Plan 04-05) — GREEN bag lives in catalog_search.go ---------

// ----- Replication (Plan 04-06) — GREEN bag lives in catalog_replication.go -

// ----- Auth (Plan 04-06) — GREEN bag lives in catalog_auth.go ---------------
