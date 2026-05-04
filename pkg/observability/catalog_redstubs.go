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
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// stubPanic is the canonical panic message used by every stub constructor.
// Calling a stub at runtime (outside a t.Skip()-guarded test) is a
// programming bug — the RED tests must skip first per CONTEXT D-01a.
const stubPanic = "observability: stub constructor invoked; the GREEN bag for this subsystem ships in a downstream Plan 04-NN — see catalog_redstubs.go header for the plan map"

// ----- Cypher (Plan 04-03) — GREEN bag lives in catalog_cypher.go ---------

// ----- Storage (Plan 04-04) — GREEN bag lives in catalog_storage.go -------

// ----- MVCC (Plan 04-04) — GREEN bag lives in catalog_mvcc.go -------------

// ----- Embeddings (Plan 04-05) — GREEN bag lives in catalog_embed.go ------

// ----- Search (Plan 04-05) — GREEN bag lives in catalog_search.go ---------

// ----- Replication (Plan 04-06) --------------------------------------------

// ReplicationMetrics is the Plan-04-06 stub.
type ReplicationMetrics struct {
	LagBytes *prometheus.GaugeVec
}

func NewReplicationMetrics(reg *prometheus.Registry, mode string, tenantLabelsEnabled bool) *ReplicationMetrics {
	panic(stubPanic)
}

// ----- Auth (Plan 04-06) ---------------------------------------------------

// AuthMetrics is the Plan-04-06 stub (GAP-6 / MET-15).
//
// Plan 04-02's pkg/bolt/server.go HELLO completion site references this
// type via `*observability.AuthMetrics` for its forward-compat nil-check
// (the auth attempt counter only fires when the bag is non-nil; Plan 04-06
// constructs the bag and wires it in via setter).
type AuthMetrics struct {
	AuthAttempts *prometheus.CounterVec
}

func NewAuthMetrics(reg *prometheus.Registry) *AuthMetrics {
	panic(stubPanic)
}
