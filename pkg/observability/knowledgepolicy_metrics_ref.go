// Package observability — knowledge-policy metrics global ref.
//
// The Badger read-path filter (pkg/storage/badger_decay_filter.go) fires on
// EVERY node returned from a Cypher MATCH when decay is enabled. Threading
// a *KnowledgePolicyMetrics handle through every storage iterator signature
// would widen CreateNode / GetNode / iterateNodesVisibleAtInScopeInTxn and their
// many callers. Instead we use the same atomic.Pointer bridge the BSP self-
// metrics pattern (bsp_self_metrics.go) already established for injecting
// observability into a deep-nested subsystem without inverting ownership.
//
// Semantics:
//   - Set once at Provider init (from cmd/nornicdb/main.go, after
//     NewKnowledgePolicyMetrics returns).
//   - Overwritten on each New() so per-test TestEnv isolation still works.
//   - Nil safe: GetKnowledgePolicyMetrics returns nil before Set is called
//     or when metrics are disabled; all IncScored / IncSuppression / etc.
//     helpers short-circuit on nil receiver.
package observability

import "sync/atomic"

// kpMetricsRefs holds the active knowledge-policy metrics handle. Read from
// pkg/storage/badger_decay_filter.go and any other site where threading a
// constructor-injected handle would widen a deeply nested call tree.
//
// Set from cmd/nornicdb/main.go alongside the existing NewCypherMetrics /
// NewStorageMetrics wiring. Nil until Set is called.
var kpMetricsRefs atomic.Pointer[KnowledgePolicyMetrics]

// SetKnowledgePolicyMetrics publishes the active metrics handle for the
// read-path filter to consume. Safe to call multiple times; the last call
// wins. Pass nil to tear down (used in tests that reset global state).
func SetKnowledgePolicyMetrics(m *KnowledgePolicyMetrics) {
	kpMetricsRefs.Store(m)
}

// GetKnowledgePolicyMetrics returns the currently-published metrics handle,
// or nil if none has been set. Callers MUST nil-check the return value
// (all Inc* / Observe* methods on *KnowledgePolicyMetrics are nil-safe, so
// the idiomatic pattern is `observability.GetKnowledgePolicyMetrics().
// IncReadFilterDropped("node", "")` without a separate nil branch).
func GetKnowledgePolicyMetrics() *KnowledgePolicyMetrics {
	return kpMetricsRefs.Load()
}
