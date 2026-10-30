// Package bolt — Plan 04-02 metric instrumentation.
//
// Three observation sites per CONTEXT D-11/a/c:
//
//  1. Connection accept (handleConnection in server.go): Inc/Dec
//     ConnectionsActive gauge; observe SessionDuration on close;
//     increment ConnectionsTotal{result} on close.
//  2. Per-message dispatch loop (handleMessage / dispatchMessage in
//     server.go): observe MessageDuration{op} per message; increment
//     MessagesTotal{op, result}.
//  3. Packstream decode boundary (packstream.go): increment
//     PackstreamDecodeErrors{reason} via reasonFromError() classifier
//     (closed enum: truncated, invalid_marker, wrong_type, oversize).
//
// PULL chunks NOT separately observed (D-11b — chunk timing rolls up
// into parent PULL message_duration_seconds; matches Phase 8 TRC-13).
//
// Auth crosswire (CONTEXT D-11 / D-05e + Plan 04-06 forward-compat):
// HELLO completion increments auth_attempts_total{result, protocol="bolt"}
// when authMetrics is non-nil. Plan 04-06 owns the AuthMetrics bag and
// wires it via SetAuthMetrics(...); this plan adds the call site behind
// a nil-check that no-ops until 04-06 ships.
//
// Hot-path discipline (MET-25): per-op BoundLatencyObserver pre-built at
// SetBoltMetrics time and indexed by op name in a small map; the
// dispatch loop pays a single map lookup per message — no
// WithLabelValues alloc.
package bolt

import (
	"github.com/orneryd/nornicdb/pkg/observability"
)

// boltOpName maps the wire-protocol message-type byte to the closed-enum
// op label value declared in pkg/observability.AllowedBoltOps. Centralized
// here (DRY) so the dispatch instrumentation, the auth-attempt site, and
// the bound observer cache share one mapping.
//
// MsgRollback maps to "ack_failure" because the Bolt 4.4 protocol
// repurposed 0x13 as ROLLBACK, but the historical telemetry name in
// AllowedBoltOps is "ack_failure" — keeping the name stable preserves
// dashboard continuity. Reviewers: the label value is a stable string,
// not a wire mnemonic.
func boltOpName(msgType byte) string {
	switch msgType {
	case MsgHello:
		return "hello"
	case MsgGoodbye:
		return "goodbye"
	case MsgReset:
		return "reset"
	case MsgRun:
		return "run"
	case MsgDiscard:
		return "discard"
	case MsgPull:
		return "pull"
	case MsgBegin:
		return "begin"
	case MsgCommit:
		return "commit"
	case MsgRollback:
		return "ack_failure"
	case MsgRoute:
		return "route"
	default:
		// Unknown message types fall to "ack_failure" so they surface in
		// error counters under a closed enum value (D-11a — closed enum
		// guarantees no per-byte cardinality bomb).
		return "ack_failure"
	}
}

// boltMetricsState bundles the bag + pre-built per-op bound observers.
// Lives on Server as a single field so the metrics-disabled path
// (SetBoltMetrics never called) is a single nil-check.
type boltMetricsState struct {
	bag *observability.BoltMetrics
	// msgDur is a pre-built per-op slice of bound observers. Index is the
	// position of the op string in observability.AllowedBoltOps. The
	// per-message dispatch loop computes the index once via the
	// boltOpName→index map and indexes directly, avoiding a per-message
	// WithLabelValues alloc.
	msgDur map[string]observability.BoundLatencyObserver
}

// newBoltMetricsState pre-builds the per-op bound observer cache. Called
// once at SetBoltMetrics time per server lifetime; the dispatch loop
// never allocates after this.
func newBoltMetricsState(bag *observability.BoltMetrics) *boltMetricsState {
	if bag == nil {
		return nil
	}
	cache := make(map[string]observability.BoundLatencyObserver, len(observability.AllowedBoltOps))
	for _, op := range observability.AllowedBoltOps {
		cache[op] = bag.BindMessageDuration(op)
	}
	return &boltMetricsState{bag: bag, msgDur: cache}
}

// SetBoltMetrics injects the Plan-04-02 Bolt catalog bag (D-02 typed
// handle DI) plus pre-built per-op bound observers (MET-25). MUST be
// called BEFORE ListenAndServe. Nil-safe: passing nil leaves the server
// in metrics-disabled mode (matches existing test fixtures).
//
// Pairs with SetAuthMetrics for the D-11 auth-attempts crosswire (Plan
// 04-06 wires the Auth bag).
func (s *Server) SetBoltMetrics(bag *observability.BoltMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsState = newBoltMetricsState(bag)
}

// SetAuthMetrics injects the Plan-04-06 Auth catalog bag for the
// auth_attempts_total{result, protocol="bolt"} crosswire (D-05e + D-11).
// Plan 04-02 ships the call site behind a nil-check; Plan 04-06 wires
// the bag at cmd/nornicdb startup.
func (s *Server) SetAuthMetrics(bag *observability.AuthMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authMetrics = bag
}

// observeAuthAttempt is the SOLE auth-attempt observation site (DRY).
// Called from handleHello on success/failure/denied. No-op when the
// auth bag is nil (Plan 04-06 wires it).
func (s *Server) observeAuthAttempt(result string) {
	s.mu.RLock()
	bag := s.authMetrics
	s.mu.RUnlock()
	if bag == nil || bag.AuthAttempts == nil {
		return
	}
	bag.AuthAttempts.WithLabelValues(result, "bolt").Inc()
}
