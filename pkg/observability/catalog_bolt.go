// Package observability — Bolt metric bag (Plan 04-02 GREEN).
//
// Owns six families per MET-07 + ADR §2.3:
//
//	nornicdb_bolt_connections_active
//	nornicdb_bolt_connections_total{result}
//	nornicdb_bolt_session_duration_seconds
//	nornicdb_bolt_messages_total{op, result}
//	nornicdb_bolt_message_duration_seconds{op}
//	nornicdb_bolt_packstream_decode_errors_total{reason}
//
// Closed enums (CONTEXT D-11 / D-11a / D-11c):
//
//	result   ∈ AllowedBoltResults     = {success, error, timeout}
//	op       ∈ AllowedBoltOps         = {hello, run, pull, begin, commit,
//	                                     discard, reset, goodbye, route,
//	                                     ack_failure}
//	reason   ∈ AllowedPackstreamReasons =
//	                                     {truncated, invalid_marker,
//	                                      wrong_type, oversize}
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `op`, `result`, and `reason` are NOT in the forbidden list — closed-enum
// string literals at the call site enforce cardinality. The reason enum is
// the highest-risk axis (free-form `err.Error()` would be a cardinality
// bomb); CONTEXT D-11c locks it to four values, classified at the
// packstream decode boundary via `reasonFromError(err)`.
//
// PULL chunks NOT separately observed (CONTEXT D-11b). Chunk timing rolls
// up into the parent PULL `message_duration_seconds`. Aligns with Phase 8
// TRC-13 ("PULL chunks do not span") — same rationale: per-chunk
// observation taxes the streaming hot path with no SRE alerting benefit.
//
// No `database` label on Bolt subsystem: sessions/messages cross databases
// via USE clauses; per-DB instrumentation lives at the Cypher subsystem.
// CONTEXT D-08 bool not threaded through this constructor.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedBoltResults is the closed enum for the `result` label per
// CONTEXT D-11. Mirrors the connection-close + message-dispatch outcome.
var AllowedBoltResults = []string{"success", "error", "timeout"}

// AllowedBoltOps is the closed enum for the `op` label per CONTEXT D-11a.
// Sourced from pkg/bolt/server.go MsgHello/MsgRun/... message-type
// constants. Adding a new Bolt message op = enum update + ADR §2.3
// amendment.
var AllowedBoltOps = []string{
	"hello",
	"run",
	"pull",
	"begin",
	"commit",
	"discard",
	"reset",
	"goodbye",
	"route",
	"ack_failure",
}

// AllowedPackstreamReasons is the closed enum for the `reason` label per
// CONTEXT D-11c. Free-form `err.Error()` strings would be a cardinality
// bomb; the four-value enum is enforced by `reasonFromError(err)` at the
// decode boundary in pkg/bolt/packstream.go.
var AllowedPackstreamReasons = []string{
	"truncated",
	"invalid_marker",
	"wrong_type",
	"oversize",
}

// AllowedBoltTransports is the closed enum for the `transport` label
// added in the bolt-over-websocket landing. The four values cover every
// wire-level transport the Bolt port multiplexes:
//
//	tcp     - raw bolt:// over plain TCP
//	tcp_tls - bolt+s:// (TLS-wrapped raw)
//	ws      - ws://    (WebSocket over plain TCP)
//	ws_tls  - wss://   (WebSocket over TLS)
//
// Cardinality ceiling for `bolt_connections_total` rises from 3 to 12
// (3 results x 4 transports) and `bolt_connections_active` from 1 to 4.
var AllowedBoltTransports = []string{"tcp", "tcp_tls", "ws", "ws_tls"}

// AllowedBoltConnectionRejectReasons is the closed enum for the new
// `bolt_connections_rejected_total` counter introduced with WebSocket
// transport selection. Each reason is a discrete, named failure mode so
// dashboards can distinguish operator-misconfiguration from attack noise
// from organic load.
var AllowedBoltConnectionRejectReasons = []string{
	"max_connections",
	"sniff_timeout",
	"auth_timeout",
	"tls_handshake",
	"ws_handshake",
	"oversized_message",
	"requires_tls",
	"unrecognized_prefix",
	"ws_disabled",
}

// BoltMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the Bolt
// subsystem. One bag per Provider; constructed at cmd/nornicdb startup and
// injected into pkg/bolt.Server via SetBoltMetrics(...) so the connection-
// accept goroutine and per-message dispatch loop observe through pre-bound
// observers (MET-25).
//
// Hot-path discipline (MET-25): the Bolt server pre-builds a per-op
// `[]BoundLatencyObserver` indexed by AllowedBoltOps so the dispatch loop
// pays zero WithLabelValues overhead per message. The
// `BindMessageDuration(op)` helper amortizes the lookup at session
// construction (the per-message observe call uses the cached observer).
type BoltMetrics struct {
	// ConnectionsActive is the live connection count gauge labelled by
	// transport. Cardinality ceiling = 4 (len(AllowedBoltTransports)).
	ConnectionsActive *prometheus.GaugeVec

	// ConnectionsTotal counts connection terminations by result and
	// transport. Cardinality ceiling = 12 (3 results x 4 transports).
	ConnectionsTotal *prometheus.CounterVec

	// ConnectionsRejectedTotal counts connections rejected before the
	// session loop started — sniff timeouts, oversized messages, TLS
	// handshake failures, RequireTLS rejections, and so on. Closed enum
	// for the reason label (AllowedBoltConnectionRejectReasons).
	ConnectionsRejectedTotal *prometheus.CounterVec

	// WebSocketOversizedTotal counts WebSocket messages that exceeded
	// the configured frame size limit. Discrete because oversized
	// messages are a WS-only failure mode worth a dedicated alert.
	WebSocketOversizedTotal prometheus.Counter

	// SessionDuration histograms the wall-clock lifetime of each Bolt session
	// (accept → close). Phase-3-locked LatencyBucketsSeconds. No labels.
	SessionDuration *LatencyHistogram

	// MessagesTotal counts dispatched Bolt messages by op + result.
	// Cardinality ceiling = 30 (10 ops × 3 results; AllowedBoltOps ×
	// AllowedBoltResults — RESEARCH §Q11).
	MessagesTotal *prometheus.CounterVec

	// MessageDuration histograms the per-message dispatch duration.
	// Cardinality ceiling = 10 (len(AllowedBoltOps)).
	MessageDuration *LatencyHistogram

	// PackstreamDecodeErrors counts decode failures classified by closed
	// reason enum. Cardinality ceiling = 4 (len(AllowedPackstreamReasons);
	// RESEARCH §Q11). Free-form `err.Error()` MUST NEVER reach this Vec —
	// classification happens via `reasonFromError(err)` at the decode
	// boundary.
	PackstreamDecodeErrors *prometheus.CounterVec
}

// NewBoltMetrics constructs the Bolt bag against reg.
//
// Validation + Pitfall-8 panic semantics inherited from Phase 3 typed
// constructors: missing _total/_seconds suffixes or forbidden labels
// panic at registration.
//
// Construction is idempotent against this bag's six families ONLY for a
// fresh registry — re-constructing on the same registry triggers
// AlreadyRegisteredError per Pitfall 8.
func NewBoltMetrics(reg *prometheus.Registry) *BoltMetrics {
	bm := &BoltMetrics{}

	bm.ConnectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "bolt",
		Name:      "connections_active",
		Help: "Number of currently-active Bolt protocol connections by transport. " +
			"Transport enum closed (tcp, tcp_tls, ws, ws_tls).",
	}, []string{"transport"})
	reg.MustRegister(bm.ConnectionsActive)

	bm.ConnectionsTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "connections_total",
			Help: "Bolt connections terminated by result and transport. " +
				"Result enum closed (CONTEXT D-11: success, error, timeout). " +
				"Transport enum closed (tcp, tcp_tls, ws, ws_tls).",
		},
		[]string{"result", "transport"})

	bm.ConnectionsRejectedTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "connections_rejected_total",
			Help: "Bolt connections rejected before session loop start. " +
				"Reason enum closed (max_connections, sniff_timeout, auth_timeout, " +
				"tls_handshake, ws_handshake, oversized_message, requires_tls, " +
				"unrecognized_prefix, ws_disabled).",
		},
		[]string{"reason"})

	bm.WebSocketOversizedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb",
		Subsystem: "bolt",
		Name:      "websocket_oversized_total",
		Help:      "WebSocket messages dropped because they exceeded the configured frame size limit.",
	})
	reg.MustRegister(bm.WebSocketOversizedTotal)

	bm.SessionDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "session_duration_seconds",
			Help:      "Wall-clock lifetime of each Bolt session (accept → close).",
		},
		nil /* no labels */)

	bm.MessagesTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "messages_total",
			Help: "Bolt messages dispatched by op and result. " +
				"Op enum closed (CONTEXT D-11a: hello, run, pull, begin, commit, " +
				"discard, reset, goodbye, route, ack_failure). " +
				"Result enum closed (success, error).",
		},
		[]string{"op", "result"})

	bm.MessageDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "message_duration_seconds",
			Help: "Per-message dispatch duration. PULL chunks NOT separately " +
				"observed (D-11b — chunk timing rolls up into parent PULL).",
		},
		[]string{"op"})

	bm.PackstreamDecodeErrors = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "bolt",
			Name:      "packstream_decode_errors_total",
			Help: "Packstream decode failures classified by closed reason enum. " +
				"Reason enum closed (CONTEXT D-11c: truncated, invalid_marker, " +
				"wrong_type, oversize). Free-form err.Error() MUST NEVER reach " +
				"this Vec — classification via reasonFromError() at decode boundary.",
		},
		[]string{"reason"})

	return bm
}

// BindMessageDuration returns a pre-bound BoundLatencyObserver for the
// given op. Used by pkg/bolt.Server to pre-build a per-op slice at session
// construction (MET-25 hot-path discipline) so the dispatch loop pays zero
// WithLabelValues overhead per message.
func (b *BoltMetrics) BindMessageDuration(op string) BoundLatencyObserver {
	return b.MessageDuration.Bind(op)
}
