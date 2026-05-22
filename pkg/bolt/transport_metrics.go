package bolt

import (
	"github.com/orneryd/nornicdb/pkg/observability"
)

// AllowedBoltTransports re-exports the closed enum from
// pkg/observability so call sites in this package can refer to it
// without importing the observability package directly.
var AllowedBoltTransports = observability.AllowedBoltTransports

// incBoltConnectionsActive increments the ConnectionsActive gauge for the
// given transport label. Closed enum: tcp / tcp_tls / ws / ws_tls.
func incBoltConnectionsActive(bag *observability.BoltMetrics, transport string) {
	if bag == nil || bag.ConnectionsActive == nil {
		return
	}
	bag.ConnectionsActive.WithLabelValues(transport).Inc()
}

// decBoltConnectionsActive decrements the ConnectionsActive gauge.
func decBoltConnectionsActive(bag *observability.BoltMetrics, transport string) {
	if bag == nil || bag.ConnectionsActive == nil {
		return
	}
	bag.ConnectionsActive.WithLabelValues(transport).Dec()
}

// incBoltConnectionsTotal increments the connections_total counter with
// the result and transport labels.
func incBoltConnectionsTotal(bag *observability.BoltMetrics, result, transport string) {
	if bag == nil || bag.ConnectionsTotal == nil {
		return
	}
	bag.ConnectionsTotal.WithLabelValues(result, transport).Inc()
}

// incBoltConnectionsRejected increments connections_rejected_total with a
// closed-enum reason label. Reasons enumerated in
// observability.AllowedBoltConnectionRejectReasons.
func incBoltConnectionsRejected(bag *observability.BoltMetrics, reason string) {
	if bag == nil || bag.ConnectionsRejectedTotal == nil {
		return
	}
	bag.ConnectionsRejectedTotal.WithLabelValues(reason).Inc()
}

// incWebSocketOversized increments websocket_oversized_total.
func incWebSocketOversized(bag *observability.BoltMetrics) {
	if bag == nil || bag.WebSocketOversizedTotal == nil {
		return
	}
	bag.WebSocketOversizedTotal.Inc()
}
