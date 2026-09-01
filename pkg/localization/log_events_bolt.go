package localization

import (
	"log/slog"
	"time"
)

const (
	// EventBoltServerListening identifies a Bolt listener ready to accept connections.
	EventBoltServerListening                    EventID = "bolt.server.listening"
	EventBoltConnectionHandlerPanic             EventID = "bolt.connection.handler_panic"
	EventBoltUnencryptedConnectionRejected      EventID = "bolt.connection.unencrypted_rejected"
	EventBoltTransportSniffFailed               EventID = "bolt.transport.sniff_failed"
	EventBoltHandshakeFailed                    EventID = "bolt.handshake.failed"
	EventBoltMessageHandlingError               EventID = "bolt.message.handling_error"
	EventBoltHelloSchemeNone                    EventID = "bolt.auth.hello_scheme_none"
	EventBoltWebSocketCookieBearerRejected      EventID = "bolt.auth.ws_cookie_bearer_rejected"
	EventBoltAuthenticationFailed               EventID = "bolt.auth.failed"
	EventBoltHello                              EventID = "bolt.session.hello"
	EventBoltQuery                              EventID = "bolt.query.executing"
	EventBoltQueryError                         EventID = "bolt.query.error"
	EventBoltRun                                EventID = "bolt.query.run"
	EventBoltDiscoveryRefreshFailed             EventID = "bolt.discovery.refresh_failed"
	EventBoltWebSocketUpgradeReadRequestFailed  EventID = "bolt.websocket.upgrade_read_request_failed"
	EventBoltWebSocketUpgradeCredentials        EventID = "bolt.websocket.upgrade_credentials"
	EventBoltWebSocketUpgradeFailed             EventID = "bolt.websocket.upgrade_failed"
	EventBoltTransactionTerminated              EventID = "bolt.transaction.explicit_terminated"
	EventBoltTransactionTimeoutCleanupRequested EventID = "bolt.transaction.timeout_cleanup_requested"
	EventBoltTransactionCommitFailed            EventID = "bolt.transaction.commit_failed"
	EventBoltTransactionCleanupFailed           EventID = "bolt.transaction.cleanup_failed"
	EventBoltTransactionTimeoutCleanupCompleted EventID = "bolt.transaction.timeout_cleanup_completed"

	MessageBoltLogServerListening                    MessageID = "bolt.log.server_listening"
	MessageBoltLogConnectionHandlerPanic             MessageID = "bolt.log.connection_handler_panic"
	MessageBoltLogUnencryptedConnectionRejected      MessageID = "bolt.log.unencrypted_connection_rejected"
	MessageBoltLogTransportSniffFailed               MessageID = "bolt.log.transport_sniff_failed"
	MessageBoltLogHandshakeFailed                    MessageID = "bolt.log.handshake_failed"
	MessageBoltLogMessageHandlingError               MessageID = "bolt.log.message_handling_error"
	MessageBoltLogHelloSchemeNone                    MessageID = "bolt.log.hello_scheme_none"
	MessageBoltLogWebSocketCookieBearerRejected      MessageID = "bolt.log.ws_cookie_bearer_rejected"
	MessageBoltLogAuthenticationFailed               MessageID = "bolt.log.authentication_failed"
	MessageBoltLogHello                              MessageID = "bolt.log.hello"
	MessageBoltLogQuery                              MessageID = "bolt.log.query"
	MessageBoltLogQueryError                         MessageID = "bolt.log.query_error"
	MessageBoltLogRun                                MessageID = "bolt.log.run"
	MessageBoltLogDiscoveryRefreshFailed             MessageID = "bolt.log.discovery_refresh_failed"
	MessageBoltLogWebSocketUpgradeReadRequestFailed  MessageID = "bolt.log.websocket_upgrade_read_request_failed"
	MessageBoltLogWebSocketUpgradeCredentials        MessageID = "bolt.log.websocket_upgrade_credentials"
	MessageBoltLogWebSocketUpgradeFailed             MessageID = "bolt.log.websocket_upgrade_failed"
	MessageBoltLogTransactionTerminated              MessageID = "bolt.log.transaction_terminated"
	MessageBoltLogTransactionTimeoutCleanupRequested MessageID = "bolt.log.transaction_timeout_cleanup_requested"
	MessageBoltLogTransactionCommitFailed            MessageID = "bolt.log.transaction_commit_failed"
	MessageBoltLogTransactionCleanupFailed           MessageID = "bolt.log.transaction_cleanup_failed"
	MessageBoltLogTransactionTimeoutCleanupCompleted MessageID = "bolt.log.transaction_timeout_cleanup_completed"
)

// BoltLogServerListening describes a Bolt listener ready to accept connections.
func BoltLogServerListening() Message {
	return Message{ID: MessageBoltLogServerListening, Fallback: "bolt server listening"}
}

// BoltLogConnectionHandlerPanic describes recovery from a connection handler panic.
func BoltLogConnectionHandlerPanic() Message {
	return Message{ID: MessageBoltLogConnectionHandlerPanic, Fallback: "connection handler panic"}
}

// BoltLogUnencryptedConnectionRejected describes a connection rejected because TLS is required.
func BoltLogUnencryptedConnectionRejected() Message {
	return Message{ID: MessageBoltLogUnencryptedConnectionRejected, Fallback: "rejecting unencrypted connection"}
}

// BoltLogTransportSniffFailed describes a transport detection failure.
func BoltLogTransportSniffFailed() Message {
	return Message{ID: MessageBoltLogTransportSniffFailed, Fallback: "transport sniff failed"}
}

// BoltLogHandshakeFailed describes a Bolt protocol handshake failure.
func BoltLogHandshakeFailed() Message {
	return Message{ID: MessageBoltLogHandshakeFailed, Fallback: "handshake failed"}
}

// BoltLogMessageHandlingError describes a session message handling failure.
func BoltLogMessageHandlingError() Message {
	return Message{ID: MessageBoltLogMessageHandlingError, Fallback: "message handling error"}
}

// BoltLogHelloSchemeNone describes an authentication diagnostic for a scheme-none HELLO.
func BoltLogHelloSchemeNone() Message {
	return Message{ID: MessageBoltLogHelloSchemeNone, Fallback: "hello scheme=none"}
}

// BoltLogWebSocketCookieBearerRejected describes a rejected WebSocket cookie bearer token.
func BoltLogWebSocketCookieBearerRejected() Message {
	return Message{ID: MessageBoltLogWebSocketCookieBearerRejected, Fallback: "ws cookie bearer rejected"}
}

// BoltLogAuthenticationFailed describes failed Bolt authentication.
func BoltLogAuthenticationFailed() Message {
	return Message{ID: MessageBoltLogAuthenticationFailed, Fallback: "auth failed"}
}

// BoltLogHello describes an authenticated Bolt session HELLO.
func BoltLogHello() Message {
	return Message{ID: MessageBoltLogHello, Fallback: "hello"}
}

// BoltLogQuery describes a query selected for operator diagnostic logging.
func BoltLogQuery() Message {
	return Message{ID: MessageBoltLogQuery, Fallback: "query"}
}

// BoltLogQueryError describes a query execution error.
func BoltLogQueryError() Message {
	return Message{ID: MessageBoltLogQueryError, Fallback: "query error"}
}

// BoltLogRun describes Bolt RUN timing and result metadata.
func BoltLogRun() Message {
	return Message{ID: MessageBoltLogRun, Fallback: "run"}
}

// BoltLogDiscoveryRefreshFailed describes a discovery response refresh failure.
func BoltLogDiscoveryRefreshFailed() Message {
	return Message{ID: MessageBoltLogDiscoveryRefreshFailed, Fallback: "discovery refresh failed"}
}

// BoltLogWebSocketUpgradeReadRequestFailed describes a failed WebSocket upgrade request read.
func BoltLogWebSocketUpgradeReadRequestFailed() Message {
	return Message{ID: MessageBoltLogWebSocketUpgradeReadRequestFailed, Fallback: "ws upgrade read request failed"}
}

// BoltLogWebSocketUpgradeCredentials describes credential presence on a WebSocket upgrade.
func BoltLogWebSocketUpgradeCredentials() Message {
	return Message{ID: MessageBoltLogWebSocketUpgradeCredentials, Fallback: "ws upgrade credentials"}
}

// BoltLogWebSocketUpgradeFailed describes a failed WebSocket protocol upgrade.
func BoltLogWebSocketUpgradeFailed() Message {
	return Message{ID: MessageBoltLogWebSocketUpgradeFailed, Fallback: "ws upgrade failed"}
}

// BoltLogTransactionTerminated describes normal explicit transaction termination.
func BoltLogTransactionTerminated() Message {
	return Message{ID: MessageBoltLogTransactionTerminated, Fallback: "explicit transaction terminated"}
}

// BoltLogTransactionTimeoutCleanupRequested describes requested cleanup after transaction timeout.
func BoltLogTransactionTimeoutCleanupRequested() Message {
	return Message{ID: MessageBoltLogTransactionTimeoutCleanupRequested, Fallback: "explicit transaction timeout cleanup requested"}
}

// BoltLogTransactionCommitFailed describes a failed explicit transaction commit.
func BoltLogTransactionCommitFailed() Message {
	return Message{ID: MessageBoltLogTransactionCommitFailed, Fallback: "explicit transaction commit failed"}
}

// BoltLogTransactionCleanupFailed describes failed explicit transaction cleanup.
func BoltLogTransactionCleanupFailed() Message {
	return Message{ID: MessageBoltLogTransactionCleanupFailed, Fallback: "explicit transaction cleanup failed"}
}

// BoltLogTransactionTimeoutCleanupCompleted describes completed cleanup after transaction timeout.
func BoltLogTransactionTimeoutCleanupCompleted() Message {
	return Message{ID: MessageBoltLogTransactionTimeoutCleanupCompleted, Fallback: "explicit transaction timeout cleanup completed"}
}

// BoltServerListeningEvent describes a Bolt listener ready to accept connections.
func BoltServerListeningEvent(host string, port int) LogEvent {
	return boltEvent(
		EventBoltServerListening,
		BoltLogServerListening(),
		slog.String("host", host),
		slog.Int("port", port),
	)
}

// BoltConnectionHandlerPanicEvent describes recovery from a connection handler panic.
func BoltConnectionHandlerPanicEvent(recovered any) LogEvent {
	return boltEvent(EventBoltConnectionHandlerPanic, BoltLogConnectionHandlerPanic(), slog.Any("recover", recovered))
}

// BoltUnencryptedConnectionRejectedEvent describes a connection rejected because TLS is required.
func BoltUnencryptedConnectionRejectedEvent(remote string) LogEvent {
	return boltEvent(EventBoltUnencryptedConnectionRejected, BoltLogUnencryptedConnectionRejected(), slog.String("remote", remote))
}

// BoltTransportSniffFailedEvent describes a transport detection failure.
func BoltTransportSniffFailedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltTransportSniffFailed, BoltLogTransportSniffFailed(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltHandshakeFailedEvent describes a Bolt protocol handshake failure.
func BoltHandshakeFailedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltHandshakeFailed, BoltLogHandshakeFailed(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltMessageHandlingErrorEvent describes a session message handling failure.
func BoltMessageHandlingErrorEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltMessageHandlingError, BoltLogMessageHandlingError(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltHelloSchemeNoneEvent describes an authentication diagnostic for a scheme-none HELLO.
func BoltHelloSchemeNoneEvent(implicitBearerPresent, allowAnonymous bool) LogEvent {
	return boltEvent(EventBoltHelloSchemeNone, BoltLogHelloSchemeNone(), slog.Bool("implicit_bearer_present", implicitBearerPresent), slog.Bool("allow_anonymous", allowAnonymous))
}

// BoltWebSocketCookieBearerRejectedEvent describes a rejected WebSocket cookie bearer token.
func BoltWebSocketCookieBearerRejectedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltWebSocketCookieBearerRejected, BoltLogWebSocketCookieBearerRejected(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltBasicAuthenticationFailedEvent describes failed Bolt basic authentication.
func BoltBasicAuthenticationFailedEvent(principal, remote string, err error) LogEvent {
	return boltEvent(EventBoltAuthenticationFailed, BoltLogAuthenticationFailed(), slog.String("scheme", "basic"), slog.String("principal", principal), slog.String("remote", remote), slog.Any("error", err))
}

// BoltBearerAuthenticationFailedEvent describes failed Bolt bearer authentication.
func BoltBearerAuthenticationFailedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltAuthenticationFailed, BoltLogAuthenticationFailed(), slog.String("scheme", "bearer"), slog.String("remote", remote), slog.Any("error", err))
}

// BoltHelloEvent describes an authenticated Bolt session HELLO.
func BoltHelloEvent(remote, user string, roles []string, database string) LogEvent {
	return boltEvent(EventBoltHello, BoltLogHello(), slog.String("remote", remote), slog.String("user", user), slog.Any("roles", roles), slog.String("database", database))
}

// BoltQueryEvent describes a query selected for operator diagnostic logging.
func BoltQueryEvent(user, remote, query string, params map[string]any) LogEvent {
	attrs := []slog.Attr{slog.String("user", user), slog.String("remote", remote), slog.String("query", query)}
	if len(params) > 0 {
		attrs = append(attrs, slog.Any("params", params))
	}
	return boltEvent(EventBoltQuery, BoltLogQuery(), attrs...)
}

// BoltQueryErrorEvent describes a query execution error when query logging is enabled.
func BoltQueryErrorEvent() LogEvent {
	return boltEvent(EventBoltQueryError, BoltLogQueryError())
}

// BoltRunErrorEvent describes failed Bolt RUN timing and result metadata.
func BoltRunErrorEvent(database, status string, rows int, duration time.Duration, err string) LogEvent {
	return boltEvent(EventBoltRun, BoltLogRun(), slog.String("database", database), slog.String("status", status), slog.Int("rows", rows), slog.Duration("duration", duration), slog.String("error", err))
}

// BoltRunEvent describes successful Bolt RUN timing and result metadata.
func BoltRunEvent(database, status string, rows int, duration time.Duration) LogEvent {
	return boltEvent(EventBoltRun, BoltLogRun(), slog.String("database", database), slog.String("status", status), slog.Int("rows", rows), slog.Duration("duration", duration))
}

// BoltDiscoveryRefreshFailedEvent describes a discovery response refresh failure.
func BoltDiscoveryRefreshFailedEvent(err error) LogEvent {
	return boltEvent(EventBoltDiscoveryRefreshFailed, BoltLogDiscoveryRefreshFailed(), slog.Any("error", err))
}

// BoltWebSocketUpgradeReadRequestFailedEvent describes a failed WebSocket upgrade request read.
func BoltWebSocketUpgradeReadRequestFailedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltWebSocketUpgradeReadRequestFailed, BoltLogWebSocketUpgradeReadRequestFailed(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltWebSocketUpgradeCredentialsEvent describes credential presence on a WebSocket upgrade.
func BoltWebSocketUpgradeCredentialsEvent(remote string, hasCookie, hasAuthorizationHeader, implicitBearerPresent bool) LogEvent {
	return boltEvent(EventBoltWebSocketUpgradeCredentials, BoltLogWebSocketUpgradeCredentials(), slog.String("remote", remote), slog.Bool("has_cookie", hasCookie), slog.Bool("has_authorization_header", hasAuthorizationHeader), slog.Bool("implicit_bearer_present", implicitBearerPresent))
}

// BoltWebSocketUpgradeFailedEvent describes a failed WebSocket protocol upgrade.
func BoltWebSocketUpgradeFailedEvent(remote string, err error) LogEvent {
	return boltEvent(EventBoltWebSocketUpgradeFailed, BoltLogWebSocketUpgradeFailed(), slog.String("remote", remote), slog.Any("error", err))
}

// BoltTransactionTerminatedEvent describes normal explicit transaction termination.
func BoltTransactionTerminatedEvent(reason, database string, duration time.Duration) LogEvent {
	return boltTransactionEvent(EventBoltTransactionTerminated, BoltLogTransactionTerminated(), reason, database, duration)
}

// BoltTransactionTimeoutCleanupRequestedEvent describes requested cleanup after transaction timeout.
func BoltTransactionTimeoutCleanupRequestedEvent(reason, database string, duration time.Duration) LogEvent {
	return boltTransactionEvent(EventBoltTransactionTimeoutCleanupRequested, BoltLogTransactionTimeoutCleanupRequested(), reason, database, duration)
}

// BoltTransactionCommitFailedEvent describes a failed explicit transaction commit.
func BoltTransactionCommitFailedEvent(reason, database string, duration time.Duration, err error) LogEvent {
	return boltTransactionEvent(EventBoltTransactionCommitFailed, BoltLogTransactionCommitFailed(), reason, database, duration, slog.Any("commit_error", err))
}

// BoltTransactionCleanupFailedEvent describes failed explicit transaction cleanup.
func BoltTransactionCleanupFailedEvent(reason, database string, duration time.Duration, err error) LogEvent {
	return boltTransactionEvent(EventBoltTransactionCleanupFailed, BoltLogTransactionCleanupFailed(), reason, database, duration, slog.Any("cleanup_error", err))
}

// BoltTransactionTimeoutCleanupCompletedEvent describes completed cleanup after transaction timeout.
func BoltTransactionTimeoutCleanupCompletedEvent(reason, database string, duration time.Duration) LogEvent {
	return boltTransactionEvent(EventBoltTransactionTimeoutCleanupCompleted, BoltLogTransactionTimeoutCleanupCompleted(), reason, database, duration)
}

func boltTransactionEvent(id EventID, message Message, reason, database string, duration time.Duration, attrs ...slog.Attr) LogEvent {
	base := []slog.Attr{slog.String("reason", reason), slog.String("database", database), slog.Duration("duration", duration)}
	return boltEvent(id, message, append(base, attrs...)...)
}

func boltEvent(id EventID, message Message, attrs ...slog.Attr) LogEvent {
	return LogEvent{ID: id, Message: message, Attrs: attrs}
}
